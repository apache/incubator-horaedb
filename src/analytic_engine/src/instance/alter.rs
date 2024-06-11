// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Alter [Schema] and [TableOptions] logic of instance.

use std::collections::HashMap;

use generic_error::BoxError;
use logger::info;
use snafu::{ensure, ResultExt};
use table_engine::table::AlterSchemaRequest;
use wal::{kv_encoder::LogBatchEncoder, manager::WriteContext};

use crate::{
    instance::{
        self,
        engine::{
            AlterDroppedTable, EncodePayloads, FlushTable, InvalidOptions, InvalidPreVersion,
            InvalidSchemaVersion, InvalidTableOptions, Result, WriteManifest, WriteWal,
        },
        flush_compaction::TableFlushOptions,
        serial_executor::TableOpSerialExecutor,
        InstanceRef,
    },
    manifest::meta_edit::{
        AlterOptionsMeta, AlterSchemaMeta, MetaEdit, MetaEditRequest, MetaUpdate,
    },
    payload::WritePayload,
    table::data::TableDataRef,
    table_options,
};

pub struct Alterer<'a> {
    table_data: TableDataRef,
    serial_exec: &'a mut TableOpSerialExecutor,

    instance: InstanceRef,
}

impl<'a> Alterer<'a> {
    pub async fn new(
        table_data: TableDataRef,
        serial_exec: &'a mut TableOpSerialExecutor,
        instance: InstanceRef,
    ) -> Alterer<'a> {
        assert_eq!(table_data.id, serial_exec.table_id());
        Self {
            table_data,
            serial_exec,
            instance,
        }
    }
}

impl<'a> Alterer<'a> {
    // Alter schema need to be handled by write worker.
    pub async fn alter_schema_of_table(&mut self, request: AlterSchemaRequest) -> Result<()> {
        info!(
            "Instance alter schema, table:{:?}, request:{:?}",
            self.table_data.name, request
        );

        // Validate alter schema request.
        // if the alter schema request is idempotent, we can skip the alter operation.
        if self.validate_before_alter(&request)? {
            info!(
                "Skip alter because of the altered schema is the same as the current, table:{}",
                self.table_data.name
            );
            return Ok(());
        }

        // Now we can persist and update the schema, since this function is called by
        // write worker, so there is no other concurrent writer altering the
        // schema.

        // First trigger a flush before alter schema, to ensure ensure all wal entries
        // with old schema are flushed
        let opts = TableFlushOptions::default();
        let flush_scheduler = self.serial_exec.flush_scheduler();
        let flusher = self.instance.make_flusher();
        flusher
            .do_flush(flush_scheduler, &self.table_data, opts)
            .await
            .context(FlushTable {
                space_id: self.table_data.space_id,
                table: &self.table_data.name,
                table_id: self.table_data.id,
            })?;

        // Build alter op
        let manifest_update = AlterSchemaMeta {
            space_id: self.table_data.space_id,
            table_id: self.table_data.id,
            schema: request.schema.clone(),
            pre_schema_version: request.pre_schema_version,
        };

        // Write AlterSchema to Data Wal
        let alter_schema_pb = manifest_update.clone().into();
        let payload = WritePayload::AlterSchema(&alter_schema_pb);

        // Encode payloads
        let table_location = self.table_data.table_location();
        let wal_location =
            instance::create_wal_location(table_location.id, table_location.shard_info);
        let log_batch_encoder = LogBatchEncoder::create(wal_location);
        let log_batch = log_batch_encoder.encode(&payload).context(EncodePayloads {
            table: &self.table_data.name,
            wal_location,
        })?;

        // Write log batch
        let write_ctx = WriteContext::default();
        self.instance
            .space_store
            .wal_manager
            .write(&write_ctx, &log_batch)
            .await
            .box_err()
            .context(WriteWal {
                space_id: self.table_data.space_id,
                table: &self.table_data.name,
                table_id: self.table_data.id,
            })?;

        info!(
            "Instance update table schema, new_schema:{:?}",
            request.schema
        );

        // Write to Manifest
        let edit_req = {
            let meta_update = MetaUpdate::AlterSchema(manifest_update);
            MetaEditRequest {
                shard_info: self.table_data.shard_info,
                meta_edit: MetaEdit::Update(meta_update),
                table_catalog_info: self.table_data.table_catalog_info.clone(),
            }
        };
        self.instance
            .space_store
            .manifest
            .apply_edit(edit_req)
            .await
            .context(WriteManifest {
                space_id: self.table_data.space_id,
                table: &self.table_data.name,
                table_id: self.table_data.id,
            })?;

        Ok(())
    }

    // Most validation should be done by catalog module, so we don't do too much
    // duplicate check here, especially the schema compatibility.
    // The returned value denotes whether the altered schema is same as the current
    // one.
    fn validate_before_alter(&self, request: &AlterSchemaRequest) -> Result<bool> {
        ensure!(
            !self.table_data.is_dropped(),
            AlterDroppedTable {
                table: &self.table_data.name,
            }
        );

        if self.table_data.schema().columns() == request.schema.columns() {
            return Ok(true);
        }

        let current_version = self.table_data.schema_version();
        ensure!(
            current_version < request.schema.version(),
            InvalidSchemaVersion {
                table: &self.table_data.name,
                current_version,
                given_version: request.schema.version(),
            }
        );

        ensure!(
            current_version == request.pre_schema_version,
            InvalidPreVersion {
                table: &self.table_data.name,
                current_version,
                pre_version: request.pre_schema_version,
            }
        );

        Ok(false)
    }

    pub async fn alter_options_of_table(
        &self,
        // todo: encapsulate this into a struct like other functions.
        options: HashMap<String, String>,
    ) -> Result<()> {
        info!(
            "Instance alter options of table, table:{:?}, options:{:?}",
            self.table_data.name, options
        );

        ensure!(
            !self.table_data.is_dropped(),
            AlterDroppedTable {
                table: &self.table_data.name,
            }
        );

        // AlterOptions doesn't need a flush.

        // Generate options after alter op
        let current_table_options = self.table_data.table_options();
        info!(
            "Instance alter options, space_id:{}, tables:{:?}, old_table_opts:{:?}, options:{:?}",
            self.table_data.space_id, self.table_data.name, current_table_options, options
        );
        let table_opts = {
            let mut opts =
                table_options::merge_table_options_for_alter(&options, &current_table_options)
                    .box_err()
                    .context(InvalidOptions {
                        table: &self.table_data.name,
                    })?;
            opts.sanitize();
            opts
        };

        // We should check the options before altering
        if let Some(reason) = table_opts.check_validity() {
            return InvalidTableOptions { reason }.fail();
        }

        let manifest_update = AlterOptionsMeta {
            space_id: self.table_data.space_id,
            table_id: self.table_data.id,
            options: table_opts.clone(),
        };

        // Now we can persist and update the options, since this function is called by
        // write worker, so there is no other concurrent writer altering the
        // options.

        // Write AlterOptions to Data Wal
        let alter_options_pb = manifest_update.clone().into();
        let payload = WritePayload::AlterOption(&alter_options_pb);

        // Encode payload
        let table_location = self.table_data.table_location();
        let wal_location =
            instance::create_wal_location(table_location.id, table_location.shard_info);
        let log_batch_encoder = LogBatchEncoder::create(wal_location);
        let log_batch = log_batch_encoder.encode(&payload).context(EncodePayloads {
            table: &self.table_data.name,
            wal_location,
        })?;

        // Write log batch
        let write_ctx = WriteContext::default();
        self.instance
            .space_store
            .wal_manager
            .write(&write_ctx, &log_batch)
            .await
            .box_err()
            .context(WriteWal {
                space_id: self.table_data.space_id,
                table: &self.table_data.name,
                table_id: self.table_data.id,
            })?;

        // Write to Manifest
        let edit_req = {
            let meta_update = MetaUpdate::AlterOptions(manifest_update);
            MetaEditRequest {
                shard_info: self.table_data.shard_info,
                meta_edit: MetaEdit::Update(meta_update),
                table_catalog_info: self.table_data.table_catalog_info.clone(),
            }
        };
        self.instance
            .space_store
            .manifest
            .apply_edit(edit_req)
            .await
            .context(WriteManifest {
                space_id: self.table_data.space_id,
                table: &self.table_data.name,
                table_id: self.table_data.id,
            })?;

        Ok(())
    }
}
