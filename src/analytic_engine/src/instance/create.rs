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

//! Create table logic of instance

use generic_error::BoxError;
use logger::info;
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::{
    engine::{CreateTableParams, CreateTableRequest},
    partition::PartitionInfo,
};

use crate::{
    instance::{
        engine::{
            CreateOpenFailedTable, InvalidOptions, InvalidTableOptions, Result, TableNotExist,
            TryCreateRandomPartitionTableInOverwriteMode, WriteManifest,
        },
        Instance,
    },
    manifest::meta_edit::{AddTableMeta, MetaEdit, MetaEditRequest, MetaUpdate},
    space::SpaceRef,
    table::data::{TableCatalogInfo, TableDataRef, TableShardInfo},
    table_options, TableOptions,
};

impl Instance {
    /// Validate the request of creating table.
    pub fn validate_create_table(&self, params: &CreateTableParams) -> Result<TableOptions> {
        let table_opts =
            table_options::merge_table_options_for_create(&params.table_options, &self.table_opts)
                .box_err()
                .context(InvalidOptions {
                    table: &params.table_name,
                })?;

        if let Some(reason) = table_opts.check_validity() {
            return InvalidTableOptions { reason }.fail();
        }

        if let Some(partition_info) = &params.partition_info {
            let dedup_on_random_partition =
                table_opts.need_dedup() && matches!(partition_info, PartitionInfo::Random(_));

            ensure!(
                !dedup_on_random_partition,
                TryCreateRandomPartitionTableInOverwriteMode {
                    table: &params.table_name,
                }
            );
        }

        Ok(table_opts)
    }

    /// Create table need to be handled by write worker.
    pub async fn do_create_table(
        &self,
        space: SpaceRef,
        request: CreateTableRequest,
    ) -> Result<TableDataRef> {
        info!("Instance create table, request:{:?}", request);

        if space.is_open_failed_table(&request.params.table_name) {
            return CreateOpenFailedTable {
                table: request.params.table_name,
            }
            .fail();
        }

        let mut table_opts = self.validate_create_table(&request.params)?;
        // Sanitize options before creating table.
        table_opts.sanitize();

        if let Some(table_data) = space.find_table_by_id(request.table_id) {
            return Ok(table_data);
        }

        // Store table info into meta both memory and storage.
        let edit_req = {
            let meta_update = MetaUpdate::AddTable(AddTableMeta {
                space_id: space.id,
                table_id: request.table_id,
                table_name: request.params.table_name.clone(),
                schema: request.params.table_schema,
                opts: table_opts,
            });
            MetaEditRequest {
                shard_info: TableShardInfo::new(request.shard_id),
                meta_edit: MetaEdit::Update(meta_update),
                table_catalog_info: TableCatalogInfo {
                    schema_id: request.schema_id,
                    schema_name: request.params.schema_name,
                    catalog_name: request.params.catalog_name,
                },
            }
        };
        self.space_store
            .manifest
            .apply_edit(edit_req)
            .await
            .context(WriteManifest {
                space_id: space.id,
                table: &request.params.table_name,
                table_id: request.table_id,
            })?;

        // Table is sure to exist here.
        space
            .find_table_by_id(request.table_id)
            .with_context(|| TableNotExist {
                msg: format!(
                    "table not exist, space_id:{}, table_id:{}, table_name:{}",
                    space.id, request.table_id, request.params.table_name
                ),
            })
    }
}
