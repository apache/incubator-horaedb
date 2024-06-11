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

//! Table engine logic of instance

use std::sync::Arc;

use common_types::{schema::Version, SequenceNumber};
use generic_error::GenericError;
use macros::define_result;
use snafu::{Backtrace, OptionExt, Snafu};
use table_engine::{
    engine::{CloseTableRequest, CreateTableRequest, DropTableRequest, OpenShardRequest},
    table::TableId,
};
use wal::manager::WalLocation;

use crate::{
    engine::build_space_id,
    instance::{
        close::Closer,
        drop::Dropper,
        open::{OpenTablesOfShardResult, TableContext, TablesOfShardContext},
        Instance,
    },
    space::{MemSizeOptions, Space, SpaceAndTable, SpaceContext, SpaceId, SpaceRef},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display(
        "The space of the table does not exist, space_id:{}, table:{}.\nBacktrace:\n{}",
        space_id,
        table,
        backtrace,
    ))]
    SpaceNotExist {
        space_id: SpaceId,
        table: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to read meta update, table_id:{}, err:{}", table_id, source))]
    ReadMetaUpdate {
        table_id: TableId,
        source: GenericError,
    },

    #[snafu(display(
        "Failed to recover table data, space_id:{}, table:{}, err:{}",
        space_id,
        table,
        source
    ))]
    RecoverTableData {
        space_id: SpaceId,
        table: String,
        source: crate::table::data::Error,
    },

    #[snafu(display("Failed to read wal, err:{}", source))]
    ReadWal { source: wal::manager::Error },

    #[snafu(display(
        "Failed to apply log entry to memtable, table:{}, table_id:{}, err:{}",
        table,
        table_id,
        source
    ))]
    ApplyMemTable {
        space_id: SpaceId,
        table: String,
        table_id: TableId,
        source: crate::instance::write::Error,
    },

    #[snafu(display(
        "Flush failed, space_id:{}, table:{}, table_id:{}, err:{}",
        space_id,
        table,
        table_id,
        source
    ))]
    FlushTable {
        space_id: SpaceId,
        table: String,
        table_id: TableId,
        source: crate::instance::flush_compaction::Error,
    },

    #[snafu(display(
        "Failed to persist meta update to manifest, space_id:{}, table:{}, table_id:{}, err:{}",
        space_id,
        table,
        table_id,
        source
    ))]
    WriteManifest {
        space_id: SpaceId,
        table: String,
        table_id: TableId,
        source: GenericError,
    },

    #[snafu(display(
        "Failed to persist meta update to WAL, space_id:{}, table:{}, table_id:{}, err:{}",
        space_id,
        table,
        table_id,
        source
    ))]
    WriteWal {
        space_id: SpaceId,
        table: String,
        table_id: TableId,
        source: GenericError,
    },

    #[snafu(display("Invalid options, table:{table}, err:{source}",))]
    InvalidOptions { table: String, source: GenericError },

    #[snafu(display(
        "Failed to create table data, space_id:{}, table:{}, table_id:{}, err:{}",
        space_id,
        table,
        table_id,
        source
    ))]
    CreateTableData {
        space_id: SpaceId,
        table: String,
        table_id: TableId,
        source: crate::table::data::Error,
    },

    #[snafu(display(
    "Try to update schema to elder version, table:{}, current_version:{}, given_version:{}.\nBacktrace:\n{}",
    table,
    current_version,
    given_version,
    backtrace,
    ))]
    InvalidSchemaVersion {
        table: String,
        current_version: Version,
        given_version: Version,
        backtrace: Backtrace,
    },

    #[snafu(display(
    "Invalid previous schema version, table:{}, current_version:{}, pre_version:{}.\nBacktrace:\n{}",
    table,
    current_version,
    pre_version,
    backtrace,
    ))]
    InvalidPreVersion {
        table: String,
        current_version: Version,
        pre_version: Version,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Alter schema of a dropped table:{}.\nBacktrace:\n{}",
        table,
        backtrace
    ))]
    AlterDroppedTable { table: String, backtrace: Backtrace },

    #[snafu(display("Failed to store version edit, err:{}", source))]
    StoreVersionEdit { source: GenericError },

    #[snafu(display(
        "Failed to encode payloads, table:{}, wal_location:{:?}, err:{}",
        table,
        wal_location,
        source
    ))]
    EncodePayloads {
        table: String,
        wal_location: WalLocation,
        source: wal::manager::Error,
    },

    #[snafu(display(
        "Failed to do manifest snapshot for table, space_id:{}, table:{}, err:{}",
        space_id,
        table,
        source
    ))]
    DoManifestSnapshot {
        space_id: SpaceId,
        table: String,
        source: GenericError,
    },

    #[snafu(display(
        "Table open failed and can not be created again, table:{}.\nBacktrace:\n{}",
        table,
        backtrace,
    ))]
    CreateOpenFailedTable { table: String, backtrace: Backtrace },

    #[snafu(display("Failed to open manifest, err:{}", source))]
    OpenManifest {
        source: crate::manifest::details::Error,
    },

    #[snafu(display("Failed to find table, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    TableNotExist { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to open shard, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    OpenTablesOfShard { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to replay wal, msg:{:?}, err:{}", msg, source))]
    ReplayWalWithCause {
        msg: Option<String>,
        source: GenericError,
    },

    #[snafu(display("Failed to replay wal, msg:{:?}.\nBacktrace:\n{}", msg, backtrace))]
    ReplayWalNoCause {
        msg: Option<String>,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Try to create a random partition table in overwrite mode, table:{table}.\nBacktrace:\n{backtrace}",
    ))]
    TryCreateRandomPartitionTableInOverwriteMode { table: String, backtrace: Backtrace },

    #[snafu(display("Found invalid table options, reason:{reason}.\nBacktrace:\n{backtrace}",))]
    InvalidTableOptions {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to purge wal, wal_location:{:?}, sequence:{}",
        wal_location,
        sequence
    ))]
    PurgeWal {
        wal_location: WalLocation,
        sequence: SequenceNumber,
        source: wal::manager::Error,
    },
}

define_result!(Error);

impl From<Error> for table_engine::engine::Error {
    fn from(err: Error) -> Self {
        match &err {
            Error::InvalidOptions { table, .. }
            | Error::SpaceNotExist { table, .. }
            | Error::TryCreateRandomPartitionTableInOverwriteMode { table, .. } => {
                Self::InvalidArguments {
                    table: table.clone(),
                    source: Box::new(err),
                }
            }
            Error::WriteManifest { .. } => Self::WriteMeta {
                source: Box::new(err),
            },
            Error::WriteWal { .. }
            | Error::InvalidSchemaVersion { .. }
            | Error::InvalidPreVersion { .. }
            | Error::CreateTableData { .. }
            | Error::AlterDroppedTable { .. }
            | Error::ReadMetaUpdate { .. }
            | Error::RecoverTableData { .. }
            | Error::ReadWal { .. }
            | Error::ApplyMemTable { .. }
            | Error::FlushTable { .. }
            | Error::StoreVersionEdit { .. }
            | Error::EncodePayloads { .. }
            | Error::CreateOpenFailedTable { .. }
            | Error::DoManifestSnapshot { .. }
            | Error::OpenManifest { .. }
            | Error::TableNotExist { .. }
            | Error::OpenTablesOfShard { .. }
            | Error::ReplayWalNoCause { .. }
            | Error::PurgeWal { .. }
            | Error::ReplayWalWithCause { .. }
            | Error::InvalidTableOptions { .. } => Self::Unexpected {
                source: Box::new(err),
            },
        }
    }
}

impl Instance {
    /// Find space by name, create if the space is not exists
    pub async fn find_or_create_space(
        self: &Arc<Self>,
        space_id: SpaceId,
        context: SpaceContext,
    ) -> Result<SpaceRef> {
        // Find space first
        if let Some(space) = self.get_space_by_read_lock(space_id) {
            return Ok(space);
        }

        let mut spaces = self.space_store.spaces.write().unwrap();
        // The space may already been created by other thread
        if let Some(space) = spaces.get_by_id(space_id) {
            return Ok(space.clone());
        }

        // Now we are the one responsible to create and persist the space info into meta

        // Create space
        let mem_size_options = MemSizeOptions {
            write_buffer_size: self.space_write_buffer_size,
            usage_collector: self.mem_usage_collector.clone(),
            size_sampling_interval: self.mem_usage_sampling_interval,
        };
        let space = Arc::new(Space::new(space_id, context, mem_size_options));

        spaces.insert(space.clone());

        Ok(space)
    }

    /// Find space by id
    pub fn find_space(&self, space_id: SpaceId) -> Option<SpaceRef> {
        let spaces = self.space_store.spaces.read().unwrap();
        spaces.get_by_id(space_id).cloned()
    }

    /// Create a table under given space
    pub async fn create_table(
        self: &Arc<Self>,
        space_id: SpaceId,
        request: CreateTableRequest,
    ) -> Result<SpaceAndTable> {
        let context = SpaceContext {
            catalog_name: request.params.catalog_name.clone(),
            schema_name: request.params.schema_name.clone(),
        };
        let space = self.find_or_create_space(space_id, context).await?;
        let table_data = self.do_create_table(space.clone(), request).await?;

        Ok(SpaceAndTable::new(space, table_data))
    }

    /// Find the table under given space by its table name
    ///
    /// Return None if space or table is not found
    pub async fn find_table(
        &self,
        space_id: SpaceId,
        table: &str,
    ) -> Result<Option<SpaceAndTable>> {
        let space = match self.find_space(space_id) {
            Some(s) => s,
            None => return Ok(None),
        };

        let space_table = space
            .find_table(table)
            .map(|table_data| SpaceAndTable::new(space, table_data));

        Ok(space_table)
    }

    /// Drop a table under given space
    pub async fn drop_table(
        self: &Arc<Self>,
        space_id: SpaceId,
        request: DropTableRequest,
    ) -> Result<bool> {
        let space = self.find_space(space_id).context(SpaceNotExist {
            space_id,
            table: &request.table_name,
        })?;
        let dropper = Dropper {
            space,
            space_store: self.space_store.clone(),
        };

        dropper.drop(request).await
    }

    /// Close the table under given space by its table name
    pub async fn close_table(
        self: &Arc<Self>,
        space_id: SpaceId,
        request: CloseTableRequest,
    ) -> Result<()> {
        let space = self.find_space(space_id).context(SpaceNotExist {
            space_id,
            table: &request.table_name,
        })?;

        let closer = Closer {
            space,
            manifest: self.space_store.manifest.clone(),
            flusher: self.make_flusher(),
        };

        closer.close(request).await
    }

    /// Open tables of same shard together
    // TODO: just return `TableRef` rather than `SpaceAndTable`.
    pub async fn open_tables_of_shard(
        self: &Arc<Self>,
        request: OpenShardRequest,
    ) -> Result<OpenTablesOfShardResult> {
        let shard_id = request.shard_id;
        let mut table_ctxs = Vec::with_capacity(request.table_defs.len());

        let mut spaces_of_tables = Vec::with_capacity(request.table_defs.len());
        for table_def in request.table_defs {
            let context = SpaceContext {
                catalog_name: table_def.catalog_name.clone(),
                schema_name: table_def.schema_name.clone(),
            };

            let space_id = build_space_id(table_def.schema_id);
            let space = self.find_or_create_space(space_id, context).await?;
            spaces_of_tables.push(((table_def.name.clone(), table_def.id), space.clone()));
            table_ctxs.push(TableContext { table_def, space });
        }
        let shard_ctx = TablesOfShardContext {
            shard_id,
            table_ctxs,
        };

        let shard_result = self.do_open_tables_of_shard(shard_ctx).await?;

        // Insert opened tables to spaces.
        for ((table_name, table_id), space) in spaces_of_tables {
            let table_result = shard_result
                .get(&table_id)
                .with_context(|| OpenTablesOfShard {
                    msg: format!(
                        "table not exist in result, table_id:{}, space_id:{shard_id}, shard_id:{}",
                        table_id, space.id
                    ),
                })?;

            // TODO: should not modify space here, maybe should place it into manifest.
            if table_result.is_err() {
                space.insert_open_failed_table(table_name);
            }
        }

        Ok(shard_result)
    }
}
