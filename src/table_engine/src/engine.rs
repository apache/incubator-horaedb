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

//! Table factory trait

use core::fmt;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use common_types::{
    schema::Schema,
    table::{ShardId, DEFAULT_SHARD_ID},
};
use generic_error::{GenericError, GenericResult};
use horaedbproto::sys_catalog as sys_catalog_pb;
use itertools::Itertools;
use macros::define_result;
use runtime::{PriorityRuntime, RuntimeRef};
use snafu::{ensure, Backtrace, Snafu};

use crate::{
    partition::PartitionInfo,
    table::{SchemaId, TableId, TableInfo, TableRef},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid table path, path:{}.\nBacktrace:\n{}", path, backtrace))]
    InvalidTablePath { path: String, backtrace: Backtrace },

    #[snafu(display("Table already exists, table:{}.\nBacktrace:\n{}", table, backtrace))]
    TableExists { table: String, backtrace: Backtrace },

    #[snafu(display("Invalid arguments, table:{table}, err:{source}"))]
    InvalidArguments { table: String, source: GenericError },

    #[snafu(display("Failed to write meta data, err:{}", source))]
    WriteMeta { source: GenericError },

    #[snafu(display("Unexpected error, err:{}", source))]
    Unexpected { source: GenericError },

    #[snafu(display("Unexpected error, msg:{}.\nBacktrace:\n{}", msg, backtrace))]
    UnexpectedNoCause { msg: String, backtrace: Backtrace },

    #[snafu(display(
        "Unknown engine type, type:{}.\nBacktrace:\n{}",
        engine_type,
        backtrace
    ))]
    UnknownEngineType {
        engine_type: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid table state transition, from:{:?}, to:{:?}.\nBacktrace:\n{}",
        from,
        to,
        backtrace
    ))]
    InvalidTableStateTransition {
        from: TableState,
        to: TableState,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to close the table engine, err:{}", source))]
    Close { source: GenericError },

    #[snafu(display("Failed to open shard, err:{}", source))]
    OpenShard { source: GenericError },

    #[snafu(display("Failed to open table, msg:{:?}.\nBacktrace:\n{}", msg, backtrace))]
    OpenTableNoCause {
        msg: Option<String>,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to open table, msg:{:?}, err:{}", msg, source))]
    OpenTableWithCause {
        msg: Option<String>,
        source: GenericError,
    },
}

define_result!(Error);

/// The state of table.
///
/// Transition rule is defined in the validate function.
#[derive(Clone, Copy, Debug)]
pub enum TableState {
    Stable = 0,
    Dropping = 1,
    Dropped = 2,
}

impl TableState {
    pub fn validate(&self, to: TableState) -> bool {
        match self {
            TableState::Stable => matches!(to, TableState::Stable | TableState::Dropping),
            TableState::Dropping => matches!(to, TableState::Dropped),
            TableState::Dropped => false,
        }
    }

    /// Try to transit from the self state to the `to` state.
    ///
    /// Returns error if it is a invalid transition.
    pub fn try_transit(&mut self, to: TableState) -> Result<()> {
        ensure!(
            self.validate(to),
            InvalidTableStateTransition { from: *self, to }
        );
        *self = to;

        Ok(())
    }
}

impl From<TableState> for sys_catalog_pb::TableState {
    fn from(state: TableState) -> Self {
        match state {
            TableState::Stable => Self::Stable,
            TableState::Dropping => Self::Dropping,
            TableState::Dropped => Self::Dropped,
        }
    }
}

impl From<sys_catalog_pb::TableState> for TableState {
    fn from(state: sys_catalog_pb::TableState) -> TableState {
        match state {
            sys_catalog_pb::TableState::Stable => TableState::Stable,
            sys_catalog_pb::TableState::Dropping => TableState::Dropping,
            sys_catalog_pb::TableState::Dropped => TableState::Dropped,
        }
    }
}

#[derive(Copy, Clone)]
pub enum TableRequestType {
    Create,
    Drop,
}

/// The necessary params used to create table.
#[derive(Clone)]
pub struct CreateTableParams {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_options: HashMap<String, String>,
    pub table_schema: Schema,
    pub partition_info: Option<PartitionInfo>,
    pub engine: String,
}

impl Debug for CreateTableParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table_opts_formatter = TableOptionsFormatter(&self.table_options);
        f.debug_struct("CreateTableParams")
            .field("catalog_name", &self.catalog_name)
            .field("schema_name", &self.schema_name)
            .field("table_name", &self.table_name)
            .field("table_options", &table_opts_formatter)
            .field("table_schema", &self.table_schema)
            .field("partition_info", &self.partition_info)
            .field("engine", &self.engine)
            .finish()
    }
}

struct TableOptionsFormatter<'a>(&'a HashMap<String, String>);

impl<'a> Debug for TableOptionsFormatter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sorted_iter = self.0.iter().sorted();
        f.debug_list().entries(sorted_iter).finish()
    }
}

/// Create table request
// TODO(yingwen): Add option for create_if_not_exists?
#[derive(Clone, Debug)]
pub struct CreateTableRequest {
    pub params: CreateTableParams,
    /// Schema id
    pub schema_id: SchemaId,
    /// Table id
    pub table_id: TableId,
    /// Tells state of the table
    pub state: TableState,
    /// Shard id, shard is the table set about scheduling from nodes
    /// It will be assigned the default value in standalone mode,
    /// and just be useful in cluster mode
    pub shard_id: ShardId,
}

impl From<CreateTableRequest> for sys_catalog_pb::TableEntry {
    fn from(req: CreateTableRequest) -> Self {
        sys_catalog_pb::TableEntry {
            catalog_name: req.params.catalog_name,
            schema_name: req.params.schema_name,
            schema_id: req.schema_id.as_u32(),
            table_id: req.table_id.as_u64(),
            table_name: req.params.table_name,
            engine: req.params.engine,
            state: sys_catalog_pb::TableState::from(req.state) as i32,
            created_time: 0,
            modified_time: 0,
        }
    }
}

impl From<CreateTableRequest> for TableInfo {
    fn from(req: CreateTableRequest) -> Self {
        Self {
            catalog_name: req.params.catalog_name,
            schema_name: req.params.schema_name,
            schema_id: req.schema_id,
            table_name: req.params.table_name,
            table_id: req.table_id,
            engine: req.params.engine,
            state: req.state,
        }
    }
}

/// Drop table request
#[derive(Debug, Clone)]
pub struct DropTableRequest {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
    /// Schema id
    pub schema_id: SchemaId,
    /// Table name
    pub table_name: String,
    /// Table engine type
    pub engine: String,
}

#[derive(Debug, Clone)]
pub struct OpenTableRequest {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
    /// Schema id
    pub schema_id: SchemaId,
    /// Table name
    pub table_name: String,
    /// Table id
    pub table_id: TableId,
    /// Table engine type
    pub engine: String,
    /// Shard id, shard is the table set about scheduling from nodes
    pub shard_id: ShardId,
}

impl From<TableInfo> for OpenTableRequest {
    /// The `shard_id` is not persisted and just assigned a default value
    /// while recovered from `TableInfo`.
    /// This conversion will just happen in standalone mode.
    fn from(table_info: TableInfo) -> Self {
        Self {
            catalog_name: table_info.catalog_name,
            schema_name: table_info.schema_name,
            schema_id: table_info.schema_id,
            table_name: table_info.table_name,
            table_id: table_info.table_id,
            engine: table_info.engine,
            shard_id: DEFAULT_SHARD_ID,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CloseTableRequest {
    /// Catalog name
    pub catalog_name: String,
    /// Schema name
    pub schema_name: String,
    /// Schema id
    pub schema_id: SchemaId,
    /// Table name
    pub table_name: String,
    /// Table id
    pub table_id: TableId,
    /// Table engine type
    pub engine: String,
}

#[derive(Debug, Clone)]
pub struct OpenShardRequest {
    /// Shard id
    pub shard_id: ShardId,

    /// Table infos
    pub table_defs: Vec<TableDef>,

    /// Table engine type
    pub engine: String,
}

#[derive(Clone, Debug)]
pub struct TableDef {
    pub catalog_name: String,
    pub schema_name: String,
    pub schema_id: SchemaId,
    pub id: TableId,
    pub name: String,
}

pub type CloseShardRequest = OpenShardRequest;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShardStats {
    pub num_written_bytes: u64,
    pub num_fetched_bytes: u64,
}

#[derive(Debug, Default)]
pub struct TableEngineStats {
    pub shard_stats: HashMap<ShardId, ShardStats>,
}

/// Table engine
// TODO(yingwen): drop table support to release resource owned by the table
#[async_trait]
pub trait TableEngine: Send + Sync {
    /// Returns the name of engine.
    fn engine_type(&self) -> &str;

    /// Close the engine gracefully.
    async fn close(&self) -> Result<()>;

    /// Validate the params used to create a table.
    ///
    /// This validation can be used before doing real table creation to avoid
    /// unnecessary works if the params is invalid.
    async fn validate_create_table(&self, request: &CreateTableParams) -> Result<()>;

    /// Create table
    async fn create_table(&self, request: CreateTableRequest) -> Result<TableRef>;

    /// Drop table
    async fn drop_table(&self, request: DropTableRequest) -> Result<bool>;

    /// Open table, return None if table not exists
    async fn open_table(&self, request: OpenTableRequest) -> Result<Option<TableRef>>;

    /// Close table
    async fn close_table(&self, request: CloseTableRequest) -> Result<()>;

    /// Open tables on same shard.
    async fn open_shard(&self, request: OpenShardRequest) -> Result<OpenShardResult>;

    /// Close tables on same shard.
    async fn close_shard(&self, request: CloseShardRequest) -> Vec<Result<String>>;

    /// Report the statistics of the table engine.
    async fn report_statistics(&self) -> Result<Option<TableEngineStats>> {
        Ok(None)
    }
}

pub type OpenShardResult = HashMap<TableId, GenericResult<Option<TableRef>>>;

/// A reference counted pointer to table engine
pub type TableEngineRef = Arc<dyn TableEngine>;

#[derive(Clone, Debug)]
pub struct EngineRuntimes {
    /// Runtime for reading data
    pub read_runtime: PriorityRuntime,
    /// Runtime for writing data
    pub write_runtime: RuntimeRef,
    /// Runtime for compacting data
    pub compact_runtime: RuntimeRef,
    /// Runtime for horaemeta communication
    pub meta_runtime: RuntimeRef,
    /// Runtime for some other tasks which are not so important
    pub default_runtime: RuntimeRef,
    /// Runtime for io task
    pub io_runtime: RuntimeRef,
}
