// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Table abstraction

use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use ceresdbproto::sys_catalog as sys_catalog_pb;
use common_types::{
    column_schema::ColumnSchema,
    datum::Datum,
    projected_schema::ProjectedSchema,
    request_id::RequestId,
    row::{Row, RowGroup},
    schema::{RecordSchemaWithKey, Schema, Version},
};
use datafusion::logical_expr::Expr;
use datafusion_proto::bytes::Serializeable;
use generic_error::{BoxError, GenericError};
use macros::define_result;
use serde::Deserialize;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use trace_metric::MetricsCollector;

use crate::{
    engine::TableState,
    partition::PartitionInfo,
    predicate::PredicateRef,
    stream::{PartitionedStreams, SendableRecordBatchStream},
};

/// Contains common error variant, implementation specific error should
/// be cast into Box<dyn Error>
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display(
        "Unsupported table method, table:{}, method:{}.\nBacktrace:\n{}",
        table,
        method,
        backtrace
    ))]
    UnsupportedMethod {
        table: String,
        method: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Get Invalid primary key, expected schema:{:?}, given_primary_keys:{:?}.\nBacktrace:\n{}",
        schema,
        primary_key_columns,
        backtrace
    ))]
    GetInvalidPrimaryKey {
        schema: RecordSchemaWithKey,
        primary_key_columns: Vec<ColumnSchema>,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Get null primary key, expected schema:{:?}, given_primary_keys:{:?}.\nBacktrace:\n{}",
        schema,
        primary_key_columns,
        backtrace
    ))]
    GetNullPrimaryKey {
        schema: RecordSchemaWithKey,
        primary_key_columns: Vec<ColumnSchema>,
        backtrace: Backtrace,
    },

    #[snafu(display("Unexpected error, err:{}", source))]
    Unexpected { source: GenericError },

    #[snafu(display("Unexpected error, msg:{}", msg))]
    UnexpectedWithMsg { msg: String },

    #[snafu(display("Invalid arguments, err:{}", source))]
    InvalidArguments { table: String, source: GenericError },

    #[snafu(display("Failed to write tables, table:{}, err:{}", table, source))]
    Write { table: String, source: GenericError },

    #[snafu(display("Failed to scan table, table:{}, err:{}", table, source))]
    Scan { table: String, source: GenericError },

    #[snafu(display("Failed to get table, table:{}, err:{}", table, source))]
    Get { table: String, source: GenericError },

    #[snafu(display("Failed to alter schema, table:{}, err:{}", table, source))]
    AlterSchema { table: String, source: GenericError },

    #[snafu(display("Failed to alter options, table:{}, err:{}", table, source))]
    AlterOptions { table: String, source: GenericError },

    #[snafu(display("Failed to flush table, table:{}, err:{}", table, source))]
    Flush { table: String, source: GenericError },

    #[snafu(display("Failed to compact table, table:{}, err:{}", table, source))]
    Compact { table: String, source: GenericError },

    #[snafu(display("Failed to convert read request to pb, msg:{}, err:{}", msg, source))]
    ReadRequestToPb { msg: String, source: GenericError },

    #[snafu(display("Empty read options.\nBacktrace:\n{}", backtrace))]
    EmptyReadOptions { backtrace: Backtrace },

    #[snafu(display("Empty projected schema.\nBacktrace:\n{}", backtrace))]
    EmptyProjectedSchema { backtrace: Backtrace },

    #[snafu(display("Empty predicate.\nBacktrace:\n{}", backtrace))]
    EmptyPredicate { backtrace: Backtrace },

    #[snafu(display("Failed to covert projected schema, err:{}", source))]
    ConvertProjectedSchema { source: GenericError },

    #[snafu(display("Failed to covert predicate, err:{}", source))]
    ConvertPredicate { source: GenericError },

    #[snafu(display("Failed to create partition rule, err:{}", source))]
    CreatePartitionRule { source: GenericError },

    #[snafu(display("Failed to locate partitions, err:{}", source))]
    LocatePartitions { source: GenericError },

    #[snafu(display("Failed to write tables in batch, tables:{:?}, err:{}", tables, source))]
    WriteBatch {
        tables: Vec<String>,
        source: GenericError,
    },

    #[snafu(display(
        "Failed to wait for pending writes, table:{table}.\nBacktrace:\n{backtrace}"
    ))]
    WaitForPendingWrites { table: String, backtrace: Backtrace },

    #[snafu(display("Reject for too many pending writes, table:{table}"))]
    TooManyPendingWrites { table: String },

    #[snafu(display("Failed to do merge write, msg:{}", msg))]
    MergeWrite { msg: String },

    #[snafu(display("Failed to render table, msg:{msg}, err:{source}"))]
    RenderTable { msg: String, source: GenericError },
}

define_result!(Error);

/// Default partition num to scan in parallelism.
pub const DEFAULT_READ_PARALLELISM: usize = 8;
pub const NO_TIMEOUT: i64 = -1;

/// Schema id (24 bits)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaId(u32);

impl SchemaId {
    /// Min schema id.
    pub const MIN: SchemaId = SchemaId(0);

    pub const fn from_u32(id: u32) -> Self {
        Self(id)
    }

    /// Convert the schema id into u32.
    #[inline]
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl PartialEq<u32> for SchemaId {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl From<u16> for SchemaId {
    fn from(id: u16) -> SchemaId {
        Self(id as u32)
    }
}

impl From<u32> for SchemaId {
    fn from(id: u32) -> SchemaId {
        Self(id)
    }
}

/// Sequence of a table under a schema (40 bits).
#[derive(Debug, Clone, Copy)]
pub struct TableSeq(u64);

impl TableSeq {
    /// Bits of schema id.
    const BITS: u64 = 40;
    /// 40 bits mask (0xffffffffff).
    const MASK: u64 = (1 << Self::BITS) - 1;
    /// Max sequence of table in a schema.
    pub const MAX: TableSeq = TableSeq(Self::MASK);
    /// Min sequence of table in a schema.
    pub const MIN: TableSeq = TableSeq(0);

    /// Create a new table sequence from u64, return None if `seq` is invalid.
    pub const fn new(seq: u64) -> Option<Self> {
        // Only need to check max as min is 0.
        if seq <= TableSeq::MAX.0 {
            Some(Self(seq))
        } else {
            None
        }
    }

    // It is safe to convert u32 into table seq.
    pub const fn from_u32(seq: u32) -> Self {
        Self(seq as u64)
    }

    /// Convert the table sequence into u64.
    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u32> for TableSeq {
    fn from(id: u32) -> TableSeq {
        TableSeq(id as u64)
    }
}

impl From<TableId> for TableSeq {
    /// Get the sequence part of the table id.
    fn from(table_id: TableId) -> TableSeq {
        let seq_part = table_id.0 & TableSeq::MASK;

        TableSeq(seq_part)
    }
}

/// Table Id (64 bits)
///
/// Table id is constructed via schema id (24 bits) and a table sequence (40
/// bits).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Deserialize)]
pub struct TableId(u64);

impl TableId {
    pub const MAX: TableId = TableId(u64::MAX);
    /// Min table id.
    pub const MIN: TableId = TableId(0);

    /// Create table id from raw u64 number.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Create a new table id from `schema_id` and `table_seq`.
    ///
    /// Return `None` If `schema_id` is not invalid.
    pub const fn with_seq(schema_id: SchemaId, table_seq: TableSeq) -> Option<Self> {
        let schema_id_data = schema_id.0 as u64;
        let schema_id_part = schema_id_data << TableSeq::BITS;
        if (schema_id_part >> TableSeq::BITS) != schema_id_data {
            None
        } else {
            Some(Self(schema_id_part | table_seq.0))
        }
    }

    /// Convert table id into u64.
    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl PartialEq<u64> for TableId {
    fn eq(&self, other: &u64) -> bool {
        self.as_u64() == *other
    }
}

impl From<u64> for TableId {
    fn from(id: u64) -> TableId {
        TableId::new(id)
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct WriteRequest {
    /// rows to write
    pub row_group: RowGroup,
}

#[derive(Clone, Debug)]
pub struct ReadOptions {
    pub batch_size: usize,
    /// Suggested read parallelism, the actual returned stream should equal to
    /// `read_parallelism`.
    pub read_parallelism: usize,
    /// Request deadline
    pub deadline: Option<Instant>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            batch_size: 10000,
            read_parallelism: DEFAULT_READ_PARALLELISM,
            deadline: None,
        }
    }
}

impl From<ceresdbproto::remote_engine::ReadOptions> for ReadOptions {
    fn from(pb: ceresdbproto::remote_engine::ReadOptions) -> Self {
        Self {
            batch_size: pb.batch_size as usize,
            read_parallelism: pb.read_parallelism as usize,
            deadline: if pb.timeout_ms == NO_TIMEOUT {
                None
            } else {
                Some(Instant::now() + Duration::from_millis(pb.timeout_ms as u64))
            },
        }
    }
}

impl From<ReadOptions> for ceresdbproto::remote_engine::ReadOptions {
    fn from(opts: ReadOptions) -> Self {
        Self {
            batch_size: opts.batch_size as u64,
            read_parallelism: opts.read_parallelism as u64,
            timeout_ms: if let Some(deadline) = opts.deadline {
                deadline.duration_since(Instant::now()).as_millis() as i64
            } else {
                NO_TIMEOUT
            },
        }
    }
}

#[derive(Debug)]
pub struct GetRequest {
    /// Query request id.
    pub request_id: RequestId,
    /// The schema and projection for get, the output data should match this
    /// schema.
    pub projected_schema: ProjectedSchema,
    /// The primary key of the row to get.
    pub primary_key: Vec<Datum>,
}

#[derive(Clone)]
pub struct ReadRequest {
    /// Read request id.
    pub request_id: RequestId,
    /// Read options.
    pub opts: ReadOptions,
    /// The schema and projection for read, the output data should match this
    /// schema.
    pub projected_schema: ProjectedSchema,
    /// Predicate of the query.
    pub predicate: PredicateRef,
    /// Collector for metrics of this read request.
    pub metrics_collector: MetricsCollector,
}

impl fmt::Debug for ReadRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let exprs = self
            .predicate
            .exprs()
            .iter()
            .map(|expr| expr.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let predicate = format!("[{}]", exprs);

        let all_fields = self
            .projected_schema
            .to_projected_arrow_schema()
            .all_fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
            .join(",");
        let projected = format!("[{}]", all_fields);

        f.debug_struct("ReadRequest")
            .field("request_id", &self.request_id)
            .field("opts", &self.opts)
            .field("projected", &projected)
            .field("predicate", &predicate)
            .finish()
    }
}

impl TryFrom<ReadRequest> for ceresdbproto::remote_engine::TableReadRequest {
    type Error = Error;

    fn try_from(request: ReadRequest) -> std::result::Result<Self, Error> {
        let predicate_pb =
            request
                .predicate
                .as_ref()
                .try_into()
                .box_err()
                .context(ReadRequestToPb {
                    msg: format!(
                        "convert predicate failed, predicate:{:?}",
                        request.predicate
                    ),
                })?;

        Ok(Self {
            request_id: request.request_id.as_u64(),
            opts: Some(request.opts.into()),
            projected_schema: Some(request.projected_schema.into()),
            predicate: Some(predicate_pb),
            order: 0,
        })
    }
}

impl TryFrom<ceresdbproto::remote_engine::TableReadRequest> for ReadRequest {
    type Error = Error;

    fn try_from(pb: ceresdbproto::remote_engine::TableReadRequest) -> Result<Self> {
        let opts = pb.opts.context(EmptyReadOptions)?.into();
        let projected_schema = pb
            .projected_schema
            .context(EmptyProjectedSchema)?
            .try_into()
            .box_err()
            .context(ConvertProjectedSchema)?;
        let predicate = Arc::new(
            pb.predicate
                .context(EmptyPredicate)?
                .try_into()
                .box_err()
                .context(ConvertPredicate)?,
        );
        Ok(Self {
            request_id: pb.request_id.into(),
            opts,
            projected_schema,
            predicate,
            metrics_collector: MetricsCollector::default(),
        })
    }
}

#[derive(Debug)]
pub struct AlterSchemaRequest {
    /// The new schema.
    pub schema: Schema,
    /// Previous schema version before alteration.
    pub pre_schema_version: Version,
}

#[derive(Debug)]
pub struct FlushRequest {
    /// Whether to wait flush task finishes, default is true.
    pub sync: bool,
}

impl Default for FlushRequest {
    fn default() -> Self {
        Self { sync: true }
    }
}

/// Table abstraction
///
/// We do not let Table trait extends datafusion's TableProvider, since
/// that will tie out abstraction with datafusion. However, we still use
/// datafusion's RecordBatchStream trait.
#[async_trait]
pub trait Table: std::fmt::Debug {
    /// Returns table name.
    fn name(&self) -> &str;

    /// Returns the id of this table.
    fn id(&self) -> TableId;

    /// Schema of this table.
    fn schema(&self) -> Schema;

    /// Options of this table.
    fn options(&self) -> HashMap<String, String>;

    fn partition_info(&self) -> Option<PartitionInfo> {
        None
    }

    /// Engine type of this table.
    fn engine_type(&self) -> &str;

    /// Get table's statistics.
    fn stats(&self) -> TableStats;

    /// Whether the columns used in filter expr can be pushdown.
    ///
    /// `read_schema` is used here to avoid upper layer see different schema
    /// during one query.
    fn support_pushdown(&self, _read_schema: &Schema, _col_names: &[String]) -> bool;

    /// Write to table.
    async fn write(&self, request: WriteRequest) -> Result<usize>;

    /// Read from table.
    async fn read(&self, request: ReadRequest) -> Result<SendableRecordBatchStream>;

    /// Get the specific row according to the primary key.
    async fn get(&self, request: GetRequest) -> Result<Option<Row>>;

    /// Read multiple partition of the table in parallel.
    async fn partitioned_read(&self, request: ReadRequest) -> Result<PartitionedStreams>;

    /// Alter table schema to the schema specific in [AlterSchemaRequest] if
    /// the `pre_schema_version` is equal to current schema version.
    ///
    /// Returns the affected rows (always 0).
    async fn alter_schema(&self, request: AlterSchemaRequest) -> Result<usize>;

    /// Alter table options.
    ///
    /// Returns the affected rows (always 0).
    async fn alter_options(&self, options: HashMap<String, String>) -> Result<usize>;

    /// Flush this table.
    async fn flush(&self, request: FlushRequest) -> Result<()>;

    /// Compact this table and wait until compaction completes.
    async fn compact(&self) -> Result<()>;
}

/// Basic statistics of table.
#[derive(Debug, Clone, Copy, Default)]
pub struct TableStats {
    /// Total write request
    pub num_write: u64,
    /// Total read request
    pub num_read: u64,
    /// Total flush request
    pub num_flush: u64,
}

/// A reference-counted pointer to Table
pub type TableRef = Arc<dyn Table + Send + Sync>;

/// Helper to generate a schema id.
pub struct SchemaIdGenerator {
    last_schema_id: AtomicU32,
}

impl SchemaIdGenerator {
    pub fn last_schema_id_u32(&self) -> u32 {
        self.last_schema_id.load(Ordering::Relaxed)
    }

    pub fn set_last_schema_id(&self, last_schema_id: SchemaId) {
        self.last_schema_id
            .store(last_schema_id.as_u32(), Ordering::Relaxed);
    }

    pub fn alloc_schema_id(&self) -> Option<SchemaId> {
        // TODO: consider the case where schema id overflows.
        let last = self.last_schema_id.fetch_add(1, Ordering::Relaxed);

        Some(SchemaId::from(last + 1))
    }
}

impl Default for SchemaIdGenerator {
    fn default() -> Self {
        Self {
            last_schema_id: AtomicU32::new(SchemaId::MIN.as_u32()),
        }
    }
}

/// Helper to generate a table sequence.
pub struct TableSeqGenerator {
    last_table_seq: AtomicU64,
}

impl TableSeqGenerator {
    pub fn last_table_seq_u64(&self) -> u64 {
        self.last_table_seq.load(Ordering::Relaxed)
    }

    pub fn set_last_table_seq(&self, last_table_seq: TableSeq) {
        self.last_table_seq
            .store(last_table_seq.as_u64(), Ordering::Relaxed);
    }

    pub fn alloc_table_seq(&self) -> Option<TableSeq> {
        // TODO: consider the case where table sequence overflows.
        let last = self.last_table_seq.fetch_add(1, Ordering::Relaxed);

        TableSeq::new(last + 1)
    }
}

impl Default for TableSeqGenerator {
    fn default() -> Self {
        Self {
            last_table_seq: AtomicU64::new(TableSeq::MIN.as_u64()),
        }
    }
}

/// Create table request in catalog
#[derive(Debug, Clone)]
pub struct TableInfo {
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
    /// Tells state of the table
    pub state: TableState,
}

impl From<sys_catalog_pb::TableEntry> for TableInfo {
    fn from(entry: sys_catalog_pb::TableEntry) -> Self {
        let state = entry.state();
        Self {
            catalog_name: entry.catalog_name,
            schema_name: entry.schema_name,
            schema_id: SchemaId(entry.schema_id),
            table_id: entry.table_id.into(),
            table_name: entry.table_name,
            engine: entry.engine,
            state: TableState::from(state),
        }
    }
}

impl From<TableInfo> for sys_catalog_pb::TableEntry {
    fn from(v: TableInfo) -> Self {
        sys_catalog_pb::TableEntry {
            catalog_name: v.catalog_name,
            schema_name: v.schema_name,
            schema_id: v.schema_id.as_u32(),
            table_id: v.table_id.as_u64(),
            table_name: v.table_name,
            engine: v.engine,
            state: sys_catalog_pb::TableState::from(v.state) as i32,
            // FIXME: Maybe [`TableInfo`] should contains such information.
            created_time: 0,
            modified_time: 0,
        }
    }
}

pub fn render_columns_and_constrains(table_ref: &TableRef) -> String {
    let table_schema = table_ref.schema();
    let key_columns = table_schema.key_columns();
    let timestamp_key = table_schema.timestamp_name();

    let mut res = String::new();
    for col in table_schema.columns() {
        res += format!("`{}` {}", col.name, col.data_type).as_str();
        if col.is_tag {
            res += " TAG";
        }

        if col.is_dictionary {
            res += " DICTIONARY";
        }

        if !col.is_nullable {
            res += " NOT NULL";
        }

        if let Some(expr) = &col.default_value {
            res += format!(" DEFAULT {expr}").as_str();
        }

        if !col.comment.is_empty() {
            res += format!(" COMMENT '{}'", col.comment).as_str();
        }
        res += ", ";
    }
    let keys: Vec<String> = key_columns.iter().map(|col| col.name.to_string()).collect();
    res += format!("PRIMARY KEY({}), ", keys.join(",")).as_str();
    res += format!("TIMESTAMP KEY({timestamp_key})").as_str();

    res
}

pub fn render_partition_info(partition_info: Option<PartitionInfo>) -> Result<Option<String>> {
    if partition_info.is_none() {
        return Ok(None);
    }

    let partition_info = partition_info.unwrap();
    let ret = match partition_info {
        PartitionInfo::Hash(v) => {
            let expr = Expr::from_bytes(&v.expr).box_err().context(RenderTable {
                msg: "render partition info",
            })?;

            if v.linear {
                format!(
                    " PARTITION BY LINEAR HASH({expr}) PARTITIONS {}",
                    v.definitions.len()
                )
            } else {
                format!(
                    " PARTITION BY HASH({expr}) PARTITIONS {}",
                    v.definitions.len()
                )
            }
        }
        PartitionInfo::Key(v) => {
            let rendered_partition_key = v.partition_key.join(",");
            if v.linear {
                format!(
                    "PARTITION BY LINEAR KEY({rendered_partition_key}) PARTITIONS {}",
                    v.definitions.len()
                )
            } else {
                format!(
                    "PARTITION BY KEY({rendered_partition_key}) PARTITIONS {}",
                    v.definitions.len()
                )
            }
        }
        PartitionInfo::Random(v) => {
            format!("PARTITION BY RANDOM PARTITIONS {}", v.definitions.len())
        }
    };
    Ok(Some(ret))
}

pub fn render_options(opts: HashMap<String, String>) -> String {
    if !opts.is_empty() {
        let mut v: Vec<String> = opts
            .into_iter()
            .map(|(k, v)| format!("{k}='{v}'"))
            .collect();
        // sorted by option name
        v.sort();
        format!(" WITH({})", v.join(", "))
    } else {
        "".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_id() {
        assert_eq!(0, SchemaId::MIN.as_u32());
    }

    #[test]
    fn test_table_seq() {
        assert_eq!(0, TableSeq::MIN.as_u64());
        assert_eq!(0xffffffffff, TableSeq::MAX.as_u64());
    }
}
