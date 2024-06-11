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

//! Table to store system catalog

use std::{collections::HashMap, mem};

use async_trait::async_trait;
use bytes_ext::{BufMut, Bytes, BytesMut, SafeBuf, SafeBufMut};
use catalog::consts;
use codec::{memcomparable::MemComparable, Encoder};
use common_types::{
    column_schema,
    datum::{Datum, DatumKind},
    projected_schema::ProjectedSchema,
    record_batch::RecordBatch,
    request_id::RequestId,
    row::{Row, RowBuilder, RowGroup},
    schema::{self, Schema},
    table::DEFAULT_SHARD_ID,
    time::Timestamp,
};
use futures::TryStreamExt;
use horaedbproto::sys_catalog::{CatalogEntry, SchemaEntry, TableEntry};
use logger::{debug, info, warn};
use macros::define_result;
use prost::Message;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::{
    self,
    engine::{
        CreateTableParams, CreateTableRequest, DropTableRequest, OpenTableRequest, TableEngineRef,
        TableRequestType, TableState,
    },
    predicate::PredicateBuilder,
    table::{
        GetRequest, ReadOptions, ReadRequest, SchemaId, TableId, TableInfo, TableRef, WriteRequest,
    },
};
use tokio::sync::Mutex;
use trace_metric::MetricsCollector;

use crate::{SYSTEM_SCHEMA_ID, SYS_CATALOG_TABLE_ID, SYS_CATALOG_TABLE_NAME};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build schema for sys_catalog, err:{}", source))]
    BuildSchema { source: common_types::schema::Error },

    #[snafu(display(
        "Failed to get column index for sys_catalog, name:{}.\nBacktrace:\n{}",
        name,
        backtrace
    ))]
    GetColumnIndex { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to build table for sys_catalog, err:{}", source))]
    BuildTable { source: table_engine::engine::Error },

    #[snafu(display("Failed to open table for sys_catalog, err:{}", source))]
    OpenTable { source: table_engine::engine::Error },

    #[snafu(display("Failed to convert into RowGroup, err:{}", source))]
    IntoRowGroup { source: common_types::row::Error },

    #[snafu(display("Failed to persist catalog to table, err:{}", source))]
    PersistCatalog { source: table_engine::table::Error },

    #[snafu(display("Failed to persist schema to table, err:{}", source))]
    PersistSchema { source: table_engine::table::Error },

    #[snafu(display("Failed to persist tables to table, err:{}", source))]
    PersistTables { source: table_engine::table::Error },

    #[snafu(display("Failed to read table, err:{}", source))]
    ReadTable { source: table_engine::table::Error },

    #[snafu(display("Failed to read stream, err:{}", source))]
    ReadStream { source: table_engine::stream::Error },

    #[snafu(display(
        "Visitor catalog not found, catalog:{}.\nBacktrace:\n{}",
        catalog,
        backtrace
    ))]
    #[snafu(visibility(pub))]
    VisitorCatalogNotFound {
        catalog: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Visitor schema not found, catalog:{}, schema:{}.\nBacktrace:\n{}",
        catalog,
        schema,
        backtrace
    ))]
    #[snafu(visibility(pub))]
    VisitorSchemaNotFound {
        catalog: String,
        schema: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Visitor Failed to open table, err:{}", source))]
    #[snafu(visibility(pub))]
    VisitorOpenTable { source: table_engine::engine::Error },

    #[snafu(display("Failed to encode entry key header, err:{}", source))]
    EncodeKeyHeader { source: bytes_ext::Error },

    #[snafu(display("Failed to encode entry body, err:{}", source))]
    EncodeKeyBody { source: codec::memcomparable::Error },

    #[snafu(display("Failed to encode table key type, err:{}", source))]
    EncodeTableKeyType { source: bytes_ext::Error },

    #[snafu(display("Failed to read entry key header, err:{}", source))]
    ReadKeyHeader { source: bytes_ext::Error },

    #[snafu(display("Failed to read table key header, err:{}", source))]
    ReadTableKeyHeader { source: bytes_ext::Error },

    #[snafu(display(
        "Invalid entry key header, value:{}.\nBacktrace:\n{}",
        value,
        backtrace
    ))]
    InvalidKeyHeader { value: u8, backtrace: Backtrace },

    #[snafu(display("Invalid table key type, value:{}.\nBacktrace:\n{}", value, backtrace))]
    InvalidTableKeyType { value: u8, backtrace: Backtrace },

    #[snafu(display(
        "Failed to encode protobuf for entry, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    EncodeEntryPb {
        source: prost::EncodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to build row for entry, err:{source}"))]
    BuildRow { source: common_types::row::Error },

    #[snafu(display("Failed to build row group, err:{source}"))]
    BuildRowGroup { source: common_types::row::Error },

    #[snafu(display(
        "Failed to decode protobuf for entry, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    DecodeEntryPb {
        source: prost::DecodeError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to decode schema for table alter entry, table:{}, err:{}",
        table,
        source
    ))]
    DecodeSchema {
        table: String,
        source: common_types::schema::Error,
    },

    #[snafu(display("Table key type not found in key.\nBacktrace:\n{}", backtrace))]
    EmptyTableKeyType { backtrace: Backtrace },

    #[snafu(display(
        "The row in the sys_catalog_table is invalid, row:{:?}.\nBacktrace:\n{}",
        row,
        backtrace
    ))]
    InvalidTableRow { row: Row, backtrace: Backtrace },

    #[snafu(display(
        "The fetched table is mismatched, expect:{}, given:{}.\nBacktrace:\n{}",
        expect_table,
        given_table,
        backtrace
    ))]
    TableKeyMismatch {
        expect_table: String,
        given_table: String,
        backtrace: Backtrace,
    },

    #[snafu(display("The table is not found, table:{}.\nBacktrace:\n{}", table, backtrace))]
    TableNotFound { table: String, backtrace: Backtrace },

    #[snafu(display("Fail to get the table info, table:{}, err:{}.", table, source))]
    GetTableInfo {
        table: String,
        source: table_engine::table::Error,
    },

    #[snafu(display("Invalid table state transition, table:{}, err:{}.", table, source))]
    InvalidTableStateTransition {
        table: String,
        source: table_engine::engine::Error,
    },

    #[snafu(display("Invalid schema id, id:{}", id))]
    InvalidSchemaId { id: u32 },
}

define_result!(Error);

/// Timestamp of entry
pub const ENTRY_TIMESTAMP: Timestamp = Timestamp::new(0);
/// Name of key column (field)
pub const KEY_COLUMN_NAME: &str = "key";
/// Name of timestamp column (field)
pub const TIMESTAMP_COLUMN_NAME: &str = "timestamp";
/// Name of value column (field)
pub const VALUE_COLUMN_NAME: &str = "value";
/// Default enable ttl is false
pub const DEFAULT_ENABLE_TTL: &str = "false";

// TODO(yingwen): Add a type column once support int8 type and maybe split key
// into multiple columns.
/// SysCatalogTable is a special table to keep tracks of the system information.
///
/// Similar to kudu's SysCatalogTable
/// - see <https://github.com/apache/kudu/blob/76cb0dd808aaef548ef80682e13a00711e7dd6a4/src/kudu/master/sys_catalog.h#L133>
/// - schema: (key, timestamp) -> metadata
///
/// The timestamp is used to support metadata ttl in the future, now it can set
/// to 0.
#[derive(Debug)]
pub struct SysCatalogTable {
    // TODO(yingwen): Table id
    /// Underlying Table to actually store data
    table: TableRef,
    /// Index of the key column
    key_column_index: usize,
    /// Index of the value column
    value_column_index: usize,
    /// Protects table create/alter/drop
    // TODO(xikai): A better way is to use a specific struct with the lock that takes
    //  responsibilities to update table.
    update_table_lock: Mutex<()>,
}

impl SysCatalogTable {
    /// Create a new [SysCatalogTable]
    pub async fn new(table_engine: TableEngineRef) -> Result<Self> {
        let table_schema = new_sys_catalog_schema().context(BuildSchema)?;
        let key_column_index = table_schema
            .index_of(KEY_COLUMN_NAME)
            .context(GetColumnIndex {
                name: KEY_COLUMN_NAME,
            })?;
        let value_column_index =
            table_schema
                .index_of(VALUE_COLUMN_NAME)
                .context(GetColumnIndex {
                    name: VALUE_COLUMN_NAME,
                })?;

        let open_request = OpenTableRequest {
            catalog_name: consts::SYSTEM_CATALOG.to_string(),
            schema_name: consts::SYSTEM_CATALOG_SCHEMA.to_string(),
            schema_id: SYSTEM_SCHEMA_ID,
            table_name: SYS_CATALOG_TABLE_NAME.to_string(),
            table_id: SYS_CATALOG_TABLE_ID,
            engine: table_engine.engine_type().to_string(),
            shard_id: DEFAULT_SHARD_ID,
        };

        let table_opt = table_engine
            .open_table(open_request)
            .await
            .context(OpenTable)?;
        match table_opt {
            Some(table) => {
                info!("Sys catalog table open existing table");

                // The sys_catalog table is already created
                return Ok(Self {
                    table,
                    key_column_index,
                    value_column_index,
                    update_table_lock: Mutex::new(()),
                });
            }
            None => {
                info!("Sys catalog table is not exists, try to create a new table");
            }
        }

        let mut options = HashMap::new();
        options.insert(
            common_types::OPTION_KEY_ENABLE_TTL.to_string(),
            DEFAULT_ENABLE_TTL.to_string(),
        );
        // Disable layered memtable for system catalog table.
        options.insert(
            common_types::LAYERED_MUTABLE_SWITCH_THRESHOLD.to_string(),
            0.to_string(),
        );
        let params = CreateTableParams {
            catalog_name: consts::SYSTEM_CATALOG.to_string(),
            schema_name: consts::SYSTEM_CATALOG_SCHEMA.to_string(),
            table_name: SYS_CATALOG_TABLE_NAME.to_string(),
            table_schema,
            partition_info: None,
            engine: table_engine.engine_type().to_string(),
            table_options: options,
        };
        let create_request = CreateTableRequest {
            params,
            schema_id: SYSTEM_SCHEMA_ID,
            table_id: SYS_CATALOG_TABLE_ID,
            state: TableState::Stable,
            shard_id: DEFAULT_SHARD_ID,
        };

        let table = table_engine
            .create_table(create_request)
            .await
            .context(BuildTable)?;

        Ok(Self {
            table,
            key_column_index,
            value_column_index,
            update_table_lock: Mutex::new(()),
        })
    }

    /// Returns the table id of the sys catalog table.
    #[inline]
    pub fn table_id(&self) -> TableId {
        SYS_CATALOG_TABLE_ID
    }

    /// Add and store the catalog info
    pub async fn create_catalog(&self, request: CreateCatalogRequest) -> Result<()> {
        info!("Add catalog to sys_catalog table, request:{:?}", request);

        let row_group = request.into_row_group(self.table.schema())?;

        let write_req = WriteRequest { row_group };
        self.table.write(write_req).await.context(PersistCatalog)?;

        Ok(())
    }

    /// Add and store the schema info
    pub async fn create_schema(&self, request: CreateSchemaRequest) -> Result<()> {
        info!("Add schema to sys_catalog table, request:{:?}", request);

        let row_group = request.into_row_group(self.table.schema())?;

        let write_req = WriteRequest { row_group };
        self.table.write(write_req).await.context(PersistSchema)?;

        Ok(())
    }

    /// Create table in the catalog.
    pub async fn create_table(&self, table_info: TableInfo) -> Result<()> {
        info!(
            "Create table to sys_catalog table, table_info:{:?}",
            table_info
        );

        let _lock = self.update_table_lock.lock().await;
        self.write_table_info(table_info, TableRequestType::Create)
            .await?;

        Ok(())
    }

    /// Prepare to drop the table.
    pub async fn prepare_drop_table(&self, request: DropTableRequest) -> Result<()> {
        info!(
            "Prepare to drop table to sys_catalog table, request:{:?}",
            request
        );

        let table_key = TableKey {
            catalog: &request.catalog_name,
            schema: &request.schema_name,
            table: &request.table_name,
        };

        // update the dropped flag the lock held.
        {
            let _lock = self.update_table_lock.lock().await;
            if let Some(mut table_info) = self.get_table_info(table_key).await? {
                table_info.state.try_transit(TableState::Dropping).context(
                    InvalidTableStateTransition {
                        table: &request.table_name,
                    },
                )?;

                self.write_table_info(table_info, TableRequestType::Drop)
                    .await?;
            } else {
                warn!("Prepare to drop a dropped table, request:{:?}", request);
            }
        }

        Ok(())
    }

    /// Drop the table.
    ///
    /// Note that [prepare_drop_table] should be called before this method.
    pub async fn drop_table(&self, request: DropTableRequest) -> Result<()> {
        info!("Drop table to sys_catalog table, request:{:?}", request);

        let table_key = TableKey {
            catalog: &request.catalog_name,
            schema: &request.schema_name,
            table: &request.table_name,
        };

        // update the table state with the lock held.
        {
            if let Some(mut table_info) = self.get_table_info(table_key).await? {
                table_info.state.try_transit(TableState::Dropped).context(
                    InvalidTableStateTransition {
                        table: &request.table_name,
                    },
                )?;

                self.write_table_info(table_info, TableRequestType::Drop)
                    .await?;
            } else {
                warn!("Drop a dropped table, request:{:?}", request);
            }
        }

        Ok(())
    }

    /// Returns the inner table of the sys catalog.
    #[inline]
    pub fn inner_table(&self) -> TableRef {
        self.table.clone()
    }

    /// Write the table info to the sys_catalog table without lock.
    async fn write_table_info(&self, table_info: TableInfo, typ: TableRequestType) -> Result<()> {
        info!(
            "Write table info to sys_catalog table, table_info:{:?}",
            table_info
        );

        let serializer = TableWriter {
            catalog_table: self.table.clone(),
            table_to_write: table_info,
            typ,
        };

        serializer.write().await?;

        Ok(())
    }

    async fn get_table_info<'a>(&'a self, table_key: TableKey<'a>) -> Result<Option<TableInfo>> {
        let projected_schema = ProjectedSchema::no_projection(self.table.schema());
        let primary_key = TableWriter::build_table_primary_key(table_key.clone())?;
        let get_req = GetRequest {
            request_id: RequestId::next_id(),
            projected_schema,
            primary_key,
        };

        match self.table.get(get_req).await.context(GetTableInfo {
            table: table_key.table,
        })? {
            Some(row) => {
                let table_info = self.decode_table_info(row)?;
                let decoded_table_key = TableKey {
                    catalog: &table_info.catalog_name,
                    schema: &table_info.schema_name,
                    table: &table_info.table_name,
                };

                ensure!(
                    table_key == decoded_table_key,
                    TableKeyMismatch {
                        expect_table: table_key.table,
                        given_table: decoded_table_key.table,
                    }
                );

                Ok(Some(table_info))
            }
            None => Ok(None),
        }
    }

    fn decode_table_info(&self, row: Row) -> Result<TableInfo> {
        ensure!(
            row.num_columns() > self.key_column_index,
            InvalidTableRow { row }
        );

        ensure!(
            row.num_columns() > self.value_column_index,
            InvalidTableRow { row }
        );

        // Key and value column is always varbinary.
        let key = &row[self.key_column_index]
            .as_varbinary()
            .with_context(|| InvalidTableRow { row: row.clone() })?;
        let value = &row[self.value_column_index]
            .as_varbinary()
            .with_context(|| InvalidTableRow { row: row.clone() })?;

        match decode_one_request(key, value)? {
            DecodedRequest::TableEntry(request) => Ok(request),
            _ => InvalidTableRow { row }.fail(),
        }
    }

    /// Visit all data in the sys catalog table
    // TODO(yingwen): Expose read options
    pub async fn visit(
        &self,
        opts: ReadOptions,
        visitor_inner: &mut dyn VisitorInner,
        options: VisitOptions,
    ) -> Result<()> {
        let read_request = ReadRequest {
            request_id: RequestId::next_id(),
            opts,
            // The schema of sys catalog table is never changed
            projected_schema: ProjectedSchema::no_projection(self.table.schema()),
            predicate: PredicateBuilder::default().build(),
            metrics_collector: MetricsCollector::default(),
            priority: Default::default(),
        };
        let mut batch_stream = self.table.read(read_request).await.context(ReadTable)?;

        info!("batch_stream schema is:{:?}", batch_stream.schema());
        // TODO(yingwen): Check stream schema and table schema?
        let mut visitor = Visitor {
            inner: visitor_inner,
            options,
        };

        while let Some(batch) = batch_stream.try_next().await.context(ReadStream)? {
            // Visit all requests in the record batch
            info!("real batch_stream schema is:{:?}", batch.schema());
            self.visit_record_batch(batch, &mut visitor).await?;
        }

        Ok(())
    }

    /// Visit the record batch
    async fn visit_record_batch(
        &self,
        batch: RecordBatch,
        visitor: &mut Visitor<'_>,
    ) -> Result<()> {
        let key_column = batch.column(self.key_column_index);
        let value_column = batch.column(self.value_column_index);

        info!(
            "Sys catalog table visit record batch, column_num:{}, row_num:{}",
            batch.num_columns(),
            batch.num_rows()
        );

        let num_rows = batch.num_rows();
        for i in 0..num_rows {
            // Key and value column is not nullable
            let key = key_column.datum(i);
            let value = value_column.datum(i);

            debug!(
                "Sys catalog table visit row, i:{}, key:{:?}, value:{:?}",
                i, key, value
            );

            // Key and value column is always varbinary.
            let request =
                decode_one_request(key.as_varbinary().unwrap(), value.as_varbinary().unwrap())?;

            visitor.visit(request)?;
        }

        Ok(())
    }
}

/// Visitor inner for sys catalog requests
// TODO(yingwen): Define an Error for visitor
#[async_trait]
pub trait VisitorInner {
    // TODO(yingwen): Use enum another type if need more operation (delete/update)
    fn visit_catalog(&mut self, request: CreateCatalogRequest) -> Result<()>;

    fn visit_schema(&mut self, request: CreateSchemaRequest) -> Result<()>;

    // FIXME(xikai): Should this method be called visit_table?
    fn visit_tables(&mut self, table_info: TableInfo) -> Result<()>;
}

/// Options for visiting sys catalog requests
///
/// Following infos can be visited:
/// + catalog
/// + schema
/// + table
/// One or more you can select.
#[derive(Debug)]
pub struct VisitOptions {
    pub visit_catalog: bool,
    pub visit_schema: bool,
    pub visit_table: bool,
}

/// Builder for [VisitOptions]
#[derive(Debug, Default)]
pub struct VisitOptionsBuilder {
    visit_catalog: bool,
    visit_schema: bool,
    visit_table: bool,
}

impl VisitOptionsBuilder {
    pub fn build(self) -> VisitOptions {
        VisitOptions {
            visit_catalog: self.visit_catalog,
            visit_schema: self.visit_schema,
            visit_table: self.visit_table,
        }
    }

    pub fn visit_catalog(mut self) -> Self {
        self.visit_catalog = true;
        self
    }

    pub fn visit_schema(mut self) -> Self {
        self.visit_schema = true;
        self
    }

    pub fn visit_table(mut self) -> Self {
        self.visit_table = true;
        self
    }
}

pub struct Visitor<'a> {
    inner: &'a mut dyn VisitorInner,
    options: VisitOptions,
}

impl<'a> Visitor<'a> {
    fn visit(&mut self, request: DecodedRequest) -> Result<()> {
        debug!("Visitor begin to visit, options:{:?}", self.options);

        match request {
            DecodedRequest::CreateCatalog(req) => {
                if self.options.visit_catalog {
                    self.inner.visit_catalog(req)
                } else {
                    Ok(())
                }
            }

            DecodedRequest::CreateSchema(req) => {
                if self.options.visit_schema {
                    self.inner.visit_schema(req)
                } else {
                    Ok(())
                }
            }
            DecodedRequest::TableEntry(req) => {
                if self.options.visit_table {
                    self.inner.visit_tables(req)
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Build a new table schema for sys catalog
fn new_sys_catalog_schema() -> schema::Result<Schema> {
    // NOTICE: Both key and value must be non-nullable, the visit function takes
    // this assumption
    schema::Builder::with_capacity(3)
        .auto_increment_column_id(true)
        // key
        .add_key_column(
            column_schema::Builder::new(KEY_COLUMN_NAME.to_string(), DatumKind::Varbinary)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .expect("Should succeed to build column schema of catalog"),
        )?
        // timestamp
        .add_key_column(
            column_schema::Builder::new(TIMESTAMP_COLUMN_NAME.to_string(), DatumKind::Timestamp)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .expect("Should succeed to build column schema of catalog"),
        )?
        // value
        .add_normal_column(
            column_schema::Builder::new(VALUE_COLUMN_NAME.to_string(), DatumKind::Varbinary)
                .is_nullable(false)
                .is_tag(false)
                .build()
                .expect("Should succeed to build column schema of catalog"),
        )?
        .primary_key_indexes(vec![0, 1])
        .build()
}

/// Request type, used as key header
///
/// 0 is reserved
#[derive(Debug, Clone, Copy)]
enum KeyType {
    CreateCatalog = 1,
    CreateSchema = 2,
    TableEntry = 3,
}

impl KeyType {
    fn to_u8(self) -> u8 {
        self as u8
    }

    fn decode_from_bytes(mut buf: &[u8]) -> Result<Self> {
        let v = buf.try_get_u8().context(ReadKeyHeader)?;

        match v {
            v if v == Self::CreateCatalog as u8 => Ok(Self::CreateCatalog),
            v if v == Self::CreateSchema as u8 => Ok(Self::CreateSchema),
            v if v == Self::TableEntry as u8 => Ok(Self::TableEntry),
            value => InvalidKeyHeader { value }.fail(),
        }
    }
}

/// Catalog entry key
///
/// Use catalog name as key
struct CatalogKey<'a>(&'a str);

/// Schema entry key
///
/// Use (catalog, schema) as key
struct SchemaKey<'a>(&'a str, &'a str);

// TODO(yingwen): Maybe use same key for create/alter table.
/// Table entry key
///
/// Use (catalog, schema, table_id) as key
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableKey<'a> {
    catalog: &'a str,
    schema: &'a str,
    table: &'a str,
}

/// Encoder for entry key
struct EntryKeyEncoder;

impl<'a> Encoder<CatalogKey<'a>> for EntryKeyEncoder {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, value: &CatalogKey) -> Result<()> {
        buf.try_put_u8(KeyType::CreateCatalog.to_u8())
            .context(EncodeKeyHeader)?;
        let encoder = MemComparable;
        encoder
            .encode(buf, value.0.as_bytes())
            .context(EncodeKeyBody)
    }

    fn estimate_encoded_size(&self, value: &CatalogKey) -> usize {
        let encoder = MemComparable;
        mem::size_of::<u8>() + encoder.estimate_encoded_size(value.0.as_bytes())
    }
}

impl<'a> Encoder<SchemaKey<'a>> for EntryKeyEncoder {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, value: &SchemaKey) -> Result<()> {
        buf.try_put_u8(KeyType::CreateSchema.to_u8())
            .context(EncodeKeyHeader)?;
        let encoder = MemComparable;
        encoder
            .encode(buf, value.0.as_bytes())
            .context(EncodeKeyBody)?;
        encoder
            .encode(buf, value.1.as_bytes())
            .context(EncodeKeyBody)
    }

    fn estimate_encoded_size(&self, value: &SchemaKey) -> usize {
        let encoder = MemComparable;
        mem::size_of::<u8>()
            + encoder.estimate_encoded_size(value.0.as_bytes())
            + encoder.estimate_encoded_size(value.1.as_bytes())
    }
}

impl<'a> Encoder<TableKey<'a>> for EntryKeyEncoder {
    type Error = Error;

    fn encode<B: BufMut>(&self, buf: &mut B, value: &TableKey) -> Result<()> {
        buf.try_put_u8(KeyType::TableEntry.to_u8())
            .context(EncodeKeyHeader)?;
        let encoder = MemComparable;
        encoder
            .encode(buf, value.catalog.as_bytes())
            .context(EncodeKeyBody)?;
        encoder
            .encode(buf, value.schema.as_bytes())
            .context(EncodeKeyBody)?;
        encoder
            .encode(buf, value.table.as_bytes())
            .context(EncodeKeyBody)?;
        Ok(())
    }

    fn estimate_encoded_size(&self, value: &TableKey) -> usize {
        let encoder = MemComparable;
        mem::size_of::<u8>()
            + encoder.estimate_encoded_size(value.catalog.as_bytes())
            + encoder.estimate_encoded_size(value.schema.as_bytes())
            + encoder.estimate_encoded_size(value.table.as_bytes())
    }
}

/// Information of the catalog to add
#[derive(Debug)]
pub struct CreateCatalogRequest {
    /// Catalog name
    pub catalog_name: String,
}

impl CreateCatalogRequest {
    /// Convert into [common_types::row::RowGroup]
    fn into_row_group(self, schema: Schema) -> Result<RowGroup> {
        let key = self.to_key()?;
        let value = self.into_bytes();
        let row = RowBuilder::new(&schema)
            // key
            .append_datum(Datum::Varbinary(key))
            .context(BuildRow)?
            // timestamp
            .append_datum(Datum::Timestamp(ENTRY_TIMESTAMP))
            .context(BuildRow)?
            // value
            .append_datum(Datum::Varbinary(value))
            .context(BuildRow)?
            .finish()
            .context(BuildRow)?;

        RowGroup::try_new(schema, vec![row]).context(BuildRowGroup)
    }

    fn to_key(&self) -> Result<Bytes> {
        let encoder = EntryKeyEncoder;
        let key = CatalogKey(&self.catalog_name);
        let mut buf = BytesMut::with_capacity(encoder.estimate_encoded_size(&key));
        encoder.encode(&mut buf, &key)?;
        Ok(buf.into())
    }

    fn into_bytes(self) -> Bytes {
        let entry = CatalogEntry::from(self);
        entry.encode_to_vec().into()
    }
}

impl From<CreateCatalogRequest> for CatalogEntry {
    fn from(v: CreateCatalogRequest) -> Self {
        CatalogEntry {
            catalog_name: v.catalog_name,
            created_time: Timestamp::now().as_i64(),
        }
    }
}

impl From<CatalogEntry> for CreateCatalogRequest {
    fn from(entry: CatalogEntry) -> Self {
        Self {
            catalog_name: entry.catalog_name,
        }
    }
}

/// Information of the schema to add.
#[derive(Debug)]
pub struct CreateSchemaRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub schema_id: SchemaId,
}

impl CreateSchemaRequest {
    /// Convert into [common_types::row::RowGroup]
    fn into_row_group(self, schema: Schema) -> Result<RowGroup> {
        let key = self.to_key()?;
        let value = self.into_bytes();
        let row = RowBuilder::new(&schema)
            // key
            .append_datum(Datum::Varbinary(key))
            .context(BuildRow)?
            // timestamp
            .append_datum(Datum::Timestamp(ENTRY_TIMESTAMP))
            .context(BuildRow)?
            // value
            .append_datum(Datum::Varbinary(value))
            .context(BuildRow)?
            .finish()
            .context(BuildRow)?;

        Ok(RowGroup::new_unchecked(schema, vec![row]))
    }

    fn to_key(&self) -> Result<Bytes> {
        let encoder = EntryKeyEncoder;
        let key = SchemaKey(&self.catalog_name, &self.schema_name);
        let mut buf = BytesMut::with_capacity(encoder.estimate_encoded_size(&key));
        encoder.encode(&mut buf, &key)?;
        Ok(buf.into())
    }

    fn into_bytes(self) -> Bytes {
        let entry = SchemaEntry::from(self);

        entry.encode_to_vec().into()
    }
}

impl From<CreateSchemaRequest> for SchemaEntry {
    fn from(v: CreateSchemaRequest) -> Self {
        SchemaEntry {
            catalog_name: v.catalog_name,
            schema_name: v.schema_name,
            schema_id: v.schema_id.as_u32(),
            created_time: Timestamp::now().as_i64(),
        }
    }
}

impl From<SchemaEntry> for CreateSchemaRequest {
    fn from(entry: SchemaEntry) -> Self {
        let schema_id = SchemaId::from(entry.schema_id);

        Self {
            catalog_name: entry.catalog_name,
            schema_name: entry.schema_name,
            schema_id,
        }
    }
}

/// Information of the alter operations to the table.
#[derive(Clone, Debug)]
pub struct AlterTableRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    /// Schema after alteration.
    pub schema: Schema,
}

/// Writer for writing the table information into the catalog table.
pub struct TableWriter {
    catalog_table: TableRef,
    table_to_write: TableInfo,
    typ: TableRequestType,
}

impl TableWriter {
    async fn write(&self) -> Result<()> {
        let row_group = self.convert_table_info_to_row_group()?;
        let write_req = WriteRequest { row_group };
        self.catalog_table
            .write(write_req)
            .await
            .context(PersistTables)?;

        Ok(())
    }

    /// Convert the table to write into [common_types::row::RowGroup].
    fn convert_table_info_to_row_group(&self) -> Result<RowGroup> {
        let schema = self.catalog_table.schema();
        let key = Self::build_create_table_key(&self.table_to_write)?;
        let value = Self::build_create_table_value(self.table_to_write.clone(), self.typ)?;

        debug!(
            "TableWriter build key value, key:{:?}, value:{:?}",
            key, value
        );

        let row = Self::build_table_info_row(&schema, key, value)?;
        let row_group = RowGroup::new_unchecked(schema, vec![row]);

        Ok(row_group)
    }

    fn build_table_info_row(schema: &Schema, key: Bytes, value: Bytes) -> Result<Row> {
        RowBuilder::new(schema)
            // key
            .append_datum(Datum::Varbinary(key))
            .context(BuildRow)?
            // timestamp
            .append_datum(Datum::Timestamp(ENTRY_TIMESTAMP))
            .context(BuildRow)?
            // value
            .append_datum(Datum::Varbinary(value))
            .context(BuildRow)?
            .finish()
            .context(BuildRow)
    }

    fn build_create_table_key(table_info: &TableInfo) -> Result<Bytes> {
        let key = TableKey {
            catalog: &table_info.catalog_name,
            schema: &table_info.schema_name,
            table: &table_info.table_name,
        };
        Self::encode_table_key(key)
    }

    fn encode_table_key(key: TableKey) -> Result<Bytes> {
        let encoder = EntryKeyEncoder;
        let mut buf = BytesMut::with_capacity(encoder.estimate_encoded_size(&key));
        encoder.encode(&mut buf, &key)?;
        Ok(buf.into())
    }

    fn build_create_table_value(table_info: TableInfo, typ: TableRequestType) -> Result<Bytes> {
        let mut table_entry = TableEntry::from(table_info);

        let now = Timestamp::now().as_i64();
        match typ {
            TableRequestType::Create => table_entry.created_time = now,
            TableRequestType::Drop => table_entry.modified_time = now,
        }

        let buf = table_entry.encode_to_vec();
        Ok(buf.into())
    }

    fn build_table_primary_key(table_key: TableKey) -> Result<Vec<Datum>> {
        let encoded_key = Self::encode_table_key(table_key)?;

        Ok(vec![
            Datum::Varbinary(encoded_key),
            Datum::Timestamp(ENTRY_TIMESTAMP),
        ])
    }
}

/// Decoded sys catalog request
#[derive(Debug)]
enum DecodedRequest {
    CreateCatalog(CreateCatalogRequest),
    CreateSchema(CreateSchemaRequest),
    TableEntry(TableInfo),
}

/// Decode request from key/value
fn decode_one_request(key: &[u8], value: &[u8]) -> Result<DecodedRequest> {
    let key_type = KeyType::decode_from_bytes(key)?;
    let req = match key_type {
        KeyType::CreateCatalog => {
            let entry = CatalogEntry::decode(value).context(DecodeEntryPb)?;
            DecodedRequest::CreateCatalog(CreateCatalogRequest::from(entry))
        }
        KeyType::CreateSchema => {
            let entry = SchemaEntry::decode(value).context(DecodeEntryPb)?;
            DecodedRequest::CreateSchema(CreateSchemaRequest::from(entry))
        }
        KeyType::TableEntry => {
            let entry = TableEntry::decode(value).context(DecodeEntryPb)?;
            let table_info = TableInfo::from(entry);
            DecodedRequest::TableEntry(table_info)
        }
    };

    Ok(req)
}
