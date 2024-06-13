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

//! Planner converts a SQL AST into logical plans

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
    mem,
    ops::ControlFlow,
    sync::{atomic::AtomicBool, Arc},
};

use arrow::{
    compute::can_cast_types,
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    error::ArrowError,
};
use catalog::consts::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use cluster::config::SchemaConfig;
use common_types::{
    column_schema::{self, ColumnSchema},
    datum::{Datum, DatumKind},
    request_id::RequestId,
    row::{RowBuilder, RowGroup},
    schema::{self, Builder as SchemaBuilder, Schema, TSID_COLUMN},
};
use datafusion::{
    common::{DFField, DFSchema},
    error::DataFusionError,
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
    physical_expr::{create_physical_expr, execution_props::ExecutionProps},
    sql::{
        planner::{ParserOptions, PlannerContext, SqlToRel},
        ResolvedTableReference,
    },
};
use generic_error::GenericError;
use horaedbproto::storage::{value::Value as PbValue, WriteTableRequest};
use influxql_parser::statement::Statement as InfluxqlStatement;
use logger::{debug, trace};
use macros::define_result;
use prom_remote_api::types::Query as PromRemoteQuery;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use sqlparser::ast::{
    visit_statements_mut, ColumnDef, ColumnOption, Expr, Expr as SqlExpr, Ident, Query, SelectItem,
    SetExpr, SqlOption, Statement as SqlStatement, TableConstraint, UnaryOperator, Value, Values,
};
use table_engine::table::TableRef;

use crate::{
    ast::{
        AlterAddColumn, AlterModifySetting, CreateTable, DescribeTable, DropTable, ExistsTable,
        ShowCreate, ShowTables, Statement, TableName,
    },
    config::DynamicConfig,
    container::TableReference,
    frontend::parse_table_name_with_standard,
    logical_optimizer::optimize_plan,
    opentsdb::{
        opentsdb_query_to_plan,
        types::{OpentsdbQueryPlan, QueryRequest},
    },
    parser,
    partition::PartitionParser,
    plan::{
        AlterTableOperation, AlterTablePlan, CreateTablePlan, DescribeTablePlan, DropTablePlan,
        ExistsTablePlan, InsertPlan, InsertSource, Plan, QueryPlan, QueryType, ShowCreatePlan,
        ShowPlan, ShowTablesPlan,
    },
    promql::{remote_query_to_plan, ColumnNames, Expr as PromExpr, RemoteQueryPlan},
    provider::{ContextProviderAdapter, MetaProvider},
};

// We do not carry backtrace in sql error because it is mainly used in server
// handler and the error is usually caused by invalid/unsupported sql, which
// should be easy to find out the reason.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(display("Failed to generate datafusion plan, err:{}", source))]
    DatafusionPlan { source: DataFusionError },

    #[snafu(display("Failed to create datafusion schema, err:{}", source))]
    CreateDatafusionSchema { source: DataFusionError },

    #[snafu(display("Failed to merge arrow schema, err:{}", source))]
    MergeArrowSchema { source: ArrowError },

    #[snafu(display("Failed to generate datafusion expr, err:{}", source))]
    DatafusionExpr { source: DataFusionError },

    #[snafu(display(
        "Failed to get data type from datafusion physical expr, err:{}",
        source
    ))]
    DatafusionDataType { source: DataFusionError },

    // Statement is too large and complicate to carry in Error, so we
    // only return error here, so the caller should attach sql to its
    // error context
    #[snafu(display("Unsupported SQL statement"))]
    UnsupportedStatement,

    #[snafu(display("Create table name is empty"))]
    CreateTableNameEmpty,

    #[snafu(display("Only support non-column-input expr in default-value-option, column name:{} default value:{}", name, default_value))]
    CreateWithComplexDefaultValue { name: String, default_value: Expr },

    #[snafu(display("Table must contain timestamp constraint"))]
    RequireTimestamp,

    #[snafu(display(
        "Table must contain only one timestamp key and it's data type must be TIMESTAMP"
    ))]
    InvalidTimestampKey,

    #[snafu(display("Invalid unsign type: {}.\nBacktrace:\n{}", kind, backtrace))]
    InvalidUnsignType {
        kind: DatumKind,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Undefined column is used in primary key, column name:{}.\nBacktrace:\n{}",
        name,
        backtrace
    ))]
    UndefinedColumnInPrimaryKey { name: String, backtrace: Backtrace },

    #[snafu(display("Primary key not found, column name:{}", name))]
    PrimaryKeyNotFound { name: String },

    #[snafu(display(
        "Duplicate definitions of primary key are found, first:{:?}, second:{:?}.\nBacktrace:\n{:?}",
        first,
        second,
        backtrace,
    ))]
    DuplicatePrimaryKey {
        first: Vec<Ident>,
        second: Vec<Ident>,
        backtrace: Backtrace,
    },

    #[snafu(display("Tag column not found, name:{}", name))]
    TagColumnNotFound { name: String },

    #[snafu(display(
        "Timestamp key column can not be tag, name:{}.\nBacktrace:\n{:?}",
        name,
        backtrace
    ))]
    TimestampKeyTag { name: String, backtrace: Backtrace },

    #[snafu(display("Timestamp column not found, name:{}", name))]
    TimestampColumnNotFound { name: String },

    #[snafu(display("{} is a reserved column name", name))]
    ColumnNameReserved { name: String },

    #[snafu(display("Invalid create table name, err:{}", source))]
    InvalidCreateTableName { source: DataFusionError },

    #[snafu(display("Failed to build schema, err:{}", source))]
    BuildTableSchema { source: common_types::schema::Error },

    #[snafu(display("Unsupported SQL data type, err:{}", source))]
    UnsupportedDataType { source: common_types::datum::Error },

    #[snafu(display("Invalid column schema, column_name:{}, err:{}", column_name, source))]
    InvalidColumnSchema {
        column_name: String,
        source: column_schema::Error,
    },

    #[snafu(display("Invalid table name, err:{}", source))]
    InvalidTableName { source: DataFusionError },

    #[snafu(display("Table not found, table:{}", name))]
    TableNotFound { name: String },

    #[snafu(display("Column is not null, table:{}, column:{}", table, column))]
    InsertMissingColumn { table: String, column: String },

    #[snafu(display("Column is reserved, table:{}, column:{}", table, column))]
    InsertReservedColumn { table: String, column: String },

    #[snafu(display("Unknown insert column, name:{}", name))]
    UnknownInsertColumn { name: String },

    #[snafu(display("Insert values not enough, len:{}, index:{}", len, index))]
    InsertValuesNotEnough { len: usize, index: usize },

    #[snafu(display("Invalid insert stmt, contains duplicate columns"))]
    InsertDuplicateColumns,

    #[snafu(display("Invalid insert stmt, source should be a set"))]
    InsertSourceBodyNotSet,

    #[snafu(display(
        "Invalid insert stmt, source expr is not value, source_expr:{:?}.\nBacktrace:\n{}",
        source_expr,
        backtrace,
    ))]
    InsertExprNotValue {
        source_expr: Expr,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Invalid insert stmt, source expr has no valid negative value, source_expr:{:?}.\nBacktrace:\n{}",
        source_expr,
        backtrace,
    ))]
    InsertExprNoNegativeValue {
        source_expr: Expr,
        backtrace: Backtrace,
    },

    #[snafu(display("Insert Failed to convert value, err:{}", source))]
    InsertConvertValue { source: common_types::datum::Error },

    #[snafu(display("Failed to build row, err:{}", source))]
    BuildRow { source: common_types::row::Error },

    #[snafu(display("MetaProvider Failed to find table, err:{}", source))]
    MetaProviderFindTable { source: crate::provider::Error },

    #[snafu(display("Failed to find meta during planning, err:{}", source))]
    FindMeta { source: crate::provider::Error },

    #[snafu(display("Invalid alter table operation, err:{}", source))]
    InvalidAlterTableOperation { source: crate::plan::Error },

    #[snafu(display("Unsupported sql option, value:{}", value))]
    UnsupportedOption { value: String },

    #[snafu(display("Failed to build plan from promql, error:{}", source))]
    BuildPromPlanError { source: crate::promql::Error },

    #[snafu(display("Failed to build opentsdb plan, error:{}", source))]
    BuildOpentsdbPlanError { source: crate::opentsdb::Error },

    #[snafu(display(
        "Failed to cast default value expr to column type, expr:{}, from:{}, to:{}",
        expr,
        from,
        to
    ))]
    InvalidDefaultValueCoercion {
        expr: Expr,
        from: ArrowDataType,
        to: ArrowDataType,
    },

    #[snafu(display(
        "Failed to parse partition statement to partition info, msg:{}, err:{}",
        msg,
        source,
    ))]
    ParsePartitionWithCause { msg: String, source: GenericError },

    #[snafu(display("Unsupported partition method, msg:{}", msg,))]
    UnsupportedPartition { msg: String },

    #[snafu(display("Failed to build plan, msg:{}", msg))]
    InvalidWriteEntry { msg: String },

    #[snafu(display("Failed to build influxql plan, err:{}", source))]
    BuildInfluxqlPlan {
        source: crate::influxql::error::Error,
    },
}

define_result!(Error);

const DEFAULT_QUOTE_CHAR: char = '`';
const DEFAULT_PARSER_OPTS: ParserOptions = ParserOptions {
    parse_float_as_decimal: false,
    enable_ident_normalization: false,
};

pub struct PlannerHint {
    pub enable_push_down_able_dist_query: AtomicBool,
}

/// Planner produces logical plans from SQL AST
// TODO(yingwen): Rewrite Planner instead of using datafusion's planner
pub struct Planner<'a, P: MetaProvider> {
    provider: &'a P,
    request_id: RequestId,
    read_parallelism: usize,
    dyn_config: &'a DynamicConfig,
}

impl<'a, P: MetaProvider> Planner<'a, P> {
    /// Create a new logical planner
    pub fn new(
        provider: &'a P,
        request_id: RequestId,
        read_parallelism: usize,
        dyn_config: &'a DynamicConfig,
    ) -> Self {
        Self {
            provider,
            request_id,
            read_parallelism,
            dyn_config,
        }
    }

    /// Create a logical plan from Statement
    ///
    /// Takes the ownership of statement because some statements like INSERT
    /// statements contains lots of data
    pub fn statement_to_plan(&self, statement: Statement) -> Result<Plan> {
        trace!(
            "Statement to plan, request_id:{}, statement:{:?}",
            self.request_id,
            statement
        );

        let adapter =
            ContextProviderAdapter::new(self.provider, self.read_parallelism, self.dyn_config);
        // SqlToRel needs to hold the reference to adapter, thus we can't both holds the
        // adapter and the SqlToRel in Planner, which is a self-referential
        // case. We wrap a PlannerDelegate to workaround this and avoid the usage of
        // pin.
        let planner = PlannerDelegate::new(adapter);

        match statement {
            Statement::Standard(s) => planner.sql_statement_to_plan(*s),
            Statement::Create(s) => planner.create_table_to_plan(*s),
            Statement::Drop(s) => planner.drop_table_to_plan(s),
            Statement::Describe(s) => planner.describe_table_to_plan(s),
            Statement::AlterModifySetting(s) => planner.alter_modify_setting_to_plan(s),
            Statement::AlterAddColumn(s) => planner.alter_add_column_to_plan(s),
            Statement::ShowCreate(s) => planner.show_create_to_plan(s),
            Statement::ShowTables(s) => planner.show_tables_to_plan(s),
            Statement::ShowDatabases => planner.show_databases_to_plan(),
            Statement::Exists(s) => planner.exists_table_to_plan(s),
        }
    }

    pub fn promql_expr_to_plan(&self, expr: PromExpr) -> Result<(Plan, Arc<ColumnNames>)> {
        let adapter =
            ContextProviderAdapter::new(self.provider, self.read_parallelism, self.dyn_config);
        // SqlToRel needs to hold the reference to adapter, thus we can't both holds the
        // adapter and the SqlToRel in Planner, which is a self-referential
        // case. We wrap a PlannerDelegate to workaround this and avoid the usage of
        // pin.
        let planner = PlannerDelegate::new(adapter);

        expr.to_plan(planner.meta_provider, self.read_parallelism)
            .context(BuildPromPlanError)
    }

    pub fn remote_prom_req_to_plan(&self, query: PromRemoteQuery) -> Result<RemoteQueryPlan> {
        let adapter =
            ContextProviderAdapter::new(self.provider, self.read_parallelism, self.dyn_config);
        let planner = PlannerDelegate::new(adapter);

        remote_query_to_plan(query, planner.meta_provider).context(BuildPromPlanError)
    }

    pub fn influxql_stmt_to_plan(&self, statement: InfluxqlStatement) -> Result<Plan> {
        let adapter =
            ContextProviderAdapter::new(self.provider, self.read_parallelism, self.dyn_config);
        let planner = crate::influxql::planner::Planner::new(adapter);
        planner
            .statement_to_plan(statement)
            .context(BuildInfluxqlPlan)
    }

    pub fn opentsdb_query_to_plan(&self, query: QueryRequest) -> Result<OpentsdbQueryPlan> {
        opentsdb_query_to_plan(query, self.provider, self.read_parallelism, self.dyn_config)
            .context(BuildOpentsdbPlanError)
    }

    pub fn write_req_to_plan(
        &self,
        schema_config: &SchemaConfig,
        write_table: &WriteTableRequest,
    ) -> Result<Plan> {
        Ok(Plan::Create(CreateTablePlan {
            engine: schema_config.default_engine_type.clone(),
            if_not_exists: true,
            table: write_table.table.clone(),
            table_schema: build_schema_from_write_table_request(schema_config, write_table)?,
            options: HashMap::default(),
            partition_info: None,
        }))
    }
}

pub fn build_column_schema(
    column_name: &str,
    data_type: DatumKind,
    is_tag: bool,
) -> Result<ColumnSchema> {
    let builder = column_schema::Builder::new(column_name.to_string(), data_type)
        .is_nullable(true)
        .is_tag(is_tag);

    builder.build().with_context(|| InvalidColumnSchema {
        column_name: column_name.to_string(),
    })
}

pub fn build_schema_from_write_table_request(
    schema_config: &SchemaConfig,
    write_table_req: &WriteTableRequest,
) -> Result<Schema> {
    let WriteTableRequest {
        table,
        field_names,
        tag_names,
        entries: write_entries,
    } = write_table_req;

    let mut schema_builder =
        SchemaBuilder::with_capacity(field_names.len()).auto_increment_column_id(true);

    ensure!(
        !write_entries.is_empty(),
        InvalidWriteEntry {
            msg: "empty write entries".to_string()
        }
    );

    let mut name_column_map: BTreeMap<_, ColumnSchema> = BTreeMap::new();
    for write_entry in write_entries {
        // parse tags
        for tag in &write_entry.tags {
            let name_index = tag.name_index as usize;
            ensure!(
                name_index < tag_names.len(),
                InvalidWriteEntry {
                    msg: format!(
                        "tag {tag:?} is not found in tag_names:{tag_names:?}, table:{table}",
                    ),
                }
            );

            let tag_name = &tag_names[name_index];

            let tag_value = tag
                .value
                .as_ref()
                .with_context(|| InvalidWriteEntry {
                    msg: format!("Tag({tag_name}) value is needed, table_name:{table} "),
                })?
                .value
                .as_ref()
                .with_context(|| InvalidWriteEntry {
                    msg: format!("Tag({tag_name}) value type is not supported, table_name:{table}"),
                })?;

            let data_type = try_get_data_type_from_value(tag_value)?;

            if let Some(column_schema) = name_column_map.get(tag_name) {
                ensure_data_type_compatible(table, tag_name, true, data_type, column_schema)?;
            }

            let column_schema = build_column_schema(tag_name, data_type, true)?;
            name_column_map.insert(tag_name, column_schema);
        }

        // parse fields
        for field_group in &write_entry.field_groups {
            for field in &field_group.fields {
                if (field.name_index as usize) < field_names.len() {
                    let field_name = &field_names[field.name_index as usize];
                    let field_value = field
                        .value
                        .as_ref()
                        .with_context(|| InvalidWriteEntry {
                            msg: format!("Field({field_name}) value is needed, table:{table}"),
                        })?
                        .value
                        .as_ref()
                        .with_context(|| InvalidWriteEntry {
                            msg: format!(
                                "Field({field_name}) value type is not supported, table:{table}"
                            ),
                        })?;

                    let data_type = try_get_data_type_from_value(field_value)?;

                    if let Some(column_schema) = name_column_map.get(field_name) {
                        ensure_data_type_compatible(
                            table,
                            field_name,
                            false,
                            data_type,
                            column_schema,
                        )?;
                    }
                    let column_schema = build_column_schema(field_name, data_type, false)?;
                    name_column_map.insert(field_name, column_schema);
                }
            }
        }
    }

    // Timestamp column will be the last column
    let timestamp_column_schema = column_schema::Builder::new(
        schema_config.default_timestamp_column_name.clone(),
        DatumKind::Timestamp,
    )
    .is_nullable(false)
    .build()
    .with_context(|| InvalidColumnSchema {
        column_name: schema_config.default_timestamp_column_name.clone(),
    })?;

    // Use (tsid, timestamp) as primary key.
    let tsid_column_schema =
        column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
            .is_nullable(false)
            .build()
            .with_context(|| InvalidColumnSchema {
                column_name: TSID_COLUMN.to_string(),
            })?;

    schema_builder = schema_builder
        .add_key_column(tsid_column_schema)
        .context(BuildTableSchema {})?
        .add_key_column(timestamp_column_schema)
        .context(BuildTableSchema {})?;

    for col in name_column_map.into_values() {
        schema_builder = schema_builder
            .add_normal_column(col)
            .context(BuildTableSchema {})?;
    }

    schema_builder
        .primary_key_indexes(vec![0, 1])
        .build()
        .context(BuildTableSchema {})
}

fn ensure_data_type_compatible(
    table_name: &str,
    column_name: &str,
    is_tag: bool,
    data_type: DatumKind,
    column_schema: &ColumnSchema,
) -> Result<()> {
    ensure!(
        column_schema.is_tag == is_tag,
        InvalidWriteEntry {
            msg: format!(
                "Duplicated column: {column_name} in fields and tags for table: {table_name}",
            ),
        }
    );

    ensure!(
        column_schema.data_type == data_type,
        InvalidWriteEntry {
            msg: format!(
                "Column: {} in table: {} data type is not same, expected: {}, actual: {}",
                column_name, table_name, column_schema.data_type, data_type,
            ),
        }
    );

    Ok(())
}

pub fn try_get_data_type_from_value(value: &PbValue) -> Result<DatumKind> {
    match value {
        PbValue::Float64Value(_) => Ok(DatumKind::Double),
        PbValue::StringValue(_) => Ok(DatumKind::String),
        PbValue::Int64Value(_) => Ok(DatumKind::Int64),
        PbValue::Float32Value(_) => Ok(DatumKind::Float),
        PbValue::Int32Value(_) => Ok(DatumKind::Int32),
        PbValue::Int16Value(_) => Ok(DatumKind::Int16),
        PbValue::Int8Value(_) => Ok(DatumKind::Int8),
        PbValue::BoolValue(_) => Ok(DatumKind::Boolean),
        PbValue::Uint64Value(_) => Ok(DatumKind::UInt64),
        PbValue::Uint32Value(_) => Ok(DatumKind::UInt32),
        PbValue::Uint16Value(_) => Ok(DatumKind::UInt16),
        PbValue::Uint8Value(_) => Ok(DatumKind::UInt8),
        PbValue::TimestampValue(_) => Ok(DatumKind::Timestamp),
        PbValue::VarbinaryValue(_) => Ok(DatumKind::Varbinary),
    }
}

/// A planner wraps the datafusion's logical planner, and delegate sql like
/// select/explain to datafusion's planner.
pub(crate) struct PlannerDelegate<'a, P: MetaProvider> {
    meta_provider: ContextProviderAdapter<'a, P>,
}

impl<'a, P: MetaProvider> PlannerDelegate<'a, P> {
    pub(crate) fn new(meta_provider: ContextProviderAdapter<'a, P>) -> Self {
        Self { meta_provider }
    }

    pub(crate) fn sql_statement_to_plan(self, mut sql_stmt: SqlStatement) -> Result<Plan> {
        match sql_stmt {
            // Query statement use datafusion planner
            SqlStatement::Explain { .. } | SqlStatement::Query(_) => {
                normalize_func_name(&mut sql_stmt);
                self.sql_statement_to_datafusion_plan(sql_stmt)
            }
            SqlStatement::Insert { .. } => self.insert_to_plan(sql_stmt),
            _ => UnsupportedStatement.fail(),
        }
    }

    fn sql_statement_to_datafusion_plan(self, sql_stmt: SqlStatement) -> Result<Plan> {
        let df_planner = SqlToRel::new_with_options(&self.meta_provider, DEFAULT_PARSER_OPTS);
        let table_name = parse_table_name_with_standard(&sql_stmt);

        let df_plan = df_planner
            .sql_statement_to_plan(sql_stmt)
            .context(DatafusionPlan)?;
        let df_plan = optimize_plan(&df_plan).context(DatafusionPlan)?;

        debug!("Sql statement to datafusion plan, df_plan:\n{:#?}", df_plan);

        // Get all tables needed in the plan
        let tables = self.meta_provider.try_into_container().context(FindMeta)?;
        Ok(Plan::Query(QueryPlan {
            df_plan,
            table_name,
            tables: Arc::new(tables),
        }))
    }

    fn tsid_column_schema() -> Result<ColumnSchema> {
        column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
            .is_nullable(false)
            .build()
            .context(InvalidColumnSchema {
                column_name: TSID_COLUMN,
            })
    }

    fn create_table_schema(
        columns: &[Ident],
        primary_key_columns: &[Ident],
        mut columns_by_name: HashMap<&str, ColumnSchema>,
        column_idxs_by_name: HashMap<&str, usize>,
    ) -> Result<Schema> {
        assert_eq!(columns_by_name.len(), column_idxs_by_name.len());

        let mut schema_builder =
            schema::Builder::with_capacity(columns_by_name.len()).auto_increment_column_id(true);

        // Collect the key columns.
        // TODO: Here we put key column in front of all columns, this may change column
        // order defined by users.
        let primary_key_indexes = (0..primary_key_columns.len()).collect();
        schema_builder = schema_builder.primary_key_indexes(primary_key_indexes);
        for key_col in primary_key_columns {
            let col_name = key_col.value.as_str();
            let col = columns_by_name
                .remove(col_name)
                .context(UndefinedColumnInPrimaryKey { name: col_name })?;
            schema_builder = schema_builder
                .add_key_column(col)
                .context(BuildTableSchema)?;
        }

        // Collect the normal columns.
        for normal_col in columns {
            let col_name = normal_col.value.as_str();
            // Only normal columns are kept in the `columns_by_name`.
            if let Some(col) = columns_by_name.remove(col_name) {
                schema_builder = schema_builder
                    .add_normal_column(col)
                    .context(BuildTableSchema)?;
            }
        }

        schema_builder.build().context(BuildTableSchema)
    }

    // Find the primary key columns and ensure at most only one exists.
    fn find_and_ensure_primary_key_columns(
        constraints: &[TableConstraint],
    ) -> Result<Option<Vec<Ident>>> {
        let mut primary_key_columns: Option<Vec<Ident>> = None;
        for constraint in constraints {
            if let TableConstraint::Unique {
                columns,
                is_primary,
                ..
            } = constraint
            {
                if *is_primary {
                    ensure!(
                        primary_key_columns.is_none(),
                        DuplicatePrimaryKey {
                            first: primary_key_columns.unwrap(),
                            second: columns.clone()
                        }
                    );
                    primary_key_columns = Some(columns.clone());
                }
            }
        }

        Ok(primary_key_columns)
    }

    // Find the timestamp column and ensure its valid existence (only one).
    fn find_and_ensure_timestamp_column(
        columns_by_name: &HashMap<&str, ColumnSchema>,
        constraints: &[TableConstraint],
    ) -> Result<Ident> {
        let mut timestamp_column_name = None;
        for constraint in constraints {
            if let TableConstraint::Unique { columns, .. } = constraint {
                if parser::is_timestamp_key_constraint(constraint) {
                    // Only one timestamp key constraint
                    ensure!(timestamp_column_name.is_none(), InvalidTimestampKey);
                    // Only one column in constraint
                    ensure!(columns.len() == 1, InvalidTimestampKey);
                    let timestamp_ident = columns[0].clone();

                    let timestamp_column = columns_by_name
                        .get(timestamp_ident.value.as_str())
                        .context(TimestampColumnNotFound {
                            name: &timestamp_ident.value,
                        })?;

                    // Ensure the timestamp key's type is timestamp.
                    ensure!(
                        timestamp_column.data_type == DatumKind::Timestamp,
                        InvalidTimestampKey
                    );
                    // Ensure the timestamp key is not a tag.
                    ensure!(
                        !timestamp_column.is_tag,
                        TimestampKeyTag {
                            name: &timestamp_ident.value,
                        }
                    );

                    timestamp_column_name = Some(timestamp_ident);
                }
            }
        }

        timestamp_column_name.context(RequireTimestamp)
    }

    fn create_table_to_plan(&self, stmt: CreateTable) -> Result<Plan> {
        ensure!(!stmt.table_name.is_empty(), CreateTableNameEmpty);

        debug!("Create table to plan, stmt:{:?}", stmt);

        // Build all column schemas.
        let mut columns_by_name = stmt
            .columns
            .iter()
            .map(|col| Ok((col.name.value.as_str(), parse_column(col)?)))
            .collect::<Result<HashMap<_, _>>>()?;

        let mut column_idxs_by_name: HashMap<_, _> = stmt
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.name.value.as_str(), idx))
            .collect();

        // Tsid column is a reserved column.
        ensure!(
            !columns_by_name.contains_key(TSID_COLUMN),
            ColumnNameReserved {
                name: TSID_COLUMN.to_string(),
            }
        );

        let timestamp_column =
            Self::find_and_ensure_timestamp_column(&columns_by_name, &stmt.constraints)?;
        let tsid_column = Ident::with_quote(DEFAULT_QUOTE_CHAR, TSID_COLUMN);
        let mut columns: Vec<_> = stmt.columns.iter().map(|col| col.name.clone()).collect();

        let mut add_tsid_column = || {
            columns_by_name.insert(TSID_COLUMN, Self::tsid_column_schema()?);
            column_idxs_by_name.insert(TSID_COLUMN, columns.len());
            columns.push(tsid_column.clone());
            Ok(())
        };
        let primary_key_columns =
            match Self::find_and_ensure_primary_key_columns(&stmt.constraints)? {
                Some(primary_key_columns) => {
                    // Ensure the primary key is defined already.
                    for col in &primary_key_columns {
                        let col_name = &col.value;
                        if col_name == TSID_COLUMN {
                            // tsid column is a reserved column which can't be
                            // defined by user, so let's add it manually.
                            add_tsid_column()?;
                        }
                    }

                    primary_key_columns
                }
                None => {
                    // No primary key is provided explicitly, so let's use `(tsid,
                    // timestamp_key)` as the default primary key.
                    add_tsid_column()?;

                    vec![tsid_column, timestamp_column]
                }
            };
        let table_schema = Self::create_table_schema(
            &columns,
            &primary_key_columns,
            columns_by_name,
            column_idxs_by_name,
        )?;

        let partition_info = match stmt.partition {
            Some(p) => Some(PartitionParser::parse(p)?),
            None => None,
        };

        let options = parse_options(stmt.options)?;

        // ensure default value options are valid
        ensure_column_default_value_valid(table_schema.columns(), &self.meta_provider)?;

        // TODO: support create table on other catalog/schema
        let table_name = stmt.table_name.to_string();
        let table_ref = get_table_ref(&table_name);
        let table = table_ref.table().to_string();

        let plan = CreateTablePlan {
            engine: stmt.engine,
            if_not_exists: stmt.if_not_exists,
            table,
            table_schema,
            options,
            // TODO: sql parse supports `partition by` syntax.
            partition_info,
        };

        debug!("Create table to plan, plan:{:?}", plan);

        Ok(Plan::Create(plan))
    }

    fn drop_table_to_plan(&self, stmt: DropTable) -> Result<Plan> {
        debug!("Drop table to plan, stmt:{:?}", stmt);

        let (table_name, partition_info) =
            if let Some(table) = self.find_table(&stmt.table_name.to_string())? {
                let table_name = table.name().to_string();
                let partition_info = table.partition_info();
                (table_name, partition_info)
            } else if stmt.if_exists {
                let table_name = stmt.table_name.to_string();
                (table_name, None)
            } else {
                return TableNotFound {
                    name: stmt.table_name.to_string(),
                }
                .fail();
            };

        let plan = DropTablePlan {
            engine: stmt.engine,
            if_exists: stmt.if_exists,
            table: table_name,
            partition_info,
        };
        debug!("Drop table to plan, plan:{:?}", plan);

        Ok(Plan::Drop(plan))
    }

    fn describe_table_to_plan(&self, stmt: DescribeTable) -> Result<Plan> {
        let table_name = stmt.table_name.to_string();

        let table = self
            .find_table(&table_name)?
            .context(TableNotFound { name: table_name })?;

        Ok(Plan::Describe(DescribeTablePlan { table }))
    }

    // REQUIRE: SqlStatement must be INSERT stmt
    fn insert_to_plan(&self, sql_stmt: SqlStatement) -> Result<Plan> {
        match sql_stmt {
            SqlStatement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let table_name = TableName::from(table_name).to_string();

                let table = self
                    .find_table(&table_name)?
                    .context(TableNotFound { name: table_name })?;

                let schema = table.schema();
                // Column name and its index in insert stmt: {column name} => index
                let column_names_idx: HashMap<_, _> = columns
                    .iter()
                    .enumerate()
                    .map(|(idx, ident)| (&ident.value, idx))
                    .collect();
                ensure!(
                    column_names_idx.len() == columns.len(),
                    InsertDuplicateColumns
                );

                validate_insert_stmt(table.name(), &schema, &column_names_idx)?;

                let df_fields = schema
                    .columns()
                    .iter()
                    .map(|column_schema| {
                        DFField::new_unqualified(
                            &column_schema.name,
                            column_schema.data_type.to_arrow_data_type(),
                            column_schema.is_nullable,
                        )
                    })
                    .collect::<Vec<_>>();
                let df_schema = DFSchema::new_with_metadata(df_fields, HashMap::new())
                    .context(CreateDatafusionSchema)?;
                let df_planner =
                    SqlToRel::new_with_options(&self.meta_provider, DEFAULT_PARSER_OPTS);

                // Index in insert values stmt of each column in table schema
                let mut column_index_in_insert = Vec::with_capacity(schema.num_columns());
                // Column index in schema to its default-value-expr
                let mut default_value_map = BTreeMap::new();

                // Check all not null columns are provided in stmt, also init
                // `column_index_in_insert`
                for (idx, column) in schema.columns().iter().enumerate() {
                    if let Some(tsid_idx) = schema.index_of_tsid() {
                        if idx == tsid_idx {
                            // This is a tsid column.
                            column_index_in_insert.push(InsertMode::Auto);
                            continue;
                        }
                    }
                    match column_names_idx.get(&column.name) {
                        Some(idx_in_insert) => {
                            // This column in schema is also in insert stmt
                            column_index_in_insert.push(InsertMode::Direct(*idx_in_insert));
                        }
                        None => {
                            // This column in schema is not in insert stmt
                            if let Some(expr) = &column.default_value {
                                let expr = df_planner
                                    .sql_to_expr(
                                        expr.clone(),
                                        &df_schema,
                                        &mut PlannerContext::new(),
                                    )
                                    .context(DatafusionExpr)?;

                                default_value_map.insert(idx, expr);
                                column_index_in_insert.push(InsertMode::Auto);
                            } else if column.is_nullable {
                                column_index_in_insert.push(InsertMode::Null);
                            } else {
                                // Column can not be null and input does not contains that column
                                return InsertMissingColumn {
                                    table: table.name(),
                                    column: &column.name,
                                }
                                .fail();
                            }
                        }
                    }
                }

                let source = build_insert_source(
                    schema,
                    source,
                    column_index_in_insert,
                    self.meta_provider.clone_with_cleared_cache(),
                )?;

                Ok(Plan::Insert(InsertPlan {
                    table,
                    source,
                    default_value_map,
                }))
            }
            // We already known this stmt is a INSERT stmt
            _ => unreachable!(),
        }
    }

    fn alter_modify_setting_to_plan(&self, stmt: AlterModifySetting) -> Result<Plan> {
        let table_name = stmt.table_name.to_string();

        let table = self
            .find_table(&table_name)?
            .context(TableNotFound { name: table_name })?;
        let plan = AlterTablePlan {
            table,
            operations: AlterTableOperation::ModifySetting(parse_options(stmt.options)?),
        };
        Ok(Plan::AlterTable(plan))
    }

    fn alter_add_column_to_plan(&self, stmt: AlterAddColumn) -> Result<Plan> {
        let table_name = stmt.table_name.to_string();
        let table = self
            .find_table(&table_name)?
            .context(TableNotFound { name: table_name })?;
        let plan = AlterTablePlan {
            table,
            operations: AlterTableOperation::AddColumn(parse_columns(stmt.columns)?),
        };
        Ok(Plan::AlterTable(plan))
    }

    fn exists_table_to_plan(&self, stmt: ExistsTable) -> Result<Plan> {
        let table = self.find_table(&stmt.table_name.to_string())?;
        match table {
            Some(_) => Ok(Plan::Exists(ExistsTablePlan { exists: true })),
            None => Ok(Plan::Exists(ExistsTablePlan { exists: false })),
        }
    }

    fn show_create_to_plan(&self, show_create: ShowCreate) -> Result<Plan> {
        let table_name = show_create.table_name.to_string();
        let table = self
            .find_table(&table_name)?
            .context(TableNotFound { name: table_name })?;
        let plan = ShowCreatePlan {
            table,
            obj_type: show_create.obj_type,
        };
        Ok(Plan::Show(ShowPlan::ShowCreatePlan(plan)))
    }

    fn show_tables_to_plan(&self, show_tables: ShowTables) -> Result<Plan> {
        let plan = ShowTablesPlan {
            pattern: show_tables.pattern,
            query_type: QueryType::Sql,
        };
        Ok(Plan::Show(ShowPlan::ShowTablesPlan(plan)))
    }

    fn show_databases_to_plan(&self) -> Result<Plan> {
        Ok(Plan::Show(ShowPlan::ShowDatabase))
    }

    pub(crate) fn find_table(&self, table_name: &str) -> Result<Option<TableRef>> {
        let table_ref = get_table_ref(table_name);
        let resolved_table = self
            .meta_provider
            .table(table_ref)
            .context(MetaProviderFindTable)?;
        Ok(resolved_table.map(|resolved| resolved.table))
    }
}

// Datafusion only support lower-case function name when
// `enable_ident_normalization` is `true`, but we want to
// function case-insensitive, so add this normalization.
fn normalize_func_name(sql_stmt: &mut SqlStatement) {
    let normalize_expr = |expr: &mut SqlExpr| {
        if let SqlExpr::Function(ref mut func) = expr {
            for ident in &mut func.name.0 {
                ident.value = ident.value.to_lowercase();
            }
        }
    };

    visit_statements_mut(sql_stmt, |stmt| {
        if let SqlStatement::Query(q) = stmt {
            if let SetExpr::Select(select) = q.body.as_mut() {
                for projection in &mut select.projection {
                    match projection {
                        SelectItem::UnnamedExpr(ref mut expr) => normalize_expr(expr),
                        SelectItem::ExprWithAlias { ref mut expr, .. } => normalize_expr(expr),
                        SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {}
                    }
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

#[derive(Debug)]
pub enum InsertMode {
    // Insert the value in expr with given index directly.
    Direct(usize),
    // No value provided, insert a null.
    Null,
    // Auto generated column, just temporary fill by default value, the real value will
    // be filled by interpreter.
    Auto,
}

/// Parse [Datum] from the [Expr].
fn parse_data_value_from_expr(data_type: DatumKind, expr: &mut Expr) -> Result<Datum> {
    match expr {
        Expr::Value(value) => {
            Datum::try_from_sql_value(&data_type, mem::replace(value, Value::Null))
                .context(InsertConvertValue)
        }
        Expr::UnaryOp {
            op,
            expr: child_expr,
        } => {
            let is_negative = match op {
                UnaryOperator::Minus => true,
                UnaryOperator::Plus => false,
                _ => InsertExprNotValue {
                    source_expr: Expr::UnaryOp {
                        op: *op,
                        expr: child_expr.clone(),
                    },
                }
                .fail()?,
            };
            let mut datum = parse_data_value_from_expr(data_type, child_expr)?;
            if is_negative {
                datum = datum
                    .to_negative()
                    .with_context(|| InsertExprNoNegativeValue {
                        source_expr: expr.clone(),
                    })?;
            }
            Ok(datum)
        }
        _ => InsertExprNotValue {
            source_expr: expr.clone(),
        }
        .fail(),
    }
}

/// Build InsertSource
fn build_insert_source<P: MetaProvider>(
    schema: Schema,
    source: Box<Query>,
    column_index_in_insert: Vec<InsertMode>,
    meta_provider: ContextProviderAdapter<P>,
) -> Result<InsertSource> {
    // Build row group by schema
    match *source.clone().body {
        SetExpr::Values(Values {
            explicit_row: _,
            rows: expr_rows,
        }) => {
            let mut rows = Vec::with_capacity(expr_rows.len());
            for mut exprs in expr_rows {
                // Try to build row
                let mut row_builder = RowBuilder::new(&schema);

                // For each column in schema, append datum into row builder
                for (index_opt, column_schema) in
                    column_index_in_insert.iter().zip(schema.columns())
                {
                    match index_opt {
                        InsertMode::Direct(index) => {
                            let exprs_len = exprs.len();
                            let expr = exprs.get_mut(*index).context(InsertValuesNotEnough {
                                len: exprs_len,
                                index: *index,
                            })?;

                            let datum = parse_data_value_from_expr(column_schema.data_type, expr)?;
                            row_builder = row_builder.append_datum(datum).context(BuildRow)?;
                        }
                        InsertMode::Null => {
                            // This is a null column
                            row_builder =
                                row_builder.append_datum(Datum::Null).context(BuildRow)?;
                        }
                        InsertMode::Auto => {
                            // This is an auto generated column, fill by default value.
                            let kind = &column_schema.data_type;
                            row_builder = row_builder
                                .append_datum(Datum::empty(kind))
                                .context(BuildRow)?;
                        }
                    }
                }

                // Finish this row and append into row group
                let row = row_builder.finish().context(BuildRow)?;
                rows.push(row);
            }

            // Build the whole row group
            Ok(InsertSource::Values {
                row_group: RowGroup::new_unchecked(schema, rows),
            })
        }
        SetExpr::Select(_) => {
            let mut select_stmt = SqlStatement::Query(source);
            normalize_func_name(&mut select_stmt);
            let select_table_name = parse_table_name_with_standard(&select_stmt);

            let df_planner = SqlToRel::new_with_options(&meta_provider, DEFAULT_PARSER_OPTS);

            let select_df_plan = df_planner
                .sql_statement_to_plan(select_stmt)
                .context(DatafusionPlan)?;
            let select_df_plan = optimize_plan(&select_df_plan).context(DatafusionPlan)?;

            // Get all tables needed in the plan
            let tables = meta_provider.try_into_container().context(FindMeta)?;
            let query = QueryPlan {
                df_plan: select_df_plan,
                table_name: select_table_name,
                tables: Arc::new(tables),
            };
            Ok(InsertSource::Select {
                query,
                column_index_in_insert,
            })
        }
        _ => InsertSourceBodyNotSet.fail(),
    }
}

#[inline]
fn is_tsid_column(name: &str) -> bool {
    name == TSID_COLUMN
}

fn validate_insert_stmt(
    table_name: &str,
    schema: &Schema,
    column_name_idx: &HashMap<&String, usize>,
) -> Result<()> {
    for name in column_name_idx.keys() {
        if is_tsid_column(name.as_str()) {
            return Err(Error::InsertReservedColumn {
                table: table_name.to_string(),
                column: name.to_string(),
            });
        }
        schema.column_with_name(name).context(UnknownInsertColumn {
            name: name.to_string(),
        })?;
    }

    Ok(())
}

fn parse_options(options: Vec<SqlOption>) -> Result<HashMap<String, String>> {
    let mut parsed_options = HashMap::with_capacity(options.len());

    for option in options {
        let key = option.name.value;
        if let Some(value) = parse_for_option(option.value)? {
            parsed_options.insert(key, value);
        };
    }

    Ok(parsed_options)
}

/// Parse value for sql option.
pub fn parse_for_option(value: Value) -> Result<Option<String>> {
    let value_opt = match value {
        Value::Number(n, _long) => Some(n),
        Value::SingleQuotedString(v) | Value::DoubleQuotedString(v) | Value::UnQuotedString(v) => {
            Some(v)
        }
        Value::NationalStringLiteral(v) | Value::HexStringLiteral(v) => {
            return UnsupportedOption { value: v }.fail();
        }
        Value::Boolean(v) => Some(v.to_string()),
        // Ignore this option if value is null.
        Value::Null
        | Value::Placeholder(_)
        | Value::EscapedStringLiteral(_)
        | Value::SingleQuotedByteStringLiteral(_)
        | Value::DoubleQuotedByteStringLiteral(_)
        | Value::RawStringLiteral(_)
        | Value::DollarQuotedString(_) => None,
    };

    Ok(value_opt)
}

fn parse_columns(cols: Vec<ColumnDef>) -> Result<Vec<ColumnSchema>> {
    let mut parsed_columns = Vec::with_capacity(cols.len());

    // Build all column schemas.
    for col in &cols {
        parsed_columns.push(parse_column(col)?);
    }

    Ok(parsed_columns)
}

fn parse_column(col: &ColumnDef) -> Result<ColumnSchema> {
    let mut data_type = DatumKind::try_from(&col.data_type).context(UnsupportedDataType)?;

    // Process column options
    let mut is_nullable = true; // A column is nullable by default.
    let mut is_tag = false;
    let mut is_dictionary = false;
    let mut is_unsign = false;
    let mut comment = String::new();
    let mut default_value = None;
    for option_def in &col.options {
        if matches!(option_def.option, ColumnOption::NotNull) {
            is_nullable = false;
        } else if parser::is_tag_column(&option_def.option) {
            is_tag = true;
        } else if parser::is_dictionary_column(&option_def.option) {
            is_dictionary = true;
        } else if parser::is_unsign_column(&option_def.option) {
            is_unsign = true;
        } else if let Some(default_value_expr) = parser::get_default_value(&option_def.option) {
            default_value = Some(default_value_expr);
        } else if let Some(v) = parser::get_column_comment(&option_def.option) {
            comment = v;
        }
    }

    if is_unsign {
        data_type = data_type
            .unsign_kind()
            .context(InvalidUnsignType { kind: data_type })?;
    }

    let builder = column_schema::Builder::new(col.name.value.clone(), data_type)
        .is_nullable(is_nullable)
        .is_tag(is_tag)
        .is_dictionary(is_dictionary)
        .comment(comment)
        .default_value(default_value);

    builder.build().context(InvalidColumnSchema {
        column_name: &col.name.value,
    })
}

// Ensure default value option of columns are valid.
fn ensure_column_default_value_valid<P: MetaProvider>(
    columns: &[ColumnSchema],
    meta_provider: &ContextProviderAdapter<'_, P>,
) -> Result<()> {
    let df_planner = SqlToRel::new_with_options(meta_provider, DEFAULT_PARSER_OPTS);
    let mut df_schema = DFSchema::empty();
    let mut arrow_schema = ArrowSchema::empty();

    for column_def in columns.iter() {
        if let Some(expr) = &column_def.default_value {
            let df_logical_expr = df_planner
                .sql_to_expr(expr.clone(), &df_schema, &mut PlannerContext::new())
                .context(DatafusionExpr)?;

            // Create physical expr
            let execution_props = ExecutionProps::default();
            let df_schema_ref = Arc::new(df_schema.clone());
            let simplifier = ExprSimplifier::new(
                SimplifyContext::new(&execution_props).with_schema(df_schema_ref.clone()),
            );
            let df_logical_expr = simplifier
                .coerce(df_logical_expr, df_schema_ref.clone())
                .context(DatafusionExpr)?;

            let df_logical_expr = simplifier
                .simplify(df_logical_expr)
                .context(DatafusionExpr)?;

            let physical_expr = create_physical_expr(
                &df_logical_expr,
                &df_schema,
                &arrow_schema,
                &execution_props,
            )
            .context(DatafusionExpr)?;

            let from_type = physical_expr
                .data_type(&arrow_schema)
                .context(DatafusionDataType)?;
            ensure! {
                can_cast_types(&from_type, &column_def.data_type.into()),
                InvalidDefaultValueCoercion::<Expr, ArrowDataType, ArrowDataType>{
                    expr: expr.clone(),
                    from: from_type,
                    to: column_def.data_type.into(),
                },
            }
        }

        // Add evaluated column to schema
        let new_arrow_field = ArrowField::from(column_def);
        let to_merged_df_schema = &DFSchema::new_with_metadata(
            vec![DFField::from(new_arrow_field.clone())],
            HashMap::new(),
        )
        .context(CreateDatafusionSchema)?;
        df_schema.merge(to_merged_df_schema);
        arrow_schema =
            ArrowSchema::try_merge(vec![arrow_schema, ArrowSchema::new(vec![new_arrow_field])])
                .context(MergeArrowSchema)?;
    }

    Ok(())
}

// Workaround for TableReference::from(&str)
// it will always convert table to lowercase when not quoted
// TODO: support catalog/schema
pub fn get_table_ref(table_name: &str) -> TableReference {
    TableReference::from(ResolvedTableReference {
        catalog: Cow::from(DEFAULT_CATALOG.as_str()),
        schema: Cow::from(DEFAULT_SCHEMA),
        table: Cow::from(table_name),
    })
}

#[cfg(test)]
pub mod tests {
    use datafusion::{
        common::tree_node::{TreeNode, TreeNodeVisitor, VisitRecursion},
        datasource::source_as_provider,
        logical_expr::LogicalPlan,
    };
    use horaedbproto::storage::{
        value, Field, FieldGroup, Tag, Value as PbValue, WriteSeriesEntry,
    };
    use partition_table_engine::scan_builder::PartitionedTableScanBuilder;
    use sqlparser::ast::Value;
    use table_engine::provider::TableProviderAdapter;

    use super::*;
    use crate::{
        parser::Parser,
        planner::{parse_for_option, Planner},
        tests::MockMetaProvider,
    };

    fn quick_test(sql: &str, expected: &str) -> Result<()> {
        let plan = sql_to_logical_plan(sql)?;
        assert_eq!(format!("{plan:#?}"), expected);
        Ok(())
    }

    pub fn sql_to_logical_plan(sql: &str) -> Result<Plan> {
        let dyn_config = DynamicConfig::default();
        sql_to_logical_plan_with_config(sql, &dyn_config)
    }

    fn sql_to_logical_plan_with_config(sql: &str, dyn_config: &DynamicConfig) -> Result<Plan> {
        let mock = MockMetaProvider::default();
        let planner = build_planner(&mock, dyn_config);
        let mut statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        planner.statement_to_plan(statements.remove(0))
    }

    fn build_planner<'a>(
        provider: &'a MockMetaProvider,
        dyn_config: &'a DynamicConfig,
    ) -> Planner<'a, MockMetaProvider> {
        Planner::new(provider, RequestId::next_id(), 1, dyn_config)
    }

    #[test]
    pub fn test_parse_for_option() {
        let test_string = "aa".to_string();
        // input is_err expected
        let test_cases = vec![
            (
                Value::Number("1000".to_string(), false),
                false,
                Some("1000".to_string()),
            ),
            (
                Value::SingleQuotedString(test_string.clone()),
                false,
                Some(test_string.clone()),
            ),
            (
                Value::DoubleQuotedString(test_string.clone()),
                false,
                Some(test_string.clone()),
            ),
            (
                Value::NationalStringLiteral(test_string.clone()),
                true,
                None,
            ),
            (Value::HexStringLiteral(test_string), true, None),
            (Value::Boolean(true), false, Some("true".to_string())),
            (Value::Null, false, None),
        ];

        for (input, is_err, expected) in test_cases {
            let ret = parse_for_option(input);
            assert_eq!(ret.is_err(), is_err);
            if !is_err {
                assert_eq!(ret.unwrap(), expected);
            }
        }
    }

    #[test]
    fn test_create_statement_to_plan() {
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 string tag not null,
                                                      ts timestamp not null,
                                                      c3 string,
                                                      c4 uint32 Default 0,
                                                      c5 uint32 Default 1+1,
                                                      c6 String Default c3,
                                                      timestamp key(ts),primary key(c1, ts)) \
        ENGINE=Analytic WITH (ttl='70d',update_mode='overwrite',arena_block_size='1KB')";
        quick_test(
            sql,
            r#"Create(
    CreateTablePlan {
        engine: "Analytic",
        if_not_exists: true,
        table: "t",
        table_schema: Schema {
            timestamp_index: 1,
            tsid_index: None,
            column_schemas: ColumnSchemas {
                columns: [
                    ColumnSchema {
                        id: 1,
                        name: "c1",
                        data_type: String,
                        is_nullable: false,
                        is_tag: true,
                        is_dictionary: false,
                        comment: "",
                        escaped_name: "c1",
                        default_value: None,
                    },
                    ColumnSchema {
                        id: 2,
                        name: "ts",
                        data_type: Timestamp,
                        is_nullable: false,
                        is_tag: false,
                        is_dictionary: false,
                        comment: "",
                        escaped_name: "ts",
                        default_value: None,
                    },
                    ColumnSchema {
                        id: 3,
                        name: "c3",
                        data_type: String,
                        is_nullable: true,
                        is_tag: false,
                        is_dictionary: false,
                        comment: "",
                        escaped_name: "c3",
                        default_value: None,
                    },
                    ColumnSchema {
                        id: 4,
                        name: "c4",
                        data_type: UInt32,
                        is_nullable: true,
                        is_tag: false,
                        is_dictionary: false,
                        comment: "",
                        escaped_name: "c4",
                        default_value: Some(
                            Value(
                                Number(
                                    "0",
                                    false,
                                ),
                            ),
                        ),
                    },
                    ColumnSchema {
                        id: 5,
                        name: "c5",
                        data_type: UInt32,
                        is_nullable: true,
                        is_tag: false,
                        is_dictionary: false,
                        comment: "",
                        escaped_name: "c5",
                        default_value: Some(
                            BinaryOp {
                                left: Value(
                                    Number(
                                        "1",
                                        false,
                                    ),
                                ),
                                op: Plus,
                                right: Value(
                                    Number(
                                        "1",
                                        false,
                                    ),
                                ),
                            },
                        ),
                    },
                    ColumnSchema {
                        id: 6,
                        name: "c6",
                        data_type: String,
                        is_nullable: true,
                        is_tag: false,
                        is_dictionary: false,
                        comment: "",
                        escaped_name: "c6",
                        default_value: Some(
                            Identifier(
                                Ident {
                                    value: "c3",
                                    quote_style: None,
                                },
                            ),
                        ),
                    },
                ],
            },
            version: 1,
            primary_key_indexes: [
                0,
                1,
            ],
        },
        options: {
            "arena_block_size": "1KB",
            "ttl": "70d",
            "update_mode": "overwrite",
        },
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_create_table_failed() {
        // HoraeDB can reference other columns in default value expr, but it is mysql
        // style, which only allow it reference columns defined before it.
        // issue: https://github.com/apache/incubator-horaedb/issues/250
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 string tag not null,
                                                      ts timestamp not null,
                                                      c3 uint32 Default c4,
                                                      c4 uint32 Default 0, timestamp key(ts),primary key(c1, ts)) \
        ENGINE=Analytic WITH (ttl='70d',update_mode='overwrite',arena_block_size='1KB')";
        assert!(quick_test(sql, "").is_err());

        // We need cast the data type of default-value-expr to the column data type
        // when default-value-expr is present, so planner will check if this cast is
        // allowed.
        // bool -> timestamp is not allowed in Arrow.
        let sql = "CREATE TABLE IF NOT EXISTS t(c1 string tag not null,
                                                      ts timestamp not null,
                                                      c3 timestamp Default 1 > 2,
                                                      timestamp key(ts),primary key(c1, ts)) \
        ENGINE=Analytic WITH (ttl='70d',update_mode='overwrite',arena_block_size='1KB')";
        assert!(quick_test(sql, "").is_err());
    }

    #[test]
    fn test_query_statement_to_plan() {
        let sql = "select * from test_tablex;";
        assert!(quick_test(sql, "").is_err());

        let sql = "select * from test_table;";
        quick_test(
            sql,
            r"Query(
    QueryPlan {
        df_plan: TableScan: test_table projection=[key1, key2, field1, field2, field3, field4],
    },
)",
        )
        .unwrap();
    }

    #[test]
    fn test_partitioned_table_query_statement_to_plan() {
        let sql = "select * from test_partitioned_table;";
        let dyn_config = DynamicConfig {
            enable_dist_query_push_down: AtomicBool::new(true),
        };
        let plan = sql_to_logical_plan_with_config(sql, &dyn_config).unwrap();
        let df_plan = if let Plan::Query(query_plan) = plan {
            query_plan.df_plan
        } else {
            panic!("It should be query plan");
        };

        // Check if it is successful to build a partitioned table source.
        struct CheckVisitor;

        impl TreeNodeVisitor for CheckVisitor {
            type N = LogicalPlan;

            fn pre_visit(
                &mut self,
                plan: &LogicalPlan,
            ) -> datafusion::error::Result<VisitRecursion> {
                if let LogicalPlan::TableScan(scan) = plan {
                    let provider = source_as_provider(&scan.source).unwrap();
                    if provider
                        .as_any()
                        .downcast_ref::<TableProviderAdapter<PartitionedTableScanBuilder>>()
                        .is_none()
                    {
                        panic!("It should be partitioned table provider");
                    }
                }

                Ok(VisitRecursion::Continue)
            }
        }

        let mut visitor = CheckVisitor;
        df_plan.visit(&mut visitor).unwrap();
    }

    #[test]
    fn test_insert_statement_to_plan() {
        let sql = "INSERT INTO test_tablex(key1, key2, field1, field2) VALUES('tagk', 1638428434000, 100, 'hello3');";
        assert!(quick_test(sql, "").is_err());

        let sql = "INSERT INTO test_table(key1, key2, field1, field2, field3, field4) VALUES('tagk', 1638428434000, 100, 'hello3', '2022-10-10', '12:00:00.456');";
        quick_test(
            sql,
            r#"Insert(
    InsertPlan {
        table: MemoryTable {
            name: "test_table",
            id: TableId(
                100,
            ),
            schema: Schema {
                timestamp_index: 1,
                tsid_index: None,
                column_schemas: ColumnSchemas {
                    columns: [
                        ColumnSchema {
                            id: 1,
                            name: "key1",
                            data_type: Varbinary,
                            is_nullable: false,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "key1",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 2,
                            name: "key2",
                            data_type: Timestamp,
                            is_nullable: false,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "key2",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 3,
                            name: "field1",
                            data_type: Double,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field1",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 4,
                            name: "field2",
                            data_type: String,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field2",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 5,
                            name: "field3",
                            data_type: Date,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field3",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 6,
                            name: "field4",
                            data_type: Time,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field4",
                            default_value: None,
                        },
                    ],
                },
                version: 1,
                primary_key_indexes: [
                    0,
                    1,
                ],
            },
        },
        source: Values {
            row_group: RowGroup {
                schema: Schema {
                    timestamp_index: 1,
                    tsid_index: None,
                    column_schemas: ColumnSchemas {
                        columns: [
                            ColumnSchema {
                                id: 1,
                                name: "key1",
                                data_type: Varbinary,
                                is_nullable: false,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "key1",
                                default_value: None,
                            },
                            ColumnSchema {
                                id: 2,
                                name: "key2",
                                data_type: Timestamp,
                                is_nullable: false,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "key2",
                                default_value: None,
                            },
                            ColumnSchema {
                                id: 3,
                                name: "field1",
                                data_type: Double,
                                is_nullable: true,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "field1",
                                default_value: None,
                            },
                            ColumnSchema {
                                id: 4,
                                name: "field2",
                                data_type: String,
                                is_nullable: true,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "field2",
                                default_value: None,
                            },
                            ColumnSchema {
                                id: 5,
                                name: "field3",
                                data_type: Date,
                                is_nullable: true,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "field3",
                                default_value: None,
                            },
                            ColumnSchema {
                                id: 6,
                                name: "field4",
                                data_type: Time,
                                is_nullable: true,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "field4",
                                default_value: None,
                            },
                        ],
                    },
                    version: 1,
                    primary_key_indexes: [
                        0,
                        1,
                    ],
                },
                rows: [
                    Row {
                        cols: [
                            Varbinary(
                                b"tagk",
                            ),
                            Timestamp(
                                Timestamp(
                                    1638428434000,
                                ),
                            ),
                            Double(
                                100.0,
                            ),
                            String(
                                StringBytes(
                                    b"hello3",
                                ),
                            ),
                            Date(
                                19275,
                            ),
                            Time(
                                43200456000000,
                            ),
                        ],
                    },
                ],
            },
        },
        default_value_map: {},
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_drop_statement_to_plan() {
        let sql = "drop table test_table;";
        quick_test(
            sql,
            r#"Drop(
    DropTablePlan {
        engine: "Analytic",
        if_exists: false,
        table: "test_table",
        partition_info: None,
    },
)"#,
        )
        .unwrap();

        let sql = "drop table test_tablex;";
        assert!(quick_test(sql, "",).is_err());

        let sql = "drop table if exists test_tablex;";
        quick_test(
            sql,
            r#"Drop(
    DropTablePlan {
        engine: "Analytic",
        if_exists: true,
        table: "test_tablex",
        partition_info: None,
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_desc_statement_to_plan() {
        let sql = "desc test_tablex;";
        assert!(quick_test(sql, "").is_err());

        let sql = "desc test_table;";
        quick_test(
            sql,
            r#"Describe(
    DescribeTablePlan {
        table: MemoryTable {
            name: "test_table",
            id: TableId(
                100,
            ),
            schema: Schema {
                timestamp_index: 1,
                tsid_index: None,
                column_schemas: ColumnSchemas {
                    columns: [
                        ColumnSchema {
                            id: 1,
                            name: "key1",
                            data_type: Varbinary,
                            is_nullable: false,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "key1",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 2,
                            name: "key2",
                            data_type: Timestamp,
                            is_nullable: false,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "key2",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 3,
                            name: "field1",
                            data_type: Double,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field1",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 4,
                            name: "field2",
                            data_type: String,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field2",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 5,
                            name: "field3",
                            data_type: Date,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field3",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 6,
                            name: "field4",
                            data_type: Time,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field4",
                            default_value: None,
                        },
                    ],
                },
                version: 1,
                primary_key_indexes: [
                    0,
                    1,
                ],
            },
        },
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_alter_column_with_dictionary_encode() {
        let sql = "ALTER TABLE test_table ADD column dic string dictionary;";
        quick_test(
            sql,
            r#"AlterTable(
    AlterTablePlan {
        table: MemoryTable {
            name: "test_table",
            id: TableId(
                100,
            ),
            schema: Schema {
                timestamp_index: 1,
                tsid_index: None,
                column_schemas: ColumnSchemas {
                    columns: [
                        ColumnSchema {
                            id: 1,
                            name: "key1",
                            data_type: Varbinary,
                            is_nullable: false,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "key1",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 2,
                            name: "key2",
                            data_type: Timestamp,
                            is_nullable: false,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "key2",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 3,
                            name: "field1",
                            data_type: Double,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field1",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 4,
                            name: "field2",
                            data_type: String,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field2",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 5,
                            name: "field3",
                            data_type: Date,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field3",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 6,
                            name: "field4",
                            data_type: Time,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field4",
                            default_value: None,
                        },
                    ],
                },
                version: 1,
                primary_key_indexes: [
                    0,
                    1,
                ],
            },
        },
        operations: AddColumn(
            [
                ColumnSchema {
                    id: 0,
                    name: "dic",
                    data_type: String,
                    is_nullable: true,
                    is_tag: false,
                    is_dictionary: true,
                    comment: "",
                    escaped_name: "dic",
                    default_value: None,
                },
            ],
        ),
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_alter_column_statement_to_plan() {
        let sql = "ALTER TABLE test_tablex ADD column add_col string;";
        assert!(quick_test(sql, "").is_err());

        let sql = "ALTER TABLE test_table ADD column add_col string;";
        quick_test(
            sql,
            r#"AlterTable(
    AlterTablePlan {
        table: MemoryTable {
            name: "test_table",
            id: TableId(
                100,
            ),
            schema: Schema {
                timestamp_index: 1,
                tsid_index: None,
                column_schemas: ColumnSchemas {
                    columns: [
                        ColumnSchema {
                            id: 1,
                            name: "key1",
                            data_type: Varbinary,
                            is_nullable: false,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "key1",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 2,
                            name: "key2",
                            data_type: Timestamp,
                            is_nullable: false,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "key2",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 3,
                            name: "field1",
                            data_type: Double,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field1",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 4,
                            name: "field2",
                            data_type: String,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field2",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 5,
                            name: "field3",
                            data_type: Date,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field3",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 6,
                            name: "field4",
                            data_type: Time,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field4",
                            default_value: None,
                        },
                    ],
                },
                version: 1,
                primary_key_indexes: [
                    0,
                    1,
                ],
            },
        },
        operations: AddColumn(
            [
                ColumnSchema {
                    id: 0,
                    name: "add_col",
                    data_type: String,
                    is_nullable: true,
                    is_tag: false,
                    is_dictionary: false,
                    comment: "",
                    escaped_name: "add_col",
                    default_value: None,
                },
            ],
        ),
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_alter_option_statement_to_plan() {
        let sql = "ALTER TABLE test_tablex modify SETTING ttl='9d';";
        assert!(quick_test(sql, "").is_err());

        let sql = "ALTER TABLE test_table modify SETTING ttl='9d';";
        quick_test(
            sql,
            r#"AlterTable(
    AlterTablePlan {
        table: MemoryTable {
            name: "test_table",
            id: TableId(
                100,
            ),
            schema: Schema {
                timestamp_index: 1,
                tsid_index: None,
                column_schemas: ColumnSchemas {
                    columns: [
                        ColumnSchema {
                            id: 1,
                            name: "key1",
                            data_type: Varbinary,
                            is_nullable: false,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "key1",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 2,
                            name: "key2",
                            data_type: Timestamp,
                            is_nullable: false,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "key2",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 3,
                            name: "field1",
                            data_type: Double,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field1",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 4,
                            name: "field2",
                            data_type: String,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field2",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 5,
                            name: "field3",
                            data_type: Date,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field3",
                            default_value: None,
                        },
                        ColumnSchema {
                            id: 6,
                            name: "field4",
                            data_type: Time,
                            is_nullable: true,
                            is_tag: false,
                            is_dictionary: false,
                            comment: "",
                            escaped_name: "field4",
                            default_value: None,
                        },
                    ],
                },
                version: 1,
                primary_key_indexes: [
                    0,
                    1,
                ],
            },
        },
        operations: ModifySetting(
            {
                "ttl": "9d",
            },
        ),
    },
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_show_create_statement_to_plan() {
        let sql = "show create table test_tablex;";
        assert!(quick_test(sql, "").is_err());

        let sql = "show create table test_table;";
        quick_test(
            sql,
            r#"Show(
    ShowCreatePlan(
        ShowCreatePlan {
            table: MemoryTable {
                name: "test_table",
                id: TableId(
                    100,
                ),
                schema: Schema {
                    timestamp_index: 1,
                    tsid_index: None,
                    column_schemas: ColumnSchemas {
                        columns: [
                            ColumnSchema {
                                id: 1,
                                name: "key1",
                                data_type: Varbinary,
                                is_nullable: false,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "key1",
                                default_value: None,
                            },
                            ColumnSchema {
                                id: 2,
                                name: "key2",
                                data_type: Timestamp,
                                is_nullable: false,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "key2",
                                default_value: None,
                            },
                            ColumnSchema {
                                id: 3,
                                name: "field1",
                                data_type: Double,
                                is_nullable: true,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "field1",
                                default_value: None,
                            },
                            ColumnSchema {
                                id: 4,
                                name: "field2",
                                data_type: String,
                                is_nullable: true,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "field2",
                                default_value: None,
                            },
                            ColumnSchema {
                                id: 5,
                                name: "field3",
                                data_type: Date,
                                is_nullable: true,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "field3",
                                default_value: None,
                            },
                            ColumnSchema {
                                id: 6,
                                name: "field4",
                                data_type: Time,
                                is_nullable: true,
                                is_tag: false,
                                is_dictionary: false,
                                comment: "",
                                escaped_name: "field4",
                                default_value: None,
                            },
                        ],
                    },
                    version: 1,
                    primary_key_indexes: [
                        0,
                        1,
                    ],
                },
            },
            obj_type: Table,
        },
    ),
)"#,
        )
        .unwrap();
    }
    #[test]
    fn test_show_tables_statement_to_plan() {
        let sql = "SHOW TABLES;";
        quick_test(
            sql,
            r#"Show(
    ShowTablesPlan(
        ShowTablesPlan {
            pattern: None,
            query_type: Sql,
        },
    ),
)"#,
        )
        .unwrap();
    }

    #[test]
    fn test_show_databases_statement_to_plan() {
        let sql = "SHOW DATABASES;";
        quick_test(
            sql,
            r#"Show(
    ShowDatabase,
)"#,
        )
        .unwrap();
    }

    fn make_test_number_expr(val: &str, sign: Option<bool>) -> Expr {
        let expr_val = Expr::Value(Value::Number(val.to_string(), false));
        match sign {
            Some(true) => Expr::UnaryOp {
                op: UnaryOperator::Plus,
                expr: Box::new(expr_val),
            },
            Some(false) => Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(expr_val),
            },
            None => expr_val,
        }
    }

    #[test]
    fn test_parse_data_value() {
        // normal cases
        let mut cases = vec![
            (make_test_number_expr("10", None), Datum::Int32(10)),
            (make_test_number_expr("10", Some(true)), Datum::Int32(10)),
            (make_test_number_expr("10", Some(false)), Datum::Int32(-10)),
        ];

        // recursive cases
        {
            let source = {
                let num_expr = make_test_number_expr("10", Some(false));
                Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(num_expr),
                }
            };
            let expect = Datum::Int32(10);
            cases.push((source, expect));
        }

        for (mut source, expect) in cases {
            let parsed = parse_data_value_from_expr(expect.kind(), &mut source)
                .expect("Fail to parse data value");
            assert_eq!(parsed, expect);
        }
    }

    const TAG1: &str = "host";
    const TAG2: &str = "idc";
    const FIELD1: &str = "cpu";
    const FIELD2: &str = "memory";
    const FIELD3: &str = "log";
    const FIELD4: &str = "ping_ok";
    const TABLE: &str = "pod_system_table";
    const TIMESTAMP_COLUMN: &str = "custom_timestamp";

    fn make_tag(name_index: u32, val: &str) -> Tag {
        Tag {
            name_index,
            value: Some(PbValue {
                value: Some(value::Value::StringValue(val.to_string())),
            }),
        }
    }

    fn make_field(name_index: u32, val: value::Value) -> Field {
        Field {
            name_index,
            value: Some(PbValue { value: Some(val) }),
        }
    }

    fn generate_write_table_request() -> WriteTableRequest {
        let tag1 = make_tag(0, "test.host");
        let tag2 = make_tag(1, "test.idc");
        let tags = vec![tag1, tag2];

        let field1 = make_field(0, value::Value::Float64Value(100.0));
        let field2 = make_field(1, value::Value::Float64Value(1024.0));
        let field3 = make_field(2, value::Value::StringValue("test log".to_string()));
        let field4 = make_field(3, value::Value::BoolValue(true));

        let field_group1 = FieldGroup {
            timestamp: 1000,
            fields: vec![field1.clone(), field4],
        };
        let field_group2 = FieldGroup {
            timestamp: 2000,
            fields: vec![field1, field2],
        };
        let field_group3 = FieldGroup {
            timestamp: 3000,
            fields: vec![field3],
        };

        let write_entry = WriteSeriesEntry {
            tags,
            field_groups: vec![field_group1, field_group2, field_group3],
        };

        let tag_names = vec![TAG1.to_string(), TAG2.to_string()];
        let field_names = vec![
            FIELD1.to_string(),
            FIELD2.to_string(),
            FIELD3.to_string(),
            FIELD4.to_string(),
        ];

        WriteTableRequest {
            table: TABLE.to_string(),
            tag_names,
            field_names,
            entries: vec![write_entry],
        }
    }

    #[test]
    fn test_build_schema_from_write_table_request() {
        let schema_config = SchemaConfig {
            default_timestamp_column_name: TIMESTAMP_COLUMN.to_string(),
            ..SchemaConfig::default()
        };
        let write_table_request = generate_write_table_request();

        let schema = build_schema_from_write_table_request(&schema_config, &write_table_request);
        assert!(schema.is_ok());

        let schema = schema.unwrap();

        assert_eq!(8, schema.num_columns());
        assert_eq!(2, schema.num_primary_key_columns());
        assert_eq!(TIMESTAMP_COLUMN, schema.timestamp_name());
        let tsid = schema.tsid_column();
        assert!(tsid.is_some());

        let key_columns = schema.key_columns();
        assert_eq!(2, key_columns.len());
        assert_eq!("tsid", key_columns[0].name);
        assert_eq!(TIMESTAMP_COLUMN, key_columns[1].name);

        let columns = schema.normal_columns();
        assert_eq!(6, columns.len());

        // sorted by column names because of btree
        assert_eq!(FIELD1, columns[0].name);
        assert!(!columns[0].is_tag);
        assert_eq!(DatumKind::Double, columns[0].data_type);
        assert_eq!(TAG1, columns[1].name);
        assert!(columns[1].is_tag);
        assert_eq!(DatumKind::String, columns[1].data_type);
        assert_eq!(TAG2, columns[2].name);
        assert!(columns[2].is_tag);
        assert_eq!(DatumKind::String, columns[2].data_type);
        assert_eq!(FIELD3, columns[3].name);
        assert!(!columns[3].is_tag);
        assert_eq!(DatumKind::String, columns[3].data_type);
        assert_eq!(FIELD2, columns[4].name);
        assert!(!columns[4].is_tag);
        assert_eq!(DatumKind::Double, columns[4].data_type);
        assert_eq!(FIELD4, columns[5].name);
        assert!(!columns[5].is_tag);
        assert_eq!(DatumKind::Boolean, columns[5].data_type);

        for column in columns {
            assert!(column.is_nullable);
        }
    }
}
