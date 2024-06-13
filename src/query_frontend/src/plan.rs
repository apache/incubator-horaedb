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

//! Logical plans such as select/insert/update/delete

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    fmt::{Debug, Formatter},
    ops::Bound,
    sync::Arc,
};

use common_types::{column_schema::ColumnSchema, row::RowGroup, schema::Schema, time::TimeRange};
use datafusion::{
    logical_expr::{
        expr::Expr as DfLogicalExpr, logical_plan::LogicalPlan as DataFusionLogicalPlan,
    },
    prelude::Column,
    scalar::ScalarValue,
};
use logger::{debug, warn};
use macros::define_result;
use runtime::Priority;
use snafu::{OptionExt, Snafu};
use table_engine::{partition::PartitionInfo, table::TableRef};

use crate::{
    ast::ShowCreateObject,
    container::TableContainer,
    planner::{get_table_ref, InsertMode},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported alter table operation."))]
    UnsupportedOperation,

    #[snafu(display("Unsupported column data type, err:{}.", source))]
    UnsupportedDataType { source: common_types::datum::Error },

    #[snafu(display("Unsupported column option:{}.", name))]
    UnsupportedColumnOption { name: String },

    #[snafu(display("Alter primary key is not allowed."))]
    AlterPrimaryKey,

    #[snafu(display("Query plan is invalid, msg:{msg}."))]
    InvalidQueryPlan { msg: String },
}

define_result!(Error);

// TODO(yingwen): Custom Debug format
/// Logical plan to be processed by interpreters
#[derive(Debug)]
pub enum Plan {
    /// A SQL SELECT plan or other plans related to query
    Query(QueryPlan),
    // TODO(yingwen): Other sql command
    Insert(InsertPlan),
    /// Create table plan
    Create(CreateTablePlan),
    /// Drop table plan
    Drop(DropTablePlan),
    /// Describe table plan
    Describe(DescribeTablePlan),
    /// Alter table plan
    AlterTable(AlterTablePlan),
    /// Show plan
    Show(ShowPlan),
    /// Exists table
    Exists(ExistsTablePlan),
}

impl Plan {
    pub fn plan_type(&self) -> &str {
        match self {
            Self::Query(_) => "query",
            Self::Insert(_) => "insert",
            Self::Create(_)
            | Self::Drop(_)
            | Self::Describe(_)
            | Self::AlterTable(_)
            | Self::Show(_)
            | Self::Exists(_) => "other",
        }
    }
}

pub struct PriorityContext {
    pub time_range_threshold: u64,
}

pub struct QueryPlan {
    pub df_plan: DataFusionLogicalPlan,
    pub table_name: Option<String>,
    // Contains the TableProviders so we can register the them to ExecutionContext later.
    // Use TableProviderAdapter here so we can get the underlying TableRef and also be
    // able to cast to Arc<dyn TableProvider + Send + Sync>
    pub tables: Arc<TableContainer>,
}

impl QueryPlan {
    fn find_timestamp_column(&self) -> Result<Option<Column>> {
        let table_name = match self.table_name.as_ref() {
            Some(v) => v,
            None => {
                return Ok(None);
            }
        };
        let table_ref = self
            .tables
            .get(get_table_ref(table_name))
            .with_context(|| InvalidQueryPlan {
                msg: format!("Couldn't find table in table container, name:{table_name}"),
            })?;
        let schema = table_ref.table.schema();
        let timestamp_name = schema.timestamp_name();
        Ok(Some(Column::from_name(timestamp_name)))
    }

    /// This function is used to extract time range from the query plan.
    /// It will return max possible time range. For example, if the query
    /// contains no timestmap filter, it will return
    /// `TimeRange::min_to_max()`
    ///
    /// Note: When it timestamp filter evals to false(such as ts < 10 and ts >
    /// 100), it will return None, which means no valid time range for this
    /// query.
    fn extract_time_range(&self) -> Result<Option<TimeRange>> {
        let ts_column = if let Some(v) = self.find_timestamp_column()? {
            v
        } else {
            warn!(
                "Couldn't find time column, plan:{:?}, table_name:{:?}",
                self.df_plan, self.table_name
            );
            return Ok(Some(TimeRange::min_to_max()));
        };
        let time_range = match influxql_query::logical_optimizer::range_predicate::find_time_range(
            &self.df_plan,
            &ts_column,
        ) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Couldn't find time range, plan:{:?}, err:{}",
                    self.df_plan, e
                );
                return Ok(Some(TimeRange::min_to_max()));
            }
        };
        debug!(
            "Extract time range, value:{time_range:?}, plan:{:?}",
            self.df_plan
        );
        let mut start = i64::MIN;
        match time_range.start {
            Bound::Included(inclusive_start) => {
                if let DfLogicalExpr::Literal(ScalarValue::TimestampMillisecond(Some(x), _)) =
                    inclusive_start
                {
                    start = start.max(x);
                }
            }
            Bound::Excluded(exclusive_start) => {
                if let DfLogicalExpr::Literal(ScalarValue::TimestampMillisecond(Some(x), _)) =
                    exclusive_start
                {
                    start = start.max(x + 1);
                }
            }
            Bound::Unbounded => {}
        }
        let mut end = i64::MAX;
        match time_range.end {
            Bound::Included(inclusive_end) => {
                if let DfLogicalExpr::Literal(ScalarValue::TimestampMillisecond(Some(x), _)) =
                    inclusive_end
                {
                    end = end.min(x + 1);
                }
            }
            Bound::Excluded(exclusive_start) => {
                if let DfLogicalExpr::Literal(ScalarValue::TimestampMillisecond(Some(x), _)) =
                    exclusive_start
                {
                    end = end.min(x);
                }
            }
            Bound::Unbounded => {}
        }

        Ok(TimeRange::new(start.into(), end.into()))
    }

    /// Decide the query priority based on the query plan.
    /// When query contains invalid time range, it will return None.
    // TODO: Currently we only consider the time range, consider other factors, such
    // as the number of series, or slow log metrics.
    pub fn decide_query_priority(&self, ctx: PriorityContext) -> Result<Option<Priority>> {
        let threshold = ctx.time_range_threshold;
        let time_range = match self.extract_time_range()? {
            Some(v) => v,
            // When there is no valid time range , we cann't decide its priority.
            None => return Ok(None),
        };
        let is_expensive = if let Some(v) = time_range
            .exclusive_end()
            .as_i64()
            .checked_sub(time_range.inclusive_start().as_i64())
        {
            v as u64 >= threshold
        } else {
            // When overflow, we treat it as expensive query.
            true
        };

        let priority = if is_expensive {
            Priority::Low
        } else {
            Priority::High
        };

        Ok(Some(priority))
    }

    /// When query contains invalid time range such as `[200, 100]`, it will
    /// return None.
    pub fn query_range(&self) -> Result<Option<i64>> {
        self.extract_time_range().map(|time_range| {
            time_range.map(|time_range| {
                time_range
                    .exclusive_end()
                    .as_i64()
                    .checked_sub(time_range.inclusive_start().as_i64())
                    .unwrap_or(i64::MAX)
            })
        })
    }
}

impl Debug for QueryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryPlan")
            .field("df_plan", &self.df_plan)
            .finish()
    }
}

pub struct CreateTablePlan {
    /// Engine
    pub engine: String,
    /// Create table if not exists
    pub if_not_exists: bool,
    /// Table name
    pub table: String,
    /// Table schema
    pub table_schema: Schema,
    /// Table options
    pub options: HashMap<String, String>,
    /// Table partition info
    pub partition_info: Option<PartitionInfo>,
}

impl Debug for CreateTablePlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTablePlan")
            .field("engine", &self.engine)
            .field("if_not_exists", &self.if_not_exists)
            .field("table", &self.table)
            .field("table_schema", &self.table_schema)
            .field(
                "options",
                &self
                    .options
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<BTreeMap<String, String>>(),
            )
            .finish()
    }
}

#[derive(Debug)]
pub struct DropTablePlan {
    /// Engine
    pub engine: String,
    /// If exists
    pub if_exists: bool,
    /// Table name
    pub table: String,
    /// Table partition info
    pub partition_info: Option<PartitionInfo>,
}

#[derive(Debug)]
pub enum InsertSource {
    Values {
        row_group: RowGroup,
    },
    Select {
        column_index_in_insert: Vec<InsertMode>,
        query: QueryPlan,
    },
}

/// Insert logical plan
#[derive(Debug)]
pub struct InsertPlan {
    /// The table to insert
    pub table: TableRef,
    /// RowGroup to insert
    pub source: InsertSource,
    /// Column indexes in schema to its default-value-expr which is used to fill
    /// values
    pub default_value_map: BTreeMap<usize, DfLogicalExpr>,
}

#[derive(Debug)]
pub struct DescribeTablePlan {
    /// The table to describe
    pub table: TableRef,
}

#[derive(Debug)]
pub enum AlterTableOperation {
    /// Add a new column, the column id will be ignored.
    AddColumn(Vec<ColumnSchema>),
    ModifySetting(HashMap<String, String>),
}

#[derive(Debug)]
pub struct AlterTablePlan {
    /// The table to alter.
    pub table: TableRef,
    // TODO(yingwen): Maybe use smallvec.
    pub operations: AlterTableOperation,
}

#[derive(Debug)]
pub struct ShowCreatePlan {
    /// The table to show.
    pub table: TableRef,
    /// The type to show
    pub obj_type: ShowCreateObject,
}

#[derive(Debug, PartialEq, Eq)]
pub enum QueryType {
    Sql,
    InfluxQL,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ShowTablesPlan {
    /// Like pattern
    pub pattern: Option<String>,
    pub query_type: QueryType,
}

#[derive(Debug)]
pub enum ShowPlan {
    /// show create table
    ShowCreatePlan(ShowCreatePlan),
    /// show tables
    ShowTablesPlan(ShowTablesPlan),
    /// show database
    ShowDatabase,
}

#[derive(Debug)]
pub struct ExistsTablePlan {
    pub exists: bool,
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::planner::tests::sql_to_logical_plan;

    #[test]
    fn test_extract_time_range() {
        // key2 is timestamp column
        let testcases = [
            (
                "select * from test_table where key2 > 1 and key2 < 10",
                Some((2, 10)),
            ),
            (
                "select field1 from test_table where key2 > 1 and key2 < 10",
                Some((2, 10)),
            ),
            (
                "select * from test_table where key2 >= 1 and key2 <= 10",
                Some((1, 11)),
            ),
            (
                "select * from test_table where key2 < 1 and key2 > 10",
                None,
            ),
            (
                "select * from test_table where key2 < 1 ",
                Some((i64::MIN, 1))
            ),
            (
                "select * from test_table where key2 > 1 ",
                Some((2, i64::MAX))
            ),
            // date literals
            (
                r#"select * from test_table where key2 >= "2023-11-21 14:12:00+08:00" and key2 < "2023-11-21 14:22:00+08:00" "#,
                Some((1700547120000, 1700547720000))
            ),
             // no timestamp filter
            ("select * from test_table", Some((i64::MIN, i64::MAX))),
            // aggr
            (
                "select key2, sum(field1) from test_table where key2 > 1 and key2 < 10 group by key2",
                Some((2, 10)),
            ),
            // aggr & sort
            (
                "select key2, sum(field1) from test_table where key2 > 1 and key2 < 10 group by key2 order by key2",     Some((2, 10)),
            ),
            // explain
            (
                "explain select * from test_table where key2 > 1 and key2 < 10",
                Some((2, 10)),
            ),
            // analyze
            (
                "explain analyze select * from test_table where key2 > 1 and key2 < 10",
                Some((2, 10)),
            ),
        ];

        for case in testcases {
            let sql = case.0;
            let plan = sql_to_logical_plan(sql).unwrap();
            let plan = match plan {
                Plan::Query(v) => v,
                _ => unreachable!(),
            };
            let expected = case
                .1
                .map(|v| TimeRange::new_unchecked(v.0.into(), v.1.into()));

            assert_eq!(plan.extract_time_range().unwrap(), expected, "sql:{}", sql);
        }
    }
}
