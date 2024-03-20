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

//! Frontend

use std::{sync::Arc, time::Instant};

use cluster::config::SchemaConfig;
use common_types::request_id::RequestId;
use generic_error::GenericError;
use horaedbproto::{prometheus::Expr as PromExpr, storage::WriteTableRequest};
use influxql_parser::statement::Statement as InfluxqlStatement;
use macros::define_result;
use prom_remote_api::types::Query as PromRemoteQuery;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use sqlparser::ast::{SetExpr, Statement as SqlStatement, TableFactor};
use table_engine::table;

use crate::{
    ast::{Statement, TableName},
    config::DynamicConfig,
    opentsdb::types::{OpentsdbQueryPlan, QueryRequest},
    parser::Parser,
    plan::Plan,
    planner::Planner,
    promql::{ColumnNames, Expr, RemoteQueryPlan},
    provider::MetaProvider,
};

#[derive(Debug, Snafu)]
pub enum Error {
    // Invalid sql is quite common, so we don't provide a backtrace now.
    #[snafu(display("Invalid sql, sql:{}, err:{}", sql, source))]
    InvalidSql {
        sql: String,
        source: sqlparser::parser::ParserError,
    },

    // TODO(yingwen): Should we store stmt here?
    #[snafu(display("Failed to create plan, err:{}", source))]
    CreatePlan { source: crate::planner::Error },

    #[snafu(display("Invalid prom request, err:{}", source))]
    InvalidPromRequest { source: crate::promql::Error },

    #[snafu(display("Expr is not found in prom request.\nBacktrace:\n{}", backtrace))]
    ExprNotFoundInPromRequest { backtrace: Backtrace },

    // invalid sql is quite common, so we don't provide a backtrace now.
    #[snafu(display("invalid influxql, influxql:{}, err:{}", influxql, parse_err))]
    InvalidInfluxql {
        influxql: String,
        parse_err: influxql_parser::common::ParseError,
    },

    #[snafu(display("Failed to build influxql plan, msg:{}, err:{}", msg, source))]
    InfluxqlPlanWithCause { msg: String, source: GenericError },

    #[snafu(display("Failed to build influxql plan, msg:{}", msg))]
    InfluxqlPlan { msg: String },
}

define_result!(Error);

pub type StatementVec = Vec<Statement>;

/// Context used by Frontend
///
/// We can collect metrics and trace info in it instead of using global
/// metrics or trace collector.
pub struct Context {
    /// Id of the query request.
    pub request_id: RequestId,
    /// Parallelism to read table.
    // TODO: seems useless, remove it?
    pub read_parallelism: usize,
    /// Deadline of this request
    pub deadline: Option<Instant>,
}

impl Context {
    pub fn new(request_id: RequestId, deadline: Option<Instant>) -> Self {
        Self {
            request_id,
            deadline,
            read_parallelism: table::DEFAULT_READ_PARALLELISM,
        }
    }
}

/// SQL frontend implementation
///
/// Thought the parser supports using multiple statements in a sql, but
/// this frontend only support planning one statement at a time now
#[derive(Debug)]
pub struct Frontend<P> {
    provider: P,
    dyn_config: Arc<DynamicConfig>,
}

impl<P> Frontend<P> {
    pub fn new(provider: P, dyn_config: Arc<DynamicConfig>) -> Self {
        Self {
            provider,
            dyn_config,
        }
    }

    /// Parse the sql and returns the statements
    pub fn parse_sql(&self, _ctx: &mut Context, sql: &str) -> Result<StatementVec> {
        Parser::parse_sql(sql).context(InvalidSql { sql })
    }

    /// Parse the request and returns the Expr
    pub fn parse_promql(&self, _ctx: &mut Context, expr: Option<PromExpr>) -> Result<Expr> {
        let expr = expr.context(ExprNotFoundInPromRequest)?;
        Expr::try_from(expr).context(InvalidPromRequest)
    }

    /// Parse the sql and returns the statements
    pub fn parse_influxql(&self, _ctx: &Context, influxql: &str) -> Result<Vec<InfluxqlStatement>> {
        match influxql_parser::parse_statements(influxql) {
            Ok(stmts) => Ok(stmts),
            Err(e) => Err(Error::InvalidInfluxql {
                influxql: influxql.to_string(),
                parse_err: e,
            }),
        }
    }
}

impl<P: MetaProvider> Frontend<P> {
    /// Create logical plan for the statement
    pub fn statement_to_plan(&self, ctx: &Context, stmt: Statement) -> Result<Plan> {
        let planner = Planner::new(
            &self.provider,
            ctx.request_id.clone(),
            ctx.read_parallelism,
            self.dyn_config.as_ref(),
        );

        planner.statement_to_plan(stmt).context(CreatePlan)
    }

    /// Experimental native promql support, not used in production yet.
    pub fn promql_expr_to_plan(
        &self,
        ctx: &Context,
        expr: Expr,
    ) -> Result<(Plan, Arc<ColumnNames>)> {
        let planner = Planner::new(
            &self.provider,
            ctx.request_id.clone(),
            ctx.read_parallelism,
            self.dyn_config.as_ref(),
        );

        planner.promql_expr_to_plan(expr).context(CreatePlan)
    }

    /// Prometheus remote query support
    pub fn prom_remote_query_to_plan(
        &self,
        ctx: &Context,
        query: PromRemoteQuery,
    ) -> Result<RemoteQueryPlan> {
        let planner = Planner::new(
            &self.provider,
            ctx.request_id.clone(),
            ctx.read_parallelism,
            self.dyn_config.as_ref(),
        );
        planner.remote_prom_req_to_plan(query).context(CreatePlan)
    }

    pub fn influxql_stmt_to_plan(&self, ctx: &Context, stmt: InfluxqlStatement) -> Result<Plan> {
        let planner = Planner::new(
            &self.provider,
            ctx.request_id.clone(),
            ctx.read_parallelism,
            self.dyn_config.as_ref(),
        );
        planner.influxql_stmt_to_plan(stmt).context(CreatePlan)
    }

    pub fn opentsdb_query_to_plan(
        &self,
        ctx: &Context,
        query: QueryRequest,
    ) -> Result<OpentsdbQueryPlan> {
        let planner = Planner::new(
            &self.provider,
            ctx.request_id.clone(),
            ctx.read_parallelism,
            self.dyn_config.as_ref(),
        );
        planner.opentsdb_query_to_plan(query).context(CreatePlan)
    }

    pub fn write_req_to_plan(
        &self,
        ctx: &Context,
        schema_config: &SchemaConfig,
        write_table: &WriteTableRequest,
    ) -> Result<Plan> {
        let planner = Planner::new(
            &self.provider,
            ctx.request_id.clone(),
            ctx.read_parallelism,
            self.dyn_config.as_ref(),
        );

        planner
            .write_req_to_plan(schema_config, write_table)
            .context(CreatePlan)
    }
}

pub fn parse_table_name_with_standard(sql_statement: &SqlStatement) -> Option<String> {
    match sql_statement.clone() {
        SqlStatement::Insert { table_name, .. } => {
            Some(TableName::from(table_name.clone()).to_string())
        }
        SqlStatement::Explain { statement, .. } => {
            if let SqlStatement::Query(q) = *statement {
                match *q.body {
                    SetExpr::Select(select) => {
                        if select.from.len() != 1 {
                            None
                        } else if let TableFactor::Table { name, .. } = &select.from[0].relation {
                            Some(TableName::from(name.clone()).to_string())
                        } else {
                            None
                        }
                    }
                    // TODO: return unsupported error rather than none.
                    _ => None,
                }
            } else {
                None
            }
        }
        SqlStatement::Query(q) => match *q.body {
            SetExpr::Select(select) => {
                if select.from.len() != 1 {
                    None
                } else if let TableFactor::Table { name, .. } = &select.from[0].relation {
                    Some(TableName::from(name.clone()).to_string())
                } else {
                    None
                }
            }
            _ => None,
        },
        _ => None,
    }
}

pub fn parse_table_name(statements: &StatementVec) -> Option<String> {
    // maybe have empty sql
    if statements.is_empty() {
        return None;
    }
    match &statements[0] {
        Statement::Standard(s) => parse_table_name_with_standard(s),
        Statement::Create(s) => Some(s.table_name.to_string()),
        Statement::Drop(s) => Some(s.table_name.to_string()),
        Statement::Describe(s) => Some(s.table_name.to_string()),
        Statement::AlterModifySetting(s) => Some(s.table_name.to_string()),
        Statement::AlterAddColumn(s) => Some(s.table_name.to_string()),
        Statement::ShowCreate(s) => Some(s.table_name.to_string()),
        Statement::ShowTables(_s) => None,
        Statement::ShowDatabases => None,
        Statement::Exists(s) => Some(s.table_name.to_string()),
    }
}

pub fn parse_table_name_with_sql(sql: &str) -> Result<Option<String>> {
    Ok(parse_table_name(
        &Parser::parse_sql(sql).context(InvalidSql { sql })?,
    ))
}

#[cfg(test)]
mod tests {
    use crate::frontend;

    #[test]
    fn test_parse_table_name() {
        let table = "test_parse_table_name";
        let test_cases = vec![
                            format!("INSERT INTO {table} (t, name, value) VALUES (1651737067000, 'horaedb', 100)"),
                            format!("INSERT INTO `{table}` (t, name, value) VALUES (1651737067000,'horaedb', 100)"),
                            format!("select * from {table}"),
                            format!("select * from `{table}`"),
                            format!("explain select * from {table}"),
                            format!("explain select * from `{table}`"),
                            format!("CREATE TABLE {table} (`name`string TAG,`value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t))"),
                            format!("CREATE TABLE `{table}` (`name`string TAG,`value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t))"),
                            format!("drop table {table}"),
                            format!("drop table `{table}`"),
                            format!("describe table {table}"),
                            format!("describe table `{table}`"),
                            format!("alter table {table} modify setting enable_ttl='false'"),
                            format!("alter table `{table}` modify setting enable_ttl='false'"),
                            format!("alter table {table} add column c1 int"),
                            format!("alter table `{table}` add column c1 int"),
                            format!("show create table {table}"),
                            format!("show create table `{table}`"),
                            format!("exists table {table}"),
                            format!("exists table `{table}`"),
        ];
        for sql in test_cases {
            assert_eq!(
                frontend::parse_table_name_with_sql(&sql).unwrap(),
                Some(table.to_string())
            );
        }
        assert!(frontend::parse_table_name_with_sql("-- just comment")
            .unwrap()
            .is_none());
    }
}
