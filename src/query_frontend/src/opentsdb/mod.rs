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

use std::{collections::HashMap, sync::Arc};

use common_types::{
    schema::Schema,
    time::{TimeRange, Timestamp},
};
use datafusion::{
    error::DataFusionError,
    logical_expr::LogicalPlanBuilder,
    optimizer::utils::conjunction,
    prelude::{avg, count, ident, lit, max, min, stddev, sum, Expr},
    sql::{planner::ContextProvider, TableReference},
};
use macros::define_result;
use snafu::{OptionExt, ResultExt, Snafu};

use self::types::{Filter, OpentsdbQueryPlan, OpentsdbSubPlan, QueryRequest, SubQuery};
use crate::{
    config::DynamicConfig,
    datafusion_util::{default_sort_exprs, timerange_to_expr},
    plan::{Plan, QueryPlan},
    provider::{ContextProviderAdapter, MetaProvider},
};

pub mod types;

pub const DEFAULT_FIELD: &str = "value";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table provider not found, table:{name}, err:{source}"))]
    TableProviderNotFound {
        name: String,
        source: DataFusionError,
    },

    #[snafu(display("Failed to build schema, err:{source}"))]
    BuildTableSchema { source: common_types::schema::Error },

    #[snafu(display("Query end should larger than start, start:{start}, end:{end}"))]
    InvalidRange { start: i64, end: i64 },

    #[snafu(display("Invalid filter, value:{filter_type}"))]
    InvalidFilter { filter_type: String },

    #[snafu(display("Invalid aggregator, value:{aggr}"))]
    InvalidAggregator { aggr: String },

    #[snafu(display("Failed to build plan, source:{source}"))]
    BuildPlanError { source: DataFusionError },

    #[snafu(display("MetaProvider {msg}, err:{source}"))]
    MetaProviderError {
        msg: String,
        source: crate::provider::Error,
    },
}

impl From<DataFusionError> for Error {
    fn from(df_err: DataFusionError) -> Self {
        Error::BuildPlanError { source: df_err }
    }
}

define_result!(Error);

fn normalize_filters(
    tags: HashMap<String, String>,
    filters: Vec<Filter>,
) -> Result<(Vec<String>, Vec<Expr>)> {
    let mut groupby_col_names = Vec::new();
    let mut exprs = Vec::with_capacity(tags.len() + filters.len());
    for (tagk, tagv) in tags {
        exprs.push(ident(&tagk).eq(lit(tagv)));
        groupby_col_names.push(tagk);
    }

    for filter in filters {
        let col_name = ident(&filter.tagk);

        if filter.group_by {
            groupby_col_names.push(filter.tagk);
        }

        // http://opentsdb.net/docs/build/html/user_guide/query/filters.html
        let expr = match filter.r#type.as_str() {
            "literal_or" => {
                let vs = filter.filter.split('|').map(lit).collect();
                col_name.in_list(vs, false)
            }
            "not_literal_or" => {
                let vs = filter.filter.split('|').map(lit).collect();
                col_name.in_list(vs, true)
            }
            filter_type => return InvalidFilter { filter_type }.fail(),
        };
        exprs.push(expr);
    }

    Ok((groupby_col_names, exprs))
}

fn build_aggr_expr(aggr: &str) -> Result<Option<Expr>> {
    // http://opentsdb.net/docs/build/html/user_guide/query/aggregators.html
    let aggr = match aggr {
        "sum" => sum(ident(DEFAULT_FIELD)).alias(DEFAULT_FIELD),
        "count" => count(ident(DEFAULT_FIELD)).alias(DEFAULT_FIELD),
        "avg" => avg(ident(DEFAULT_FIELD)).alias(DEFAULT_FIELD),
        "min" => min(ident(DEFAULT_FIELD)).alias(DEFAULT_FIELD),
        "max" => max(ident(DEFAULT_FIELD)).alias(DEFAULT_FIELD),
        "dev" => stddev(ident(DEFAULT_FIELD)).alias(DEFAULT_FIELD),
        "none" => return Ok(None),
        _ => return InvalidAggregator { aggr }.fail(),
    };

    Ok(Some(aggr))
}

pub fn subquery_to_plan<P: MetaProvider>(
    meta_provider: ContextProviderAdapter<'_, P>,
    query_range: &TimeRange,
    sub_query: SubQuery,
) -> Result<OpentsdbSubPlan> {
    let metric = sub_query.metric;
    let table_provider = meta_provider
        .get_table_provider(TableReference::bare(&metric))
        .context(TableProviderNotFound { name: &metric })?;
    let schema = Schema::try_from(table_provider.schema()).context(BuildTableSchema)?;

    let timestamp_col_name = schema.timestamp_name();
    let mut tags = schema
        .columns()
        .iter()
        .filter(|column| column.is_tag)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let (mut groupby_col_names, filter_exprs) = {
        let (groupby, mut filters) = normalize_filters(sub_query.tags, sub_query.filters)?;
        filters.push(timerange_to_expr(query_range, timestamp_col_name));
        let anded_filters = conjunction(filters).expect("at least one filter(timestamp)");
        (groupby, anded_filters)
    };
    let sort_exprs = default_sort_exprs(timestamp_col_name);

    let mut builder = LogicalPlanBuilder::scan(metric.clone(), table_provider, None)?
        .filter(filter_exprs)?
        .sort(sort_exprs)?;

    match build_aggr_expr(&sub_query.aggregator)? {
        Some(aggr_expr) => {
            let mut group_expr = groupby_col_names.iter().map(ident).collect::<Vec<_>>();
            group_expr.push(ident(timestamp_col_name));
            builder = builder.aggregate(group_expr, [aggr_expr])?;
            tags = groupby_col_names.clone();
        }
        None => groupby_col_names.clear(),
    }

    let df_plan = builder.build().context(BuildPlanError)?;
    let tables = Arc::new(
        meta_provider
            .try_into_container()
            .context(MetaProviderError {
                msg: "Failed to find meta",
            })?,
    );

    Ok(OpentsdbSubPlan {
        plan: Plan::Query(QueryPlan {
            df_plan,
            table_name: Some(metric.clone()),
            tables,
        }),
        metric,
        timestamp_col_name: timestamp_col_name.to_string(),
        field_col_name: DEFAULT_FIELD.to_string(),
        tags,
        aggregated_tags: groupby_col_names,
    })
}

pub fn opentsdb_query_to_plan<P: MetaProvider>(
    query: QueryRequest,
    provider: &P,
    read_parallelism: usize,
    dyn_config: &DynamicConfig,
) -> Result<OpentsdbQueryPlan> {
    let range = TimeRange::new(Timestamp::new(query.start), Timestamp::new(query.end + 1))
        .context(InvalidRange {
            start: query.start,
            end: query.end,
        })?;

    let plans = query
        .queries
        .into_iter()
        .map(|sub_query| {
            subquery_to_plan(
                ContextProviderAdapter::new(provider, read_parallelism, dyn_config),
                &range,
                sub_query,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(OpentsdbQueryPlan { plans })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::tests::MockMetaProvider;

    #[test]
    fn test_normalize_filters() {
        let mut tags = HashMap::new();
        tags.insert("tag1".to_string(), "tag1_value".to_string());

        let (groupby_col_names, exprs) = normalize_filters(tags.clone(), vec![]).unwrap();
        assert_eq!(groupby_col_names, vec!["tag1"]);
        assert_eq!(exprs.len(), 1);

        let (groupby_col_names, exprs) = normalize_filters(
            tags.clone(),
            vec![Filter {
                r#type: "literal_or".to_string(),
                tagk: "tag2".to_string(),
                filter: "tag2_value|tag2_value2".to_string(),
                group_by: false,
            }],
        )
        .unwrap();
        assert_eq!(groupby_col_names, vec!["tag1"]);
        assert_eq!(exprs.len(), 2);

        let (groupby_col_names, exprs) = normalize_filters(
            tags.clone(),
            vec![Filter {
                r#type: "literal_or".to_string(),
                tagk: "tag2".to_string(),
                filter: "tag2_value|tag2_value2".to_string(),
                group_by: true,
            }],
        )
        .unwrap();
        assert_eq!(groupby_col_names, vec!["tag1", "tag2"]);
        assert_eq!(exprs.len(), 2);

        let (groupby_col_names, exprs) = normalize_filters(
            tags.clone(),
            vec![Filter {
                r#type: "not_literal_or".to_string(),
                tagk: "tag2".to_string(),
                filter: "tag2_value|tag2_value2".to_string(),
                group_by: false,
            }],
        )
        .unwrap();
        assert_eq!(groupby_col_names, vec!["tag1"]);
        assert_eq!(exprs.len(), 2);
    }

    #[test]
    fn test_build_aggr_expr() {
        let aggr = build_aggr_expr("sum").unwrap().unwrap();
        assert_eq!(aggr.to_string(), "SUM(value) AS value");

        let aggr = build_aggr_expr("count").unwrap().unwrap();
        assert_eq!(aggr.to_string(), "COUNT(value) AS value");

        let aggr = build_aggr_expr("avg").unwrap().unwrap();
        assert_eq!(aggr.to_string(), "AVG(value) AS value");

        let aggr = build_aggr_expr("min").unwrap().unwrap();
        assert_eq!(aggr.to_string(), "MIN(value) AS value");

        let aggr = build_aggr_expr("max").unwrap().unwrap();
        assert_eq!(aggr.to_string(), "MAX(value) AS value");

        let aggr = build_aggr_expr("dev").unwrap().unwrap();
        assert_eq!(aggr.to_string(), "STDDEV(value) AS value");

        let aggr = build_aggr_expr("none").unwrap();
        assert!(aggr.is_none());

        let err = build_aggr_expr("invalid").unwrap_err();
        assert!(err.to_string().contains("Invalid aggregator"));
    }

    #[test]
    fn test_subquery_to_plan() {
        let meta_provider = MockMetaProvider::default();

        let query_range = TimeRange::new(Timestamp::new(0), Timestamp::new(100)).unwrap();
        let sub_query = SubQuery {
            metric: "metric".to_string(),
            aggregator: "sum".to_string(),
            rate: false,
            downsample: None,
            tags: vec![("tag1".to_string(), "tag1_value".to_string())]
                .into_iter()
                .collect(),
            filters: vec![Filter {
                r#type: "literal_or".to_string(),
                tagk: "tag2".to_string(),
                filter: "tag2_value|tag2_value2".to_string(),
                group_by: true,
            }],
        };

        let plan = subquery_to_plan(
            ContextProviderAdapter::new(&meta_provider, 1, &Default::default()),
            &query_range,
            sub_query,
        )
        .unwrap();

        assert_eq!(plan.metric, "metric");
        assert_eq!(plan.field_col_name, "value");
        assert_eq!(plan.timestamp_col_name, "timestamp");
        assert_eq!(plan.tags, vec!["tag1", "tag2"]);
        assert_eq!(plan.aggregated_tags, vec!["tag1", "tag2"]);

        let df_plan = match plan.plan {
            Plan::Query(QueryPlan { df_plan, .. }) => df_plan,
            _ => panic!("expect query plan"),
        };
        assert_eq!(df_plan.schema().fields().len(), 4);
    }

    #[test]
    fn test_opentsdb_query_to_plan() {
        let meta_provider = MockMetaProvider::default();

        let query = QueryRequest {
            start: 0,
            end: 100,
            queries: vec![
                SubQuery {
                    metric: "metric".to_string(),
                    aggregator: "none".to_string(),
                    rate: false,
                    downsample: None,
                    tags: vec![("tag1".to_string(), "tag1_value".to_string())]
                        .into_iter()
                        .collect(),
                    filters: vec![],
                },
                SubQuery {
                    metric: "metric".to_string(),
                    aggregator: "sum".to_string(),
                    rate: false,
                    downsample: None,
                    tags: vec![("tag1".to_string(), "tag1_value".to_string())]
                        .into_iter()
                        .collect(),
                    filters: vec![],
                },
            ],
            ms_resolution: false,
        };

        let plan =
            opentsdb_query_to_plan(query, &meta_provider, 1, &DynamicConfig::default()).unwrap();

        assert_eq!(plan.plans.len(), 2);

        let plan0 = &plan.plans[0];
        assert_eq!(plan0.metric, "metric");
        assert_eq!(plan0.field_col_name, "value");
        assert_eq!(plan0.timestamp_col_name, "timestamp");
        assert_eq!(plan0.tags, vec!["tag1", "tag2"]);
        assert!(plan0.aggregated_tags.is_empty());

        let df_plan = match &plan0.plan {
            Plan::Query(QueryPlan { df_plan, .. }) => df_plan,
            _ => panic!("expect query plan"),
        };
        assert_eq!(df_plan.schema().fields().len(), 5);

        let plan1 = &plan.plans[1];
        assert_eq!(plan1.metric, "metric");
        assert_eq!(plan1.field_col_name, "value");
        assert_eq!(plan1.timestamp_col_name, "timestamp");
        assert_eq!(plan1.tags, vec!["tag1"]);
        assert_eq!(plan1.aggregated_tags, vec!["tag1"]);

        let df_plan = match &plan1.plan {
            Plan::Query(QueryPlan { df_plan, .. }) => df_plan,
            _ => panic!("expect query plan"),
        };
        assert_eq!(df_plan.schema().fields().len(), 3);
    }
}
