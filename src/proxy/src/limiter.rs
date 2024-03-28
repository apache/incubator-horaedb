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

use std::{collections::HashSet, str::FromStr, sync::RwLock};

use datafusion::logical_expr::logical_plan::LogicalPlan;
use logger::error;
use macros::define_result;
use query_frontend::plan::Plan;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use time_ext::ReadableDuration;

use crate::metrics::BLOCKED_REQUEST_COUNTER_VEC_GLOBAL;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Table operation is blocked, table:{}, op:{}", table, op))]
    BlockedTable { table: String, op: String },

    #[snafu(display("Table operation is blocked by rule:{:?}, op:{}", rule, op))]
    BlockedByRule { rule: BlockRule, op: String },
}

define_result!(Error);

#[derive(Clone, Copy, Deserialize, Debug, PartialEq, Eq, Hash, Serialize, PartialOrd, Ord)]
#[serde(tag = "type", content = "content")]
pub enum BlockRule {
    QueryWithoutPredicate,
    /// Max time range a query can scan.
    #[serde(deserialize_with = "deserialize_readable_duration")]
    QueryRange(i64),
    AnyQuery,
    AnyInsert,
}

fn deserialize_readable_duration<'de, D>(deserializer: D) -> std::result::Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    ReadableDuration::from_str(s)
        .map(|d| d.0.as_millis() as i64)
        .map_err(serde::de::Error::custom)
}

#[derive(Default, Clone, Deserialize, Debug, Serialize)]
#[serde(default)]
pub struct LimiterConfig {
    pub write_block_list: Vec<String>,
    pub read_block_list: Vec<String>,
    pub rules: Vec<BlockRule>,
}

impl BlockRule {
    fn should_limit(&self, plan: &Plan) -> bool {
        match self {
            BlockRule::QueryWithoutPredicate => self.is_query_without_predicate(plan),
            BlockRule::AnyQuery => matches!(plan, Plan::Query(_)),
            BlockRule::QueryRange(threshold) => {
                if let Plan::Query(plan) = plan {
                    let range = match plan.query_range() {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Find query range failed, err:{e}");
                            return false;
                        }
                    };
                    if let Some(range) = range {
                        if range > *threshold {
                            return true;
                        }
                    }
                }

                false
            }
            BlockRule::AnyInsert => matches!(plan, Plan::Insert(_)),
        }
    }

    fn is_query_without_predicate(&self, plan: &Plan) -> bool {
        if let Plan::Query(query) = plan {
            !Self::contains_filter(&query.df_plan)
        } else {
            false
        }
    }

    fn contains_filter(plan: &LogicalPlan) -> bool {
        if let LogicalPlan::Filter(filter) = plan {
            return matches!(&*filter.input, LogicalPlan::TableScan(_));
        }

        for input in plan.inputs() {
            if Self::contains_filter(input) {
                return true;
            }
        }

        false
    }
}

pub struct Limiter {
    write_block_list: RwLock<HashSet<String>>,
    read_block_list: RwLock<HashSet<String>>,
    rules: RwLock<HashSet<BlockRule>>,
}

impl Default for Limiter {
    fn default() -> Self {
        Self {
            write_block_list: RwLock::new(HashSet::new()),
            read_block_list: RwLock::new(HashSet::new()),
            rules: RwLock::new(HashSet::new()),
        }
    }
}

impl Limiter {
    pub fn new(limit_config: LimiterConfig) -> Self {
        Self {
            write_block_list: RwLock::new(limit_config.write_block_list.into_iter().collect()),
            read_block_list: RwLock::new(limit_config.read_block_list.into_iter().collect()),
            rules: RwLock::new(limit_config.rules.into_iter().collect()),
        }
    }

    fn try_limit_by_block_list(&self, plan: &Plan) -> Result<()> {
        match plan {
            Plan::Query(query) => {
                self.read_block_list
                    .read()
                    .unwrap()
                    .iter()
                    .try_for_each(|blocked_table| {
                        if query
                            .tables
                            .get(query_frontend::planner::get_table_ref(blocked_table))
                            .is_some()
                        {
                            BlockedTable {
                                table: blocked_table,
                                op: plan.plan_type(),
                            }
                            .fail()?;
                        }

                        Ok(())
                    })?;
            }
            Plan::Insert(insert) => {
                if self
                    .write_block_list
                    .read()
                    .unwrap()
                    .contains(insert.table.name())
                {
                    BlockedTable {
                        table: insert.table.name(),
                        op: plan.plan_type(),
                    }
                    .fail()?;
                }
            }
            _ => (),
        }

        Ok(())
    }

    fn try_limit_by_rules(&self, plan: &Plan) -> Result<()> {
        self.rules.read().unwrap().iter().try_for_each(|rule| {
            if rule.should_limit(plan) {
                BlockedByRule {
                    rule: *rule,
                    op: plan.plan_type(),
                }
                .fail()?;
            }

            Ok(())
        })
    }

    /// Try to limit the plan according the configured limiter.
    ///
    /// Error will throws if the plan is forbidden to execute.
    pub fn try_limit(&self, plan: &Plan) -> Result<()> {
        let result = {
            self.try_limit_by_block_list(plan)?;
            self.try_limit_by_rules(plan)
        };

        if result.is_err() {
            BLOCKED_REQUEST_COUNTER_VEC_GLOBAL
                .with_label_values(&[plan.plan_type()])
                .inc();
        }

        result
    }

    pub fn add_write_block_list(&self, block_list: Vec<String>) {
        self.write_block_list.write().unwrap().extend(block_list)
    }

    pub fn add_read_block_list(&self, block_list: Vec<String>) {
        self.read_block_list.write().unwrap().extend(block_list)
    }

    pub fn set_write_block_list(&self, block_list: Vec<String>) {
        *self.write_block_list.write().unwrap() = block_list.into_iter().collect();
    }

    pub fn set_read_block_list(&self, block_list: Vec<String>) {
        *self.read_block_list.write().unwrap() = block_list.into_iter().collect();
    }

    pub fn get_write_block_list(&self) -> HashSet<String> {
        self.write_block_list.read().unwrap().clone()
    }

    pub fn get_read_block_list(&self) -> HashSet<String> {
        self.read_block_list.read().unwrap().clone()
    }

    pub fn remove_write_block_list(&self, block_list: Vec<String>) {
        let mut write_block_list = self.write_block_list.write().unwrap();
        for value in block_list {
            write_block_list.remove(&value);
        }
    }

    pub fn remove_read_block_list(&self, block_list: Vec<String>) {
        let mut read_block_list = self.read_block_list.write().unwrap();
        for value in block_list {
            read_block_list.remove(&value);
        }
    }

    pub fn get_block_rules(&self) -> HashSet<BlockRule> {
        self.rules.read().unwrap().clone()
    }

    pub fn add_block_rules(&self, rules: Vec<BlockRule>) {
        self.rules.write().unwrap().extend(rules);
    }

    pub fn remove_block_rules(&self, rules_to_remove: &[BlockRule]) {
        let mut rules = self.rules.write().unwrap();

        for rule_to_remove in rules_to_remove {
            rules.remove(rule_to_remove);
        }
    }

    pub fn set_block_rules(&self, new_rules: Vec<BlockRule>) {
        let new_rule_set: HashSet<_> = new_rules.into_iter().collect();
        *self.rules.write().unwrap() = new_rule_set;
    }
}

#[cfg(test)]
mod tests {
    use common_types::request_id::RequestId;
    use query_frontend::{
        config::DynamicConfig, parser::Parser, plan::Plan, planner::Planner,
        tests::MockMetaProvider,
    };

    use super::{BlockRule, LimiterConfig};
    use crate::limiter::Limiter;

    fn sql_to_plan(meta_provider: &MockMetaProvider, sql: &str) -> Plan {
        let dyn_config = DynamicConfig::default();
        let planner = Planner::new(meta_provider, RequestId::next_id(), 1, &dyn_config);
        let mut statements = Parser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        planner.statement_to_plan(statements.remove(0)).unwrap()
    }

    fn prepare_limiter_with_block_list() -> (MockMetaProvider, Limiter) {
        let mock = MockMetaProvider::default();

        let block_list = vec!["test_table".to_string()];
        let limiter = Limiter::default();
        limiter.set_read_block_list(block_list.clone());
        limiter.set_write_block_list(block_list);
        (mock, limiter)
    }

    fn prepare_limiter_with_rules(rules: Vec<BlockRule>) -> (MockMetaProvider, Limiter) {
        let mock = MockMetaProvider::default();
        let config = LimiterConfig {
            write_block_list: vec![],
            read_block_list: vec![],
            rules,
        };

        let limiter = Limiter::new(config);
        (mock, limiter)
    }

    #[test]
    fn test_limit_by_rule() {
        let (mock, limiter) = prepare_limiter_with_rules(vec![BlockRule::QueryWithoutPredicate]);
        let query = "select * from test_table";
        let query_plan = sql_to_plan(&mock, query);
        assert!(limiter.try_limit(&query_plan).is_err());

        let insert = "select * from test_table where field1 = 10";
        let insert_plan = sql_to_plan(&mock, insert);
        assert!(limiter.try_limit(&insert_plan).is_ok());

        let insert="INSERT INTO test_table(key1, key2, field1, field2) VALUES('tagk', 1638428434000, 100, 'hello3')";
        let insert_plan = sql_to_plan(&mock, insert);
        assert!(limiter.try_limit(&insert_plan).is_ok());

        let (mock, limiter) = prepare_limiter_with_rules(vec![BlockRule::AnyQuery]);
        let query = "select * from test_table";
        let query_plan = sql_to_plan(&mock, query);
        assert!(limiter.try_limit(&query_plan).is_err());

        let (mock, limiter) = prepare_limiter_with_rules(vec![BlockRule::AnyInsert]);
        let insert="INSERT INTO test_table(key1, key2, field1, field2) VALUES('tagk', 1638428434000, 100, 'hello3')";
        let insert_plan = sql_to_plan(&mock, insert);
        assert!(limiter.try_limit(&insert_plan).is_err());
    }

    #[test]
    fn test_limiter() {
        let (mock, limiter) = prepare_limiter_with_block_list();
        let query = "select * from test_table";
        let query_plan = sql_to_plan(&mock, query);
        assert!(limiter.try_limit(&query_plan).is_err());

        let insert="INSERT INTO test_table(key1, key2, field1, field2) VALUES('tagk', 1638428434000, 100, 'hello3')";
        let insert_plan = sql_to_plan(&mock, insert);
        assert!(limiter.try_limit(&insert_plan).is_err());
    }

    #[test]
    fn test_limiter_remove() {
        let (mock, limiter) = prepare_limiter_with_block_list();
        let test_data = vec!["test_table".to_string()];

        let query = "select * from test_table";
        let query_plan = sql_to_plan(&mock, query);
        assert!(limiter.try_limit(&query_plan).is_err());

        let insert="INSERT INTO test_table(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3')";
        let insert_plan = sql_to_plan(&mock, insert);
        assert!(limiter.try_limit(&insert_plan).is_err());

        limiter.remove_write_block_list(test_data.clone());
        limiter.remove_read_block_list(test_data);
        assert!(limiter.try_limit(&query_plan).is_ok());
        assert!(limiter.try_limit(&insert_plan).is_ok());

        limiter.add_block_rules(vec![BlockRule::QueryWithoutPredicate]);
        assert!(limiter.try_limit(&query_plan).is_err());
        limiter.remove_block_rules(&[BlockRule::QueryWithoutPredicate]);
        assert!(limiter.try_limit(&query_plan).is_ok());
    }

    #[test]
    fn test_limiter_add() {
        let (mock, limiter) = prepare_limiter_with_block_list();
        let test_data = vec!["test_table2".to_string()];

        let query = "select * from test_table2";
        let query_plan = sql_to_plan(&mock, query);
        assert!(limiter.try_limit(&query_plan).is_ok());

        let insert="INSERT INTO test_table2(key1, key2, field1, field2) VALUES('tagk', 1638428434000,100, 'hello3')";
        let insert_plan = sql_to_plan(&mock, insert);
        assert!(limiter.try_limit(&insert_plan).is_ok());

        limiter.add_write_block_list(test_data.clone());
        limiter.add_read_block_list(test_data);
        assert!(limiter.try_limit(&query_plan).is_err());
        assert!(limiter.try_limit(&insert_plan).is_err());

        limiter.add_block_rules(vec![BlockRule::QueryWithoutPredicate]);
        assert!(limiter.try_limit(&query_plan).is_err());
    }

    #[test]
    fn test_limiter_set() {
        let (mock, limiter) = prepare_limiter_with_block_list();
        let test_data = vec!["test_table2".to_string()];

        let query = "select * from test_table";
        let query_plan = sql_to_plan(&mock, query);
        assert!(limiter.try_limit(&query_plan).is_err());

        let query2 = "select * from test_table2";
        let query_plan2 = sql_to_plan(&mock, query2);
        assert!(limiter.try_limit(&query_plan2).is_ok());

        let insert="INSERT INTO test_table(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3')";
        let insert_plan = sql_to_plan(&mock, insert);
        assert!(limiter.try_limit(&insert_plan).is_err());

        let insert2="INSERT INTO test_table2(key1, key2, field1,field2) VALUES('tagk', 1638428434000,100, 'hello3')";
        let insert_plan2 = sql_to_plan(&mock, insert2);
        assert!(limiter.try_limit(&insert_plan2).is_ok());

        limiter.set_read_block_list(test_data.clone());
        limiter.set_write_block_list(test_data);
        assert!(limiter.try_limit(&query_plan).is_ok());
        assert!(limiter.try_limit(&insert_plan).is_ok());
        assert!(limiter.try_limit(&query_plan2).is_err());
        assert!(limiter.try_limit(&insert_plan2).is_err());

        limiter.set_block_rules(vec![BlockRule::QueryWithoutPredicate]);
        assert!(limiter.try_limit(&query_plan).is_err());
    }
}
