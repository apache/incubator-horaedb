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

use std::sync::Arc;

use catalog::consts::{DEFAULT_CATALOG, DEFAULT_SCHEMA};
use common_types::tests::{
    build_default_value_schema, build_schema, build_schema_for_cpu, build_schema_for_metric,
};
use datafusion::catalog::TableReference;
use df_operator::{scalar::ScalarUdf, udaf::AggregateUdf};
use partition_table_engine::test_util::PartitionedMemoryTable;
use table_engine::{
    memory::MemoryTable,
    partition::{KeyPartitionInfo, PartitionDefinition, PartitionInfo},
    table::{TableId, TableRef},
    ANALYTIC_ENGINE_TYPE,
};

use crate::provider::{MetaProvider, ResolvedTable};

pub struct MockMetaProvider {
    tables: Vec<TableRef>,
}

impl Default for MockMetaProvider {
    fn default() -> Self {
        let partition_info = PartitionInfo::Key(KeyPartitionInfo {
            version: 0,
            definitions: vec![PartitionDefinition::default(); 4],
            partition_key: vec!["tag1".to_string()],
            linear: false,
        });
        let test_partitioned_table = PartitionedMemoryTable::new(
            "test_partitioned_table".to_string(),
            TableId::from(105),
            build_schema_for_cpu(),
            ANALYTIC_ENGINE_TYPE.to_string(),
            partition_info,
        );

        Self {
            tables: vec![
                Arc::new(MemoryTable::new(
                    "test_table".to_string(),
                    TableId::from(100),
                    build_schema(),
                    ANALYTIC_ENGINE_TYPE.to_string(),
                )),
                Arc::new(MemoryTable::new(
                    "test_table2".to_string(),
                    TableId::from(101),
                    build_schema(),
                    ANALYTIC_ENGINE_TYPE.to_string(),
                )),
                Arc::new(MemoryTable::new(
                    "test_missing_columns_table".to_string(),
                    TableId::from(102),
                    build_default_value_schema(),
                    ANALYTIC_ENGINE_TYPE.to_string(),
                )),
                Arc::new(MemoryTable::new(
                    "__test_table".to_string(),
                    TableId::from(103),
                    build_schema(),
                    ANALYTIC_ENGINE_TYPE.to_string(),
                )),
                // Used in `test_remote_query_to_plan`
                Arc::new(MemoryTable::new(
                    "cpu".to_string(),
                    TableId::from(104),
                    build_schema_for_cpu(),
                    ANALYTIC_ENGINE_TYPE.to_string(),
                )),
                // Used in `test_partitioned_table_query_to_plan`
                Arc::new(test_partitioned_table),
                // Used in `test_subquery_to_plan` and `test_opentsdb_query_to_plan`
                Arc::new(MemoryTable::new(
                    "metric".to_string(),
                    TableId::from(106),
                    build_schema_for_metric(),
                    ANALYTIC_ENGINE_TYPE.to_string(),
                )),
            ],
        }
    }
}

impl MetaProvider for MockMetaProvider {
    fn default_catalog_name(&self) -> &str {
        &DEFAULT_CATALOG
    }

    fn default_schema_name(&self) -> &str {
        DEFAULT_SCHEMA
    }

    fn table(&self, name: TableReference) -> crate::provider::Result<Option<ResolvedTable>> {
        let resolved = name.resolve(self.default_catalog_name(), self.default_schema_name());
        for table in &self.tables {
            if resolved.table == table.name() {
                return Ok(Some(ResolvedTable {
                    catalog: resolved.catalog.to_string(),
                    schema: resolved.schema.to_string(),
                    table: table.clone(),
                }));
            }
        }

        Ok(None)
    }

    fn scalar_udf(&self, _name: &str) -> crate::provider::Result<Option<ScalarUdf>> {
        Ok(None)
    }

    fn aggregate_udf(&self, _name: &str) -> crate::provider::Result<Option<AggregateUdf>> {
        Ok(None)
    }

    fn all_tables(&self) -> crate::provider::Result<Vec<TableRef>> {
        Ok(Vec::new())
    }
}
