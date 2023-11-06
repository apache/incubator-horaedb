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

use std::{convert::TryInto, sync::Arc};

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use logger::warn;
use query_frontend::{ast::ShowCreateObject, plan::ShowCreatePlan};
use snafu::ensure;
use table_engine::{table, table::TableRef};

use crate::{
    interpreter::Output,
    show::{Result, UnsupportedType},
    RecordBatchVec,
};

pub struct ShowCreateInterpreter {
    plan: ShowCreatePlan,
}

impl ShowCreateInterpreter {
    pub fn create(plan: ShowCreatePlan) -> ShowCreateInterpreter {
        Self { plan }
    }

    pub fn execute_show_create(self) -> Result<Output> {
        let ShowCreatePlan { table, obj_type } = self.plan;

        ensure!(
            obj_type == ShowCreateObject::Table,
            UnsupportedType { obj_type }
        );

        Self::table_ref_to_record_batch(table).map(Output::Records)
    }

    fn table_ref_to_record_batch(table_ref: TableRef) -> Result<RecordBatchVec> {
        let tables = vec![table_ref.name().to_string()];
        let sqls = vec![Self::render_table_sql(table_ref)];

        let schema = Schema::new(vec![
            Field::new("Table", DataType::Utf8, false),
            Field::new("Create Table", DataType::Utf8, false),
        ]);

        let arrow_record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(tables)),
                Arc::new(StringArray::from(sqls)),
            ],
        )
        .unwrap();

        let record_batch = arrow_record_batch.try_into().unwrap();

        Ok(vec![record_batch])
    }

    fn render_table_sql(table_ref: TableRef) -> String {
        let render_partition_info = table::render_partition_info(table_ref.partition_info())
            .map_or_else(
                |e| {
                    warn!("Failed to render partition info, err:{e}");
                    String::new()
                },
                |v| match v {
                    Some(v) => format!(" {v}"),
                    None => String::new(),
                },
            );
        //TODO(boyan) pretty output
        format!(
            "CREATE TABLE `{}` ({}){} ENGINE={}{}",
            table_ref.name(),
            table::render_columns_and_constrains(&table_ref),
            render_partition_info,
            table_ref.engine_type(),
            table::render_options(table_ref.options())
        )
    }
}

#[cfg(test)]
mod test {
    use std::ops::Add;

    use datafusion::logical_expr::col;
    use datafusion_proto::bytes::Serializeable;
    use table_engine::partition::{
        HashPartitionInfo, KeyPartitionInfo, PartitionDefinition, PartitionInfo,
    };

    use super::*;

    #[test]
    fn test_render_hash_partition_info() {
        let expr = col("col1").add(col("col2"));
        let partition_info = PartitionInfo::Hash(HashPartitionInfo {
            version: 0,
            definitions: vec![
                PartitionDefinition {
                    name: "p0".to_string(),
                    origin_name: None,
                },
                PartitionDefinition {
                    name: "p1".to_string(),
                    origin_name: None,
                },
            ],
            expr: expr.to_bytes().unwrap(),
            linear: false,
        });

        let expected = " PARTITION BY HASH(col1 + col2) PARTITIONS 2".to_string();
        assert_eq!(
            expected,
            table::render_partition_info(Some(partition_info))
                .unwrap()
                .unwrap()
        );
    }

    #[test]
    fn test_render_key_partition_info() {
        let partition_key_col_name = "col1";
        let partition_info = PartitionInfo::Key(KeyPartitionInfo {
            version: 0,
            definitions: vec![
                PartitionDefinition {
                    name: "p0".to_string(),
                    origin_name: None,
                },
                PartitionDefinition {
                    name: "p1".to_string(),
                    origin_name: None,
                },
            ],
            partition_key: vec![partition_key_col_name.to_string()],
            linear: false,
        });

        let expected = " PARTITION BY KEY(col1) PARTITIONS 2".to_string();
        assert_eq!(
            expected,
            table::render_partition_info(Some(partition_info))
                .unwrap()
                .unwrap()
        );
    }
}
