// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Partition rule factory

use common_types::schema::Schema;
use snafu::OptionExt;

use super::{key::KeyRule, ColumnWithType};
use crate::partition::{
    rule::PartitionRule, BuildPartitionRule, KeyPartitionInfo, PartitionInfo, Result,
};

pub struct PartitionRuleFactory;

impl PartitionRuleFactory {
    pub fn create(partition_info: PartitionInfo, schema: &Schema) -> Result<PartitionRuleRef> {
        match partition_info {
            PartitionInfo::Key(key_info) => Self::create_key_rule(key_info, schema),
            _ => BuildPartitionRule {
                msg: format!(
                    "unsupported partition strategy, strategy:{:?}",
                    partition_info
                ),
            }
            .fail(),
        }
    }

    fn create_key_rule(key_info: KeyPartitionInfo, schema: &Schema) -> Result<PartitionRuleRef> {
        let typed_columns = key_info
            .partition_key
            .into_iter()
            .map(|col| {
                schema
                    .column_with_name(col.as_str())
                    .with_context(|| BuildPartitionRule {
                        msg: format!(
                            "column in key partition info not found in schema, column:{}",
                            col
                        ),
                    })
                    .map(|col_schema| ColumnWithType::new(col, col_schema.data_type))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::new(KeyRule {
            typed_columns,
            partition_num: key_info.definitions.len() as u64,
        }))
    }
}

pub type PartitionRuleRef = Box<dyn PartitionRule>;
