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

//! Interpreter for insert statement

use std::{
    collections::{BTreeMap, HashMap},
    ops::IndexMut,
    sync::Arc,
};

use arrow::{array::ArrayRef, error::ArrowError, record_batch::RecordBatch};
use async_trait::async_trait;
use codec::{compact::MemCompactEncoder, Encoder};
use common_types::{
    column_block::{ColumnBlock, ColumnBlockBuilder},
    column_schema::ColumnId,
    datum::Datum,
    row::{Row, RowBuilder, RowGroup},
    schema::Schema,
};
use datafusion::{
    common::ToDFSchema,
    error::DataFusionError,
    logical_expr::{expr::Expr as DfLogicalExpr, ColumnarValue as DfColumnarValue},
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
    physical_expr::{
        create_physical_expr, execution_props::ExecutionProps, expressions::TryCastExpr,
    },
};
use df_operator::visitor::find_columns_by_expr;
use futures::TryStreamExt;
use generic_error::{BoxError, GenericError};
use hash_ext::hash64;
use macros::define_result;
use query_engine::{executor::ExecutorRef, physical_planner::PhysicalPlannerRef};
use query_frontend::{
    plan::{InsertPlan, InsertSource, QueryPlan},
    planner::InsertMode,
};
use runtime::Priority;
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use table_engine::table::{TableRef, WriteRequest};

use crate::{
    context::Context,
    interpreter::{Insert, Interpreter, InterpreterPtr, Output, Result as InterpreterResult},
    RecordBatchVec,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to generate datafusion expr, err:{}", source))]
    DatafusionExpr { source: DataFusionError },

    #[snafu(display(
        "Failed to get data type from datafusion physical expr, err:{}",
        source
    ))]
    DatafusionDataType { source: DataFusionError },

    #[snafu(display("Failed to get arrow schema, err:{}", source))]
    ArrowSchema { source: ArrowError },

    #[snafu(display("Failed to get datafusion schema, err:{}", source))]
    DatafusionSchema { source: DataFusionError },

    #[snafu(display("Failed to evaluate datafusion physical expr, err:{}", source))]
    DatafusionExecutor { source: DataFusionError },

    #[snafu(display("Failed to build arrow record batch, err:{}", source))]
    BuildArrowRecordBatch { source: ArrowError },

    #[snafu(display("Failed to write table, err:{}", source))]
    WriteTable { source: table_engine::table::Error },

    #[snafu(display("Failed to encode tsid, err:{}", source))]
    EncodeTsid { source: codec::compact::Error },

    #[snafu(display("Failed to convert arrow array to column block, err:{}", source))]
    ConvertColumnBlock {
        source: common_types::column_block::Error,
    },

    #[snafu(display("Failed to find input columns of expr, column_name:{}", column_name))]
    FindExpressionInput { column_name: String },

    #[snafu(display("Failed to build column block, err:{}", source))]
    BuildColumnBlock {
        source: common_types::column_block::Error,
    },

    #[snafu(display("Failed to create query context, err:{}", source))]
    CreateQueryContext { source: crate::context::Error },

    #[snafu(display("Failed to execute select physical plan, msg:{}, err:{}", msg, source))]
    ExecuteSelectPlan { msg: String, source: GenericError },

    #[snafu(display("Failed to build row, err:{}", source))]
    BuildRow { source: common_types::row::Error },

    #[snafu(display("Record columns not enough, len:{}, index:{}", len, index))]
    RecordColumnsNotEnough { len: usize, index: usize },
}

define_result!(Error);

pub struct InsertInterpreter {
    ctx: Context,
    plan: InsertPlan,
    executor: ExecutorRef,
    physical_planner: PhysicalPlannerRef,
}

impl InsertInterpreter {
    pub fn create(
        ctx: Context,
        plan: InsertPlan,
        executor: ExecutorRef,
        physical_planner: PhysicalPlannerRef,
    ) -> InterpreterPtr {
        Box::new(Self {
            ctx,
            plan,
            executor,
            physical_planner,
        })
    }
}

#[async_trait]
impl Interpreter for InsertInterpreter {
    async fn execute(mut self: Box<Self>) -> InterpreterResult<Output> {
        // Generate tsid if needed.
        let InsertPlan {
            table,
            source,
            default_value_map,
        } = self.plan;

        let mut rows;
        match source {
            InsertSource::Values { row_group } => {
                rows = row_group;
            }
            InsertSource::Select {
                query: query_plan,
                column_index_in_insert,
            } => {
                let record_batches = exec_select_logical_plan(
                    self.ctx,
                    query_plan,
                    self.executor,
                    self.physical_planner,
                )
                .await
                .context(Insert)?;

                if record_batches.is_empty() {
                    return Ok(Output::AffectedRows(0));
                }

                rows = convert_records_to_row_group(
                    record_batches,
                    column_index_in_insert,
                    table.schema(),
                )
                .context(Insert)?;
            }
        }

        maybe_generate_tsid(&mut rows).context(Insert)?;

        // Fill default values
        fill_default_values(table.clone(), &mut rows, &default_value_map).context(Insert)?;

        let request = WriteRequest { row_group: rows };

        let num_rows = table
            .write(request)
            .await
            .context(WriteTable)
            .context(Insert)?;

        Ok(Output::AffectedRows(num_rows))
    }
}

async fn exec_select_logical_plan(
    ctx: Context,
    query_plan: QueryPlan,
    executor: ExecutorRef,
    physical_planner: PhysicalPlannerRef,
) -> Result<RecordBatchVec> {
    let priority = Priority::High;

    let query_ctx = ctx
        .new_query_context(priority)
        .context(CreateQueryContext)?;

    // Create select physical plan.
    let physical_plan = physical_planner
        .plan(&query_ctx, query_plan)
        .await
        .box_err()
        .context(ExecuteSelectPlan {
            msg: "failed to build select physical plan",
        })?;

    // Execute select physical plan.
    let record_batch_stream = executor
        .execute(&query_ctx, physical_plan)
        .await
        .box_err()
        .context(ExecuteSelectPlan {
            msg: "failed to execute select physical plan",
        })?;

    let record_batches =
        record_batch_stream
            .try_collect()
            .await
            .box_err()
            .context(ExecuteSelectPlan {
                msg: "failed to collect select execution results",
            })?;

    Ok(record_batches)
}

fn convert_records_to_row_group(
    record_batches: RecordBatchVec,
    column_index_in_insert: Vec<InsertMode>,
    schema: Schema,
) -> Result<RowGroup> {
    let mut data_rows: Vec<Row> = Vec::new();

    for record in &record_batches {
        let num_cols = record.num_columns();
        let num_rows = record.num_rows();
        for row_idx in 0..num_rows {
            let mut row_builder = RowBuilder::new(&schema);
            // For each column in schema, append datum into row builder
            for (index_opt, column_schema) in column_index_in_insert.iter().zip(schema.columns()) {
                match index_opt {
                    InsertMode::Direct(index) => {
                        ensure!(
                            *index < num_cols,
                            RecordColumnsNotEnough {
                                len: num_cols,
                                index: *index
                            }
                        );
                        let datum = record.column(*index).datum(row_idx);
                        row_builder = row_builder.append_datum(datum).context(BuildRow)?;
                    }
                    InsertMode::Null => {
                        // This is a null column
                        row_builder = row_builder.append_datum(Datum::Null).context(BuildRow)?;
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
            let row = row_builder.finish().context(BuildRow)?;
            data_rows.push(row);
        }
    }
    RowGroup::try_new(schema, data_rows).context(BuildRow)
}

fn maybe_generate_tsid(rows: &mut RowGroup) -> Result<()> {
    let schema = rows.schema();
    let tsid_idx = schema.index_of_tsid();

    if let Some(idx) = tsid_idx {
        // Vec of (`index of tag`, `column id of tag`).
        let tag_idx_column_ids: Vec<_> = schema
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(i, column)| {
                if column.is_tag {
                    Some((i, column.id))
                } else {
                    None
                }
            })
            .collect();

        let mut hash_bytes = Vec::new();
        for i in 0..rows.num_rows() {
            let row = rows.get_row_mut(i).unwrap();

            let mut tsid_builder = TsidBuilder::new(&mut hash_bytes);

            for (idx, column_id) in &tag_idx_column_ids {
                tsid_builder.maybe_write_datum(*column_id, &row[*idx])?;
            }

            let tsid = tsid_builder.finish();
            row[idx] = Datum::UInt64(tsid);
        }
    }
    Ok(())
}

struct TsidBuilder<'a> {
    encoder: MemCompactEncoder,
    hash_bytes: &'a mut Vec<u8>,
}

impl<'a> TsidBuilder<'a> {
    fn new(hash_bytes: &'a mut Vec<u8>) -> Self {
        // Clear the bytes buffer.
        hash_bytes.clear();

        Self {
            encoder: MemCompactEncoder,
            hash_bytes,
        }
    }

    fn maybe_write_datum(&mut self, column_id: ColumnId, datum: &Datum) -> Result<()> {
        // Null datum will be ignored, so tsid remains unchanged after adding a null
        // column.
        if datum.is_null() {
            return Ok(());
        }

        // Write column id first.
        self.encoder
            .encode(self.hash_bytes, &Datum::UInt64(u64::from(column_id)))
            .context(EncodeTsid)?;
        // Write datum.
        self.encoder
            .encode(self.hash_bytes, datum)
            .context(EncodeTsid)?;
        Ok(())
    }

    fn finish(self) -> u64 {
        hash64(&self.hash_bytes[..])
    }
}

/// Fill missing columns which can be calculated via default value expr.
fn fill_default_values(
    table: TableRef,
    row_groups: &mut RowGroup,
    default_value_map: &BTreeMap<usize, DfLogicalExpr>,
) -> Result<()> {
    let mut cached_column_values: HashMap<usize, DfColumnarValue> = HashMap::new();
    let table_arrow_schema = table.schema().to_arrow_schema_ref();
    let df_schema_ref = table_arrow_schema
        .clone()
        .to_dfschema_ref()
        .context(DatafusionSchema)?;

    for (column_idx, default_value_expr) in default_value_map.iter() {
        let execution_props = ExecutionProps::default();

        // Optimize logical expr
        let simplifier = ExprSimplifier::new(
            SimplifyContext::new(&execution_props).with_schema(df_schema_ref.clone()),
        );
        let default_value_expr = simplifier
            .coerce(default_value_expr.clone(), df_schema_ref.clone())
            .context(DatafusionExpr)?;
        let simplified_expr = simplifier
            .simplify(default_value_expr)
            .context(DatafusionExpr)?;

        // Find input columns
        let required_column_idxes = find_columns_by_expr(&simplified_expr)
            .iter()
            .map(|column_name| {
                table
                    .schema()
                    .index_of(column_name)
                    .context(FindExpressionInput { column_name })
            })
            .collect::<Result<Vec<usize>>>()?;
        let input_arrow_schema = table_arrow_schema
            .project(&required_column_idxes)
            .context(ArrowSchema)?;
        let input_df_schema = input_arrow_schema
            .clone()
            .to_dfschema()
            .context(DatafusionSchema)?;

        // Create physical expr
        let physical_expr = create_physical_expr(
            &simplified_expr,
            &input_df_schema,
            &input_arrow_schema,
            &execution_props,
        )
        .context(DatafusionExpr)?;

        let from_type = physical_expr
            .data_type(&input_arrow_schema)
            .context(DatafusionDataType)?;
        let to_type = row_groups.schema().column(*column_idx).data_type;

        let casted_physical_expr = if from_type != to_type.into() {
            Arc::new(TryCastExpr::new(physical_expr, to_type.into()))
        } else {
            physical_expr
        };

        // Build input record batch
        let input_arrays = required_column_idxes
            .into_iter()
            .map(|col_idx| {
                get_or_extract_column_from_row_groups(
                    col_idx,
                    row_groups,
                    &mut cached_column_values,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let input = if input_arrays.is_empty() {
            RecordBatch::new_empty(Arc::new(input_arrow_schema))
        } else {
            RecordBatch::try_new(Arc::new(input_arrow_schema), input_arrays)
                .context(BuildArrowRecordBatch)?
        };

        let output = casted_physical_expr
            .evaluate(&input)
            .context(DatafusionExecutor)?;

        fill_column_to_row_group(*column_idx, &output, row_groups)?;

        // Write output to cache.
        cached_column_values.insert(*column_idx, output);
    }

    Ok(())
}

fn fill_column_to_row_group(
    column_idx: usize,
    column: &DfColumnarValue,
    rows: &mut RowGroup,
) -> Result<()> {
    match column {
        DfColumnarValue::Array(array) => {
            let datum_kind = rows.schema().column(column_idx).data_type;
            let column_block = ColumnBlock::try_from_arrow_array_ref(&datum_kind, array)
                .context(ConvertColumnBlock)?;
            for row_idx in 0..rows.num_rows() {
                let datum = column_block.datum(row_idx);
                rows.get_row_mut(row_idx)
                    .map(|row| std::mem::replace(row.index_mut(column_idx), datum.clone()));
            }
        }
        DfColumnarValue::Scalar(scalar) => {
            if let Some(datum) = Datum::from_scalar_value(scalar) {
                for row_idx in 0..rows.num_rows() {
                    rows.get_row_mut(row_idx)
                        .map(|row| std::mem::replace(row.index_mut(column_idx), datum.clone()));
                }
            }
        }
    };

    Ok(())
}

/// This method is used to get specific column data.
/// There are two pathes:
///  1. get from cached_column_values
///  2. extract from row_groups
///
/// For performance reasons, we cached the columns extracted from row_groups
/// before, and we will also cache the output of the exprs.
fn get_or_extract_column_from_row_groups(
    column_idx: usize,
    row_groups: &RowGroup,
    cached_column_values: &mut HashMap<usize, DfColumnarValue>,
) -> Result<ArrayRef> {
    let num_rows = row_groups.num_rows();
    let column = cached_column_values
        .get(&column_idx)
        .map(|c| Ok(c.clone()))
        .unwrap_or_else(|| {
            let data_type = row_groups.schema().column(column_idx).data_type;
            let iter = row_groups.iter_column(column_idx);
            let mut builder = ColumnBlockBuilder::with_capacity(
                &data_type,
                iter.size_hint().0,
                row_groups.schema().column(column_idx).is_dictionary,
            );

            for datum in iter {
                builder.append(datum.clone()).context(BuildColumnBlock)?;
            }

            let columnar_value = DfColumnarValue::Array(builder.build().to_arrow_array_ref());
            cached_column_values.insert(column_idx, columnar_value.clone());
            Ok(columnar_value)
        })?;

    column.into_array(num_rows).context(DatafusionExecutor)
}
