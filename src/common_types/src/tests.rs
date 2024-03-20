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

use bytes_ext::Bytes;
use sqlparser::ast::{BinaryOperator, Expr, Value};

use crate::{
    column_schema,
    datum::{Datum, DatumKind},
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{FetchedRecordBatch, FetchedRecordBatchBuilder},
    row::{
        contiguous::{ContiguousRowReader, ContiguousRowWriter, ProjectedContiguousRow},
        Row,
    },
    schema,
    schema::{IndexInWriterSchema, Schema, TSID_COLUMN},
    string::StringBytes,
    time::Timestamp,
};

fn base_schema_builder() -> schema::Builder {
    schema::Builder::new()
        .auto_increment_column_id(true)
        .add_key_column(
            column_schema::Builder::new("key1".to_string(), DatumKind::Varbinary)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("key2".to_string(), DatumKind::Timestamp)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field1".to_string(), DatumKind::Double)
                .is_nullable(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field2".to_string(), DatumKind::String)
                .is_nullable(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field3".to_string(), DatumKind::Date)
                .is_nullable(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field4".to_string(), DatumKind::Time)
                .is_nullable(true)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .primary_key_indexes(vec![0, 1])
}

fn default_value_schema_builder() -> schema::Builder {
    schema::Builder::new()
        .auto_increment_column_id(true)
        .primary_key_indexes(vec![0, 1])
        .add_key_column(
            column_schema::Builder::new("key1".to_string(), DatumKind::Varbinary)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("key2".to_string(), DatumKind::Timestamp)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            // The data type of column and its default value will not be the same in most time.
            // So we need check if the type coercion is legal and do type coercion when legal.
            // In he following, the data type of column is `Int64`, and the type of default value
            // expr is `Int64`. So we use this column to cover the test, which has the same type.
            column_schema::Builder::new("field1".to_string(), DatumKind::Int64)
                .default_value(Some(Expr::Value(Value::Number("10".to_string(), false))))
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            // The data type of column is `UInt32`, and the type of default value expr is `Int64`.
            // So we use this column to cover the test, which has different type.
            column_schema::Builder::new("field2".to_string(), DatumKind::UInt32)
                .default_value(Some(Expr::Value(Value::Number("20".to_string(), false))))
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field3".to_string(), DatumKind::UInt32)
                .default_value(Some(Expr::BinaryOp {
                    left: Box::new(Expr::Value(Value::Number("1".to_string(), false))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Value(Value::Number("2".to_string(), false))),
                }))
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field4".to_string(), DatumKind::UInt32)
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field5".to_string(), DatumKind::UInt32)
                .default_value(Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier("field4".into())),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Value(Value::Number("2".to_string(), false))),
                }))
                .build()
                .expect("should succeed build column schema"),
        )
        .unwrap()
}

/// Build a schema for testing, which contains 6 columns:
/// - key1(varbinary)
/// - key2(timestamp)
/// - field1(double)
/// - field2(string)
/// - field3(Time)
/// - field4(Date)
pub fn build_schema() -> Schema {
    base_schema_builder().build().unwrap()
}

/// Build a schema for testing:
/// key1(varbinary), key2(timestamp),
/// field1(int64, default 10),
/// field2(uint32, default 20),
/// field3(uint32, default 1 + 2)
/// field4(uint32),
/// field5(uint32, default field4 + 2)
pub fn build_default_value_schema() -> Schema {
    default_value_schema_builder().build().unwrap()
}

/// Build a schema for testing:
/// (key1(varbinary), key2(timestamp), field1(double), field2(string),
/// field3(date), field4(time)) tag1(string dictionary), tag2(string dictionary)
pub fn build_schema_with_dictionary() -> Schema {
    let builder = base_schema_builder()
        .add_normal_column(
            column_schema::Builder::new("tag1".to_string(), DatumKind::String)
                .is_tag(true)
                .is_dictionary(true)
                .is_nullable(true)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("tag2".to_string(), DatumKind::String)
                .is_tag(true)
                .is_dictionary(true)
                .build()
                .unwrap(),
        )
        .unwrap();

    builder.build().unwrap()
}

/// Build a schema for testing:
/// (tsid(uint64), key2(timestamp), tag1(string), tag2(string), value(int8),
/// field2(float))
pub fn build_schema_for_cpu() -> Schema {
    let builder = schema::Builder::new()
        .auto_increment_column_id(true)
        .add_key_column(
            column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("time".to_string(), DatumKind::Timestamp)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("tag1".to_string(), DatumKind::String)
                .is_tag(true)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("tag2".to_string(), DatumKind::String)
                .is_tag(true)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("value".to_string(), DatumKind::Int8)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("field2".to_string(), DatumKind::Float)
                .build()
                .unwrap(),
        )
        .unwrap();

    builder.primary_key_indexes(vec![0, 1]).build().unwrap()
}

/// Build a schema for testing:
/// (tsid(uint64), key2(timestamp), tag1(string), tag2(string), value(double),
pub fn build_schema_for_metric() -> Schema {
    let builder = schema::Builder::new()
        .auto_increment_column_id(true)
        .add_key_column(
            column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_key_column(
            column_schema::Builder::new("timestamp".to_string(), DatumKind::Timestamp)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("tag1".to_string(), DatumKind::String)
                .is_tag(true)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("tag2".to_string(), DatumKind::String)
                .is_tag(true)
                .build()
                .unwrap(),
        )
        .unwrap()
        .add_normal_column(
            column_schema::Builder::new("value".to_string(), DatumKind::Double)
                .build()
                .unwrap(),
        )
        .unwrap();

    builder.primary_key_indexes(vec![0, 1]).build().unwrap()
}

#[allow(clippy::too_many_arguments)]
pub fn build_row_for_dictionary(
    key1: &[u8],
    key2: i64,
    field1: f64,
    field2: &str,
    field3: i32,
    field4: i64,
    tag1: Option<&str>,
    tag2: &str,
) -> Row {
    let datums = vec![
        Datum::Varbinary(Bytes::copy_from_slice(key1)),
        Datum::Timestamp(Timestamp::new(key2)),
        Datum::Double(field1),
        Datum::String(StringBytes::from(field2)),
        Datum::Date(field3),
        Datum::Time(field4),
        tag1.map(|v| Datum::String(StringBytes::from(v)))
            .unwrap_or(Datum::Null),
        Datum::String(StringBytes::from(tag2)),
    ];

    Row::from_datums(datums)
}

pub fn build_row_for_cpu(
    tsid: u64,
    ts: i64,
    tag1: &str,
    tag2: &str,
    value: i8,
    field2: f32,
) -> Row {
    let datums = vec![
        Datum::UInt64(tsid),
        Datum::Timestamp(Timestamp::new(ts)),
        Datum::String(StringBytes::from(tag1)),
        Datum::String(StringBytes::from(tag2)),
        Datum::Int8(value),
        Datum::Float(field2),
    ];

    Row::from_datums(datums)
}

pub fn build_projected_schema() -> ProjectedSchema {
    let schema = build_schema();
    assert!(schema.num_columns() > 1);
    let projection: Vec<usize> = (0..schema.num_columns() - 1).collect();
    ProjectedSchema::new(schema, Some(projection)).unwrap()
}

pub fn build_row(
    key1: &[u8],
    key2: i64,
    field1: f64,
    field2: &str,
    field3: i32,
    field4: i64,
) -> Row {
    let datums = vec![
        Datum::Varbinary(Bytes::copy_from_slice(key1)),
        Datum::Timestamp(Timestamp::new(key2)),
        Datum::Double(field1),
        Datum::String(StringBytes::from(field2)),
        Datum::Date(field3),
        Datum::Time(field4),
    ];

    Row::from_datums(datums)
}

pub fn build_row_opt(
    key1: &[u8],
    key2: i64,
    field1: Option<f64>,
    field2: Option<&str>,
    field3: Option<i32>,
    field4: Option<i64>,
) -> Row {
    let datums = vec![
        Datum::Varbinary(Bytes::copy_from_slice(key1)),
        Datum::Timestamp(Timestamp::new(key2)),
        field1.map(Datum::Double).unwrap_or(Datum::Null),
        field2
            .map(|v| Datum::String(StringBytes::from(v)))
            .unwrap_or(Datum::Null),
        field3.map(Datum::Date).unwrap_or(Datum::Null),
        field4.map(Datum::Time).unwrap_or(Datum::Null),
    ];

    Row::from_datums(datums)
}

pub fn build_rows() -> Vec<Row> {
    vec![
        build_row(b"binary key", 1000000, 10.0, "string value", 0, 0),
        build_row(
            b"binary key1",
            1000001,
            11.0,
            "string value 1",
            1000,
            1000000,
        ),
        build_row_opt(
            b"binary key2",
            1000002,
            None,
            Some("string value 2"),
            Some(1000),
            Some(1000000),
        ),
        build_row_opt(b"binary key3", 1000003, Some(13.0), None, Some(1000), None),
        build_row_opt(b"binary key4", 1000004, None, None, None, Some(1000000)),
    ]
}

pub fn build_fetched_record_batch_by_rows(rows: Vec<Row>) -> FetchedRecordBatch {
    let schema = build_schema();
    assert!(schema.num_columns() > 1);
    let projection: Vec<usize> = (0..schema.num_columns() - 1).collect();
    let projected_schema = ProjectedSchema::new(schema.clone(), Some(projection)).unwrap();
    let row_projector =
        RowProjector::new(&projected_schema.to_record_schema(), None, &schema, &schema).unwrap();

    let mut builder =
        FetchedRecordBatchBuilder::with_capacity(row_projector.fetched_schema().clone(), None, 2);
    let index_in_writer = IndexInWriterSchema::for_same_schema(schema.num_columns());

    let mut buf = Vec::new();
    for row in rows {
        let mut writer = ContiguousRowWriter::new(&mut buf, &schema, &index_in_writer);

        writer.write_row(&row).unwrap();

        let source_row = ContiguousRowReader::try_new(&buf, &schema).unwrap();
        let projected_row = ProjectedContiguousRow::new(source_row, &row_projector);
        builder
            .append_projected_contiguous_row(&projected_row)
            .unwrap();
    }
    builder.build().unwrap()
}

pub fn check_record_batch_with_key_with_rows(
    record_batch_with_key: &FetchedRecordBatch,
    row_num: usize,
    column_num: usize,
    rows: Vec<Row>,
) -> bool {
    for (i, row) in rows.iter().enumerate().take(row_num) {
        for j in 0..column_num {
            let datum = &row[j];
            let datum2 = record_batch_with_key.column(j).datum(i);

            if *datum != datum2 {
                return false;
            }
        }
    }
    true
}
