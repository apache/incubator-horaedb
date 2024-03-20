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

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    default::Default,
    fmt::Debug,
};

use bytes::Bytes;
use common_types::{datum::DatumKind, record_batch::RecordBatch, schema::RecordSchema};
use generic_error::BoxError;
use horaedbproto::storage::{
    value, Field, FieldGroup, Tag, Value as ProtoValue, WriteSeriesEntry, WriteTableRequest,
};
use http::StatusCode;
use interpreters::interpreter::Output;
use query_frontend::opentsdb::DEFAULT_FIELD;
use serde::{Deserialize, Serialize};
use serde_json::from_slice;
use snafu::{ensure, OptionExt, ResultExt};
use time_ext::try_to_millis;

use crate::error::{ErrNoCause, ErrWithCause, InternalNoCause, Result};

#[derive(Debug)]
pub struct PutRequest {
    pub points: Bytes,

    pub summary: Option<String>,
    pub details: Option<String>,
    pub sync: Option<String>,
    pub sync_timeout: i32,
}

impl PutRequest {
    pub fn new(points: Bytes, params: PutParams) -> Self {
        PutRequest {
            points,
            summary: params.summary,
            details: params.details,
            sync: params.sync,
            sync_timeout: params.sync_timeout,
        }
    }
}

pub type PutResponse = ();

/// Query string parameters for put api
///
/// It's derived from query string parameters of put described in
/// doc of OpenTSDB 2.4:
///     http://opentsdb.net/docs/build/html/api_http/put.html#requests
///
/// NOTE:
///     - all the params is unimplemented.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct PutParams {
    pub summary: Option<String>,
    pub details: Option<String>,
    pub sync: Option<String>,
    pub sync_timeout: i32,
}

#[derive(Debug, Deserialize)]
pub struct Point {
    pub metric: String,
    pub timestamp: i64,
    pub value: Value,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Value {
    // TODO: telegraf parse 0.0 as 0, which will confuse int and double type
    // IntegerValue(i64),
    F64Value(f64),
}

pub(crate) fn convert_put_request(req: PutRequest) -> Result<Vec<WriteTableRequest>> {
    let points = {
        // multi points represent as json array
        let parse_array = from_slice::<Vec<Point>>(&req.points);
        match parse_array {
            Ok(points) => Ok(points),
            Err(_e) => {
                // single point represent as json object
                let parse_object = from_slice::<Point>(&req.points);
                match parse_object {
                    Ok(point) => Ok(vec![point]),
                    Err(e) => Err(e),
                }
            }
        }
    };
    let points = points.box_err().with_context(|| ErrWithCause {
        code: StatusCode::BAD_REQUEST,
        msg: "Json parse error".to_string(),
    })?;
    validate(&points)?;

    let mut points_per_metric = HashMap::with_capacity(100);
    for point in points {
        points_per_metric
            .entry(point.metric.clone())
            .or_insert(Vec::new())
            .push(point);
    }

    let mut requests = Vec::with_capacity(points_per_metric.len());
    for (metric, points) in points_per_metric {
        let mut tag_names_set = HashSet::with_capacity(points[0].tags.len() * 2);
        for point in &points {
            for tag_name in point.tags.keys() {
                tag_names_set.insert(tag_name.clone());
            }
        }

        let mut tag_name_to_tag_index: HashMap<String, u32> =
            HashMap::with_capacity(tag_names_set.len());
        let mut tag_names = Vec::with_capacity(tag_names_set.len());
        for (idx, tag_name) in tag_names_set.into_iter().enumerate() {
            tag_name_to_tag_index.insert(tag_name.clone(), idx as u32);
            tag_names.push(tag_name);
        }

        let mut req = WriteTableRequest {
            table: metric,
            tag_names,
            field_names: vec![String::from(DEFAULT_FIELD)],
            entries: Vec::with_capacity(points.len()),
        };

        for point in points {
            let timestamp = point.timestamp;
            let timestamp = try_to_millis(timestamp)
                .with_context(|| ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: format!("Invalid timestamp: {}", point.timestamp),
                })?
                .as_i64();

            let mut tags = Vec::with_capacity(point.tags.len());
            for (tag_name, tag_value) in point.tags {
                let &tag_index = tag_name_to_tag_index.get(&tag_name).unwrap();
                tags.push(Tag {
                    name_index: tag_index,
                    value: Some(ProtoValue {
                        value: Some(value::Value::StringValue(tag_value)),
                    }),
                });
            }

            let value = match point.value {
                Value::F64Value(v) => value::Value::Float64Value(v),
            };
            let fields = vec![Field {
                name_index: 0,
                value: Some(ProtoValue { value: Some(value) }),
            }];

            let field_groups = vec![FieldGroup { timestamp, fields }];

            req.entries.push(WriteSeriesEntry { tags, field_groups });
        }
        requests.push(req);
    }

    Ok(requests)
}

pub(crate) fn validate(points: &[Point]) -> Result<()> {
    for point in points {
        if point.metric.is_empty() {
            return ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Metric must not be empty",
            }
            .fail();
        }
        if point.tags.is_empty() {
            return ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "At least one tag must be supplied",
            }
            .fail();
        }
        for tag_name in point.tags.keys() {
            if tag_name.is_empty() {
                return ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: "Tag name must not be empty",
                }
                .fail();
            }
        }
    }

    Ok(())
}

// http://opentsdb.net/docs/build/html/api_http/query/index.html#response
#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct QueryResponse {
    pub(crate) metric: String,
    /// A list of tags only returned when the results are for a single time
    /// series. If results are aggregated, this value may be null or an empty
    /// map
    pub(crate) tags: BTreeMap<String, String>,
    /// If more than one timeseries were included in the result set, i.e. they
    /// were aggregated, this will display a list of tag names that were found
    /// in common across all time series.
    #[serde(rename = "aggregatedTags")]
    pub(crate) aggregated_tags: Vec<String>,
    pub(crate) dps: BTreeMap<String, f64>,
}

#[derive(Default)]
struct QueryConverter {
    metric: String,
    timestamp_idx: usize,
    value_idx: usize,
    // (column_name, index)
    tags_idx: Vec<(String, usize)>,
    aggregated_tags: Vec<String>,
    // (time_series, (tagk, tagv))
    tags: BTreeMap<String, BTreeMap<String, String>>,
    // (time_series, (timestamp, value))
    values: BTreeMap<String, BTreeMap<String, f64>>,

    resp: Vec<QueryResponse>,
}

impl QueryConverter {
    fn try_new(
        schema: &RecordSchema,
        metric: String,
        timestamp_col_name: String,
        field_col_name: String,
        tags: Vec<String>,
        aggregated_tags: Vec<String>,
    ) -> Result<Self> {
        let timestamp_idx = schema
            .index_of(&timestamp_col_name)
            .context(InternalNoCause {
                msg: "Timestamp column is missing in query response",
            })?;

        let value_idx = schema.index_of(&field_col_name).context(InternalNoCause {
            msg: "Value column is missing in query response",
        })?;

        let tags_idx = tags
            .iter()
            .map(|tag| {
                let idx = schema.index_of(tag).context(InternalNoCause {
                    msg: format!("Tag column is missing in query response, tag:{}", tag),
                })?;
                ensure!(
                    matches!(schema.column(idx).data_type, DatumKind::String),
                    InternalNoCause {
                        msg: format!(
                            "Tag must be string type, current:{}",
                            schema.column(idx).data_type
                        )
                    }
                );
                Ok((tag.to_string(), idx))
            })
            .collect::<Result<Vec<_>>>()?;

        ensure!(
            schema.column(timestamp_idx).data_type.is_timestamp(),
            InternalNoCause {
                msg: format!(
                    "Timestamp wrong type, current:{}",
                    schema.column(timestamp_idx).data_type
                )
            }
        );

        ensure!(
            schema.column(value_idx).data_type.is_f64_castable(),
            InternalNoCause {
                msg: format!(
                    "Value must be f64 compatible type, current:{}",
                    schema.column(value_idx).data_type
                )
            }
        );

        Ok(QueryConverter {
            metric,
            timestamp_idx,
            value_idx,
            tags_idx,
            aggregated_tags,
            ..Default::default()
        })
    }

    fn add_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        let row_num = record_batch.num_rows();
        for row_idx in 0..row_num {
            let mut tags = BTreeMap::new();
            // tags_key is used to identify a time series
            let mut tags_key = String::new();
            for (tag_key, idx) in &self.tags_idx {
                let tag_value = record_batch
                    .column(*idx)
                    .datum(row_idx)
                    .as_str()
                    .context(InternalNoCause {
                        msg: "Tag must be String compatible type".to_string(),
                    })?
                    .to_string();
                tags_key += &tag_value;
                tags.insert(tag_key.clone(), tag_value);
            }

            let timestamp = record_batch
                .column(self.timestamp_idx)
                .datum(row_idx)
                .as_timestamp()
                .context(InternalNoCause {
                    msg: "Timestamp wrong type".to_string(),
                })?
                .as_i64()
                .to_string();

            let value = record_batch
                .column(self.value_idx)
                .datum(row_idx)
                .as_f64()
                .context(InternalNoCause {
                    msg: "Value must be f64 compatible type".to_string(),
                })?;

            if let Some(values) = self.values.get_mut(&tags_key) {
                values.insert(timestamp, value);
            } else {
                self.tags.insert(tags_key.clone(), tags);
                self.values
                    .entry(tags_key)
                    .or_default()
                    .insert(timestamp, value);
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Vec<QueryResponse> {
        for (key, tags) in self.tags {
            let dps = self.values.remove(&key).unwrap();
            self.resp.push(QueryResponse {
                metric: self.metric.clone(),
                tags,
                aggregated_tags: self.aggregated_tags.clone(),
                dps,
            });
        }

        self.resp
    }
}

pub(crate) fn convert_output_to_response(
    output: Output,
    metric: String,
    field_col_name: String,
    timestamp_col_name: String,
    tags: Vec<String>,
    aggregated_tags: Vec<String>,
) -> Result<Vec<QueryResponse>> {
    let records = match output {
        Output::Records(records) => records,
        Output::AffectedRows(_) => {
            return InternalNoCause {
                msg: "output in opentsdb query should not be affected rows",
            }
            .fail()
        }
    };

    let mut converter = match records.first() {
        None => {
            return Ok(Vec::new());
        }
        Some(batch) => {
            let record_schema = batch.schema();
            QueryConverter::try_new(
                record_schema,
                metric,
                timestamp_col_name,
                field_col_name,
                tags,
                aggregated_tags,
            )?
        }
    };

    for record in records {
        converter.add_batch(record)?;
    }

    Ok(converter.finish())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray, UInt64Array},
        record_batch::RecordBatch as ArrowRecordBatch,
    };
    use common_types::{
        column_schema,
        datum::DatumKind,
        record_batch::RecordBatch,
        schema::{self, Schema, TIMESTAMP_COLUMN, TSID_COLUMN},
    };
    use interpreters::RecordBatchVec;
    use query_frontend::opentsdb::DEFAULT_FIELD;

    use super::*;

    fn build_schema() -> Schema {
        schema::Builder::new()
            .auto_increment_column_id(true)
            .primary_key_indexes(vec![0, 1])
            .add_key_column(
                column_schema::Builder::new(TSID_COLUMN.to_string(), DatumKind::UInt64)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new(TIMESTAMP_COLUMN.to_string(), DatumKind::Timestamp)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new(DEFAULT_FIELD.to_string(), DatumKind::Double)
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
            .build()
            .unwrap()
    }

    fn build_record_batch(schema: &Schema) -> RecordBatchVec {
        let tsid: ArrayRef = Arc::new(UInt64Array::from(vec![1, 1, 2, 3, 3]));
        let timestamp: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            11111111, 11111112, 11111113, 11111111, 11111112,
        ]));
        let values: ArrayRef =
            Arc::new(Float64Array::from(vec![100.0, 101.0, 200.0, 300.0, 301.0]));
        let tag1: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b", "c", "c"]));
        let tag2: ArrayRef = Arc::new(StringArray::from(vec!["x", "x", "y", "z", "z"]));

        let batch = ArrowRecordBatch::try_new(
            schema.to_arrow_schema_ref(),
            vec![tsid, timestamp, values, tag1, tag2],
        )
        .unwrap();

        vec![RecordBatch::try_from(batch).unwrap()]
    }

    #[test]
    fn test_convert_output_to_response() {
        let metric = "metric".to_string();
        let tags = vec!["tag1".to_string(), "tag2".to_string()];
        let schema = build_schema();
        let record_batch = build_record_batch(&schema);
        let result = convert_output_to_response(
            Output::Records(record_batch),
            metric.clone(),
            DEFAULT_FIELD.to_string(),
            TIMESTAMP_COLUMN.to_string(),
            tags,
            vec![],
        )
        .unwrap();

        assert_eq!(
            vec![
                QueryResponse {
                    metric: metric.clone(),
                    tags: vec![
                        ("tag1".to_string(), "a".to_string()),
                        ("tag2".to_string(), "x".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                    aggregated_tags: vec![],
                    dps: vec![
                        ("11111111".to_string(), 100.0),
                        ("11111112".to_string(), 101.0),
                    ]
                    .into_iter()
                    .collect(),
                },
                QueryResponse {
                    metric: metric.clone(),
                    tags: vec![
                        ("tag1".to_string(), "b".to_string()),
                        ("tag2".to_string(), "y".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                    aggregated_tags: vec![],
                    dps: vec![("11111113".to_string(), 200.0),].into_iter().collect(),
                },
                QueryResponse {
                    metric: metric.clone(),
                    tags: vec![
                        ("tag1".to_string(), "c".to_string()),
                        ("tag2".to_string(), "z".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                    aggregated_tags: vec![],
                    dps: vec![
                        ("11111111".to_string(), 300.0),
                        ("11111112".to_string(), 301.0),
                    ]
                    .into_iter()
                    .collect(),
                },
            ],
            result
        );
    }
}
