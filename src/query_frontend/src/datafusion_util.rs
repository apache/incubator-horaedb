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

use common_types::{schema::TSID_COLUMN, time::TimeRange};
use datafusion::{
    logical_expr::Between,
    prelude::{col, lit, Expr},
};

pub fn timerange_to_expr(query_range: &TimeRange, column_name: &str) -> Expr {
    Expr::Between(Between {
        expr: Box::new(col(column_name)),
        negated: false,
        low: Box::new(lit(query_range.inclusive_start().as_i64())),
        high: Box::new(lit(query_range.exclusive_end().as_i64() - 1)),
    })
}

pub fn default_sort_exprs(timestamp_column: &str) -> Vec<Expr> {
    vec![
        col(TSID_COLUMN).sort(true, true),
        col(timestamp_column).sort(true, true),
    ]
}
