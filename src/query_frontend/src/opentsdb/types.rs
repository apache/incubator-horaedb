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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::plan::Plan;

#[derive(Serialize, Deserialize, Debug)]
pub struct Filter {
    pub r#type: String,
    pub tagk: String,
    pub filter: String,
    #[serde(rename = "groupBy")]
    pub group_by: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubQuery {
    pub metric: String,
    pub aggregator: String,
    #[serde(default)]
    pub rate: bool,
    pub downsample: Option<String>,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    #[serde(default)]
    pub filters: Vec<Filter>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryRequest {
    pub start: i64,
    pub end: i64,
    pub queries: Vec<SubQuery>,
    #[serde(rename = "msResolution", default)]
    pub ms_resolution: bool,
}

pub struct OpentsdbSubPlan {
    pub plan: Plan,
    pub metric: String,
    pub field_col_name: String,
    pub timestamp_col_name: String,
    pub tags: Vec<String>,
    pub aggregated_tags: Vec<String>,
}

pub struct OpentsdbQueryPlan {
    pub plans: Vec<OpentsdbSubPlan>,
}
