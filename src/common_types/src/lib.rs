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

//! Contains common types

pub mod bitset;
pub mod column;
pub mod column_block;
pub mod column_schema;
pub mod datum;
pub(crate) mod hex;
pub mod projected_schema;
pub mod record_batch;
pub mod request_id;
pub mod row;
pub mod schema;
pub mod string;
pub mod table;
pub mod time;

/// Sequence number
pub type SequenceNumber = u64;
/// Maximum sequence number, all sequence number should less than this.
pub const MAX_SEQUENCE_NUMBER: u64 = u64::MAX;
/// Minimum sequence number, all sequence number should greater than this, so
/// sequence number should starts from 1.
pub const MIN_SEQUENCE_NUMBER: u64 = 0;

/// Enable ttl key
pub const OPTION_KEY_ENABLE_TTL: &str = "enable_ttl";
pub const SEGMENT_DURATION: &str = "segment_duration";
pub const ENABLE_TTL: &str = OPTION_KEY_ENABLE_TTL;
pub const TTL: &str = "ttl";
pub const ARENA_BLOCK_SIZE: &str = "arena_block_size";
pub const WRITE_BUFFER_SIZE: &str = "write_buffer_size";
pub const COMPACTION_STRATEGY: &str = "compaction_strategy";
pub const NUM_ROWS_PER_ROW_GROUP: &str = "num_rows_per_row_group";
pub const UPDATE_MODE: &str = "update_mode";
pub const COMPRESSION: &str = "compression";
pub const STORAGE_FORMAT: &str = "storage_format";
pub const MEMTABLE_TYPE: &str = "memtable_type";
pub const LAYERED_MUTABLE_SWITCH_THRESHOLD: &str = "layered_mutable_switch_threshold";
pub const LAYERED_ENABLE: &str = "layered_enable";

#[cfg(any(test, feature = "test"))]
pub mod tests;
