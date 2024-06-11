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

//! MemTable

pub mod columnar;
pub mod error;
pub mod factory;
pub mod key;
pub mod layered;
mod reversed_iter;
pub mod skiplist;
pub mod test_util;

use std::{ops::Bound, sync::Arc, time::Instant};

use bytes_ext::{ByteVec, Bytes};
use common_types::{
    projected_schema::RowProjectorBuilder,
    record_batch::FetchedRecordBatch,
    row::Row,
    schema::{IndexInWriterSchema, Schema},
    time::TimeRange,
    SequenceNumber,
};
pub use error::Error;
use horaedbproto::manifest;
use macros::define_result;
use serde::{Deserialize, Serialize};
use size_ext::ReadableSize;
use trace_metric::MetricsCollector;

use crate::memtable::key::KeySequence;

const DEFAULT_SCAN_BATCH_SIZE: usize = 500;
const MEMTABLE_TYPE_SKIPLIST: &str = "skiplist";
const MEMTABLE_TYPE_COLUMNAR: &str = "columnar";

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub enum MemtableType {
    SkipList,
    Column,
}

impl MemtableType {
    pub fn parse_from(s: &str) -> Self {
        if s.eq_ignore_ascii_case(MEMTABLE_TYPE_COLUMNAR) {
            MemtableType::Column
        } else {
            MemtableType::SkipList
        }
    }
}

impl ToString for MemtableType {
    fn to_string(&self) -> String {
        match self {
            MemtableType::SkipList => MEMTABLE_TYPE_SKIPLIST.to_string(),
            MemtableType::Column => MEMTABLE_TYPE_COLUMNAR.to_string(),
        }
    }
}

/// Layered memtable options
/// If `mutable_segment_switch_threshold` is set zero, layered memtable will be
/// disable.
#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct LayeredMemtableOptions {
    pub enable: bool,
    pub mutable_segment_switch_threshold: ReadableSize,
}

impl LayeredMemtableOptions {
    #[inline]
    pub fn enable_layered_memtable(&self) -> bool {
        self.enable && self.mutable_segment_switch_threshold.0 > 0
    }
}

impl Default for LayeredMemtableOptions {
    fn default() -> Self {
        Self {
            enable: false,
            mutable_segment_switch_threshold: ReadableSize::mb(3),
        }
    }
}

impl From<manifest::LayeredMemtableOptions> for LayeredMemtableOptions {
    fn from(value: manifest::LayeredMemtableOptions) -> Self {
        // For compatibility here.
        // Layered memtable is enabled default in former,
        // so some horaedb service is running with layered memtable in production
        // and we shouldn't make difference to such exist running services
        // after switching to control layered memtable's on/off with the new added
        // `enable` field in manifest(that says `enable` should assume to true when not
        // exist).
        // However, pb version used now don't support to define default value
        // explicitly, and default value of bool is always false...
        // So we use `disable` rather than `enable` in pb to reach it
        // (disable: false --> enable: true).
        let enable = !value.disable;
        let mutable_segment_switch_threshold = ReadableSize(value.mutable_segment_switch_threshold);

        Self {
            enable,
            mutable_segment_switch_threshold,
        }
    }
}

impl From<LayeredMemtableOptions> for manifest::LayeredMemtableOptions {
    fn from(value: LayeredMemtableOptions) -> Self {
        Self {
            mutable_segment_switch_threshold: value.mutable_segment_switch_threshold.0,
            disable: !value.enable,
        }
    }
}

define_result!(error::Error);

/// Options for put and context for tracing
pub struct PutContext {
    /// Buffer for encoding key, can reuse during put
    pub key_buf: ByteVec,
    /// Buffer for encoding value, can reuse during put
    pub value_buf: ByteVec,
    /// Used to encode row.
    pub index_in_writer: IndexInWriterSchema,
}

impl PutContext {
    pub fn new(index_in_writer: IndexInWriterSchema) -> Self {
        Self {
            key_buf: ByteVec::new(),
            value_buf: ByteVec::new(),
            index_in_writer,
        }
    }
}

/// Options for scan and context for tracing
#[derive(Debug, Clone)]
pub struct ScanContext {
    /// Suggested row number per batch
    pub batch_size: usize,
    pub deadline: Option<Instant>,
}

impl Default for ScanContext {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_SCAN_BATCH_SIZE,
            deadline: None,
        }
    }
}

/// Scan request
///
/// Now we only support forward scan.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    /// The start key of the encoded user key (without sequence).
    pub start_user_key: Bound<Bytes>,
    /// The end key of the encoded user key (without sequence).
    pub end_user_key: Bound<Bytes>,
    /// Max visible sequence (inclusive), row key with sequence <= this can be
    /// visible.
    pub sequence: SequenceNumber,
    /// Schema and projection to read.
    pub row_projector_builder: RowProjectorBuilder,
    pub need_dedup: bool,
    pub reverse: bool,
    /// Collector for scan metrics.
    pub metrics_collector: Option<MetricsCollector>,
    pub time_range: TimeRange,
}

/// In memory storage for table's data.
///
/// # Concurrency
/// The memtable is designed for single-writer and multiple-reader usage, so
/// not all function supports concurrent writer, the caller should guarantee not
/// writing to the memtable concurrently.
// All operation is done in memory, no need to use async trait
pub trait MemTable {
    /// Schema of this memtable
    ///
    /// The schema of a memtable is not allowed to change now. Modifying the
    /// schema of a table requires a memtable switch and external
    /// synchronization
    fn schema(&self) -> &Schema;

    /// Peek the min key of this memtable.
    fn min_key(&self) -> Option<Bytes>;

    /// Peek the max key of this memtable.
    fn max_key(&self) -> Option<Bytes>;

    /// Insert one row into the memtable.
    ///
    /// .- ctx: The put context
    /// - sequence: The sequence of the row
    /// - row: The row to insert
    /// - schema: The schema of the row
    ///
    /// REQUIRE:
    /// - The schema of RowGroup must equal to the schema of memtable. How to
    /// handle duplicate entries is implementation specific.
    fn put(
        &self,
        ctx: &mut PutContext,
        sequence: KeySequence,
        row_group: &Row,
        schema: &Schema,
    ) -> Result<()>;

    /// Scan the memtable.
    ///
    /// Returns the data in columnar format. The returned rows is guaranteed
    /// to be ordered by the primary key.
    fn scan(&self, ctx: ScanContext, request: ScanRequest) -> Result<ColumnarIterPtr>;

    /// Returns an estimate of the number of bytes of data in used
    fn approximate_memory_usage(&self) -> usize;

    /// Set last sequence of the memtable, returns error if the given `sequence`
    /// is less than existing last sequence.
    ///
    /// REQUIRE:
    /// - External synchronization is required.
    fn set_last_sequence(&self, sequence: SequenceNumber) -> Result<()>;

    /// Returns the last sequence of the memtable.
    ///
    /// If the memtable is empty, then the last sequence is 0.
    fn last_sequence(&self) -> SequenceNumber;

    /// Time range of written rows.
    fn time_range(&self) -> Option<TimeRange>;

    /// Metrics of inner state.
    fn metrics(&self) -> Metrics;
}

#[derive(Debug, Default)]
pub struct Metrics {
    /// Size of original rows.
    pub row_raw_size: usize,
    /// Size of rows after encoded.
    pub row_encoded_size: usize,
    /// Row number count.
    pub row_count: usize,
}

/// A reference to memtable
pub type MemTableRef = Arc<dyn MemTable + Send + Sync>;

/// A pointer to columnar iterator
pub type ColumnarIterPtr = Box<dyn Iterator<Item = Result<FetchedRecordBatch>> + Send + Sync>;
