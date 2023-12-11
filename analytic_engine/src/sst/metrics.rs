// Copyright 2023 The HoraeDB Authors
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

use std::sync::atomic::{AtomicU64, Ordering};

use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets, register_counter, register_histogram, register_histogram_vec,
    register_int_counter_vec, Counter, Histogram, HistogramVec, IntCounter, IntCounterVec,
};

lazy_static! {
    // Histogram:
    // Buckets: 100B,200B,400B,...,2KB
    pub static ref SST_GET_RANGE_HISTOGRAM: Histogram = register_histogram!(
        "sst_get_range_length",
        "Histogram for sst get range length",
        exponential_buckets(100.0, 2.0, 5).unwrap()
    ).unwrap();

    pub static ref META_DATA_CACHE_HIT_COUNTER: Counter = register_counter!(
        "META_DATA_CACHE_HIT",
        "The counter for meta data cache hit"
    ).unwrap();

    pub static ref META_DATA_CACHE_MISS_COUNTER: Counter = register_counter!(
        "META_DATA_CACHE_MISS",
        "The counter for meta data cache miss"
    ).unwrap();

    static ref ROW_GROUP_BEFORE_PRUNE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "row_group_before_prune",
        "The counter for row group before prune",
        &["table"]
    ).unwrap();

    static ref ROW_GROUP_AFTER_PRUNE_COUNTER: IntCounterVec = register_int_counter_vec!(
        "row_group_after_prune",
        "The counter for row group after prune",
        &["table"]
    ).unwrap();

    static ref FETCHED_SST_BYTES_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "fetched_sst_bytes",
        "Histogram for sst get range length",
        &["table"],
        exponential_buckets(100.0, 2.0, 5).unwrap()
    ).unwrap();
}

#[derive(Debug)]
pub struct MaybeTableLevelMetrics {
    pub row_group_before_prune_counter: IntCounter,
    pub row_group_after_prune_counter: IntCounter,
    pub fetched_sst_bytes_hist: Histogram,
    pub fetched_sst_bytes: AtomicU64,
}

impl MaybeTableLevelMetrics {
    pub fn new(table: &str) -> Self {
        Self {
            row_group_before_prune_counter: ROW_GROUP_BEFORE_PRUNE_COUNTER
                .with_label_values(&[table]),
            row_group_after_prune_counter: ROW_GROUP_AFTER_PRUNE_COUNTER
                .with_label_values(&[table]),
            fetched_sst_bytes_hist: FETCHED_SST_BYTES_HISTOGRAM.with_label_values(&[table]),
            fetched_sst_bytes: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn observe_fetched_sst_bytes(&self) {
        self.fetched_sst_bytes_hist
            .observe(self.fetched_sst_bytes.load(Ordering::Relaxed) as f64)
    }
}

impl Drop for MaybeTableLevelMetrics {
    fn drop(&mut self) {
        self.observe_fetched_sst_bytes();
    }
}
