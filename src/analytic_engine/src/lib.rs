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

//! Analytic table engine implementations

#![feature(option_get_or_insert_default)]

mod compaction;
mod context;
mod engine;
pub mod error;
mod instance;
mod manifest;
pub mod memtable;
mod payload;
pub mod prefetchable_stream;
pub mod row_iter;
mod sampler;
pub mod setup;
pub mod space;
pub mod sst;
pub mod table;
pub mod table_options;

pub mod table_meta_set_impl;
#[cfg(any(test, feature = "test"))]
pub mod tests;

use error::ErrorKind;
use manifest::details::Options as ManifestOptions;
use object_store::config::StorageOptions;
use serde::{Deserialize, Serialize};
use size_ext::ReadableSize;
use time_ext::ReadableDuration;
use wal::config::Config as WalConfig;

pub use crate::{
    compaction::scheduler::SchedulerConfig,
    instance::{ScanType, SstReadOptionsBuilder},
    table_options::TableOptions,
};

/// Config of analytic engine
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    /// Storage options of the engine
    pub storage: StorageOptions,

    /// Batch size to read records from wal to replay
    pub replay_batch_size: usize,
    /// Batch size to replay tables
    pub max_replay_tables_per_batch: usize,

    /// Default options for table
    pub table_opts: TableOptions,

    pub compaction: SchedulerConfig,

    /// sst meta cache capacity
    pub sst_meta_cache_cap: Option<usize>,
    /// sst data cache capacity
    pub sst_data_cache_cap: Option<usize>,

    /// Manifest options
    pub manifest: ManifestOptions,

    /// The maximum rows in the write queue.
    pub max_rows_in_write_queue: usize,
    /// The maximum write buffer size used for single space.
    pub space_write_buffer_size: usize,
    /// The maximum size of all Write Buffers across all spaces.
    pub db_write_buffer_size: usize,
    /// The ratio of table's write buffer size to trigger preflush, and it
    /// should be in the range (0, 1].
    pub preflush_write_buffer_size_ratio: f32,

    pub enable_primary_key_sampling: bool,

    // Iterator scanning options
    /// Batch size for iterator.
    ///
    /// The `num_rows_per_row_group` in `table options` will be used if this is
    /// not set.
    pub scan_batch_size: Option<usize>,
    /// Max record batches in flight when scan
    pub scan_max_record_batches_in_flight: usize,
    /// Sst background reading parallelism
    pub sst_background_read_parallelism: usize,
    /// Number of streams to prefetch
    pub num_streams_to_prefetch: usize,
    /// Max buffer size for writing sst
    pub write_sst_max_buffer_size: ReadableSize,
    /// Max retry limit After flush failed
    pub max_retry_flush_limit: usize,
    /// The min interval between two consecutive flushes
    pub min_flush_interval: ReadableDuration,
    /// Max bytes per write batch.
    ///
    /// If this is set, the atomicity of write request will be broken.
    pub max_bytes_per_write_batch: Option<ReadableSize>,
    /// The interval for sampling the memory usage
    pub mem_usage_sampling_interval: ReadableDuration,
    /// The config for log in the wal.
    // TODO: move this to WalConfig.
    pub wal_encode: WalEncodeConfig,

    /// Wal storage config
    ///
    /// Now, following three storages are supported:
    /// + RocksDB
    /// + OBKV
    /// + Kafka
    pub wal: WalConfig,

    /// Recover mode
    ///
    /// + TableBased, tables on same shard will be recovered table by table.
    /// + ShardBased, tables on same shard will be recovered together.
    pub recover_mode: RecoverMode,

    pub remote_engine_client: remote_engine_client::config::Config,

    pub metrics: MetricsOptions,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct MetricsOptions {
    enable_table_level_metrics: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum RecoverMode {
    TableBased,
    ShardBased,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum WalEncodeFormat {
    RowWise,
    Columnar,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WalEncodeConfig {
    /// The threshold of columnar bytes to do compression.
    pub num_bytes_compress_threshold: ReadableSize,
    /// Encode the data in a columnar layout if it is set.
    pub format: WalEncodeFormat,
}

impl Default for WalEncodeConfig {
    fn default() -> Self {
        Self {
            num_bytes_compress_threshold: ReadableSize::kb(1),
            format: WalEncodeFormat::RowWise,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: Default::default(),
            replay_batch_size: 500,
            max_replay_tables_per_batch: 64,
            table_opts: TableOptions::default(),
            compaction: SchedulerConfig::default(),
            sst_meta_cache_cap: Some(1000),
            sst_data_cache_cap: Some(1000),
            manifest: ManifestOptions::default(),
            max_rows_in_write_queue: 0,
            // Zero means disabling this param, give a positive value to enable
            // it.
            space_write_buffer_size: 0,
            // Zero means disabling this param, give a positive value to enable
            // it.
            db_write_buffer_size: 0,
            preflush_write_buffer_size_ratio: 0.75,
            enable_primary_key_sampling: false,
            scan_batch_size: None,
            sst_background_read_parallelism: 8,
            num_streams_to_prefetch: 2,
            scan_max_record_batches_in_flight: 1024,
            write_sst_max_buffer_size: ReadableSize::mb(10),
            max_retry_flush_limit: 0,
            min_flush_interval: ReadableDuration::minutes(1),
            max_bytes_per_write_batch: None,
            mem_usage_sampling_interval: ReadableDuration::secs(0),
            wal_encode: WalEncodeConfig::default(),
            wal: WalConfig::default(),
            remote_engine_client: remote_engine_client::config::Config::default(),
            recover_mode: RecoverMode::TableBased,
            metrics: MetricsOptions::default(),
        }
    }
}
