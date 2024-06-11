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

//! Constants for table options.

use std::{collections::HashMap, str::FromStr, string::ToString, time::Duration};

use common_types::{
    time::Timestamp, ARENA_BLOCK_SIZE, COMPACTION_STRATEGY, COMPRESSION, ENABLE_TTL,
    LAYERED_ENABLE, LAYERED_MUTABLE_SWITCH_THRESHOLD, MEMTABLE_TYPE, NUM_ROWS_PER_ROW_GROUP,
    OPTION_KEY_ENABLE_TTL, SEGMENT_DURATION, STORAGE_FORMAT, TTL, UPDATE_MODE, WRITE_BUFFER_SIZE,
};
use datafusion::parquet::basic::Compression as ParquetCompression;
use horaedbproto::manifest as manifest_pb;
use macros::define_result;
use serde::{Deserialize, Serialize};
use size_ext::ReadableSize;
use snafu::{Backtrace, GenerateBacktrace, OptionExt, ResultExt, Snafu};
use time_ext::{parse_duration, DurationExt, ReadableDuration, TimeUnit};

use crate::{
    compaction::{
        self, CompactionStrategy, SizeTieredCompactionOptions, TimeWindowCompactionOptions,
    },
    memtable::{LayeredMemtableOptions, MemtableType},
};

const UPDATE_MODE_OVERWRITE: &str = "OVERWRITE";
const UPDATE_MODE_APPEND: &str = "APPEND";
const COMPRESSION_UNCOMPRESSED: &str = "UNCOMPRESSED";
const COMPRESSION_LZ4: &str = "LZ4";
const COMPRESSION_SNAPPY: &str = "SNAPPY";
const COMPRESSION_ZSTD: &str = "ZSTD";
const STORAGE_FORMAT_AUTO: &str = "AUTO";
const STORAGE_FORMAT_COLUMNAR: &str = "COLUMNAR";

/// Default bucket duration (1d)
const BUCKET_DURATION_1D: Duration = Duration::from_secs(24 * 60 * 60);
/// Default duration of a segment (2h).
pub const DEFAULT_SEGMENT_DURATION: Duration = Duration::from_secs(60 * 60 * 2);
/// Default arena block size (2M).
const DEFAULT_ARENA_BLOCK_SIZE: u32 = 2 * 1024 * 1024;
/// Default write buffer size (32M).
const DEFAULT_WRITE_BUFFER_SIZE: u32 = 32 * 1024 * 1024;
/// Default ttl of table (7d).
const DEFAULT_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Default row number of a row group.
const DEFAULT_NUM_ROW_PER_ROW_GROUP: usize = 8192;

/// Max arena block size (2G)
const MAX_ARENA_BLOCK_SIZE: u32 = 2 * 1024 * 1024 * 1024;
/// Min arena block size (1K)
const MIN_ARENA_BLOCK_SIZE: u32 = 1024;
const MIN_NUM_ROWS_PER_ROW_GROUP: usize = 100;
const MAX_NUM_ROWS_PER_ROW_GROUP: usize = 10_000_000;

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Failed to parse duration, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ParseDuration {
        source: time_ext::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to parse size, err:{}.\nBacktrace:\n{}", err, backtrace))]
    ParseSize { err: String, backtrace: Backtrace },

    #[snafu(display("Failed to parse compaction strategy: {}, err: {}", value, source))]
    ParseStrategy {
        value: String,
        source: crate::compaction::Error,
    },
    #[snafu(display("Failed to parse int, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ParseInt {
        source: std::num::ParseIntError,
        backtrace: Backtrace,
    },
    #[snafu(display("Failed to parse bool, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ParseBool {
        source: std::str::ParseBoolError,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "Failed to parse update mode, raw str:{}.\nBacktrace:\n{}",
        s,
        backtrace
    ))]
    ParseUpdateMode { s: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to parse compression, name:{}.\nBacktrace:\n{}",
        name,
        backtrace
    ))]
    ParseCompressionName { name: String, backtrace: Backtrace },

    #[snafu(display(
        "Unknown storage format. value:{:?}.\nBacktrace:\n{}",
        value,
        backtrace
    ))]
    UnknownStorageFormat { value: String, backtrace: Backtrace },

    #[snafu(display(
        "Unknown storage format. value:{:?}.\nBacktrace:\n{}",
        value,
        backtrace
    ))]
    UnknownStorageFormatType { value: i32, backtrace: Backtrace },

    #[snafu(display(
        "Unknown storage format hint. value:{:?}.\nBacktrace:\n{}",
        value,
        backtrace
    ))]
    UnknownStorageFormatHint { value: String, backtrace: Backtrace },

    #[snafu(display("Storage format hint is missing.\nBacktrace:\n{}", backtrace))]
    MissingStorageFormatHint { backtrace: Backtrace },

    #[snafu(display(
        "Hybrid format is deprecated, and cannot be used any more.\nBacktrace:\n{}",
        backtrace
    ))]
    HybridDeprecated { backtrace: Backtrace },

    #[snafu(display(
        "Failed to parse layered memtable options, msg:{msg}.\nBacktrace:\n{backtrace}",
    ))]
    ParseLayeredMemtableOptions { msg: String, backtrace: Backtrace },

    #[snafu(display("Layered memtable options is missing.\nBacktrace:\n{backtrace}",))]
    MissingLayeredMemtableOptions { backtrace: Backtrace },
}

define_result!(Error);

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub enum UpdateMode {
    Overwrite,
    Append,
}

impl UpdateMode {
    pub fn parse_from(s: &str) -> Result<Self> {
        if s.eq_ignore_ascii_case(UPDATE_MODE_OVERWRITE) {
            Ok(UpdateMode::Overwrite)
        } else if s.eq_ignore_ascii_case(UPDATE_MODE_APPEND) {
            Ok(UpdateMode::Append)
        } else {
            ParseUpdateMode { s }.fail()
        }
    }
}

impl ToString for UpdateMode {
    fn to_string(&self) -> String {
        match self {
            UpdateMode::Append => UPDATE_MODE_APPEND.to_string(),
            UpdateMode::Overwrite => UPDATE_MODE_OVERWRITE.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum Compression {
    Uncompressed,
    Lz4,
    Snappy,
    Zstd,
}

impl Compression {
    pub fn parse_from(name: &str) -> Result<Self> {
        if name.eq_ignore_ascii_case(COMPRESSION_UNCOMPRESSED) {
            Ok(Compression::Uncompressed)
        } else if name.eq_ignore_ascii_case(COMPRESSION_LZ4) {
            Ok(Compression::Lz4)
        } else if name.eq_ignore_ascii_case(COMPRESSION_SNAPPY) {
            Ok(Compression::Snappy)
        } else if name.eq_ignore_ascii_case(COMPRESSION_ZSTD) {
            Ok(Compression::Zstd)
        } else {
            ParseCompressionName { name }.fail()
        }
    }
}

impl ToString for Compression {
    fn to_string(&self) -> String {
        match self {
            Compression::Uncompressed => COMPRESSION_UNCOMPRESSED.to_string(),
            Compression::Lz4 => COMPRESSION_LZ4.to_string(),
            Compression::Snappy => COMPRESSION_SNAPPY.to_string(),
            Compression::Zstd => COMPRESSION_ZSTD.to_string(),
        }
    }
}

impl From<Compression> for manifest_pb::Compression {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::Uncompressed => manifest_pb::Compression::Uncompressed,
            Compression::Lz4 => manifest_pb::Compression::Lz4,
            Compression::Snappy => manifest_pb::Compression::Snappy,
            Compression::Zstd => manifest_pb::Compression::Zstd,
        }
    }
}

impl From<manifest_pb::Compression> for Compression {
    fn from(compression: manifest_pb::Compression) -> Self {
        match compression {
            manifest_pb::Compression::Uncompressed => Compression::Uncompressed,
            manifest_pb::Compression::Lz4 => Compression::Lz4,
            manifest_pb::Compression::Snappy => Compression::Snappy,
            manifest_pb::Compression::Zstd => Compression::Zstd,
        }
    }
}

impl From<Compression> for ParquetCompression {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::Uncompressed => ParquetCompression::UNCOMPRESSED,
            Compression::Lz4 => ParquetCompression::LZ4,
            Compression::Snappy => ParquetCompression::SNAPPY,
            Compression::Zstd => ParquetCompression::ZSTD(Default::default()),
        }
    }
}

/// A hint for building sst.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub enum StorageFormatHint {
    /// Which storage format is chosen to encode one sst depends on the data
    /// pattern.
    #[default]
    Auto,
    Specific(StorageFormat),
}

/// StorageFormat specify how records are saved in persistent storage
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Serialize)]
pub enum StorageFormat {
    /// Traditional columnar format, every column is saved in one exact one
    /// column, for example:
    ///
    /// ```plaintext
    /// | Timestamp | Device ID | Status Code | Tag 1 | Tag 2 |
    /// | --------- |---------- | ----------- | ----- | ----- |
    /// | 12:01     | A         | 0           | v1    | v1    |
    /// | 12:01     | B         | 0           | v2    | v2    |
    /// | 12:02     | A         | 0           | v1    | v1    |
    /// | 12:02     | B         | 1           | v2    | v2    |
    /// | 12:03     | A         | 0           | v1    | v1    |
    /// | 12:03     | B         | 0           | v2    | v2    |
    /// | .....     |           |             |       |       |
    /// ```
    Columnar,
}

impl From<StorageFormatHint> for manifest_pb::StorageFormatHint {
    fn from(hint: StorageFormatHint) -> Self {
        match hint {
            StorageFormatHint::Auto => Self {
                hint: Some(manifest_pb::storage_format_hint::Hint::Auto(0)),
            },
            StorageFormatHint::Specific(format) => {
                let format = manifest_pb::StorageFormat::from(format);
                Self {
                    hint: Some(manifest_pb::storage_format_hint::Hint::Specific(
                        format as i32,
                    )),
                }
            }
        }
    }
}

impl TryFrom<manifest_pb::StorageFormatHint> for StorageFormatHint {
    type Error = Error;

    fn try_from(hint: manifest_pb::StorageFormatHint) -> Result<Self> {
        let format_hint = match hint.hint.context(MissingStorageFormatHint)? {
            manifest_pb::storage_format_hint::Hint::Auto(_) => StorageFormatHint::Auto,
            manifest_pb::storage_format_hint::Hint::Specific(format) => {
                let storage_format = manifest_pb::StorageFormat::from_i32(format)
                    .context(UnknownStorageFormatType { value: format })?;
                StorageFormatHint::Specific(storage_format.try_into()?)
            }
        };

        Ok(format_hint)
    }
}

impl ToString for StorageFormatHint {
    fn to_string(&self) -> String {
        match self {
            Self::Auto => STORAGE_FORMAT_AUTO.to_string(),
            Self::Specific(format) => format.to_string(),
        }
    }
}

impl TryFrom<&str> for StorageFormatHint {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let format = match value.to_uppercase().as_str() {
            STORAGE_FORMAT_COLUMNAR => Self::Specific(StorageFormat::Columnar),
            STORAGE_FORMAT_AUTO => Self::Auto,
            _ => return UnknownStorageFormatHint { value }.fail(),
        };
        Ok(format)
    }
}

impl From<StorageFormat> for manifest_pb::StorageFormat {
    fn from(format: StorageFormat) -> Self {
        match format {
            StorageFormat::Columnar => Self::Columnar,
        }
    }
}

impl TryFrom<manifest_pb::StorageFormat> for StorageFormat {
    type Error = Error;

    fn try_from(format: manifest_pb::StorageFormat) -> Result<Self> {
        match format {
            manifest_pb::StorageFormat::Columnar => Ok(Self::Columnar),
            manifest_pb::StorageFormat::Hybrid => HybridDeprecated {}.fail(),
        }
    }
}

impl TryFrom<&str> for StorageFormat {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let format = match value.to_uppercase().as_str() {
            STORAGE_FORMAT_COLUMNAR => Self::Columnar,
            _ => return UnknownStorageFormat { value }.fail(),
        };
        Ok(format)
    }
}

impl ToString for StorageFormat {
    fn to_string(&self) -> String {
        match self {
            Self::Columnar => STORAGE_FORMAT_COLUMNAR,
        }
        .to_string()
    }
}

impl Default for StorageFormat {
    fn default() -> Self {
        Self::Columnar
    }
}

/// Options for a table.
#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct TableOptions {
    // The following options are immutable once table was created.
    /// Segment duration of the table.
    ///
    /// `None` means the table is doing the segment duration sampling and
    /// the actual duration is still unknown.
    pub segment_duration: Option<ReadableDuration>,
    /// Table update mode, now support Overwrite(Default) and Append
    pub update_mode: UpdateMode,
    /// Hint for storage format.
    pub storage_format_hint: StorageFormatHint,

    // The following options can be altered.
    /// Enable ttl
    pub enable_ttl: bool,
    /// Time-to-live of the data.
    pub ttl: ReadableDuration,
    /// Arena block size of memtable.
    pub arena_block_size: u32,
    /// Write buffer size of memtable.
    pub write_buffer_size: u32,
    /// Compaction strategy of the table.
    pub compaction_strategy: CompactionStrategy,
    /// Row number in a row group.
    pub num_rows_per_row_group: usize,
    /// Table Compression
    pub compression: Compression,

    /// Memtable type
    pub memtable_type: MemtableType,
    /// Layered memtable options
    pub layered_memtable_opts: LayeredMemtableOptions,
}

impl TableOptions {
    pub fn from_map(map: &HashMap<String, String>, is_create: bool) -> Result<Self> {
        let opt = Self::default();
        merge_table_options(map, &opt, is_create)
    }

    #[inline]
    pub fn segment_duration(&self) -> Option<Duration> {
        self.segment_duration.map(|v| v.0)
    }

    #[inline]
    pub fn ttl(&self) -> Option<ReadableDuration> {
        if self.enable_ttl {
            Some(self.ttl)
        } else {
            None
        }
    }

    // for show create table
    pub fn to_raw_map(&self) -> HashMap<String, String> {
        let mut m = [
            (
                SEGMENT_DURATION.to_string(),
                self.segment_duration
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
            ),
            (UPDATE_MODE.to_string(), self.update_mode.to_string()),
            (ENABLE_TTL.to_string(), self.enable_ttl.to_string()),
            (TTL.to_string(), format!("{}", self.ttl)),
            (
                ARENA_BLOCK_SIZE.to_string(),
                format!("{}", self.arena_block_size),
            ),
            (
                WRITE_BUFFER_SIZE.to_string(),
                format!("{}", self.write_buffer_size),
            ),
            (
                NUM_ROWS_PER_ROW_GROUP.to_string(),
                format!("{}", self.num_rows_per_row_group),
            ),
            (COMPRESSION.to_string(), self.compression.to_string()),
            (
                STORAGE_FORMAT.to_string(),
                self.storage_format_hint.to_string(),
            ),
            (MEMTABLE_TYPE.to_string(), self.memtable_type.to_string()),
            (
                LAYERED_ENABLE.to_string(),
                self.layered_memtable_opts.enable.to_string(),
            ),
            (
                LAYERED_MUTABLE_SWITCH_THRESHOLD.to_string(),
                self.layered_memtable_opts
                    .mutable_segment_switch_threshold
                    .0
                    .to_string(),
            ),
        ]
        .into_iter()
        .collect();
        self.compaction_strategy.fill_raw_map(&mut m);

        m
    }

    /// Check if the options are valid.
    /// If invalid, Some(reason) will be returned.
    /// If valid, None will be returned
    pub fn check_validity(&self) -> Option<String> {
        if self.layered_memtable_opts.enable
            && self
                .layered_memtable_opts
                .mutable_segment_switch_threshold
                .0
                == 0
        {
            return Some(format!(
                "layered memtable is enabled but mutable_switch_threshold is 0, layered_memtable_opts:{:?}",
                self.layered_memtable_opts,
            ));
        }

        // layered memtable is not support in overwrite mode
        if self.need_dedup() && self.layered_memtable_opts.enable {
            return Some(format!(
                "layered memtable is enabled for table needing dedup, layered_memtable_opts:{:?}, update_mode:{:?}",
                self.layered_memtable_opts, self.update_mode,
            ));
        }

        None
    }

    /// Sanitize options silently.
    pub fn sanitize(&mut self) {
        let one_day_secs = BUCKET_DURATION_1D.as_secs();

        if let Some(segment_duration) = self.segment_duration {
            let mut segment_duration_secs = segment_duration.as_secs();
            if segment_duration_secs == 0 {
                segment_duration_secs = DEFAULT_SEGMENT_DURATION.as_secs()
            };
            self.segment_duration = Some(ReadableDuration::secs(segment_duration_secs));
        }

        let ttl_secs = self.ttl.as_secs();
        // Ttl must align to day.
        let ttl_secs = ttl_secs / one_day_secs * one_day_secs;
        self.ttl = ReadableDuration::secs(ttl_secs);

        if self.arena_block_size < MIN_ARENA_BLOCK_SIZE {
            self.arena_block_size = MIN_ARENA_BLOCK_SIZE;
        }

        if self.arena_block_size > MAX_ARENA_BLOCK_SIZE {
            self.arena_block_size = MAX_ARENA_BLOCK_SIZE;
        }

        if self.num_rows_per_row_group < MIN_NUM_ROWS_PER_ROW_GROUP {
            self.num_rows_per_row_group = MIN_NUM_ROWS_PER_ROW_GROUP;
        }

        if self.num_rows_per_row_group > MAX_NUM_ROWS_PER_ROW_GROUP {
            self.num_rows_per_row_group = MAX_NUM_ROWS_PER_ROW_GROUP;
        }
    }

    pub fn need_dedup(&self) -> bool {
        match self.update_mode {
            UpdateMode::Overwrite => true,
            UpdateMode::Append => false,
        }
    }

    // Only support sample primary key for APPEND.
    pub fn support_sample_pk(&self) -> bool {
        match self.update_mode {
            UpdateMode::Overwrite => false,
            UpdateMode::Append => true,
        }
    }

    pub fn is_expired(&self, timestamp: Timestamp) -> bool {
        self.enable_ttl && timestamp.is_expired(Timestamp::expire_time(self.ttl.0))
    }
}

impl From<SizeTieredCompactionOptions> for manifest_pb::CompactionOptions {
    fn from(opts: SizeTieredCompactionOptions) -> Self {
        manifest_pb::CompactionOptions {
            bucket_low: opts.bucket_low,
            bucket_high: opts.bucket_high,
            min_sstable_size: opts.min_sstable_size.0 as u32,
            min_threshold: opts.min_threshold as u32,
            max_threshold: opts.max_threshold as u32,
            // FIXME: Is it ok to use the default timestamp resolution here?
            timestamp_resolution: manifest_pb::TimeUnit::Nanoseconds as i32,
        }
    }
}

impl From<manifest_pb::CompactionOptions> for SizeTieredCompactionOptions {
    fn from(opts: manifest_pb::CompactionOptions) -> Self {
        Self {
            bucket_low: opts.bucket_low,
            bucket_high: opts.bucket_high,
            min_sstable_size: ReadableSize(opts.min_sstable_size as u64),
            min_threshold: opts.min_threshold as usize,
            max_threshold: opts.max_threshold as usize,
            max_input_sstable_size: compaction::get_max_input_sstable_size(),
        }
    }
}

impl From<TimeWindowCompactionOptions> for manifest_pb::CompactionOptions {
    fn from(v: TimeWindowCompactionOptions) -> Self {
        manifest_pb::CompactionOptions {
            bucket_low: v.size_tiered.bucket_low,
            bucket_high: v.size_tiered.bucket_high,
            min_sstable_size: v.size_tiered.min_sstable_size.0 as u32,
            min_threshold: v.size_tiered.min_threshold as u32,
            max_threshold: v.size_tiered.max_threshold as u32,
            timestamp_resolution: manifest_pb::TimeUnit::from(v.timestamp_resolution) as i32,
        }
    }
}

impl From<manifest_pb::CompactionOptions> for TimeWindowCompactionOptions {
    fn from(opts: manifest_pb::CompactionOptions) -> Self {
        let size_tiered: SizeTieredCompactionOptions = opts.clone().into();

        Self {
            size_tiered,
            timestamp_resolution: TimeUnit::from(opts.timestamp_resolution()),
        }
    }
}

impl From<TableOptions> for manifest_pb::TableOptions {
    fn from(opts: TableOptions) -> Self {
        let segment_duration = opts
            .segment_duration
            .map(|v| v.0.as_millis_u64())
            .unwrap_or(0);
        let sampling_segment_duration = opts.segment_duration.is_none();

        let (compaction_strategy, compaction_options) = match opts.compaction_strategy {
            CompactionStrategy::Default => (manifest_pb::CompactionStrategy::Default, None),
            CompactionStrategy::SizeTiered(v) => (
                manifest_pb::CompactionStrategy::SizeTiered,
                Some(manifest_pb::CompactionOptions::from(v)),
            ),
            CompactionStrategy::TimeWindow(v) => (
                manifest_pb::CompactionStrategy::TimeWindow,
                Some(manifest_pb::CompactionOptions::from(v)),
            ),
        };

        let layered_memtable_opts = opts.layered_memtable_opts.into();

        manifest_pb::TableOptions {
            segment_duration,
            enable_ttl: opts.enable_ttl,
            ttl: opts.ttl.0.as_millis_u64(),
            arena_block_size: opts.arena_block_size,
            num_rows_per_row_group: opts.num_rows_per_row_group as u64,
            compaction_strategy: compaction_strategy as i32,
            compaction_options,
            update_mode: manifest_pb::UpdateMode::from(opts.update_mode) as i32,
            write_buffer_size: opts.write_buffer_size,
            compression: manifest_pb::Compression::from(opts.compression) as i32,
            sampling_segment_duration,
            storage_format_hint: Some(manifest_pb::StorageFormatHint::from(
                opts.storage_format_hint,
            )),
            layered_memtable_options: Some(layered_memtable_opts),
            // TODO: persist `memtable_type` in PB.
        }
    }
}

impl From<UpdateMode> for manifest_pb::UpdateMode {
    fn from(v: UpdateMode) -> Self {
        match v {
            UpdateMode::Overwrite => manifest_pb::UpdateMode::Overwrite,
            UpdateMode::Append => manifest_pb::UpdateMode::Append,
        }
    }
}

impl From<manifest_pb::UpdateMode> for UpdateMode {
    fn from(v: manifest_pb::UpdateMode) -> Self {
        match v {
            manifest_pb::UpdateMode::Overwrite => UpdateMode::Overwrite,
            manifest_pb::UpdateMode::Append => UpdateMode::Append,
        }
    }
}

impl TryFrom<manifest_pb::TableOptions> for TableOptions {
    type Error = Error;

    fn try_from(opts: manifest_pb::TableOptions) -> Result<Self> {
        let compression = opts.compression();
        let update_mode = opts.update_mode();

        let compaction_strategy = match opts.compaction_strategy() {
            manifest_pb::CompactionStrategy::Default => CompactionStrategy::default(),
            manifest_pb::CompactionStrategy::SizeTiered => {
                let opts = opts
                    .compaction_options
                    .map(SizeTieredCompactionOptions::from)
                    .unwrap_or_default();
                CompactionStrategy::SizeTiered(opts)
            }
            manifest_pb::CompactionStrategy::TimeWindow => {
                let opts = opts
                    .compaction_options
                    .map(TimeWindowCompactionOptions::from)
                    .unwrap_or_default();
                CompactionStrategy::TimeWindow(opts)
            }
        };

        let segment_duration = if opts.sampling_segment_duration {
            None
        } else if opts.segment_duration == 0 {
            // If segment duration is still zero. If the data had been used by an elder
            // version release that not yet support sampling, the
            // `sampling_segment_duration` flag would be truncated after
            // manifest snapshot, but left segment duration zero.
            Some(DEFAULT_SEGMENT_DURATION.into())
        } else {
            Some(Duration::from_millis(opts.segment_duration).into())
        };

        let storage_format_hint = opts.storage_format_hint.context(MissingStorageFormatHint)?;
        // For compatible with old `table_options`, `layered_memtable_options` is
        // allowed to be `None`, and when found `None`, we disable `layered_memtable`.
        let layered_memtable_opts = match opts.layered_memtable_options {
            Some(v) => v.into(),
            None => LayeredMemtableOptions {
                mutable_segment_switch_threshold: ReadableSize(0),
                enable: false,
            },
        };

        let table_opts = Self {
            segment_duration,
            enable_ttl: opts.enable_ttl,
            ttl: Duration::from_millis(opts.ttl).into(),
            arena_block_size: opts.arena_block_size,
            compaction_strategy,
            num_rows_per_row_group: opts.num_rows_per_row_group as usize,
            update_mode: UpdateMode::from(update_mode),
            write_buffer_size: opts.write_buffer_size,
            compression: Compression::from(compression),
            storage_format_hint: StorageFormatHint::try_from(storage_format_hint)?,
            memtable_type: MemtableType::SkipList,
            layered_memtable_opts,
        };

        Ok(table_opts)
    }
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            segment_duration: None,
            enable_ttl: true,
            ttl: DEFAULT_TTL.into(),
            arena_block_size: DEFAULT_ARENA_BLOCK_SIZE,
            compaction_strategy: CompactionStrategy::default(),
            num_rows_per_row_group: DEFAULT_NUM_ROW_PER_ROW_GROUP,
            update_mode: UpdateMode::Overwrite,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            compression: Compression::Zstd,
            storage_format_hint: StorageFormatHint::default(),
            memtable_type: MemtableType::SkipList,
            layered_memtable_opts: LayeredMemtableOptions::default(),
        }
    }
}

pub fn merge_table_options_for_create(
    options: &HashMap<String, String>,
    table_opts: &TableOptions,
) -> Result<TableOptions> {
    merge_table_options(options, table_opts, true)
}

pub fn merge_table_options_for_alter(
    options: &HashMap<String, String>,
    table_opts: &TableOptions,
) -> Result<TableOptions> {
    merge_table_options(options, table_opts, false)
}

/// The options will override the old options.
fn merge_table_options(
    options: &HashMap<String, String>,
    base_table_opts: &TableOptions,
    is_create: bool,
) -> Result<TableOptions> {
    let mut base_table_opts = base_table_opts.clone();
    if is_create {
        if let Some(v) = options.get(SEGMENT_DURATION) {
            if v.is_empty() {
                base_table_opts.segment_duration = None;
            } else {
                base_table_opts.segment_duration = Some(parse_duration(v).context(ParseDuration)?);
            }
        }
        if let Some(v) = options.get(UPDATE_MODE) {
            base_table_opts.update_mode = UpdateMode::parse_from(v)?;
        }
    }

    if let Some(v) = options.get(TTL) {
        base_table_opts.ttl = parse_duration(v).context(ParseDuration)?;
    }
    if let Some(v) = options.get(OPTION_KEY_ENABLE_TTL) {
        base_table_opts.enable_ttl = v.parse::<bool>().context(ParseBool)?;
    }
    if let Some(v) = options.get(ARENA_BLOCK_SIZE) {
        let size = parse_size(v)?;
        base_table_opts.arena_block_size = size.0 as u32;
    }
    if let Some(v) = options.get(WRITE_BUFFER_SIZE) {
        let size = parse_size(v)?;
        base_table_opts.write_buffer_size = size.0 as u32;
    }
    if let Some(v) = options.get(COMPACTION_STRATEGY) {
        base_table_opts.compaction_strategy =
            CompactionStrategy::parse_from(v, options).context(ParseStrategy { value: v })?;
    }
    if let Some(v) = options.get(NUM_ROWS_PER_ROW_GROUP) {
        base_table_opts.num_rows_per_row_group = v.parse().context(ParseInt)?;
    }
    if let Some(v) = options.get(COMPRESSION) {
        base_table_opts.compression = Compression::parse_from(v)?;
    }
    if let Some(v) = options.get(STORAGE_FORMAT) {
        base_table_opts.storage_format_hint = v.as_str().try_into()?;
    }
    if let Some(v) = options.get(MEMTABLE_TYPE) {
        base_table_opts.memtable_type = MemtableType::parse_from(v);
    }
    if let Some(v) = options.get(LAYERED_ENABLE) {
        let enable = match v.parse::<bool>() {
            Ok(v) => v,
            Err(e) => {
                return ParseLayeredMemtableOptions {
                    msg: format!("invalid layered_enable setting, err:{e}"),
                }
                .fail()
            }
        };
        base_table_opts.layered_memtable_opts.enable = enable;
    }
    if let Some(v) = options.get(LAYERED_MUTABLE_SWITCH_THRESHOLD) {
        let threshold = match ReadableSize::from_str(v) {
            Ok(v) => v,
            Err(e) => {
                return ParseLayeredMemtableOptions {
                    msg: format!("invalid layered_mutable_switch_threshold setting, err:{e}"),
                }
                .fail()
            }
        };

        base_table_opts
            .layered_memtable_opts
            .mutable_segment_switch_threshold = threshold;
    }

    Ok(base_table_opts)
}

fn parse_size(v: &str) -> Result<ReadableSize> {
    v.parse::<ReadableSize>().map_err(|err| Error::ParseSize {
        err,
        backtrace: Backtrace::generate(),
    })
}
