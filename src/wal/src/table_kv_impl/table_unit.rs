// Copyright 2023 The CeresDB Authors
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

//! Table unit in wal.

use std::{
    cmp,
    convert::TryInto,
    mem,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes_ext::BytesMut;
use common_types::table::TableId;
use generic_error::{BoxError, GenericError};
use logger::{debug, warn};
use macros::define_result;
use runtime::{self, Runtime};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use table_kv::{
    KeyBoundary, ScanContext, ScanIter, ScanRequest, TableError, TableKv, WriteBatch, WriteContext,
};
use tokio::sync::Mutex;

use crate::{
    kv_encoder::{CommonLogEncoding, CommonLogKey},
    log_batch::{LogEntry, LogWriteBatch, LogWriteEntry},
    manager::{
        self, ReadContext, ReadRequest, RegionId, SequenceNumber, SyncLogIterator, WalRuntimes,
    },
    table_kv_impl::{encoding, model::TableUnitEntry, namespace::BucketRef},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get value, key:{}, err:{}", key, source,))]
    GetValue { key: String, source: GenericError },

    #[snafu(display("Failed to decode entry, key:{}, err:{}", key, source,))]
    Decode {
        key: String,
        source: crate::table_kv_impl::model::Error,
    },

    #[snafu(display(
        "Failed to encode entry, key:{}, meta table:{}, err:{}",
        key,
        meta_table,
        source,
    ))]
    Encode {
        key: String,
        meta_table: String,
        source: crate::table_kv_impl::model::Error,
    },

    #[snafu(display("Try to split empty logs.\nBacktrace\n:{backtrace}"))]
    SplitEmptyLogs { backtrace: Backtrace },

    #[snafu(display("The size limit of one write batch is not enough for just one key-value pair, size_limit:{limit}.\nBacktrace\n:{backtrace}"))]
    TooSmallSizeLimitPerBatch { limit: usize, backtrace: Backtrace },

    #[snafu(display("Too large payload, payload_len:{payload_size}.\nBacktrace\n:{backtrace}"))]
    TooLargePayload {
        payload_size: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to do log codec, err:{}.", source))]
    LogCodec { source: crate::kv_encoder::Error },

    #[snafu(display("Failed to scan table, err:{}", source))]
    Scan { source: GenericError },

    #[snafu(display(
        "Failed to write value, key:{}, meta table:{}, err:{}",
        key,
        meta_table,
        source
    ))]
    WriteValue {
        key: String,
        meta_table: String,
        source: GenericError,
    },

    #[snafu(display(
        "Sequence of region overflow, table_id:{}.\nBacktrace:\n{}",
        table_id,
        backtrace
    ))]
    SequenceOverflow {
        table_id: TableId,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to write log to table, region_id:{}, err:{}",
        region_id,
        source
    ))]
    WriteLog {
        region_id: u64,
        source: GenericError,
    },

    #[snafu(display(
        "Region not exists, region_id:{}.\nBacktrace:\n{}",
        region_id,
        backtrace
    ))]
    TableUnitNotExists {
        region_id: u64,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to execute in runtime, err:{}", source))]
    RuntimeExec { source: runtime::Error },

    #[snafu(display("Failed to delete table, region_id:{}, err:{}", region_id, source))]
    Delete {
        region_id: u64,
        source: GenericError,
    },

    #[snafu(display(
        "Failed to load last sequence, table_id:{}, msg:{}.\nBacktrace:\n{}",
        msg,
        table_id,
        backtrace
    ))]
    LoadLastSequence {
        table_id: TableId,
        msg: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Default batch size (100) to clean records.
const DEFAULT_CLEAN_BATCH_SIZE: i32 = 100;

struct TableUnitState {
    /// Region id of this table unit
    region_id: u64,
    /// Table id of this table unit
    table_id: TableId,
    /// Start sequence (inclusive) of this table unit, update is protected by
    /// the `writer` lock.
    start_sequence: AtomicU64,
    /// Last sequence (inclusive) of this table unit, update is protected by the
    /// `writer` lock.
    last_sequence: AtomicU64,
}

impl TableUnitState {
    #[inline]
    fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence.load(Ordering::Relaxed)
    }

    #[inline]
    fn start_sequence(&self) -> SequenceNumber {
        self.start_sequence.load(Ordering::Relaxed)
    }

    #[inline]
    fn set_start_sequence(&self, sequence: SequenceNumber) {
        self.start_sequence.store(sequence, Ordering::Relaxed);
    }

    #[inline]
    fn table_unit_entry(&self) -> TableUnitEntry {
        TableUnitEntry {
            table_id: self.table_id,
            start_sequence: self.start_sequence.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CleanContext {
    pub scan_timeout: Duration,
    pub batch_size: usize,
}

impl Default for CleanContext {
    fn default() -> Self {
        Self {
            scan_timeout: Duration::from_secs(10),
            batch_size: DEFAULT_CLEAN_BATCH_SIZE as usize,
        }
    }
}

/// Table unit can be viewed as an append only log file.
pub struct TableUnit {
    runtimes: WalRuntimes,
    state: TableUnitState,
    writer: Mutex<TableUnitWriter>,
}

impl std::fmt::Debug for TableUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableUnit")
            .field("region_id", &self.state.region_id)
            .field("table_id", &self.state.table_id)
            .field("start_sequence", &self.state.start_sequence)
            .field("last_sequence", &self.state.last_sequence)
            .finish()
    }
}

// Async or non-blocking operations.
impl TableUnit {
    /// Open table unit of given `region_id` and `table_id`, the caller should
    /// ensure the meta data of this table unit is stored in
    /// `table_unit_meta_table`, and the wal log records are stored in
    /// `buckets`.
    pub async fn open<T: TableKv>(
        runtimes: WalRuntimes,
        table_kv: &T,
        scan_ctx: ScanContext,
        table_unit_meta_table: &str,
        region_id: u64,
        table_id: TableId,
        // Buckets ordered by time.
        buckets: Vec<BucketRef>,
    ) -> Result<Option<TableUnit>> {
        let table_kv = table_kv.clone();
        let table_unit_meta_table = table_unit_meta_table.to_string();
        let rt = runtimes.default_runtime.clone();

        rt.spawn_blocking(move || {
            // Load of create table unit entry.
            let table_unit_entry =
                match Self::load_table_unit_entry(&table_kv, &table_unit_meta_table, table_id)? {
                    Some(v) => v,
                    None => return Ok(None),
                };
            debug!(
                "Open table unit, table unit entry:{:?}, region id:{}, table id:{}",
                table_unit_entry, region_id, table_id
            );

            // Load last sequence of this table unit.
            let last_sequence = Self::load_last_sequence(
                &table_kv,
                scan_ctx,
                region_id,
                table_id,
                &buckets,
                table_unit_entry.start_sequence,
            )?;

            Ok(Some(Self {
                runtimes,
                state: TableUnitState {
                    region_id,
                    table_id,
                    start_sequence: AtomicU64::new(table_unit_entry.start_sequence),
                    last_sequence: AtomicU64::new(last_sequence),
                },
                writer: Mutex::new(TableUnitWriter),
            }))
        })
        .await
        .context(RuntimeExec)?
    }

    /// Similar to `open()`, open table unit of given `region_id` and
    /// `table_id`. If the table unit doesn't exists, insert a new table
    /// unit entry into `table_unit_meta_table`. Only one writer is allowed to
    /// insert the new table unit entry.
    pub async fn open_or_create<T: TableKv>(
        runtimes: WalRuntimes,
        table_kv: &T,
        scan_ctx: ScanContext,
        table_unit_meta_table: &str,
        region_id: u64,
        table_id: TableId,
        // Buckets ordered by time.
        buckets: Vec<BucketRef>,
    ) -> Result<TableUnit> {
        let table_kv = table_kv.clone();
        let table_unit_meta_table = table_unit_meta_table.to_string();
        let rt = runtimes.default_runtime.clone();

        rt.spawn_blocking(move || {
            // Load of create table unit entry.
            let mut writer = TableUnitWriter;
            let table_unit_entry =
                match Self::load_table_unit_entry(&table_kv, &table_unit_meta_table, table_id)? {
                    Some(v) => v,
                    None => {
                        let entry = TableUnitEntry::new(table_id);
                        writer.insert_or_load_table_unit_entry(
                            &table_kv,
                            &table_unit_meta_table,
                            entry,
                        )?
                    }
                };

            // Load last sequence of this table unit.
            let last_sequence = Self::load_last_sequence(
                &table_kv,
                scan_ctx,
                region_id,
                table_id,
                &buckets,
                table_unit_entry.start_sequence,
            )?;

            Ok(Self {
                runtimes,
                state: TableUnitState {
                    region_id,
                    table_id,
                    start_sequence: AtomicU64::new(table_unit_entry.start_sequence),
                    last_sequence: AtomicU64::new(last_sequence),
                },
                writer: Mutex::new(writer),
            })
        })
        .await
        .context(RuntimeExec)?
    }

    pub async fn write_log<T: TableKv>(
        &self,
        table_kv: &T,
        bucket: &BucketRef,
        ctx: &manager::WriteContext,
        log_batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        let mut writer = self.writer.lock().await;
        writer
            .write_log(
                &self.runtimes.write_runtime,
                table_kv,
                &self.state,
                bucket,
                ctx,
                log_batch,
            )
            .await
    }

    pub async fn read_log<T: TableKv>(
        &self,
        table_kv: &T,
        buckets: Vec<BucketRef>,
        ctx: &ReadContext,
        request: &ReadRequest,
    ) -> Result<TableLogIterator<T>> {
        // Prepare start/end sequence to read, now this doesn't provide snapshot
        // isolation semantics since delete and write operations may happen
        // during reading start/end sequence.
        let start_sequence = match request.start.as_start_sequence_number() {
            Some(request_start_sequence) => {
                let table_unit_start_sequence = self.state.start_sequence();
                // Avoid reading deleted log entries.
                cmp::max(table_unit_start_sequence, request_start_sequence)
            }
            None => return Ok(TableLogIterator::new_empty(table_kv.clone())),
        };
        let end_sequence = match request.end.as_end_sequence_number() {
            Some(request_end_sequence) => {
                let table_unit_last_sequence = self.state.last_sequence();
                // Avoid reading entries newer than current last sequence.
                cmp::min(table_unit_last_sequence, request_end_sequence)
            }
            None => return Ok(TableLogIterator::new_empty(table_kv.clone())),
        };

        let region_id = request.location.region_id;
        let table_id = request.location.table_id;
        let min_log_key = CommonLogKey::new(region_id, table_id, start_sequence);
        let max_log_key = CommonLogKey::new(region_id, table_id, end_sequence);

        let scan_ctx = ScanContext {
            timeout: ctx.timeout,
            batch_size: ctx.batch_size as i32,
        };

        Ok(TableLogIterator::new(
            buckets,
            min_log_key,
            max_log_key,
            scan_ctx,
            table_kv.clone(),
        ))
    }

    pub async fn delete_entries_up_to<T: TableKv>(
        &self,
        table_kv: &T,
        table_unit_meta_table: &str,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        let mut writer = self.writer.lock().await;
        writer
            .delete_entries_up_to(
                &self.runtimes.write_runtime,
                table_kv,
                &self.state,
                table_unit_meta_table,
                sequence_num,
            )
            .await
    }

    #[inline]
    pub fn table_id(&self) -> TableId {
        self.state.table_id
    }

    #[inline]
    pub fn region_id(&self) -> u64 {
        self.state.region_id
    }

    #[inline]
    pub fn last_sequence(&self) -> SequenceNumber {
        self.state.last_sequence()
    }
}

// Blocking operations:
impl TableUnit {
    fn load_table_unit_entry<T: TableKv>(
        table_kv: &T,
        table_unit_meta_table: &str,
        table_id: TableId,
    ) -> Result<Option<TableUnitEntry>> {
        let key = encoding::format_table_unit_key(table_id);
        table_kv
            .get(table_unit_meta_table, key.as_bytes())
            .box_err()
            .context(GetValue { key: &key })?
            .map(|value| TableUnitEntry::decode(&value).context(Decode { key }))
            .transpose()
    }

    // TODO(yingwen): We can cache last sequence of several buckets (be sure not to
    // leak buckets that has been deleted).
    fn load_last_sequence<T: TableKv>(
        table_kv: &T,
        scan_ctx: ScanContext,
        region_id: u64,
        table_id: TableId,
        buckets: &[BucketRef],
        start_sequence: u64,
    ) -> Result<SequenceNumber> {
        debug!(
            "Load last sequence, buckets{:?}, region id:{}, table id:{}",
            buckets, region_id, table_id
        );

        // Starts from the latest bucket, find last sequence of given region id.
        // It is likely that, table has just been moved to an new shard, so we should
        // pick `start_sequence - 1`(`start_sequence` equal to flushed_sequence + 1)
        // as the `last_sequence`.
        for bucket in buckets.iter().rev() {
            let table_name = bucket.wal_shard_table(region_id);

            if let Some(sequence) = Self::load_last_sequence_from_table(
                table_kv,
                scan_ctx.clone(),
                table_name,
                region_id,
                table_id,
            )? {
                #[rustfmt::skip]
                // FIXME: In some cases, the `flushed sequence`
                // may be greater than the `actual last sequence of written logs`.
                //
                // Such as following case:
                //  + Write wal logs failed(last sequence stored in memory will increase when write failed).
                //  + Get last sequence from memory(greater then actual last sequence now).
                //  + Mark the got last sequence as flushed sequence.
                let actual_next_sequence = sequence + 1;
                if actual_next_sequence < start_sequence {
                    warn!("TableKv WAL found start_sequence greater than actual_next_sequence,
                    start_sequence:{start_sequence}, actual_next_sequence:{actual_next_sequence}, table_id:{table_id}, region_id:{region_id}");

                    break;
                }

                return Ok(sequence);
            }
        }

        // If no flush ever happened, start_sequence will equal to 0.
        let last_sequence = if start_sequence > 0 {
            start_sequence - 1
        } else {
            start_sequence
        };
        Ok(last_sequence)
    }

    fn load_last_sequence_from_table<T: TableKv>(
        table_kv: &T,
        scan_ctx: ScanContext,
        table_name: &str,
        region_id: u64,
        table_id: TableId,
    ) -> Result<Option<SequenceNumber>> {
        let log_encoding = CommonLogEncoding::newest();
        let mut encode_buf = BytesMut::new();

        let start_log_key =
            CommonLogKey::new(region_id, table_id, common_types::MIN_SEQUENCE_NUMBER);
        log_encoding
            .encode_key(&mut encode_buf, &start_log_key)
            .context(LogCodec)?;
        let scan_start = KeyBoundary::included(&encode_buf);

        encode_buf.clear();
        let end_log_key = CommonLogKey::new(region_id, table_id, common_types::MAX_SEQUENCE_NUMBER);
        log_encoding
            .encode_key(&mut encode_buf, &end_log_key)
            .context(LogCodec)?;
        let scan_end = KeyBoundary::included(&encode_buf);

        let scan_req = ScanRequest {
            start: scan_start,
            end: scan_end,
            // We need to find the maximum sequence number.
            reverse: true,
        };

        let iter = table_kv
            .scan(scan_ctx, table_name, scan_req)
            .box_err()
            .context(Scan)?;

        if !iter.valid() {
            return Ok(None);
        }

        if !log_encoding.is_log_key(iter.key()).context(LogCodec)? {
            return Ok(None);
        }

        let log_key = log_encoding.decode_key(iter.key()).context(LogCodec)?;

        Ok(Some(log_key.sequence_num))
    }

    // TODO: unfortunately, we can just check and delete the
    pub fn clean_deleted_logs<T: TableKv>(
        &self,
        table_kv: &T,
        ctx: &CleanContext,
        buckets: &[BucketRef],
    ) -> Result<()> {
        // Inclusive min log key.
        let min_log_key = CommonLogKey::new(
            self.state.region_id,
            self.state.table_id,
            common_types::MIN_SEQUENCE_NUMBER,
        );
        // Exlusive max log key.
        let max_log_key = CommonLogKey::new(
            self.state.region_id,
            self.state.table_id,
            self.state.start_sequence(),
        );

        let mut seek_key_buf = BytesMut::new();
        let log_encoding = CommonLogEncoding::newest();
        log_encoding
            .encode_key(&mut seek_key_buf, &min_log_key)
            .context(LogCodec)?;
        let start = KeyBoundary::included(&seek_key_buf);
        log_encoding
            .encode_key(&mut seek_key_buf, &max_log_key)
            .context(LogCodec)?;
        // We should not clean record with start sequence, so we use exclusive boundary.
        let end = KeyBoundary::excluded(&seek_key_buf);

        let scan_req = ScanRequest {
            start,
            end,
            reverse: false,
        };
        let scan_ctx = ScanContext {
            timeout: ctx.scan_timeout,
            batch_size: ctx
                .batch_size
                .try_into()
                .unwrap_or(DEFAULT_CLEAN_BATCH_SIZE),
        };

        for bucket in buckets {
            let table_name = bucket.wal_shard_table(self.state.region_id);
            let iter = table_kv
                .scan(scan_ctx.clone(), table_name, scan_req.clone())
                .box_err()
                .context(Scan)?;

            self.clean_logs_from_iter(table_kv, ctx, table_name, iter)?;
        }

        Ok(())
    }

    fn clean_logs_from_iter<T: TableKv>(
        &self,
        table_kv: &T,
        ctx: &CleanContext,
        table_name: &str,
        mut iter: T::ScanIter,
    ) -> Result<()> {
        let mut write_batch = T::WriteBatch::with_capacity(ctx.batch_size);
        let (mut write_batch_size, mut total_deleted) = (0, 0);
        while iter.valid() {
            write_batch.delete(iter.key());
            write_batch_size += 1;
            total_deleted += 1;

            if write_batch_size >= ctx.batch_size {
                let wb = mem::replace(
                    &mut write_batch,
                    T::WriteBatch::with_capacity(ctx.batch_size),
                );
                write_batch_size = 0;
                table_kv
                    .write(WriteContext::default(), table_name, wb)
                    .box_err()
                    .context(Delete {
                        region_id: self.state.table_id,
                    })?;
            }

            let has_next = iter.next().box_err().context(Scan)?;
            if !has_next {
                let wb = mem::replace(
                    &mut write_batch,
                    T::WriteBatch::with_capacity(ctx.batch_size),
                );
                table_kv
                    .write(WriteContext::default(), table_name, wb)
                    .box_err()
                    .context(Delete {
                        region_id: self.state.table_id,
                    })?;

                break;
            }
        }

        if total_deleted > 0 {
            debug!(
                "Clean logs of table unit, region_id:{}, table_name:{}, total_deleted:{}",
                self.state.table_id, table_name, total_deleted
            );
        }

        Ok(())
    }
}

pub type TableUnitRef = Arc<TableUnit>;

#[derive(Debug)]
pub struct TableLogIterator<T: TableKv> {
    buckets: Vec<BucketRef>,
    /// Inclusive max log key.
    max_log_key: CommonLogKey,
    scan_ctx: ScanContext,
    table_kv: T,

    current_log_key: CommonLogKey,
    // The iterator is exhausted if `current_bucket_index >= bucets.size()`.
    current_bucket_index: usize,
    // The `current_iter` should be either a valid iterator or None.
    current_iter: Option<T::ScanIter>,
    log_encoding: CommonLogEncoding,
    // TODO(ygf11): Remove this after issue#120 is resolved.
    previous_value: Vec<u8>,
}

impl<T: TableKv> TableLogIterator<T> {
    pub fn new_empty(table_kv: T) -> Self {
        Self {
            buckets: Vec::new(),
            max_log_key: CommonLogKey::new(0, 0, 0),
            scan_ctx: ScanContext::default(),
            table_kv,
            current_log_key: CommonLogKey::new(0, 0, 0),
            current_bucket_index: 0,
            current_iter: None,
            log_encoding: CommonLogEncoding::newest(),
            previous_value: Vec::default(),
        }
    }

    pub fn new(
        buckets: Vec<BucketRef>,
        min_log_key: CommonLogKey,
        max_log_key: CommonLogKey,
        scan_ctx: ScanContext,
        table_kv: T,
    ) -> Self {
        TableLogIterator {
            buckets,
            max_log_key,
            scan_ctx,
            table_kv,
            current_log_key: min_log_key,
            current_bucket_index: 0,
            current_iter: None,
            log_encoding: CommonLogEncoding::newest(),
            previous_value: Vec::default(),
        }
    }

    #[inline]
    fn no_more_data(&self) -> bool {
        self.current_bucket_index >= self.buckets.len() || self.current_log_key > self.max_log_key
    }

    fn new_scan_request(&self) -> Result<ScanRequest> {
        let mut seek_key_buf = BytesMut::new();
        self.log_encoding
            .encode_key(&mut seek_key_buf, &self.current_log_key)
            .context(LogCodec)?;
        let start = KeyBoundary::included(&seek_key_buf);
        self.log_encoding
            .encode_key(&mut seek_key_buf, &self.max_log_key)
            .context(LogCodec)?;
        let end = KeyBoundary::included(&seek_key_buf);

        Ok(ScanRequest {
            start,
            end,
            reverse: false,
        })
    }

    /// Scan buckets to find next valid iterator, returns true if such iterator
    /// has been found.
    fn scan_buckets(&mut self) -> Result<bool> {
        let region_id = self.max_log_key.region_id;
        let scan_req = self.new_scan_request()?;

        while self.current_bucket_index < self.buckets.len() {
            if self.current_bucket_index > 0 {
                assert!(
                    self.buckets[self.current_bucket_index - 1].gmt_start_ms()
                        < self.buckets[self.current_bucket_index].gmt_start_ms()
                );
            }

            let table_name = self.buckets[self.current_bucket_index].wal_shard_table(region_id);
            let iter = self
                .table_kv
                .scan(self.scan_ctx.clone(), table_name, scan_req.clone())
                .box_err()
                .context(Scan)?;
            if iter.valid() {
                self.current_iter = Some(iter);
                return Ok(true);
            }

            self.current_bucket_index += 1;
        }

        Ok(false)
    }

    fn step_current_iter(&mut self) -> Result<()> {
        if let Some(iter) = &mut self.current_iter {
            if !iter.next().box_err().context(Scan)? {
                self.current_iter = None;
                self.current_bucket_index += 1;
            }
        }

        Ok(())
    }

    /// Collect log from current iterator, returns true if a log iterator is not
    /// exhausted.
    fn collect_log_from_one_kv(
        &mut self,
        log_collector: &mut IntegrateLogCollector,
    ) -> manager::Result<bool> {
        if self.no_more_data() {
            return Ok(false);
        }

        // If `current_iter` is None, scan from current to last bucket util we get a
        // valid iterator.
        if self.current_iter.is_none() {
            let has_valid_iter = self.scan_buckets().box_err().context(manager::Read)?;
            if !has_valid_iter {
                assert!(self.no_more_data());
                return Ok(false);
            }
        }

        // Fetch and decode current log entry.
        let current_iter = self.current_iter.as_ref().unwrap();
        let current_log_key = self
            .log_encoding
            .decode_key(current_iter.key())
            .box_err()
            .context(manager::Decoding)?;
        let payload = self
            .log_encoding
            .decode_value(current_iter.value())
            .box_err()
            .context(manager::Encoding)?;

        self.current_log_key = current_log_key.clone();
        log_collector.collect(current_log_key, payload);

        Ok(true)
    }
}

impl<T: TableKv> SyncLogIterator for TableLogIterator<T> {
    fn next_log_entry(&mut self) -> manager::Result<Option<LogEntry<&'_ [u8]>>> {
        let mut log_collector = IntegrateLogCollector::default();
        let log_payload = loop {
            let has_more = self
                .collect_log_from_one_kv(&mut log_collector)
                .box_err()
                .context(manager::Read)?;

            if log_collector.is_integrate() {
                break log_collector.take_log().1;
            }
            if !has_more {
                return Ok(None);
            }
        };

        // To unblock pr#119, we use the following to simple resolve borrow-check error.
        // detail info: https://github.com/CeresDB/ceresdb/issues/120
        self.previous_value = log_payload;

        // Step current iterator, if it becomes invalid, reset `current_iter` to None
        // and advance `current_bucket_index`.
        self.step_current_iter().box_err().context(manager::Read)?;

        let log_entry = LogEntry {
            table_id: self.current_log_key.table_id,
            sequence: self.current_log_key.sequence_num,
            payload: self.previous_value.as_slice(),
        };

        Ok(Some(log_entry))
    }
}

/// Collect an integrate log from multiple log parts.
#[derive(Default)]
struct IntegrateLogCollector {
    log_key: Option<CommonLogKey>,
    log_payload: Vec<u8>,
}

impl IntegrateLogCollector {
    /// Collect the new log part.
    ///
    /// If the collected log is already integrate, nothing will be changed.
    /// If the new log is not part of the collected log, the collected log will
    /// be dropped and the collector tries to collect the new log.
    fn collect(&mut self, new_log_key: CommonLogKey, new_log_payload: &[u8]) {
        if self.log_key.is_none() {
            // Init the log key and value.
            self.init_log(new_log_key, new_log_payload);
            return;
        }

        // Check whether the log key/value is integrate.
        if self.is_integrate() {
            return;
        }

        // Check whether the new log matches the being-collected log.
        if self.is_part_log(&new_log_key) {
            // The new log is part of the being-collected log, and let's merge them.
            self.merge_log(new_log_key, new_log_payload);
        } else {
            // Ignore the collected log and reset the log state because it's not integrate.
            self.init_log(new_log_key, new_log_payload);
        }
    }

    #[inline]
    fn take_log(&mut self) -> (Option<CommonLogKey>, Vec<u8>) {
        (self.log_key.take(), std::mem::take(&mut self.log_payload))
    }

    #[inline]
    fn is_integrate(&self) -> bool {
        self.log_key
            .as_ref()
            .map(|v| !v.has_remaining())
            .unwrap_or(false)
    }

    #[inline]
    fn init_log(&mut self, new_log_key: CommonLogKey, new_log_payload: &[u8]) {
        self.log_key = Some(new_log_key);
        self.log_payload = new_log_payload.to_vec();
    }

    fn is_part_log(&self, new_log_key: &CommonLogKey) -> bool {
        let curr_log_key = self.log_key.as_ref().unwrap();

        curr_log_key.region_id == new_log_key.region_id
            && curr_log_key.table_id == new_log_key.table_id
            && curr_log_key.sequence_num == new_log_key.sequence_num
    }

    fn merge_log(&mut self, new_log_key: CommonLogKey, new_log_payload: &[u8]) {
        self.log_key = Some(new_log_key);
        self.log_payload.extend_from_slice(new_log_payload);
    }
}

struct TableUnitWriter;

// Blocking operations.
impl TableUnitWriter {
    fn insert_or_load_table_unit_entry<T: TableKv>(
        &mut self,
        table_kv: &T,
        table_unit_meta_table: &str,
        table_unit_entry: TableUnitEntry,
    ) -> Result<TableUnitEntry> {
        let key = encoding::format_table_unit_key(table_unit_entry.table_id);
        let value = table_unit_entry.encode().context(Encode {
            key: &key,
            meta_table: table_unit_meta_table,
        })?;

        let mut batch = T::WriteBatch::default();
        batch.insert(key.as_bytes(), &value);

        let res = table_kv.write(WriteContext::default(), table_unit_meta_table, batch);

        match &res {
            Ok(()) => Ok(table_unit_entry),
            Err(e) => {
                if e.is_primary_key_duplicate() {
                    TableUnit::load_table_unit_entry(
                        table_kv,
                        table_unit_meta_table,
                        table_unit_entry.table_id,
                    )?
                    .context(TableUnitNotExists {
                        region_id: table_unit_entry.table_id,
                    })
                } else {
                    res.box_err().context(WriteValue {
                        key: &key,
                        meta_table: table_unit_meta_table,
                    })?;

                    Ok(table_unit_entry)
                }
            }
        }
    }

    fn update_table_unit_entry<T: TableKv>(
        table_kv: &T,
        table_unit_meta_table: &str,
        table_unit_entry: &TableUnitEntry,
    ) -> Result<()> {
        let key = encoding::format_table_unit_key(table_unit_entry.table_id);
        let value = table_unit_entry.encode().context(Encode {
            key: &key,
            meta_table: table_unit_meta_table,
        })?;

        let mut batch = T::WriteBatch::default();
        batch.insert_or_update(key.as_bytes(), &value);

        table_kv
            .write(WriteContext::default(), table_unit_meta_table, batch)
            .box_err()
            .context(WriteValue {
                key: &key,
                meta_table: table_unit_meta_table,
            })
    }

    /// Allocate a continuous range of [SequenceNumber] and returns the starts
    /// [SequenceNumber] of the range [start, start + `number`].
    fn alloc_sequence_num(
        &mut self,
        table_unit_state: &TableUnitState,
        number: u64,
    ) -> Result<SequenceNumber> {
        ensure!(
            table_unit_state.last_sequence() < common_types::MAX_SEQUENCE_NUMBER,
            SequenceOverflow {
                table_id: table_unit_state.table_id,
            }
        );

        let last_sequence = table_unit_state
            .last_sequence
            .fetch_add(number, Ordering::Relaxed);
        Ok(last_sequence + 1)
    }
}

impl TableUnitWriter {
    async fn write_log<T: TableKv>(
        &mut self,
        runtime: &Runtime,
        table_kv: &T,
        table_unit_state: &TableUnitState,
        bucket: &BucketRef,
        ctx: &manager::WriteContext,
        log_batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        debug!(
            "Wal table unit begin writing, ctx:{:?}, wal location:{:?}, log_entries_num:{}",
            ctx,
            log_batch.location,
            log_batch.entries.len()
        );

        let region_id = log_batch.location.region_id;
        let table_id = log_batch.location.table_id;

        let splitter = LogBatchSplitEncoder {
            log_value_size_limit: 1000,
            write_batch_size_limit: 1000 * 1000,

            region_id,
            table_id,
        };
        let next_seq = self.alloc_sequence_num(table_unit_state, log_batch.entries.len() as u64)?;
        let (write_batches, max_seq_num) = splitter.encode(&log_batch.entries, next_seq)?;
        let table_kv = table_kv.clone();
        let bucket = bucket.clone();
        runtime
            .spawn_blocking(move || {
                let table_name = bucket.wal_shard_table(region_id);

                for wb in write_batches {
                    table_kv
                        .write(WriteContext::default(), table_name, wb)
                        .box_err()
                        .context(WriteLog { region_id })?
                }

                Ok(())
            })
            .await
            .context(RuntimeExec)??;

        Ok(max_seq_num)
    }

    /// Delete entries in the range `[0, sequence_num]`.
    ///
    /// The delete procedure is ensured to be sequential.
    async fn delete_entries_up_to<T: TableKv>(
        &mut self,
        runtime: &Runtime,
        table_kv: &T,
        table_unit_state: &TableUnitState,
        table_unit_meta_table: &str,
        mut sequence_num: SequenceNumber,
    ) -> Result<()> {
        debug!(
            "Try to delete entries, table_id:{}, sequence_num:{}, meta table:{}",
            table_unit_state.table_id, sequence_num, table_unit_meta_table
        );

        let last_sequence = table_unit_state.last_sequence();
        if sequence_num > last_sequence {
            sequence_num = last_sequence;
        }

        // Update min_sequence.
        let mut table_unit_entry = table_unit_state.table_unit_entry();
        if table_unit_entry.start_sequence <= sequence_num {
            table_unit_entry.start_sequence = sequence_num + 1;
        }

        debug!(
            "Update table unit entry due to deletion, table:{}, table_unit_entry:{:?}, meta table:{}",
            table_unit_meta_table, table_unit_entry, table_unit_meta_table
        );

        let table_kv = table_kv.clone();
        let table_unit_meta_table = table_unit_meta_table.to_string();
        runtime
            .spawn_blocking(move || {
                // Persist modification to table unit meta table.
                Self::update_table_unit_entry(&table_kv, &table_unit_meta_table, &table_unit_entry)
            })
            .await
            .context(RuntimeExec)??;

        // Update table unit state in memory.
        table_unit_state.set_start_sequence(table_unit_entry.start_sequence);

        Ok(())
    }
}

/// The location in the original log entry slice.
#[derive(Clone, Debug)]
enum LogEntryLocation {
    Integrate {
        index: usize,
    },
    Splitted {
        index: usize,
        /// The element in the vector is (offset, len).
        sub_entry_offsets: Vec<(usize, usize)>,
    },
}

struct SplittedEntriesInserter<'a, W> {
    write_batches_builder: &'a mut WriteBatchesBuilder<W>,
}

impl<'a, W: WriteBatch> SplittedEntriesInserter<'a, W> {
    fn insert(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.write_batches_builder.insert_entry(key, val)
    }

    fn finish(&mut self) {
        self.write_batches_builder.next_seq_num += 1;
    }
}
/// A write batch builder considering the size limit of a write batch, that is
/// to say, the built write batches's size won't exceed the
/// `size_limit_per_batch`.
struct WriteBatchesBuilder<W> {
    write_batches: Vec<W>,
    index_of_curr_batch: usize,
    size_of_curr_batch: usize,
    next_seq_num: u64,
    num_inserted_kvs: usize,

    size_limit_per_batch: usize,
    num_total_kvs: usize,
}

impl<W: WriteBatch> WriteBatchesBuilder<W> {
    fn new(num_total_kvs: usize, size_limit_per_batch: usize, next_seq_num: u64) -> Self {
        Self {
            write_batches: vec![W::with_capacity(num_total_kvs)],
            index_of_curr_batch: 0,
            size_of_curr_batch: 0,
            next_seq_num,
            num_inserted_kvs: 0,

            size_limit_per_batch,
            num_total_kvs,
        }
    }

    fn insert_entry(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        ensure!(
            key.len() + val.len() < self.size_limit_per_batch,
            TooSmallSizeLimitPerBatch {
                limit: self.size_limit_per_batch
            }
        );

        if !self.is_enough(key, val) {
            self.prepare_next_write_batch();
        }

        let curr_batch = &mut self.write_batches[self.index_of_curr_batch];
        curr_batch.insert(key, val);
        self.size_of_curr_batch += key.len() + val.len();
        self.num_inserted_kvs += 1;

        Ok(())
    }

    fn insert_splitted_entries(&mut self) -> SplittedEntriesInserter<'_, W> {
        SplittedEntriesInserter {
            write_batches_builder: self,
        }
    }

    fn insert_integrate_entry(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.insert_entry(key, val)?;
        self.next_seq_num += 1;

        Ok(())
    }

    #[inline]
    fn is_enough(&self, key: &[u8], val: &[u8]) -> bool {
        self.size_of_curr_batch + key.len() + val.len() < self.size_limit_per_batch
    }

    #[inline]
    fn next_seq_num(&self) -> u64 {
        self.next_seq_num
    }

    /// Return the sequence number of the last key/value pair.
    ///
    /// Return None if no entry is inserted.
    #[inline]
    fn last_seq_num(&self) -> Option<u64> {
        (self.num_inserted_kvs > 0).then_some(self.next_seq_num - 1)
    }

    fn prepare_next_write_batch(&mut self) {
        let remaining_kvs = self.num_total_kvs - self.num_inserted_kvs;
        let next_write_batch = W::with_capacity(remaining_kvs);
        self.index_of_curr_batch = self.write_batches.len();
        self.size_of_curr_batch = 0;

        self.write_batches.push(next_write_batch);
    }

    #[inline]
    fn build(self) -> Vec<W> {
        self.write_batches
    }
}

/// An encoder to encode logs into [WriteBatch], considering:
/// - The size limit for every log value
/// - The size limit for every write batch
struct LogBatchSplitEncoder {
    log_value_size_limit: usize,
    write_batch_size_limit: usize,

    region_id: RegionId,
    table_id: TableId,
}

impl LogBatchSplitEncoder {
    fn encode<W: WriteBatch>(
        &self,
        entries: &[LogWriteEntry],
        start_seq: u64,
    ) -> Result<(Vec<W>, SequenceNumber)> {
        ensure!(!entries.is_empty(), SplitEmptyLogs);
        let num_kvs: usize = entries
            .iter()
            .map(|v| (v.payload.len() + self.log_value_size_limit - 1) / self.log_value_size_limit)
            .sum();
        let mut write_batch_builder =
            WriteBatchesBuilder::<W>::new(num_kvs, self.write_batch_size_limit, start_seq);

        let mut key_buf = BytesMut::new();
        let log_encoding = CommonLogEncoding::newest();
        let locations = self.log_entry_locations(entries);
        for location in locations {
            match location {
                LogEntryLocation::Integrate { index } => {
                    let key = CommonLogKey::new(
                        self.region_id,
                        self.table_id,
                        write_batch_builder.next_seq_num(),
                    );
                    log_encoding
                        .encode_key(&mut key_buf, &key)
                        .context(LogCodec)?;
                    write_batch_builder
                        .insert_integrate_entry(&key_buf, &entries[index].payload)?;
                }
                LogEntryLocation::Splitted {
                    index,
                    sub_entry_offsets,
                } => {
                    let next_seq_num = write_batch_builder.next_seq_num;
                    let mut inserter = write_batch_builder.insert_splitted_entries();
                    for (offset, len) in sub_entry_offsets {
                        // The offset and len has been ensured to be in the valid range.
                        let num_remaining_bytes = entries[index].payload.len() - offset - len;
                        let key = CommonLogKey::part(
                            self.region_id,
                            self.table_id,
                            next_seq_num,
                            Some(num_remaining_bytes as u32),
                        );
                        log_encoding
                            .encode_key(&mut key_buf, &key)
                            .context(LogCodec)?;

                        let val = &entries[index].payload[offset..offset + len];
                        inserter.insert(&key_buf, val)?;
                    }
                    inserter.finish();
                }
            }
        }

        let last_seq_num = write_batch_builder.last_seq_num().unwrap();
        Ok((write_batch_builder.build(), last_seq_num))
    }

    fn log_entry_locations<'a>(
        &'a self,
        entries: &'a [LogWriteEntry],
    ) -> impl Iterator<Item = LogEntryLocation> + 'a {
        entries.iter().enumerate().map(|(index, entry)| {
            let payload_size = entry.payload.len();
            if payload_size > self.log_value_size_limit {
                let offsets = self.compute_sub_entry_offsets(payload_size);
                LogEntryLocation::Splitted {
                    index,
                    sub_entry_offsets: offsets,
                }
            } else {
                LogEntryLocation::Integrate { index }
            }
        })
    }

    #[inline]
    fn compute_sub_entry_offsets(&self, total_size: usize) -> Vec<(usize, usize)> {
        let num_sub_entries =
            (total_size + self.log_value_size_limit - 1) / self.log_value_size_limit;

        let mut sub_entry_offsets = Vec::with_capacity(num_sub_entries);
        for idx in 0..num_sub_entries {
            let offset = idx * self.log_value_size_limit;
            let len = if offset + self.log_value_size_limit > total_size {
                total_size - offset
            } else {
                self.log_value_size_limit
            };
            sub_entry_offsets.push((offset, len))
        }

        sub_entry_offsets
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[derive(Default)]
    struct MockWriteBatch {
        key_values: Vec<(Vec<u8>, Vec<u8>)>,
    }

    impl WriteBatch for MockWriteBatch {
        fn with_capacity(capacity: usize) -> Self {
            Self {
                key_values: Vec::with_capacity(capacity),
            }
        }

        fn insert(&mut self, key: &[u8], value: &[u8]) {
            self.key_values.push((key.to_vec(), value.to_vec()))
        }

        fn insert_or_update(&mut self, _key: &[u8], _value: &[u8]) {
            todo!()
        }

        fn delete(&mut self, _key: &[u8]) {
            todo!()
        }
    }

    fn check_encoded_write_batches(
        entries: &[LogWriteEntry],
        encoded_write_batches: &[MockWriteBatch],
        payload_size_limit: usize,
        write_batch_size_limit: usize,
    ) {
        let mut decoded_payloads = Vec::with_capacity(entries.len());
        let mut curr_payload: Vec<u8> = Vec::new();
        let mut prev_seq = None;

        let encoding = CommonLogEncoding::newest();
        for write_batch in encoded_write_batches {
            let mut write_batch_size = 0;

            for (key, val) in &write_batch.key_values {
                assert!(val.len() <= payload_size_limit);
                write_batch_size += key.len() + val.len();

                let log_key = encoding.decode_key(key).unwrap();

                match prev_seq {
                    None => curr_payload.extend_from_slice(val),
                    Some(prev_seq) => {
                        if prev_seq != log_key.sequence_num {
                            decoded_payloads.push(std::mem::take(&mut curr_payload));
                        }
                        curr_payload.extend_from_slice(val);
                    }
                }

                prev_seq = Some(log_key.sequence_num);
            }

            assert!(write_batch_size <= write_batch_size_limit);
        }
        decoded_payloads.push(std::mem::take(&mut curr_payload));

        let expect_payloads: Vec<_> = entries.iter().map(|v| v.payload.clone()).collect();
        assert_eq!(decoded_payloads, expect_payloads);
    }

    fn split_encode_and_check(
        payloads: Vec<Vec<u8>>,
        log_value_size_limit: usize,
        write_batch_size_limit: usize,
    ) {
        let entries: Vec<_> = payloads
            .into_iter()
            .map(|v| LogWriteEntry { payload: v })
            .collect();

        let encoder = LogBatchSplitEncoder {
            log_value_size_limit,
            write_batch_size_limit,
            region_id: 0,
            table_id: 0,
        };

        let next_seq = 100;
        let (write_batches, max_seq): (Vec<MockWriteBatch>, _) =
            encoder.encode(&entries, next_seq).unwrap();

        assert_eq!(max_seq, next_seq + entries.len() as u64 - 1);
        check_encoded_write_batches(
            &entries,
            &write_batches,
            log_value_size_limit,
            write_batch_size_limit,
        );
    }

    #[test]
    fn no_split_encode() {
        let payloads = vec![b"111000".to_vec(), b"00000".to_vec()];
        split_encode_and_check(payloads, 1000, 1000);
    }

    #[test]
    fn split_encode_large_payload() {
        let payloads = vec![
            b"111000".to_vec(),
            b"00000".to_vec(),
            b"000000xxxxxxxxxxxxxx".to_vec(),
        ];
        split_encode_and_check(payloads, 5, 1000);
    }

    #[test]
    fn split_encode_large_payload_and_large_write_batch() {
        let payloads = vec![
            b"111000".to_vec(),
            b"00000".to_vec(),
            b"000000xxxxxxxxxxxxxx".to_vec(),
            b"000000xxxxxxxxxxxxxx".to_vec(),
            b"000000xxxxxxxxxxxxxx".to_vec(),
            b"000000xxxxxxxxxxxxxx".to_vec(),
            b"000000xxxxxxxxxxxxxx".to_vec(),
            b"000000xxxxxxxxxxxxxx".to_vec(),
            b"000000xxxxxxxxxxxxxx".to_vec(),
        ];
        split_encode_and_check(payloads, 5, 40);
    }

    #[test]
    fn test_collect_normal_log() {
        let mut collector = IntegrateLogCollector::default();
        let key0 = CommonLogKey::part(0, 0, 0, None);
        collector.collect(key0.clone(), b"x");
        assert!(collector.is_integrate());
        assert_eq!((Some(key0), vec![b'x']), collector.take_log());
    }

    #[test]
    fn test_collect_multiple_keys_logs() {
        let mut collector = IntegrateLogCollector::default();
        let key0 = CommonLogKey::part(0, 0, 0, None);
        let key1 = CommonLogKey::part(0, 0, 1, None);
        collector.collect(key0.clone(), b"0");
        assert!(collector.is_integrate());
        collector.collect(key1, b"1");
        assert!(collector.is_integrate());
        assert_eq!((Some(key0), vec![b'0']), collector.take_log());
    }

    #[test]
    fn test_collect_multiple_part_logs() {
        let mut collector = IntegrateLogCollector::default();

        let key0 = CommonLogKey::part(0, 0, 1, Some(5));
        let key1 = CommonLogKey::part(0, 0, 1, Some(2));
        let key2 = CommonLogKey::part(0, 0, 1, Some(0));
        let key3 = CommonLogKey::part(0, 0, 2, None);
        collector.collect(key0, b"0x");
        assert!(!collector.is_integrate());
        collector.collect(key1, b"999");
        assert!(!collector.is_integrate());
        collector.collect(key2.clone(), b"22");
        assert!(collector.is_integrate());
        collector.collect(key3, b"33");

        assert_eq!((Some(key2), b"0x99922".to_vec()), collector.take_log())
    }
}
