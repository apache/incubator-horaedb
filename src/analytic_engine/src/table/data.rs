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

//! Table data

use std::{
    collections::HashMap,
    convert::TryInto,
    fmt,
    fmt::Formatter,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use arena::CollectorRef;
use common_types::{
    self,
    schema::{Schema, Version},
    table::ShardId,
    time::{TimeRange, Timestamp},
    SequenceNumber,
};
use generic_error::{GenericError, GenericResult};
use id_allocator::IdAllocator;
use logger::{debug, info};
use macros::define_result;
use object_store::Path;
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::table::{SchemaId, TableId};
use time_ext::ReadableDuration;

use crate::{
    instance::serial_executor::TableOpSerialExecutor,
    manifest::{
        meta_edit::{AddTableMeta, MetaEdit, MetaEditRequest, MetaUpdate, VersionEditMeta},
        ManifestRef,
    },
    memtable::{
        columnar::factory::ColumnarMemTableFactory,
        factory::{FactoryRef as MemTableFactoryRef, Options as MemTableOptions},
        layered::factory::LayeredMemtableFactory,
        skiplist::factory::SkiplistMemTableFactory,
        MemtableType,
    },
    space::SpaceId,
    sst::{file::FilePurger, manager::FileId},
    table::{
        metrics::{Metrics, MetricsContext},
        sst_util,
        version::{MemTableForWrite, MemTableState, SamplingMemTable, TableVersion},
    },
    MetricsOptions, TableOptions,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create memtable, err:{}", source))]
    CreateMemTable { source: crate::memtable::Error },

    #[snafu(display(
        "Failed to find or create memtable, timestamp overflow, timestamp:{:?}, duration:{:?}.\nBacktrace:\n{}",
        timestamp,
        duration,
        backtrace,
    ))]
    TimestampOverflow {
        timestamp: Timestamp,
        duration: Duration,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to find memtable for write, err:{}", source))]
    FindMemTable {
        source: crate::table::version::Error,
    },

    #[snafu(display("Failed to alloc file id, err:{}", source))]
    AllocFileId { source: GenericError },

    #[snafu(display("Found invalid table opts, msg:{msg}.\nBacktrace:\n{backtrace}"))]
    InvalidTableOpts { msg: String, backtrace: Backtrace },
}

define_result!(Error);

pub type MemTableId = u64;

pub const DEFAULT_ALLOC_STEP: u64 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableShardInfo {
    pub shard_id: ShardId,
}

impl TableShardInfo {
    pub fn new(shard_id: ShardId) -> Self {
        Self { shard_id }
    }
}

/// `atomic_enum` macro will expand method like
/// ```text
/// compare_exchange(..) -> Result<TableStatus, TableStatus>
/// ```
/// The result type is conflict with outer
/// Result, so add this hack
// TODO: fix this in atomic_enum crate.
mod hack {
    use atomic_enum::atomic_enum;

    #[atomic_enum]
    #[derive(PartialEq)]
    pub enum TableStatus {
        Ok = 0,
        Closed,
        /// No write/alter are allowed after table is dropped.
        Dropped,
    }
}

use self::hack::{AtomicTableStatus, TableStatus};

pub struct TableDesc {
    pub id: TableId,
    pub shard_id: ShardId,
    pub space_id: SpaceId,
    pub schema_id: SchemaId,
    pub schema_name: String,
    pub catalog_name: String,
    pub name: String,
    pub schema: Schema,
}

pub struct TableConfig {
    pub preflush_write_buffer_size_ratio: f32,
    pub manifest_snapshot_every_n_updates: NonZeroUsize,
    pub metrics_opt: MetricsOptions,
    pub enable_primary_key_sampling: bool,
}

#[derive(Debug, Clone)]
pub struct TableCatalogInfo {
    pub schema_id: SchemaId,
    pub schema_name: String,
    pub catalog_name: String,
}

impl TableCatalogInfo {
    pub fn new(schema_id: SchemaId, schema_name: String, catalog_name: String) -> Self {
        Self {
            schema_id,
            schema_name,
            catalog_name,
        }
    }
}

/// Data of a table
pub struct TableData {
    /// Id of this table
    pub id: TableId,
    /// Name of this table
    pub name: String,
    /// Schema of this table
    schema: Mutex<Schema>,
    pub table_catalog_info: TableCatalogInfo,
    /// Space id of this table
    pub space_id: SpaceId,

    /// Mutable memtable memory size limitation
    mutable_limit: AtomicU32,
    /// Mutable memtable memory usage ratio of the write buffer size.
    mutable_limit_write_buffer_ratio: f32,

    /// Options of this table
    ///
    /// Most modification to `opts` can be done by replacing the old options
    /// with a new one. However, altering the segment duration should be done
    /// carefully to avoid the reader seeing inconsistent segment duration
    /// and memtables/ssts during query/compaction/flush .
    opts: ArcSwap<TableOptions>,
    /// MemTable factory of this table
    memtable_factory: MemTableFactoryRef,
    /// Space memtable memory usage collector
    mem_usage_collector: CollectorRef,

    /// Current table version
    current_version: TableVersion,
    /// Last sequence visible to the reads
    ///
    /// Write to last_sequence should be guarded by a mutex and only done by
    /// single writer, but reads are allowed to be done concurrently without
    /// mutex protected
    last_sequence: AtomicU64,

    /// Auto incremented id to track memtable, reset on engine open
    ///
    /// Allocating memtable id should be guarded by write lock
    last_memtable_id: AtomicU64,

    /// Allocating file id
    allocator: IdAllocator,

    /// Last flush time
    ///
    /// Not persist, used to determine if this table should flush.
    last_flush_time_ms: AtomicU64,

    /// Table Status
    status: AtomicTableStatus,

    /// Manifest updates after last snapshot
    manifest_updates: AtomicUsize,

    /// Every n manifest updates to trigger a snapshot
    manifest_snapshot_every_n_updates: NonZeroUsize,

    /// Whether enable primary key sampling
    enable_primary_key_sampling: bool,

    /// Whether enable layered memtable
    pub enable_layered_memtable: bool,

    /// Metrics of this table
    pub metrics: Metrics,

    /// Shard info of the table
    pub shard_info: TableShardInfo,

    /// The table operation serial_exec
    pub serial_exec: tokio::sync::Mutex<TableOpSerialExecutor>,
}

impl fmt::Debug for TableData {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableData")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("space", &self.space_id)
            .field("mutable_limit", &self.mutable_limit)
            .field("opts", &self.opts)
            .field("last_sequence", &self.last_sequence)
            .field("last_memtable_id", &self.last_memtable_id)
            .field("status", &self.status.load(Ordering::Relaxed))
            .field("shard_info", &self.shard_info)
            .finish()
    }
}

impl Drop for TableData {
    fn drop(&mut self) {
        debug!("TableData is dropped, id:{}, name:{}", self.id, self.name);
    }
}

#[inline]
fn compute_mutable_limit(
    write_buffer_size: u32,
    mutable_limit_write_buffer_size_ratio: f32,
) -> u32 {
    assert!((0.0..=1.0).contains(&mutable_limit_write_buffer_size_ratio));

    let limit = write_buffer_size as f32 * mutable_limit_write_buffer_size_ratio;
    // This is safe because the limit won't be larger than the write_buffer_size.
    limit as u32
}

pub struct MemSizeOptions {
    pub collector: CollectorRef,
    pub size_sampling_interval: ReadableDuration,
}

impl TableData {
    /// Create a new TableData
    ///
    /// This function should only be called when a new table is creating and
    /// there is no existing data of the table
    pub fn new(
        desc: TableDesc,
        opts: TableOptions,
        config: TableConfig,
        purger: &FilePurger,
        mem_size_options: MemSizeOptions,
    ) -> Result<Self> {
        // TODO: Validate TableOptions, such as bucket_duration >=
        // segment_duration and bucket_duration is aligned to segment_duration

        let TableDesc {
            space_id,
            schema_id,
            schema_name,
            catalog_name,
            shard_id,
            id,
            name,
            schema,
        } = desc;
        let TableConfig {
            preflush_write_buffer_size_ratio,
            manifest_snapshot_every_n_updates,
            metrics_opt,
            enable_primary_key_sampling,
        } = config;

        let memtable_factory: MemTableFactoryRef = match opts.memtable_type {
            MemtableType::SkipList => Arc::new(SkiplistMemTableFactory),
            MemtableType::Column => Arc::new(ColumnarMemTableFactory),
        };

        let enable_layered_memtable = opts.layered_memtable_opts.enable;
        let memtable_factory = if enable_layered_memtable {
            let mutable_segment_switch_threshold = opts
                .layered_memtable_opts
                .mutable_segment_switch_threshold
                .0 as usize;

            ensure!(
                mutable_segment_switch_threshold > 0,
                InvalidTableOpts {
                    msg: "layered memtable is enabled but mutable_switch_threshold is 0",
                }
            );

            Arc::new(LayeredMemtableFactory::new(
                memtable_factory,
                mutable_segment_switch_threshold,
            ))
        } else {
            memtable_factory
        };

        let purge_queue = purger.create_purge_queue(space_id, id);
        let current_version =
            TableVersion::new(mem_size_options.size_sampling_interval, purge_queue);
        let metrics_ctx = MetricsContext::new(&name, shard_id, metrics_opt);
        let metrics = Metrics::new(metrics_ctx);
        let mutable_limit = AtomicU32::new(compute_mutable_limit(
            opts.write_buffer_size,
            preflush_write_buffer_size_ratio,
        ));

        Ok(Self {
            id,
            name,
            schema: Mutex::new(schema),
            table_catalog_info: TableCatalogInfo {
                schema_id,
                schema_name,
                catalog_name,
            },
            space_id,
            mutable_limit,
            mutable_limit_write_buffer_ratio: preflush_write_buffer_size_ratio,
            opts: ArcSwap::new(Arc::new(opts)),
            memtable_factory,
            mem_usage_collector: mem_size_options.collector,
            current_version,
            last_sequence: AtomicU64::new(0),
            last_memtable_id: AtomicU64::new(0),
            allocator: IdAllocator::new(0, 0, DEFAULT_ALLOC_STEP),
            last_flush_time_ms: AtomicU64::new(0),
            status: TableStatus::Ok.into(),
            metrics,
            shard_info: TableShardInfo::new(shard_id),
            serial_exec: tokio::sync::Mutex::new(TableOpSerialExecutor::new(id)),
            manifest_updates: AtomicUsize::new(0),
            manifest_snapshot_every_n_updates,
            enable_primary_key_sampling,
            enable_layered_memtable,
        })
    }

    /// Recover table from add table meta
    ///
    /// This wont recover sequence number, which will be set after wal replayed
    pub fn recover_from_add(
        add_meta: AddTableMeta,
        purger: &FilePurger,
        shard_id: ShardId,
        config: TableConfig,
        mem_size_options: MemSizeOptions,
        allocator: IdAllocator,
        table_catalog_info: TableCatalogInfo,
    ) -> Result<Self> {
        let TableConfig {
            preflush_write_buffer_size_ratio,
            manifest_snapshot_every_n_updates,
            metrics_opt,
            enable_primary_key_sampling,
        } = config;

        let memtable_factory: MemTableFactoryRef = match add_meta.opts.memtable_type {
            MemtableType::SkipList => Arc::new(SkiplistMemTableFactory),
            MemtableType::Column => Arc::new(ColumnarMemTableFactory),
        };
        // Maybe wrap it by `LayeredMemtable`.
        let enable_layered_memtable = add_meta.opts.layered_memtable_opts.enable;
        let memtable_factory = if enable_layered_memtable {
            let mutable_segment_switch_threshold = add_meta
                .opts
                .layered_memtable_opts
                .mutable_segment_switch_threshold
                .0 as usize;

            ensure!(
                mutable_segment_switch_threshold > 0,
                InvalidTableOpts {
                    msg: "layered memtable is enabled but mutable_switch_threshold is 0",
                }
            );

            Arc::new(LayeredMemtableFactory::new(
                memtable_factory,
                mutable_segment_switch_threshold,
            )) as _
        } else {
            memtable_factory as _
        };

        let purge_queue = purger.create_purge_queue(add_meta.space_id, add_meta.table_id);
        let current_version =
            TableVersion::new(mem_size_options.size_sampling_interval, purge_queue);
        let metrics_ctx = MetricsContext::new(&add_meta.table_name, shard_id, metrics_opt);
        let metrics = Metrics::new(metrics_ctx);
        let mutable_limit = AtomicU32::new(compute_mutable_limit(
            add_meta.opts.write_buffer_size,
            preflush_write_buffer_size_ratio,
        ));

        Ok(Self {
            id: add_meta.table_id,
            name: add_meta.table_name,
            schema: Mutex::new(add_meta.schema),
            table_catalog_info,
            space_id: add_meta.space_id,
            mutable_limit,
            mutable_limit_write_buffer_ratio: preflush_write_buffer_size_ratio,
            opts: ArcSwap::new(Arc::new(add_meta.opts)),
            memtable_factory,
            mem_usage_collector: mem_size_options.collector,
            current_version,
            last_sequence: AtomicU64::new(0),
            last_memtable_id: AtomicU64::new(0),
            allocator,
            last_flush_time_ms: AtomicU64::new(0),
            status: TableStatus::Ok.into(),
            metrics,
            shard_info: TableShardInfo::new(shard_id),
            serial_exec: tokio::sync::Mutex::new(TableOpSerialExecutor::new(add_meta.table_id)),
            manifest_updates: AtomicUsize::new(0),
            manifest_snapshot_every_n_updates,
            enable_primary_key_sampling,
            enable_layered_memtable,
        })
    }

    /// Get current schema of the table.
    pub fn schema(&self) -> Schema {
        self.schema.lock().unwrap().clone()
    }

    /// Set current schema of the table.
    pub fn set_schema(&self, schema: Schema) {
        *self.schema.lock().unwrap() = schema;
    }

    /// Get current version of schema.
    pub fn schema_version(&self) -> Version {
        self.schema.lock().unwrap().version()
    }

    /// Get current table version
    #[inline]
    pub fn current_version(&self) -> &TableVersion {
        &self.current_version
    }

    /// Get last sequence number
    #[inline]
    pub fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence.load(Ordering::Acquire)
    }

    /// Set last sequence number
    #[inline]
    pub fn set_last_sequence(&self, seq: SequenceNumber) {
        self.last_sequence.store(seq, Ordering::Release);
    }

    #[inline]
    pub fn next_sequence(&self) -> SequenceNumber {
        self.last_sequence.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Get last flush time
    #[inline]
    pub fn last_flush_time(&self) -> u64 {
        self.last_flush_time_ms.load(Ordering::Relaxed)
    }

    /// Set last flush time
    #[inline]
    pub fn set_last_flush_time(&self, time: u64) {
        self.last_flush_time_ms.store(time, Ordering::Release);
    }

    #[inline]
    pub fn table_options(&self) -> Arc<TableOptions> {
        self.opts.load().clone()
    }

    /// Update table options.
    #[inline]
    pub fn set_table_options(&self, opts: TableOptions) {
        let mutable_limit = compute_mutable_limit(
            opts.write_buffer_size,
            self.mutable_limit_write_buffer_ratio,
        );
        self.mutable_limit.store(mutable_limit, Ordering::Relaxed);
        self.opts.store(Arc::new(opts))
    }

    #[inline]
    pub fn is_dropped(&self) -> bool {
        self.status.load(Ordering::SeqCst) == TableStatus::Dropped
    }

    /// Set the table is dropped and forbid any writes/alter on this table.
    #[inline]
    pub fn set_dropped(&self) {
        self.status.store(TableStatus::Dropped, Ordering::SeqCst)
    }

    #[inline]
    pub fn set_closed(&self) {
        self.status.store(TableStatus::Closed, Ordering::SeqCst)
    }

    #[inline]
    pub fn allow_compaction(&self) -> bool {
        match self.status.load(Ordering::SeqCst) {
            TableStatus::Ok => true,
            TableStatus::Closed | TableStatus::Dropped => false,
        }
    }

    /// Returns total memtable memory usage in bytes.
    #[inline]
    pub fn memtable_memory_usage(&self) -> usize {
        self.current_version.total_memory_usage()
    }

    /// Returns mutable memtable memory usage in bytes.
    #[inline]
    pub fn mutable_memory_usage(&self) -> usize {
        self.current_version.mutable_memory_usage()
    }

    /// Find memtable for given timestamp to insert, create if not exists
    ///
    /// If the memtable schema is outdated, switch all memtables and create the
    /// needed mutable memtable by current schema. The returned memtable is
    /// guaranteed to have same schema of current table
    pub fn find_or_create_mutable(
        &self,
        timestamp: Timestamp,
        table_schema: &Schema,
    ) -> Result<MemTableForWrite> {
        let last_sequence = self.last_sequence();

        if let Some(mem) = self
            .current_version
            .memtable_for_write(timestamp, table_schema.version())
            .context(FindMemTable)?
        {
            return Ok(mem);
        }

        // Mutable memtable for this timestamp not found, need to create a new one.
        let table_options = self.table_options();
        let memtable_opts = MemTableOptions {
            schema: table_schema.clone(),
            arena_block_size: table_options.arena_block_size,
            creation_sequence: last_sequence,
            collector: self.mem_usage_collector.clone(),
        };
        let mem = self
            .memtable_factory
            .create_memtable(memtable_opts)
            .context(CreateMemTable)?;

        // Currently only do sample when segment duration is none.
        // TODO: add a new flag to represent this since we may need to sample other
        // info, such as primary keys.
        match table_options.segment_duration() {
            Some(segment_duration) => {
                let time_range = TimeRange::bucket_of(timestamp, segment_duration).context(
                    TimestampOverflow {
                        timestamp,
                        duration: segment_duration,
                    },
                )?;
                let mem_state = MemTableState {
                    mem,
                    aligned_time_range: time_range,
                    id: self.alloc_memtable_id(),
                };

                // Insert memtable into mutable memtables of current version.
                self.current_version.insert_mutable(mem_state.clone());

                Ok(MemTableForWrite::Normal(mem_state))
            }
            None => {
                let mut sampling_mem = SamplingMemTable::new(mem, self.alloc_memtable_id());
                if self.enable_primary_key_sampling && table_options.support_sample_pk() {
                    sampling_mem.set_pk_sampler(table_schema);
                }

                debug!(
                    "create sampling mem table:{}, schema:{:#?}",
                    sampling_mem.id, table_schema
                );
                // Set sampling memtables of current version.
                self.current_version.set_sampling(sampling_mem.clone());

                Ok(MemTableForWrite::Sampling(sampling_mem))
            }
        }
    }

    /// Returns true if the memory usage of this table reaches flush threshold
    pub fn should_flush_table(&self, in_flush: bool) -> bool {
        // Fallback to usize::MAX if Failed to convert arena_block_size into
        // usize (overflow)
        let max_write_buffer_size = self
            .table_options()
            .write_buffer_size
            .try_into()
            .unwrap_or(usize::MAX);
        let mutable_limit = self
            .mutable_limit
            .load(Ordering::Relaxed)
            .try_into()
            .unwrap_or(usize::MAX);

        let mutable_usage = self.current_version.mutable_memory_usage();
        let total_usage = self.current_version.total_memory_usage();
        // Inspired by https://github.com/facebook/rocksdb/blob/main/include/rocksdb/write_buffer_manager.h#L94
        if mutable_usage > mutable_limit && !in_flush {
            info!(
                "TableData should flush by mutable limit, table:{}, table_id:{}, mutable_usage:{}, mutable_limit: {}, total_usage:{}, max_write_buffer_size:{}",
                self.name, self.id, mutable_usage, mutable_limit, total_usage, max_write_buffer_size
            );
            return true;
        }

        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        let should_flush =
            total_usage >= max_write_buffer_size && mutable_usage >= max_write_buffer_size / 2;

        debug!(
            "Check should flush, table:{}, table_id:{}, mutable_usage:{}, mutable_limit: {}, total_usage:{}, max_write_buffer_size:{}",
            self.name, self.id, mutable_usage, mutable_limit, total_usage, max_write_buffer_size
        );

        if should_flush {
            info!(
                "TableData should flush by total usage, table:{}, table_id:{}, mutable_usage:{}, mutable_limit: {}, total_usage:{}, max_write_buffer_size:{}",
                self.name, self.id, mutable_usage, mutable_limit, total_usage, max_write_buffer_size
            );
        }

        should_flush
    }

    /// Use allocator to alloc a file id for a new file.
    pub async fn alloc_file_id(&self, manifest: &ManifestRef) -> Result<FileId> {
        // Persist next max file id to manifest.
        let persist_max_file_id = move |next_max_file_id| async move {
            self.persist_max_file_id(manifest, next_max_file_id).await
        };

        self.allocator
            .alloc_id(persist_max_file_id)
            .await
            .context(AllocFileId)
    }

    async fn persist_max_file_id(
        &self,
        manifest: &ManifestRef,
        next_max_file_id: FileId,
    ) -> GenericResult<()> {
        let manifest_update = VersionEditMeta {
            space_id: self.space_id,
            table_id: self.id,
            flushed_sequence: 0,
            files_to_add: vec![],
            files_to_delete: vec![],
            mems_to_remove: vec![],
            max_file_id: next_max_file_id,
        };
        let edit_req = {
            let meta_update = MetaUpdate::VersionEdit(manifest_update);
            MetaEditRequest {
                shard_info: self.shard_info,
                meta_edit: MetaEdit::Update(meta_update),
                table_catalog_info: self.table_catalog_info.clone(),
            }
        };
        // table version's max file id will be update when apply this meta update.
        manifest.apply_edit(edit_req).await?;
        Ok(())
    }

    /// Get sst file path in the object storage
    pub fn sst_file_path(&self, file_id: FileId) -> Path {
        sst_util::new_sst_file_path(self.space_id, self.id, file_id)
    }

    pub fn compaction_task_key(&self, file_id: FileId) -> String {
        format!("{}-{file_id}", self.id)
    }

    /// Allocate next memtable id
    fn alloc_memtable_id(&self) -> MemTableId {
        let last = self.last_memtable_id.fetch_add(1, Ordering::Relaxed);
        last + 1
    }

    /// Returns last memtable id
    pub fn last_memtable_id(&self) -> MemTableId {
        self.last_memtable_id.load(Ordering::Relaxed)
    }

    pub fn dedup(&self) -> bool {
        self.table_options().need_dedup()
    }

    pub fn is_expired(&self, timestamp: Timestamp) -> bool {
        self.table_options().is_expired(timestamp)
    }

    pub fn table_location(&self) -> TableLocation {
        TableLocation {
            id: self.id.as_u64(),
            shard_info: self.shard_info,
        }
    }

    pub fn increase_manifest_updates(&self, updates_num: usize) {
        self.manifest_updates
            .fetch_add(updates_num, Ordering::Relaxed);
    }

    pub fn should_do_manifest_snapshot(&self) -> bool {
        let updates = self.manifest_updates.load(Ordering::Relaxed);
        updates >= self.manifest_snapshot_every_n_updates.get()
    }

    pub fn reset_manifest_updates(&self) {
        self.manifest_updates.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TableLocation {
    pub id: common_types::table::TableId,
    pub shard_info: TableShardInfo,
}

/// Table data reference
pub type TableDataRef = Arc<TableData>;

/// Manages TableDataRef
#[derive(Debug, Default)]
pub struct TableDataSet {
    /// Name to table data
    table_datas: HashMap<String, TableDataRef>,
    /// Id to table data
    id_to_tables: HashMap<TableId, TableDataRef>,
}

impl TableDataSet {
    /// Create an empty TableDataSet
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert if absent, if successfully inserted, return true and return
    /// false if the data already exists
    pub fn insert_if_absent(&mut self, table_data_ref: TableDataRef) -> bool {
        let table_name = &table_data_ref.name;
        if self.table_datas.contains_key(table_name) {
            let exist_table = self.table_datas.get(table_name).unwrap();
            logger::error!(
                "found duplicated table_name:{}, exist_table_id:{}, exist_table_shard_id:{}, inserted_table_id:{}, inserted_table_shard_id:{}",
                table_name,
                exist_table.id,
                exist_table.shard_info.shard_id,
                table_data_ref.id,
                table_data_ref.shard_info.shard_id,
            );
            return false;
        }
        self.table_datas
            .insert(table_name.to_string(), table_data_ref.clone());
        self.id_to_tables.insert(table_data_ref.id, table_data_ref);
        true
    }

    /// Find table by table name
    pub fn find_table(&self, table_name: &str) -> Option<TableDataRef> {
        self.table_datas.get(table_name).cloned()
    }

    /// Find table by table id
    pub fn find_table_by_id(&self, table_id: TableId) -> Option<TableDataRef> {
        self.id_to_tables.get(&table_id).cloned()
    }

    /// Remove table by table name
    pub fn remove_table(&mut self, table_name: &str) -> Option<TableDataRef> {
        let table = self.table_datas.remove(table_name)?;
        self.id_to_tables.remove(&table.id);
        Some(table)
    }

    /// Returns the total table num in this set
    pub fn table_num(&self) -> usize {
        self.table_datas.len()
    }

    pub fn find_maximum_memory_usage_table(&self) -> Option<TableDataRef> {
        // TODO: Possible performance issue here when there are too many tables.
        self.table_datas
            .values()
            .max_by_key(|t| t.memtable_memory_usage())
            .cloned()
    }

    pub fn total_memory_usage(&self) -> usize {
        if self.table_datas.is_empty() {
            return 0;
        }
        // TODO: Possible performance issue here when there are too many tables.
        self.table_datas
            .values()
            .map(|t| t.memtable_memory_usage())
            .sum()
    }

    pub fn find_maximum_mutable_memory_usage_table(&self) -> Option<TableDataRef> {
        // TODO: Possible performance issue here when there are too many tables.
        self.table_datas
            .values()
            .max_by_key(|t| t.mutable_memory_usage())
            .cloned()
    }

    /// List all tables to `tables`
    pub fn list_all_tables(&self, tables: &mut Vec<TableDataRef>) {
        for table_data in self.table_datas.values().cloned() {
            tables.push(table_data);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use arena::NoopCollector;
    use common_types::{datum::DatumKind, table::DEFAULT_SHARD_ID};
    use table_engine::{
        engine::{CreateTableParams, CreateTableRequest, TableState},
        table::SchemaId,
    };
    use time_ext::ReadableDuration;

    use super::*;
    use crate::{
        memtable::{factory::Factory, MemTableRef},
        sst::file::tests::FilePurgerMocker,
        table_options,
        tests::table,
    };

    const DEFAULT_SPACE_ID: SpaceId = 1;
    const DEFAULT_SCHEMA_ID: SchemaId = SchemaId::from_u32(2);

    pub fn default_schema() -> Schema {
        table::create_schema_builder(
            &[("key", DatumKind::Timestamp)],
            &[("value", DatumKind::Double)],
        )
        .build()
        .unwrap()
    }

    #[derive(Default)]
    pub struct MemTableMocker;

    impl MemTableMocker {
        pub fn build(&self) -> MemTableRef {
            let memtable_opts = MemTableOptions {
                schema: default_schema(),
                arena_block_size: 1024 * 1024,
                creation_sequence: 1000,
                collector: Arc::new(NoopCollector),
            };

            let factory = SkiplistMemTableFactory;
            factory.create_memtable(memtable_opts).unwrap()
        }

        pub fn build_columnar(&self) -> MemTableRef {
            let memtable_opts = MemTableOptions {
                schema: default_schema(),
                arena_block_size: 1024 * 1024,
                creation_sequence: 1000,
                collector: Arc::new(NoopCollector),
            };

            let factory = ColumnarMemTableFactory;
            factory.create_memtable(memtable_opts).unwrap()
        }
    }

    #[must_use]
    pub struct TableDataMocker {
        table_id: TableId,
        table_name: String,
        shard_id: ShardId,
        manifest_snapshot_every_n_updates: NonZeroUsize,
    }

    impl TableDataMocker {
        pub fn table_id(mut self, table_id: TableId) -> Self {
            self.table_id = table_id;
            self
        }

        pub fn table_name(mut self, table_name: String) -> Self {
            self.table_name = table_name;
            self
        }

        pub fn shard_id(mut self, shard_id: ShardId) -> Self {
            self.shard_id = shard_id;
            self
        }

        pub fn manifest_snapshot_every_n_updates(
            mut self,
            manifest_snapshot_every_n_updates: NonZeroUsize,
        ) -> Self {
            self.manifest_snapshot_every_n_updates = manifest_snapshot_every_n_updates;
            self
        }

        pub fn build(self) -> TableData {
            let space_id = DEFAULT_SPACE_ID;
            let schema_id = DEFAULT_SCHEMA_ID;
            let table_schema = default_schema();
            let params = CreateTableParams {
                catalog_name: "test_catalog".to_string(),
                schema_name: "public".to_string(),
                table_name: self.table_name,
                table_schema,
                engine: table_engine::ANALYTIC_ENGINE_TYPE.to_string(),
                table_options: HashMap::new(),
                partition_info: None,
            };
            let create_request = CreateTableRequest {
                params,
                schema_id: SchemaId::from_u32(DEFAULT_SPACE_ID),
                table_id: self.table_id,
                state: TableState::Stable,
                shard_id: self.shard_id,
            };

            let table_opts = TableOptions::default();
            let purger = FilePurgerMocker::mock();
            let collector = Arc::new(NoopCollector);

            let mem_size_options = MemSizeOptions {
                collector,
                size_sampling_interval: Default::default(),
            };

            TableData::new(
                TableDesc {
                    id: create_request.table_id,
                    shard_id: create_request.shard_id,
                    space_id,
                    schema_id,
                    catalog_name: "test_catalog".to_string(),
                    schema_name: "public".to_string(),
                    name: create_request.params.table_name,
                    schema: create_request.params.table_schema,
                },
                table_opts,
                TableConfig {
                    preflush_write_buffer_size_ratio: 0.75,
                    manifest_snapshot_every_n_updates: self.manifest_snapshot_every_n_updates,
                    metrics_opt: MetricsOptions::default(),
                    enable_primary_key_sampling: false,
                },
                &purger,
                mem_size_options,
            )
            .unwrap()
        }
    }

    impl Default for TableDataMocker {
        fn default() -> Self {
            Self {
                table_id: table::new_table_id(2, 1),
                table_name: "mocked_table".to_string(),
                shard_id: DEFAULT_SHARD_ID,
                manifest_snapshot_every_n_updates: NonZeroUsize::new(usize::MAX).unwrap(),
            }
        }
    }

    #[test]
    fn test_new_table_data() {
        let table_id = table::new_table_id(100, 30);
        let table_name = "new_table".to_string();
        let shard_id = 42;
        let table_data = TableDataMocker::default()
            .table_id(table_id)
            .table_name(table_name.clone())
            .shard_id(shard_id)
            .build();

        assert_eq!(table_id, table_data.id);
        assert_eq!(table_name, table_data.name);
        assert_eq!(TableShardInfo::new(shard_id), table_data.shard_info);
        assert_eq!(0, table_data.last_sequence());
        assert!(!table_data.is_dropped());
        assert_eq!(0, table_data.last_memtable_id());
        assert!(table_data.dedup());
    }

    #[test]
    fn test_find_or_create_mutable() {
        let table_data = TableDataMocker::default().build();
        let schema = table_data.schema();

        // Create sampling memtable.
        let zero_ts = Timestamp::new(0);
        let mutable = table_data.find_or_create_mutable(zero_ts, &schema).unwrap();
        assert!(mutable.accept_timestamp(zero_ts));
        let sampling_mem = mutable.as_sampling();
        let sampling_id = sampling_mem.id;
        assert_eq!(1, sampling_id);

        // Test memtable is reused.
        let now_ts = Timestamp::now();
        let mutable = table_data.find_or_create_mutable(now_ts, &schema).unwrap();
        assert!(mutable.accept_timestamp(now_ts));
        let sampling_mem = mutable.as_sampling();
        // Use same sampling memtable.
        assert_eq!(sampling_id, sampling_mem.id);

        let current_version = table_data.current_version();
        // Set segment duration manually.
        let mut table_opts = (*table_data.table_options()).clone();
        table_opts.segment_duration =
            Some(ReadableDuration(table_options::DEFAULT_SEGMENT_DURATION));
        table_data.set_table_options(table_opts);
        // Freeze sampling memtable.
        current_version.freeze_sampling_memtable();

        // A new mutable memtable should be created.
        let mutable = table_data.find_or_create_mutable(now_ts, &schema).unwrap();
        assert!(mutable.accept_timestamp(now_ts));
        let mem_state = mutable.as_normal();
        assert_eq!(2, mem_state.id);
        let time_range =
            TimeRange::bucket_of(now_ts, table_options::DEFAULT_SEGMENT_DURATION).unwrap();
        assert_eq!(time_range, mem_state.aligned_time_range);
    }

    #[test]
    fn test_compute_mutable_limit() {
        // Build the cases for compute_mutable_limit.
        let cases = vec![
            (80, 0.8, 64),
            (80, 0.5, 40),
            (80, 0.1, 8),
            (80, 0.0, 0),
            (80, 1.0, 80),
            (0, 0.8, 0),
            (0, 0.5, 0),
            (0, 0.1, 0),
            (0, 0.0, 0),
            (0, 1.0, 0),
        ];

        for (write_buffer_size, ratio, expected) in cases {
            let limit = compute_mutable_limit(write_buffer_size, ratio);
            assert_eq!(expected, limit);
        }
    }

    #[should_panic]
    #[test]
    fn test_compute_mutable_limit_panic() {
        compute_mutable_limit(80, 1.1);
        compute_mutable_limit(80, -0.1);
    }

    #[test]
    fn test_manifest_snapshot_trigger() {
        // When snapshot_every_n_updates is not zero.
        let table_data = TableDataMocker::default()
            .manifest_snapshot_every_n_updates(NonZeroUsize::new(5).unwrap())
            .build();

        check_manifest_snapshot_trigger(&table_data);
        // Reset and check again.
        table_data.reset_manifest_updates();
        check_manifest_snapshot_trigger(&table_data);
    }

    fn check_manifest_snapshot_trigger(table_data: &TableData) {
        // When no updates yet, result should be false.
        assert!(!table_data.should_do_manifest_snapshot());

        // Eq case.
        table_data.increase_manifest_updates(5);
        assert!(table_data.should_do_manifest_snapshot());

        // Greater case.
        table_data.increase_manifest_updates(5);
        assert!(table_data.should_do_manifest_snapshot());
    }
}
