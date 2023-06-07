// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Open logic of instance

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use common_types::table::ShardId;
use log::info;
use object_store::ObjectStoreRef;
use snafu::ResultExt;
use table_engine::{engine::TableDef, table::TableId};
use wal::manager::WalManagerRef;

use super::{engine::OpenTablesOfShard, flush_compaction::Flusher};
use crate::{
    compaction::scheduler::SchedulerImpl,
    context::OpenContext,
    engine,
    instance::{
        engine::{OpenManifest, ReadMetaUpdate, Result},
        mem_collector::MemUsageCollector,
        wal_replayer::WalReplayer,
        Instance, SpaceStore,
    },
    manifest::{details::ManifestImpl, LoadRequest, Manifest, ManifestRef},
    row_iter::IterOptions,
    space::{SpaceAndTable, SpaceRef, Spaces},
    sst::{
        factory::{FactoryRef as SstFactoryRef, ObjectStorePickerRef, ScanOptions},
        file::FilePurger,
    },
    table::data::TableDataRef,
    table_meta_set_impl::TableMetaSetImpl,
};

const MAX_RECORD_BATCHES_IN_FLIGHT_WHEN_COMPACTION_READ: usize = 64;

pub(crate) struct ManifestStorages {
    pub wal_manager: WalManagerRef,
    pub oss_storage: ObjectStoreRef,
}

impl Instance {
    /// Open a new instance
    pub(crate) async fn open(
        ctx: OpenContext,
        manifest_storages: ManifestStorages,
        wal_manager: WalManagerRef,
        store_picker: ObjectStorePickerRef,
        sst_factory: SstFactoryRef,
    ) -> Result<Arc<Self>> {
        let spaces: Arc<RwLock<Spaces>> = Arc::new(RwLock::new(Spaces::default()));
        let default_runtime = ctx.runtimes.default_runtime.clone();
        let file_purger = Arc::new(FilePurger::start(
            &default_runtime,
            store_picker.default_store().clone(),
        ));

        let table_meta_set_impl = Arc::new(TableMetaSetImpl {
            spaces: spaces.clone(),
            file_purger: file_purger.clone(),
            preflush_write_buffer_size_ratio: ctx.config.preflush_write_buffer_size_ratio,
        });
        let manifest = ManifestImpl::open(
            ctx.config.manifest.clone(),
            manifest_storages.wal_manager,
            manifest_storages.oss_storage,
            table_meta_set_impl,
        )
        .await
        .context(OpenManifest)?;

        let space_store = Arc::new(SpaceStore {
            spaces,
            manifest: Arc::new(manifest),
            wal_manager: wal_manager.clone(),
            store_picker: store_picker.clone(),
            sst_factory,
            meta_cache: ctx.meta_cache.clone(),
        });

        let scheduler_config = ctx.config.compaction.clone();
        let scan_options_for_compaction = ScanOptions {
            background_read_parallelism: 1,
            max_record_batches_in_flight: MAX_RECORD_BATCHES_IN_FLIGHT_WHEN_COMPACTION_READ,
        };
        let compaction_runtime = ctx.runtimes.compact_runtime.clone();
        let compaction_scheduler = Arc::new(SchedulerImpl::new(
            space_store.clone(),
            compaction_runtime,
            scheduler_config,
            ctx.config.write_sst_max_buffer_size.as_byte() as usize,
            scan_options_for_compaction,
        ));

        let scan_options = ScanOptions {
            background_read_parallelism: ctx.config.sst_background_read_parallelism,
            max_record_batches_in_flight: ctx.config.scan_max_record_batches_in_flight,
        };

        let iter_options = ctx
            .config
            .scan_batch_size
            .map(|batch_size| IterOptions { batch_size });
        let instance = Arc::new(Instance {
            space_store,
            runtimes: ctx.runtimes.clone(),
            table_opts: ctx.config.table_opts.clone(),

            compaction_scheduler,
            file_purger,
            meta_cache: ctx.meta_cache.clone(),
            mem_usage_collector: Arc::new(MemUsageCollector::default()),
            max_rows_in_write_queue: ctx.config.max_rows_in_write_queue,
            db_write_buffer_size: ctx.config.db_write_buffer_size,
            space_write_buffer_size: ctx.config.space_write_buffer_size,
            replay_batch_size: ctx.config.replay_batch_size,
            write_sst_max_buffer_size: ctx.config.write_sst_max_buffer_size.as_byte() as usize,
            max_retry_flush_limit: ctx.config.max_retry_flush_limit,
            max_bytes_per_write_batch: ctx
                .config
                .max_bytes_per_write_batch
                .map(|v| v.as_byte() as usize),
            iter_options,
            scan_options,
        });

        Ok(instance)
    }

    /// Open the table.
    pub async fn do_open_tables_of_shard(
        self: &Arc<Self>,
        context: TablesOfShardContext,
    ) -> Result<OpenTablesOfShardResult> {
        let mut shard_opener = ShardOpener::init(
            context,
            self.space_store.manifest.clone(),
            self.space_store.wal_manager.clone(),
            self.replay_batch_size,
            self.make_flusher(),
            self.max_retry_flush_limit,
        )?;

        shard_opener.open().await
    }
}

#[derive(Debug, Clone)]
pub struct TablesOfShardContext {
    /// Shard id
    pub shard_id: ShardId,
    /// Table infos
    pub table_ctxs: Vec<TableContext>,
}

#[derive(Clone, Debug)]
pub struct TableContext {
    pub table_def: TableDef,
    pub space: SpaceRef,
}

#[derive(Debug)]
enum TableOpenStage {
    RecoverTableMeta(RecoverTableMetaContext),
    RecoverTableData(RecoverTableDataContext),
    Failed(crate::instance::engine::Error),
    Success(Option<SpaceAndTable>),
}

#[derive(Debug)]
struct RecoverTableMetaContext {
    table_def: TableDef,
    space: SpaceRef,
}

#[derive(Debug)]
struct RecoverTableDataContext {
    table_data: TableDataRef,
    space: SpaceRef,
}

pub type OpenTablesOfShardResult = HashMap<TableId, Result<Option<SpaceAndTable>>>;

/// Opener for tables of the same shard
struct ShardOpener {
    shard_id: ShardId,
    manifest: ManifestRef,
    wal_manager: WalManagerRef,
    states: HashMap<TableId, TableOpenStage>,
    wal_replay_batch_size: usize,
    flusher: Flusher,
    max_retry_flush_limit: usize,
}

impl ShardOpener {
    fn init(
        shard_context: TablesOfShardContext,
        manifest: ManifestRef,
        wal_manager: WalManagerRef,
        wal_replay_batch_size: usize,
        flusher: Flusher,
        max_retry_flush_limit: usize,
    ) -> Result<Self> {
        let mut states = HashMap::with_capacity(shard_context.table_ctxs.len());
        for table_ctx in shard_context.table_ctxs {
            let space = &table_ctx.space;
            let table_id = table_ctx.table_def.id;
            let state = if let Some(table_data) = space.find_table_by_id(table_id) {
                // Table is possible to have been opened, we just mark it ready and ignore in
                // recovery.
                TableOpenStage::Success(Some(SpaceAndTable::new(space.clone(), table_data)))
            } else {
                TableOpenStage::RecoverTableMeta(RecoverTableMetaContext {
                    table_def: table_ctx.table_def,
                    space: table_ctx.space,
                })
            };
            states.insert(table_id, state);
        }

        Ok(Self {
            shard_id: shard_context.shard_id,
            manifest,
            wal_manager,
            states,
            wal_replay_batch_size,
            flusher,
            max_retry_flush_limit,
        })
    }

    async fn open(&mut self) -> Result<OpenTablesOfShardResult> {
        // Recover tables' metadata.
        self.recover_table_metas().await?;

        // Recover table' data.
        self.recover_table_datas().await?;

        // Retrieve the table results and return.
        let states = std::mem::take(&mut self.states);
        let mut table_results = HashMap::with_capacity(states.len());
        for (table_id, state) in states {
            match state {
                TableOpenStage::Failed(e) => {
                    table_results.insert(table_id, Err(e));
                }
                TableOpenStage::Success(data) => {
                    table_results.insert(table_id, Ok(data));
                }
                TableOpenStage::RecoverTableMeta(_) | TableOpenStage::RecoverTableData(_) => {
                    return OpenTablesOfShard {
                        msg: format!(
                            "unexpected table state, state:{state:?}, table_id:{table_id}",
                        ),
                    }
                    .fail()
                }
            }
        }

        Ok(table_results)
    }

    /// Recover table meta data from manifest based on shard.
    async fn recover_table_metas(&mut self) -> Result<()> {
        for (table_id, state) in self.states.iter_mut() {
            match state {
                // Only do the meta recovery work in `RecoverTableMeta` state.
                TableOpenStage::RecoverTableMeta(ctx) => {
                    let result = match Self::recover_single_table_meta(
                        self.manifest.as_ref(),
                        self.shard_id,
                        &ctx.table_def,
                    )
                    .await
                    {
                        Ok(()) => {
                            let table_data = ctx.space.find_table_by_id(*table_id);
                            Ok(table_data.map(|data| (data, ctx.space.clone())))
                        }
                        Err(e) => Err(e),
                    };

                    match result {
                        Ok(Some((table_data, space))) => {
                            *state = TableOpenStage::RecoverTableData(RecoverTableDataContext {
                                table_data,
                                space,
                            })
                        }
                        Ok(None) => *state = TableOpenStage::Success(None),
                        Err(e) => *state = TableOpenStage::Failed(e),
                    }
                }
                // Table was found to be opened in init stage.
                TableOpenStage::Success(_) => {}
                TableOpenStage::RecoverTableData(_) | TableOpenStage::Failed(_) => {
                    return OpenTablesOfShard {
                        msg: format!("unexpected table state:{state:?}"),
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    /// Recover table data based on shard.
    async fn recover_table_datas(&mut self) -> Result<()> {
        // Replay wal logs of tables.
        let mut replay_table_datas = Vec::with_capacity(self.states.len());
        for (table_id, stage) in self.states.iter_mut() {
            match stage {
                // Only do the wal recovery work in `RecoverTableData` state.
                TableOpenStage::RecoverTableData(ctx) => {
                    replay_table_datas.push(ctx.table_data.clone());
                }
                // Table was found opened, or failed in meta recovery stage.
                TableOpenStage::Failed(_) | TableOpenStage::Success(_) => {}
                TableOpenStage::RecoverTableMeta(_) => {
                    return OpenTablesOfShard {
                        msg: format!(
                            "unexpected stage, stage:{stage:?}, table_id:{table_id}, shard_id:{}",
                            self.shard_id
                        ),
                    }
                    .fail();
                }
            }
        }
        let mut wal_replayer = WalReplayer::new(
            &replay_table_datas,
            self.shard_id,
            self.wal_manager.clone(),
            self.wal_replay_batch_size,
            self.flusher.clone(),
            self.max_retry_flush_limit,
        );
        let mut table_results = wal_replayer.replay().await?;

        // Process the replay results.
        for table_data in replay_table_datas {
            let table_id = table_data.id;
            let stage = self.states.get_mut(&table_id).unwrap();
            let table_result = table_results.remove(&table_id).unwrap();

            match (&stage, table_result) {
                (TableOpenStage::RecoverTableData(ctx), Ok(())) => {
                    let space_table = SpaceAndTable::new(ctx.space.clone(), ctx.table_data.clone());
                    *stage = TableOpenStage::Success(Some(space_table));
                }

                (TableOpenStage::RecoverTableData(_), Err(e)) => {
                    *stage = TableOpenStage::Failed(e);
                }

                (other_stage, _) => {
                    return OpenTablesOfShard {
                        msg: format!("unexpected stage, stage:{other_stage:?}, table_id:{table_id}, shard_id:{}", self.shard_id),
                    }.fail();
                }
            }
        }

        Ok(())
    }

    /// Recover meta data from manifest.
    ///
    /// Return None if no meta data is found for the table.
    async fn recover_single_table_meta(
        manifest: &dyn Manifest,
        shard_id: ShardId,
        table_def: &TableDef,
    ) -> Result<()> {
        info!(
            "Instance recover table meta begin, table_id:{}, table_name:{}, shard_id:{shard_id}",
            table_def.id, table_def.name
        );

        // Load manifest, also create a new snapshot at startup.
        let table_id = table_def.id;
        let space_id = engine::build_space_id(table_def.schema_id);
        let load_req = LoadRequest {
            space_id,
            table_id,
            shard_id,
        };
        manifest.recover(&load_req).await.context(ReadMetaUpdate {
            table_id: table_def.id,
        })?;

        info!(
            "Instance recover table meta end, table_id:{}, table_name:{}, shard_id:{shard_id}",
            table_def.id, table_def.name
        );

        Ok(())
    }
}
