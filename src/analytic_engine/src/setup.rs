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

//! Setup the analytic engine

use std::{num::NonZeroUsize, path::Path, pin::Pin, sync::Arc};

use futures::Future;
use macros::define_result;
use object_store::{
    aliyun,
    config::{ObjectStoreOptions, StorageOptions},
    disk_cache::DiskCacheStore,
    in_memory_multipart,
    mem_cache::{MemCache, MemCacheStore},
    metrics::StoreWithMetrics,
    obkv,
    prefix::StoreWithPrefix,
    s3, LocalFileSystem, ObjectStoreRef,
};
use snafu::{ResultExt, Snafu};
use table_engine::engine::{EngineRuntimes, TableEngineRef};
use table_kv::obkv::ObkvImpl;
use wal::manager::{OpenedWals, WalManagerRef};

use crate::{
    compaction::runner::CompactionRunnerRef,
    context::OpenContext,
    engine::TableEngineImpl,
    instance::open::{InstanceContext, ManifestStorages},
    sst::{
        factory::{FactoryImpl, ObjectStorePicker, ObjectStorePickerRef, ReadFrequency},
        meta_data::cache::{MetaCache, MetaCacheRef},
    },
    Config,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to open engine instance, err:{}", source))]
    OpenInstance {
        source: crate::instance::engine::Error,
    },

    #[snafu(display("Failed to open obkv, err:{}", source))]
    OpenObkv { source: table_kv::obkv::Error },

    #[snafu(display("Failed to execute in runtime, err:{}", source))]
    RuntimeExec { source: runtime::Error },

    #[snafu(display("Failed to open object store, err:{}", source))]
    OpenObjectStore {
        source: object_store::ObjectStoreError,
    },

    #[snafu(display("Failed to create dir for {}, err:{}", path, source))]
    CreateDir {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to create mem cache, err:{}", source))]
    OpenMemCache {
        source: object_store::mem_cache::Error,
    },
}

define_result!(Error);

const STORE_DIR_NAME: &str = "store";
const DISK_CACHE_DIR_NAME: &str = "sst_cache";

pub struct TableEngineContext {
    pub table_engine: TableEngineRef,
    // TODO: unused now, will be used in remote compaction.
    pub local_compaction_runner: Option<CompactionRunnerRef>,
}

/// Builder for [TableEngine].
///
/// [TableEngine]: table_engine::engine::TableEngine
#[derive(Clone)]
pub struct EngineBuilder<'a> {
    pub config: &'a Config,
    pub engine_runtimes: Arc<EngineRuntimes>,
    pub opened_wals: OpenedWals,
}

impl<'a> EngineBuilder<'a> {
    pub async fn build(self) -> Result<TableEngineContext> {
        let opened_storages =
            open_storage(self.config.storage.clone(), self.engine_runtimes.clone()).await?;
        let manifest_storages = ManifestStorages {
            wal_manager: self.opened_wals.manifest_wal.clone(),
            oss_storage: opened_storages.default_store().clone(),
        };

        let InstanceContext {
            instance,
            local_compaction_runner,
        } = build_instance_context(
            self.config.clone(),
            self.engine_runtimes,
            self.opened_wals.data_wal,
            manifest_storages,
            Arc::new(opened_storages),
        )
        .await?;

        let table_engine = Arc::new(TableEngineImpl::new(instance));

        Ok(TableEngineContext {
            table_engine,
            local_compaction_runner,
        })
    }
}

async fn build_instance_context(
    config: Config,
    engine_runtimes: Arc<EngineRuntimes>,
    wal_manager: WalManagerRef,
    manifest_storages: ManifestStorages,
    store_picker: ObjectStorePickerRef,
) -> Result<InstanceContext> {
    let meta_cache: Option<MetaCacheRef> = config
        .sst_meta_cache_cap
        .map(|cap| Arc::new(MetaCache::new(cap)));

    let open_ctx = OpenContext {
        config,
        runtimes: engine_runtimes,
        meta_cache,
    };

    let instance_ctx = InstanceContext::new(
        open_ctx,
        manifest_storages,
        wal_manager,
        store_picker,
        Arc::new(FactoryImpl),
    )
    .await
    .context(OpenInstance)?;

    Ok(instance_ctx)
}

#[derive(Debug)]
struct OpenedStorages {
    default_store: ObjectStoreRef,
    store_with_readonly_cache: ObjectStoreRef,
}

impl ObjectStorePicker for OpenedStorages {
    fn default_store(&self) -> &ObjectStoreRef {
        &self.default_store
    }

    fn pick_by_freq(&self, freq: ReadFrequency) -> &ObjectStoreRef {
        match freq {
            ReadFrequency::Once => &self.store_with_readonly_cache,
            ReadFrequency::Frequent => &self.default_store,
        }
    }
}

// Build store in multiple layer, access speed decrease in turn.
// MemCacheStore           → DiskCacheStore → real ObjectStore(OSS/S3...)
// MemCacheStore(ReadOnly) ↑
// ```plaintext
// +-------------------------------+
// |    MemCacheStore              |
// |       +-----------------------+
// |       |    DiskCacheStore     |
// |       |      +----------------+
// |       |      |                |
// |       |      |    OSS/S3....  |
// +-------+------+----------------+
// ```
fn open_storage(
    opts: StorageOptions,
    engine_runtimes: Arc<EngineRuntimes>,
) -> Pin<Box<dyn Future<Output = Result<OpenedStorages>> + Send>> {
    Box::pin(async move {
        let mut store = match opts.object_store {
            ObjectStoreOptions::Local(local_opts) => {
                let data_path = Path::new(&local_opts.data_dir);
                let sst_path = data_path.join(STORE_DIR_NAME);
                tokio::fs::create_dir_all(&sst_path)
                    .await
                    .context(CreateDir {
                        path: sst_path.to_string_lossy().into_owned(),
                    })?;
                let store = LocalFileSystem::new_with_prefix(sst_path).context(OpenObjectStore)?;
                Arc::new(store) as _
            }
            ObjectStoreOptions::Aliyun(aliyun_opts) => {
                let oss: ObjectStoreRef =
                    Arc::new(aliyun::try_new(&aliyun_opts).context(OpenObjectStore)?);
                let store_with_in_memory_multipart = Arc::new(
                    in_memory_multipart::StoreInMemory::new(aliyun_opts.enable_multipart, oss),
                );
                let store_with_prefix =
                    StoreWithPrefix::new(aliyun_opts.prefix, store_with_in_memory_multipart)
                        .context(OpenObjectStore)?;
                Arc::new(store_with_prefix) as _
            }
            ObjectStoreOptions::Obkv(obkv_opts) => {
                let obkv_config = obkv_opts.client;
                let obkv = engine_runtimes
                    .write_runtime
                    .spawn_blocking(move || ObkvImpl::new(obkv_config).context(OpenObkv))
                    .await
                    .context(RuntimeExec)??;

                let oss: ObjectStoreRef = Arc::new(
                    obkv::ObkvObjectStore::try_new(
                        Arc::new(obkv),
                        obkv_opts.shard_num,
                        obkv_opts.part_size.0 as usize,
                        obkv_opts.max_object_size.0 as usize,
                        obkv_opts.upload_parallelism,
                    )
                    .context(OpenObjectStore)?,
                );
                Arc::new(StoreWithPrefix::new(obkv_opts.prefix, oss).context(OpenObjectStore)?) as _
            }
            ObjectStoreOptions::S3(s3_option) => {
                let oss: ObjectStoreRef =
                    Arc::new(s3::try_new(&s3_option).context(OpenObjectStore)?);
                let store_with_prefix = StoreWithPrefix::new(s3_option.prefix, oss);
                Arc::new(store_with_prefix.context(OpenObjectStore)?) as _
            }
        };

        store = Arc::new(StoreWithMetrics::new(
            store,
            engine_runtimes.io_runtime.clone(),
        ));

        if opts.disk_cache_capacity.as_byte() > 0 {
            let path = Path::new(&opts.disk_cache_dir).join(DISK_CACHE_DIR_NAME);
            tokio::fs::create_dir_all(&path).await.context(CreateDir {
                path: path.to_string_lossy().into_owned(),
            })?;

            // TODO: Consider the readonly cache.
            store = Arc::new(
                DiskCacheStore::try_new(
                    path.to_string_lossy().into_owned(),
                    opts.disk_cache_capacity.as_byte() as usize,
                    opts.disk_cache_page_size.as_byte() as usize,
                    store,
                    opts.disk_cache_partition_bits,
                    engine_runtimes.io_runtime.clone(),
                )
                .await
                .context(OpenObjectStore)?,
            ) as _;
        }

        if opts.mem_cache_capacity.as_byte() > 0 {
            let mem_cache = Arc::new(
                MemCache::try_new(
                    opts.mem_cache_partition_bits,
                    NonZeroUsize::new(opts.mem_cache_capacity.as_byte() as usize).unwrap(),
                )
                .context(OpenMemCache)?,
            );
            let default_store = Arc::new(MemCacheStore::new(mem_cache.clone(), store.clone())) as _;
            let store_with_readonly_cache =
                Arc::new(MemCacheStore::new_with_readonly_cache(mem_cache, store)) as _;
            Ok(OpenedStorages {
                default_store,
                store_with_readonly_cache,
            })
        } else {
            let store_with_readonly_cache = store.clone();
            Ok(OpenedStorages {
                default_store: store,
                store_with_readonly_cache,
            })
        }
    })
}
