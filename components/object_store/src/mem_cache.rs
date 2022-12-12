// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! An implementation of ObjectStore, which support
//! 1. Cache based on memory, and support evict based on memory usage
//! 2. Builtin Partition to reduce lock contention

use std::{
    collections::hash_map::{DefaultHasher, RandomState},
    fmt::{self, Display},
    hash::{Hash, Hasher},
    num::NonZeroUsize,
    ops::Range,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use clru::{CLruCache, CLruCacheConfig, WeightScale};
use futures::stream::BoxStream;
use tokio::{io::AsyncWrite, sync::Mutex};
use upstream::{path::Path, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result};

struct CustomScale;

impl WeightScale<String, Bytes> for CustomScale {
    fn weight(&self, _key: &String, value: &Bytes) -> usize {
        value.len()
    }
}

struct Partition {
    inner: Mutex<CLruCache<String, Bytes, RandomState, CustomScale>>,
}

impl Partition {
    fn new(mem_cap: usize) -> Self {
        let cache = CLruCache::with_config(
            CLruCacheConfig::new(NonZeroUsize::new(mem_cap).expect("mem cap must large than 0"))
                .with_scale(CustomScale),
        );

        Self {
            inner: Mutex::new(cache),
        }
    }
}
impl Partition {
    async fn get(&self, key: &str) -> Option<Bytes> {
        let mut guard = self.inner.lock().await;
        guard.get(key).cloned()
    }

    async fn insert(&self, key: String, value: Bytes) {
        let mut guard = self.inner.lock().await;
        // don't care error now.
        _ = guard.put_with_weight(key, value);
    }

    #[cfg(test)]
    async fn keys(&self) -> Vec<String> {
        let guard = self.inner.lock().await;
        guard
            .iter()
            .map(|(key, _)| key)
            .cloned()
            .collect::<Vec<_>>()
    }
}

struct MemCache {
    /// Max memory this store can use
    mem_cap: usize,
    partitions: Vec<Arc<Partition>>,
    partition_mask: usize,
}

impl MemCache {
    fn new(partition_bits: usize, mem_cap: usize) -> Self {
        let partition_num = 1 << partition_bits;
        let cap_per_part = mem_cap / partition_num;
        let partitions = (0..partition_num)
            .map(|_| Arc::new(Partition::new(cap_per_part)))
            .collect::<Vec<_>>();

        Self {
            mem_cap,
            partitions,
            partition_mask: partition_num - 1,
        }
    }

    fn locate_partition(&self, key: &str) -> Arc<Partition> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        self.partitions[hasher.finish() as usize & self.partition_mask].clone()
    }

    async fn get(&self, key: &str) -> Option<Bytes> {
        let partition = self.locate_partition(key);
        partition.get(key).await
    }

    async fn insert(&self, key: String, value: Bytes) {
        let partition = self.locate_partition(&key);
        partition.insert(key, value).await;
    }

    #[cfg(test)]
    async fn to_string(&self) -> String {
        futures::future::join_all(
            self.partitions
                .iter()
                .map(|part| async { part.keys().await.join(",") }),
        )
        .await
        .into_iter()
        .enumerate()
        .map(|(part_no, keys)| format!("{}: [{}]", part_no, keys))
        .collect::<Vec<_>>()
        .join("\n")
    }
}

impl Display for MemCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemCache")
            .field("mem_cap", &self.mem_cap)
            .field("mask", &self.partition_mask)
            .field("partitons", &self.partitions.len())
            .finish()
    }
}

pub struct MemCacheStore {
    cache: MemCache,
    underlying_store: Arc<dyn ObjectStore>,
}

impl MemCacheStore {
    // Note: mem_cap must be larger than 0
    pub fn new(
        partition_bits: usize,
        mem_cap: usize,
        underlying_store: Arc<dyn ObjectStore>,
    ) -> Self {
        assert!(mem_cap > 0);

        Self {
            cache: MemCache::new(partition_bits, mem_cap),
            underlying_store,
        }
    }

    fn cache_key(location: &Path, range: &Range<usize>) -> String {
        format!("{}-{}-{}", location, range.start, range.end)
    }
}

impl Display for MemCacheStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cache.fmt(f)
    }
}

impl fmt::Debug for MemCacheStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemCacheStore").finish()
    }
}

#[async_trait]
impl ObjectStore for MemCacheStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.underlying_store.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.underlying_store.put_multipart(location).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.underlying_store
            .abort_multipart(location, multipart_id)
            .await
    }

    // TODO(chenxiang): don't cache whole path for reasons below
    // 1. cache key don't support overlapping
    // 2. In sst module, we only use get_range, get is not used
    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.underlying_store.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        // TODO(chenxiang): What if there are some overlapping range in cache?
        // A request with range [5, 10) can also use [0, 20) cache
        let cache_key = Self::cache_key(location, &range);
        if let Some(bytes) = self.cache.get(&cache_key).await {
            return Ok(bytes);
        }

        // TODO(chenxiang): What if two threads reach here? It's better to
        // pend one thread, and only let one to fetch data from underlying store.
        let bytes = self.underlying_store.get_range(location, range).await;
        if let Ok(bytes) = &bytes {
            self.cache.insert(cache_key, bytes.clone()).await;
        }

        bytes
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.underlying_store.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.underlying_store.delete(location).await
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.underlying_store.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.underlying_store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.underlying_store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.underlying_store.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;
    use upstream::local::LocalFileSystem;

    use super::*;

    async fn prepare_store(bits: usize, mem_cap: usize) -> MemCacheStore {
        let local_path = tempdir().unwrap();
        let local_store = Arc::new(LocalFileSystem::new_with_prefix(local_path.path()).unwrap());

        MemCacheStore::new(bits, mem_cap, local_store)
    }

    #[tokio::test]
    async fn test_mem_cache_evict() {
        // single partition
        let store = prepare_store(0, 13).await;

        // write date
        let location = Path::from("1.sst");
        store
            .put(&location, Bytes::from_static(&[1; 1024]))
            .await
            .unwrap();

        // get bytes from [0, 5), insert to cache
        let range0_5 = 0..5;
        _ = store.get_range(&location, range0_5.clone()).await.unwrap();
        assert!(store
            .cache
            .get(&MemCacheStore::cache_key(&location, &range0_5))
            .await
            .is_some());

        // get bytes from [5, 10), insert to cache
        let range5_10 = 5..10;
        _ = store.get_range(&location, range5_10.clone()).await.unwrap();
        assert!(store
            .cache
            .get(&MemCacheStore::cache_key(&location, &range0_5))
            .await
            .is_some());
        assert!(store
            .cache
            .get(&MemCacheStore::cache_key(&location, &range5_10))
            .await
            .is_some());

        // get bytes from [10, 15), insert to cache
        // cache is full, evict [0, 5)
        let range10_15 = 10..15;
        _ = store
            .get_range(&location, range10_15.clone())
            .await
            .unwrap();
        assert!(store
            .cache
            .get(&MemCacheStore::cache_key(&location, &range0_5))
            .await
            .is_none());
        assert!(store
            .cache
            .get(&MemCacheStore::cache_key(&location, &range5_10))
            .await
            .is_some());
        assert!(store
            .cache
            .get(&MemCacheStore::cache_key(&location, &range10_15))
            .await
            .is_some());
    }

    #[tokio::test]
    async fn test_mem_cache_partition() {
        // 4 partitions
        let store = prepare_store(2, 100).await;
        let location = Path::from("partition.sst");
        store
            .put(&location, Bytes::from_static(&[1; 1024]))
            .await
            .unwrap();

        let range0_5 = 0..5;
        let range100_105 = 100..105;
        _ = store.get_range(&location, range0_5.clone()).await.unwrap();
        _ = store
            .get_range(&location, range100_105.clone())
            .await
            .unwrap();

        assert_eq!(
            r#"0: []
1: [partition.sst-100-105]
2: []
3: [partition.sst-0-5]"#,
            store.cache.to_string().await
        );

        assert!(store
            .cache
            .get(&MemCacheStore::cache_key(&location, &range0_5))
            .await
            .is_some());
        assert!(store
            .cache
            .get(&MemCacheStore::cache_key(&location, &range100_105))
            .await
            .is_some());
    }
}
