use std::{
    fmt::{Display, Formatter},
    future::Future,
    io,
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::ok, stream::BoxStream};
use logger::{error, info};
use tokio::io::AsyncWrite;
use upstream::{path::Path, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore};
use uuid::Uuid;

use crate::ObjectStoreRef;

/// Wrap a real store and save data in memory before flush to
/// the target location.
#[derive(Debug)]
pub struct StoreInMemory {
    enable: bool,
    store: ObjectStoreRef,
}

impl Display for StoreInMemory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl StoreInMemory {
    pub fn new(enable: bool, store: ObjectStoreRef) -> Self {
        Self { enable, store }
    }
}

#[async_trait]
impl ObjectStore for StoreInMemory {
    async fn put(&self, location: &Path, bytes: Bytes) -> upstream::Result<()> {
        info!("object store begin put");
        let result = self.store.put(location, bytes).await;
        info!("object store end put");
        return result;
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> upstream::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        if !self.enable {
            return self.store.put_multipart(location).await;
        }
        let upload_id = Uuid::new_v4().to_string();
        let upload = InMemoryUpload {
            location: location.clone(),
            data: Vec::new(),
            storage: self.store.clone(),
            completion_task: None,
        };
        Ok((upload_id, Box::new(upload)))
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> upstream::Result<()> {
        // Do nothing
        Ok(())
    }

    async fn get(&self, location: &Path) -> upstream::Result<GetResult> {
        self.store.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> upstream::Result<Bytes> {
        self.store.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> upstream::Result<ObjectMeta> {
        self.store.head(location).await
    }

    async fn delete(&self, location: &Path) -> upstream::Result<()> {
        self.store.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> upstream::Result<BoxStream<'_, upstream::Result<ObjectMeta>>> {
        self.store.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> upstream::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> upstream::Result<()> {
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> upstream::Result<()> {
        self.store.copy_if_not_exists(from, to).await
    }
}

type BoxedTryFuture<T> = Pin<Box<dyn Future<Output = upstream::Result<T, io::Error>> + Send>>;

struct InMemoryUpload {
    location: Path,
    data: Vec<u8>,
    storage: ObjectStoreRef,
    completion_task: Option<BoxedTryFuture<()>>,
}

impl AsyncWrite for InMemoryUpload {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<upstream::Result<usize, io::Error>> {
        self.data.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<upstream::Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<upstream::Result<(), io::Error>> {
        let data = Bytes::from(std::mem::take(&mut self.data));

        let inner = Arc::clone(&self.storage);
        let location = self.location.clone();

        let completion_task = self.completion_task.get_or_insert_with(|| {
            Box::pin(async move {
                info!("Flushing data to object store: {:?}", location);
                match inner.put(&location, data).await {
                    Ok(_) => {
                        info!("Successfully put object: {:?}", location);
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to put object: {:?}", e);
                        Err(io::Error::new(io::ErrorKind::Other, e))
                    }
                }
            })
        });
        Pin::new(completion_task).poll(cx)
    }
}
