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

// Remote engine rpc service implementation.

use std::{
    hash::Hash,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use arrow_ext::ipc::{self, CompressOptions, CompressOutput, CompressionMethod};
use async_trait::async_trait;
use catalog::{manager::ManagerRef, schema::SchemaRef};
use common_types::{record_batch::RecordBatch, request_id::RequestId};
use fb_util::{
    common::FlatBufferBytes,
    remote_engine_fb_service_server::RemoteEngineFbService,
    remote_engine_generated::fbprotocol::{
        ContiguousRows as FBContiguousRows, ResponseHeader, ResponseHeaderArgs,
        WriteBatchRequest as FBWriteBatchRequest, WriteRequest as FBWriteRequest,
        WriteResponse as FBWriteResponse, WriteResponseArgs as FBWriteResponseArgs,
    },
};
use futures::{
    stream::{self, BoxStream, FuturesUnordered, StreamExt},
    Future,
};
use generic_error::BoxError;
use horaedbproto::{
    remote_engine::{
        execute_plan_request,
        read_response::Output::{Arrow, Metric},
        remote_engine_service_server::RemoteEngineService,
        row_group, AlterTableOptionsRequest, AlterTableOptionsResponse, AlterTableSchemaRequest,
        AlterTableSchemaResponse, ExecContext, ExecutePlanRequest, GetTableInfoRequest,
        GetTableInfoResponse, MetricPayload, QueryPriority, ReadRequest, ReadResponse,
        WriteBatchRequest, WriteRequest, WriteResponse,
    },
    storage::{arrow_payload, ArrowPayload},
};
use logger::{debug, error, info, slow_query};
use notifier::notifier::{ExecutionGuard, RequestNotifiers, RequestResult};
use proxy::{
    hotspot::{HotspotRecorder, Message},
    instance::InstanceRef,
};
use query_engine::{
    context::Context as QueryContext,
    datafusion_impl::physical_plan::{DataFusionPhysicalPlanAdapter, TypedPlan},
    physical_planner::PhysicalPlanRef,
    QueryEngineRef, QueryEngineType,
};
use runtime::{Priority, RuntimeRef};
use snafu::{OptionExt, ResultExt};
use table_engine::{
    engine::EngineRuntimes,
    predicate::PredicateRef,
    remote::model::{self, ContiguousRowsExt, TableIdentifier},
    stream::{PartitionedStreams, SendableRecordBatchStream},
    table::{AlterSchemaRequest, TableRef},
};
use time_ext::InstantExt;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Code, Request, Response, Status};

use crate::{
    config::QueryDedupConfig,
    grpc::{
        metrics::{
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC,
            REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC,
            REMOTE_ENGINE_WRITE_BATCH_NUM_ROWS_HISTOGRAM,
        },
        remote_engine_service::{
            error::{ErrNoCause, ErrWithCause, Result, StatusCode},
            metrics::REMOTE_ENGINE_QUERY_COUNTER,
        },
    },
};
pub mod error;
mod metrics;

const STREAM_QUERY_CHANNEL_LEN: usize = 200;
const DEFAULT_COMPRESS_MIN_LENGTH: usize = 80 * 1024;

enum TonicWriteBatchRequestExt {
    Proto(Request<WriteBatchRequest>),
    Flatbuffer(tonic::Request<FlatBufferBytes>),
}

enum TonicWriteRequestExt {
    Proto(Request<WriteRequest>),
    Flatbuffer(tonic::Request<FlatBufferBytes>),
}

enum TonicWriteResponseExt {
    Proto(tonic::Response<WriteResponse>),
    Flatbuffer(tonic::Response<FlatBufferBytes>),
}

enum WriteRequestExt<'a> {
    Proto(WriteRequest),
    Flatbuffer(FBWriteRequest<'a>),
}

enum RowsPayloadExt<'a> {
    Proto(row_group::Rows),
    Flatbuffer(FBContiguousRows<'a>),
}

#[derive(Debug, Clone)]
pub enum RecordBatchWithMetric {
    RecordBatch(RecordBatch),
    Metric(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamReadReqKey {
    table: String,
    predicate: PredicateRef,
    projection: Option<Vec<usize>>,
}

impl StreamReadReqKey {
    pub fn new(table: String, predicate: PredicateRef, projection: Option<Vec<usize>>) -> Self {
        Self {
            table,
            predicate,
            projection,
        }
    }
}

pub type StreamReadRequestNotifiers =
    Arc<RequestNotifiers<StreamReadReqKey, mpsc::Sender<Result<RecordBatch>>>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PhysicalPlanKey {
    encoded_plan: Vec<u8>,
}

pub type PhysicalPlanNotifiers =
    Arc<RequestNotifiers<PhysicalPlanKey, mpsc::Sender<Result<RecordBatch>>>>;

/// Stream metric
trait MetricCollector: 'static + Send + Unpin {
    fn collect(self);
}

struct StreamReadMetricCollector(Instant);

impl MetricCollector for StreamReadMetricCollector {
    fn collect(self) {
        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .stream_read
            .observe(self.0.saturating_elapsed().as_secs_f64());
    }
}

struct ExecutePlanMetricCollector {
    start: Instant,
    query: String,
    request_id: RequestId,
    slow_threshold: Duration,
    priority: Priority,
}

impl ExecutePlanMetricCollector {
    fn new(
        request_id: RequestId,
        query: String,
        slow_threshold_secs: u64,
        priority: Priority,
    ) -> Self {
        Self {
            start: Instant::now(),
            query,
            request_id,
            slow_threshold: Duration::from_secs(slow_threshold_secs),
            priority,
        }
    }
}

impl MetricCollector for ExecutePlanMetricCollector {
    fn collect(self) {
        let cost = self.start.elapsed();
        if cost > self.slow_threshold {
            slow_query!(
                "Remote query elapsed:{:?}, id:{}, priority:{}, query:{}",
                cost,
                self.request_id,
                self.priority.as_str(),
                self.query
            );
        }

        REMOTE_ENGINE_QUERY_COUNTER
            .with_label_values(&[self.priority.as_str()])
            .inc();
        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .execute_physical_plan
            .observe(cost.as_secs_f64());
    }
}

/// Stream with metric
struct StreamWithMetric<M: MetricCollector> {
    inner: BoxStream<'static, Result<RecordBatch>>,
    metric: Option<M>,
}

impl<M: MetricCollector> StreamWithMetric<M> {
    fn new(inner: BoxStream<'static, Result<RecordBatch>>, metric: M) -> Self {
        Self {
            inner,
            metric: Some(metric),
        }
    }
}

impl<M: MetricCollector> Stream for StreamWithMetric<M> {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

impl<M: MetricCollector> Drop for StreamWithMetric<M> {
    fn drop(&mut self) {
        let metric = self.metric.take();
        if let Some(metric) = metric {
            metric.collect();
        }
    }
}

struct RemoteExecStream {
    inner: BoxStream<'static, Result<RecordBatch>>,
    physical_plan_for_explain: Option<PhysicalPlanRef>,
}

impl RemoteExecStream {
    fn new(
        inner: BoxStream<'static, Result<RecordBatch>>,
        physical_plan_for_explain: Option<PhysicalPlanRef>,
    ) -> Self {
        Self {
            inner,
            physical_plan_for_explain,
        }
    }
}

impl Stream for RemoteExecStream {
    type Item = Result<RecordBatchWithMetric>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let is_explain = this.physical_plan_for_explain.is_some();
        loop {
            match this.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(res)) => {
                    // If the request is explain, we try drain the stream to get the metrics.
                    if !is_explain {
                        return Poll::Ready(Some(res.map(RecordBatchWithMetric::RecordBatch)));
                    }
                }
                Poll::Ready(None) => match &this.physical_plan_for_explain {
                    Some(physical_plan) => {
                        let metrics = physical_plan.metrics_to_string();
                        this.physical_plan_for_explain = None;
                        return Poll::Ready(Some(Ok(RecordBatchWithMetric::Metric(metrics))));
                    }
                    None => return Poll::Ready(None),
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

macro_rules! record_stream_to_response_stream {
    ($record_stream_result:ident, $StreamType:ident) => {
        match $record_stream_result {
            Ok(stream) => {
                let new_stream: Self::$StreamType = Box::pin(stream.map(|res| match res {
                    Ok(res) => match res {
                        RecordBatchWithMetric::Metric(metric) => {
                            let resp = ReadResponse {
                                header: Some(error::build_ok_header()),
                                output: Some(Metric(MetricPayload { metric })),
                            };
                            Ok(resp)
                        }
                        RecordBatchWithMetric::RecordBatch(record_batch) => {
                            let resp = match ipc::encode_record_batch(
                                &record_batch.into_arrow_record_batch(),
                                CompressOptions {
                                    compress_min_length: DEFAULT_COMPRESS_MIN_LENGTH,
                                    method: CompressionMethod::Zstd,
                                },
                            )
                            .box_err()
                            .context(ErrWithCause {
                                code: StatusCode::Internal,
                                msg: "encode record batch failed",
                            }) {
                                Err(e) => ReadResponse {
                                    header: Some(error::build_err_header(e)),
                                    ..Default::default()
                                },
                                Ok(CompressOutput { payload, method }) => {
                                    let compression = match method {
                                        CompressionMethod::None => arrow_payload::Compression::None,
                                        CompressionMethod::Zstd => arrow_payload::Compression::Zstd,
                                    };

                                    ReadResponse {
                                        header: Some(error::build_ok_header()),
                                        output: Some(Arrow(ArrowPayload {
                                            record_batches: vec![payload],
                                            compression: compression as i32,
                                        })),
                                    }
                                }
                            };

                            Ok(resp)
                        }
                    },
                    Err(e) => {
                        let resp = ReadResponse {
                            header: Some(error::build_err_header(e)),
                            ..Default::default()
                        };
                        Ok(resp)
                    }
                }));
                Ok(Response::new(new_stream))
            }
            Err(e) => {
                let resp = ReadResponse {
                    header: Some(error::build_err_header(e)),
                    ..Default::default()
                };
                let stream = stream::once(async { Ok(resp) });
                Ok(Response::new(Box::pin(stream)))
            }
        }
    };
}

#[derive(Clone)]
pub struct QueryDedup {
    pub config: QueryDedupConfig,
    pub request_notifiers: StreamReadRequestNotifiers,
    pub physical_plan_notifiers: PhysicalPlanNotifiers,
}

#[derive(Clone)]
pub struct RemoteEngineServiceImpl {
    pub instance: InstanceRef,
    pub runtimes: Arc<EngineRuntimes>,
    pub query_dedup: Option<QueryDedup>,
    pub hotspot_recorder: Arc<HotspotRecorder>,
}

impl RemoteEngineServiceImpl {
    async fn stream_read_internal(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<RemoteExecStream> {
        let metric = StreamReadMetricCollector(Instant::now());

        let ctx = self.handler_ctx();
        let (tx, rx) = mpsc::channel(STREAM_QUERY_CHANNEL_LEN);
        let handle = self.runtimes.read_runtime.spawn(async move {
            let read_request = request.into_inner();
            handle_stream_read(ctx, read_request).await.map_err(|e| {
                error!("Handle stream read failed, err:{e}");
                e
            })
        });
        let streams = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        })??;

        for stream in streams.streams {
            let mut stream = stream.map(|result| {
                result.box_err().context(ErrWithCause {
                    code: StatusCode::Internal,
                    msg: "record batch failed",
                })
            });
            let tx = tx.clone();
            self.runtimes.read_runtime.spawn(async move {
                let mut num_rows = 0;
                while let Some(batch) = stream.next().await {
                    if let Ok(record_batch) = &batch {
                        num_rows += record_batch.num_rows();
                    }
                    if let Err(e) = tx.send(batch).await {
                        error!("Failed to send handler result, err:{}.", e);
                        break;
                    }
                }
                REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                    .query_succeeded_row
                    .inc_by(num_rows as u64);
            });
        }

        let stream = StreamWithMetric::new(Box::pin(ReceiverStream::new(rx)), metric);
        Ok(RemoteExecStream::new(Box::pin(stream), None))
    }

    async fn dedup_stream_read_internal(
        &self,
        query_dedup: QueryDedup,
        request: Request<ReadRequest>,
    ) -> Result<RemoteExecStream> {
        let metric = StreamReadMetricCollector(Instant::now());

        let request = request.into_inner();
        let table_engine::remote::model::ReadRequest {
            table,
            read_request,
        } = request.clone().try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert read request",
        })?;

        let request_key = StreamReadReqKey::new(
            table.table.clone(),
            read_request.predicate.clone(),
            read_request.projected_schema.projection(),
        );

        let QueryDedup {
            config,
            request_notifiers,
            ..
        } = query_dedup;

        let (tx, rx) = mpsc::channel(config.notify_queue_cap);
        match request_notifiers.insert_notifier(request_key.clone(), tx) {
            // The first request, need to handle it, and then notify the other requests.
            RequestResult::First => {
                let ctx = self.handler_ctx();
                let query = async move { handle_stream_read(ctx, request).await };
                self.read_and_send_dedupped_resps(
                    request_key,
                    query,
                    request_notifiers.clone(),
                    config.notify_timeout.0,
                    // TODO: decide runtime from request priority.
                    self.runtimes.read_runtime.high(),
                )
                .await?;
            }
            // The request is waiting for the result of first request.
            RequestResult::Wait => {
                // TODO: add metrics to collect the time cost of waited stream
                // read.
            }
        }

        let stream = StreamWithMetric::new(Box::pin(ReceiverStream::new(rx)), metric);
        Ok(RemoteExecStream::new(Box::pin(stream), None))
    }

    async fn read_and_send_dedupped_resps<K, F>(
        &self,
        request_key: K,
        query: F,
        notifiers: Arc<RequestNotifiers<K, mpsc::Sender<Result<RecordBatch>>>>,
        notify_timeout: Duration,
        rt: &RuntimeRef,
    ) -> Result<()>
    where
        K: Hash + PartialEq + Eq,
        F: Future<Output = Result<PartitionedStreams>> + Send + 'static,
    {
        // This is used to remove key when future is cancelled.
        let mut guard = ExecutionGuard::new(|| {
            notifiers.take_notifiers(&request_key);
        });
        let handle = rt.spawn(query);
        let streams = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        })??;

        let mut stream_read = FuturesUnordered::new();
        for stream in streams.streams {
            let mut stream = stream.map(|result| {
                result.box_err().context(ErrWithCause {
                    code: StatusCode::Internal,
                    msg: "record batch failed",
                })
            });

            let handle = rt.spawn(async move {
                let mut batches = Vec::new();
                while let Some(batch) = stream.next().await {
                    batches.push(batch)
                }

                batches
            });
            stream_read.push(handle);
        }

        // Collect all the data from the stream to let more duplicate request query to
        // be batched.
        let mut resps = Vec::new();
        while let Some(result) = stream_read.next().await {
            let batch = result.box_err().context(ErrWithCause {
                code: StatusCode::Internal,
                msg: "failed to join task",
            })?;
            resps.extend(batch);
        }

        // We should set cancel to guard, otherwise the key will be removed twice.
        guard.cancel();
        let notifiers = notifiers.take_notifiers(&request_key).unwrap();

        // Do send in background to avoid blocking the rpc procedure.
        rt.spawn(async move {
            Self::send_dedupped_resps(resps, notifiers, notify_timeout).await;
        });

        Ok(())
    }

    /// Send the response to the queriers that share the same query request.
    async fn send_dedupped_resps(
        resps: Vec<Result<RecordBatch>>,
        notifiers: Vec<Sender<Result<RecordBatch>>>,
        notify_timeout: Duration,
    ) {
        let mut num_rows = 0;
        for resp in resps {
            match resp {
                Ok(batch) => {
                    num_rows += batch.num_rows();
                    for notifier in &notifiers {
                        if let Err(e) = notifier
                            .send_timeout(Ok(batch.clone()), notify_timeout)
                            .await
                        {
                            error!("Failed to send handler result, err:{}.", e);
                        }
                    }
                }
                Err(_) => {
                    for notifier in &notifiers {
                        let err = ErrNoCause {
                            code: StatusCode::Internal,
                            msg: "failed to handler request".to_string(),
                        }
                        .fail();

                        if let Err(e) = notifier.send_timeout(err, notify_timeout).await {
                            error!("Failed to send handler result, err:{}.", e);
                        }
                    }
                    break;
                }
            }
        }

        let total_num_rows = (num_rows * notifiers.len()) as u64;
        let num_dedupped_reqs = (notifiers.len() - 1) as u64;
        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
            .query_succeeded_row
            .inc_by(total_num_rows);
        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
            .dedupped_stream_query
            .inc_by(num_dedupped_reqs);
    }

    async fn write_internal(
        &self,
        request: TonicWriteRequestExt,
    ) -> std::result::Result<TonicWriteResponseExt, Status> {
        let begin_instant = Instant::now();
        let ctx = self.handler_ctx();
        let (handle, is_flatbuffer) = match request {
            TonicWriteRequestExt::Proto(v) => (
                self.runtimes.write_runtime.spawn(async move {
                    let request = WriteRequestExt::Proto(v.into_inner());

                    handle_write(ctx, request).await.map_err(|e| {
                        error!("Handle write failed, err:{e}");
                        e
                    })
                }),
                false,
            ),
            TonicWriteRequestExt::Flatbuffer(v) => (
                self.runtimes.write_runtime.spawn(async move {
                    let request = v.into_inner();
                    let request = request.deserialize::<FBWriteRequest>().unwrap();
                    let request = WriteRequestExt::Flatbuffer(request);

                    handle_write(ctx, request).await.map_err(|e| {
                        error!("Handle write failed, err:{e}");
                        e
                    })
                }),
                true,
            ),
        };

        let res = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        });

        let mut resp = WriteResponse::default();
        match res {
            Ok(Ok(v)) => {
                resp.header = Some(error::build_ok_header());
                resp.affected_rows = v.affected_rows;
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(error::build_err_header(e));
            }
        };

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .write
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        match is_flatbuffer {
            true => Ok(TonicWriteResponseExt::Flatbuffer(Response::new(
                build_fb_write_response(resp),
            ))),
            false => Ok(TonicWriteResponseExt::Proto(Response::new(resp))),
        }
    }

    async fn get_table_info_internal(
        &self,
        request: Request<GetTableInfoRequest>,
    ) -> std::result::Result<Response<GetTableInfoResponse>, Status> {
        let begin_instant = Instant::now();
        let ctx = self.handler_ctx();
        let handle = self.runtimes.read_runtime.spawn(async move {
            let request = request.into_inner();
            handle_get_table_info(ctx, request).await.map_err(|e| {
                error!("Handle get table info failed, err:{e}");
                e
            })
        });

        let res = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        });

        let mut resp = GetTableInfoResponse::default();
        match res {
            Ok(Ok(v)) => {
                resp.header = Some(error::build_ok_header());
                resp.table_info = v.table_info;
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(error::build_err_header(e));
            }
        };

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .get_table_info
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
        Ok(Response::new(resp))
    }

    async fn write_batch_internal(
        &self,
        request: TonicWriteBatchRequestExt,
    ) -> std::result::Result<TonicWriteResponseExt, Status> {
        let begin_instant = Instant::now();

        let (write_table_handles, is_flatbuffer) = match request {
            TonicWriteBatchRequestExt::Proto(v) => {
                let request = v.into_inner();
                let mut write_table_handles = Vec::with_capacity(request.batch.len());
                for one_request in request.batch {
                    let ctx = self.handler_ctx();
                    let handle = self
                        .runtimes
                        .write_runtime
                        .spawn(handle_write(ctx, WriteRequestExt::Proto(one_request)));
                    write_table_handles.push(handle);
                }
                (write_table_handles, false)
            }
            TonicWriteBatchRequestExt::Flatbuffer(v) => {
                let request = Arc::new(v.into_inner());
                let req = request.deserialize::<FBWriteBatchRequest>().unwrap();
                let batch_len = req.batch().unwrap().len();
                let mut write_table_handles = Vec::with_capacity(batch_len);
                for i in 0..batch_len {
                    let r = request.clone();
                    let ctx = self.handler_ctx();
                    let handle = self.runtimes.write_runtime.spawn(async move {
                        let r = r.deserialize::<FBWriteBatchRequest>().unwrap();
                        let batch = r.batch().unwrap();
                        handle_write(ctx, WriteRequestExt::Flatbuffer(batch.get(i)))
                            .await
                            .map_err(|e| {
                                error!("Handle write failed, err:{e}");
                                e
                            })
                    });
                    write_table_handles.push(handle);
                }
                (write_table_handles, true)
            }
        };

        let mut batch_resp = WriteResponse {
            header: Some(error::build_ok_header()),
            affected_rows: 0,
        };

        for write_handle in write_table_handles {
            let write_result = write_handle.await.box_err().context(ErrWithCause {
                code: StatusCode::Internal,
                msg: "fail to run the join task",
            });
            // The underlying write can't be cancelled, so just ignore the left write
            // handles (don't abort them) if any error is encountered.
            match write_result {
                Ok(res) => match res {
                    Ok(resp) => batch_resp.affected_rows += resp.affected_rows,
                    Err(e) => {
                        error!("Failed to write batches, err:{e}");
                        batch_resp.header = Some(error::build_err_header(e));
                        break;
                    }
                },
                Err(e) => {
                    error!("Failed to write batches, err:{e}");
                    batch_resp.header = Some(error::build_err_header(e));
                    break;
                }
            };
        }

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .write_batch
            .observe(begin_instant.saturating_elapsed().as_secs_f64());

        match is_flatbuffer {
            true => Ok(TonicWriteResponseExt::Flatbuffer(Response::new(
                build_fb_write_response(batch_resp),
            ))),
            false => Ok(TonicWriteResponseExt::Proto(Response::new(batch_resp))),
        }
    }

    async fn execute_physical_plan_internal(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<RemoteExecStream> {
        let request = request.into_inner();
        let query_engine = self.instance.query_engine.clone();
        let (ctx, encoded_plan) = extract_plan_from_req(request)?;
        let slow_threshold_secs = self
            .instance
            .dyn_config
            .slow_threshold
            .load(std::sync::atomic::Ordering::Relaxed);

        let priority = ctx.priority();
        let query_ctx = create_query_ctx(
            ctx.request_id,
            ctx.default_catalog,
            ctx.default_schema,
            ctx.timeout_ms,
            priority,
        );

        debug!("Execute remote query, id:{}", query_ctx.request_id.as_str());

        let metric = ExecutePlanMetricCollector::new(
            query_ctx.request_id.clone(),
            ctx.displayable_query,
            slow_threshold_secs,
            query_ctx.priority,
        );
        let physical_plan: PhysicalPlanRef = Arc::new(DataFusionPhysicalPlanAdapter::new(
            TypedPlan::Remote(encoded_plan),
        ));
        // TODO: Use in handle_execute_plan fn to build stream with metrics
        let physical_plan_for_explain = ctx.explain.map(|_| physical_plan.clone());

        let rt = self
            .runtimes
            .read_runtime
            .choose_runtime(&query_ctx.priority);

        let stream = rt
            .spawn(async move { handle_execute_plan(query_ctx, physical_plan, query_engine).await })
            .await
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::Internal,
                msg: "failed to run execute physical plan task",
            })??
            .map(|result| {
                result.box_err().context(ErrWithCause {
                    code: StatusCode::Internal,
                    msg: "failed to poll record batch for remote physical plan",
                })
            });

        let stream = StreamWithMetric::new(Box::pin(stream), metric);
        Ok(RemoteExecStream::new(
            Box::pin(stream),
            physical_plan_for_explain,
        ))
    }

    async fn dedup_execute_physical_plan_internal(
        &self,
        query_dedup: QueryDedup,
        request: Request<ExecutePlanRequest>,
    ) -> Result<RemoteExecStream> {
        let request = request.into_inner();
        let query_engine = self.instance.query_engine.clone();
        let (ctx, encoded_plan) = extract_plan_from_req(request)?;
        let slow_threshold_secs = self
            .instance
            .dyn_config
            .slow_threshold
            .load(std::sync::atomic::Ordering::Relaxed);
        let priority = ctx.priority();
        let query_ctx = create_query_ctx(
            ctx.request_id,
            ctx.default_catalog,
            ctx.default_schema,
            ctx.timeout_ms,
            priority,
        );
        debug!(
            "Execute dedupped remote query, id:{}",
            query_ctx.request_id.as_str()
        );
        let metric = ExecutePlanMetricCollector::new(
            query_ctx.request_id.clone(),
            ctx.displayable_query,
            slow_threshold_secs,
            query_ctx.priority,
        );
        let key = PhysicalPlanKey {
            encoded_plan: encoded_plan.clone(),
        };

        let physical_plan: PhysicalPlanRef = Arc::new(DataFusionPhysicalPlanAdapter::new(
            TypedPlan::Remote(encoded_plan),
        ));
        // TODO: Use in handle_execute_plan fn to build stream with metrics
        let physical_plan_for_explain = ctx.explain.map(|_| physical_plan.clone());

        let QueryDedup {
            config,
            physical_plan_notifiers,
            ..
        } = query_dedup;

        let rt = self
            .runtimes
            .read_runtime
            .choose_runtime(&query_ctx.priority);
        let (tx, rx) = mpsc::channel(config.notify_queue_cap);
        match physical_plan_notifiers.insert_notifier(key.clone(), tx) {
            // The first request, need to handle it, and then notify the other requests.
            RequestResult::First => {
                let query = async move {
                    handle_execute_plan(query_ctx, physical_plan, query_engine)
                        .await
                        .map(PartitionedStreams::one_stream)
                };
                self.read_and_send_dedupped_resps(
                    key,
                    query,
                    physical_plan_notifiers,
                    config.notify_timeout.0,
                    rt,
                )
                .await?;
            }
            // The request is waiting for the result of first request.
            RequestResult::Wait => {
                // TODO: add metrics to collect the time cost of waited stream
                // read.
            }
        }

        let stream = StreamWithMetric::new(Box::pin(ReceiverStream::new(rx)), metric);
        Ok(RemoteExecStream::new(
            Box::pin(stream),
            physical_plan_for_explain,
        ))
    }

    async fn alter_table_schema_internal(
        &self,
        request: Request<AlterTableSchemaRequest>,
    ) -> std::result::Result<Response<AlterTableSchemaResponse>, Status> {
        let begin_instant = Instant::now();
        let ctx = self.handler_ctx();
        let handle = self.runtimes.read_runtime.spawn(async move {
            let request = request.into_inner();
            handle_alter_table_schema(ctx, request).await.map_err(|e| {
                error!("Handle alter table schema failed, err:{e}");
                e
            })
        });

        let res = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        });

        let mut resp = AlterTableSchemaResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(error::build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(error::build_err_header(e));
            }
        };

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .alter_table_schema
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
        Ok(Response::new(resp))
    }

    async fn alter_table_options_internal(
        &self,
        request: Request<AlterTableOptionsRequest>,
    ) -> std::result::Result<Response<AlterTableOptionsResponse>, Status> {
        let begin_instant = Instant::now();
        let ctx = self.handler_ctx();
        let handle = self.runtimes.read_runtime.spawn(async move {
            let request = request.into_inner();
            handle_alter_table_options(ctx, request).await
        });

        let res = handle.await.box_err().context(ErrWithCause {
            code: StatusCode::Internal,
            msg: "fail to join task",
        });

        let mut resp = AlterTableOptionsResponse::default();
        match res {
            Ok(Ok(_)) => {
                resp.header = Some(error::build_ok_header());
            }
            Ok(Err(e)) | Err(e) => {
                resp.header = Some(error::build_err_header(e));
            }
        };

        REMOTE_ENGINE_GRPC_HANDLER_DURATION_HISTOGRAM_VEC
            .alter_table_options
            .observe(begin_instant.saturating_elapsed().as_secs_f64());
        Ok(Response::new(resp))
    }

    fn handler_ctx(&self) -> HandlerContext {
        HandlerContext {
            catalog_manager: self.instance.catalog_manager.clone(),
            hotspot_recorder: self.hotspot_recorder.clone(),
        }
    }
}

/// Context for handling all kinds of remote engine service.
#[derive(Clone)]
struct HandlerContext {
    catalog_manager: ManagerRef,
    hotspot_recorder: Arc<HotspotRecorder>,
}

#[async_trait]
impl RemoteEngineFbService for RemoteEngineServiceImpl {
    async fn write(
        &self,
        request: tonic::Request<FlatBufferBytes>,
    ) -> std::result::Result<tonic::Response<FlatBufferBytes>, tonic::Status> {
        let result = self
            .write_internal(TonicWriteRequestExt::Flatbuffer(request))
            .await?;
        match result {
            TonicWriteResponseExt::Flatbuffer(v) => Ok(v),
            TonicWriteResponseExt::Proto(_) => Err(Status::new(Code::Internal, "logic error")),
        }
    }

    async fn write_batch(
        &self,
        request: tonic::Request<FlatBufferBytes>,
    ) -> std::result::Result<tonic::Response<FlatBufferBytes>, tonic::Status> {
        let result = self
            .write_batch_internal(TonicWriteBatchRequestExt::Flatbuffer(request))
            .await?;
        match result {
            TonicWriteResponseExt::Flatbuffer(v) => Ok(v),
            TonicWriteResponseExt::Proto(_) => Err(Status::new(Code::Internal, "logic error")),
        }
    }
}

#[async_trait]
impl RemoteEngineService for RemoteEngineServiceImpl {
    type ExecutePhysicalPlanStream = BoxStream<'static, std::result::Result<ReadResponse, Status>>;
    type ReadStream = BoxStream<'static, std::result::Result<ReadResponse, Status>>;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> std::result::Result<Response<Self::ReadStream>, Status> {
        if let Some(table) = &request.get_ref().table {
            self.hotspot_recorder
                .send_msg_or_log(
                    "inc_query_reqs",
                    Message::Query(format_hot_key(&table.schema, &table.table)),
                )
                .await
        }

        REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC.stream_query.inc();
        let result = match self.query_dedup.clone() {
            Some(query_dedup) => self.dedup_stream_read_internal(query_dedup, request).await,
            None => self.stream_read_internal(request).await,
        };

        record_stream_to_response_stream!(result, ReadStream)
    }

    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        let result = self
            .write_internal(TonicWriteRequestExt::Proto(request))
            .await?;
        match result {
            TonicWriteResponseExt::Proto(v) => Ok(v),
            TonicWriteResponseExt::Flatbuffer(_) => Err(Status::new(Code::Internal, "logic error")),
        }
    }

    async fn get_table_info(
        &self,
        request: Request<GetTableInfoRequest>,
    ) -> std::result::Result<Response<GetTableInfoResponse>, Status> {
        self.get_table_info_internal(request).await
    }

    async fn write_batch(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> std::result::Result<Response<WriteResponse>, Status> {
        let result = self
            .write_batch_internal(TonicWriteBatchRequestExt::Proto(request))
            .await?;
        match result {
            TonicWriteResponseExt::Proto(v) => Ok(v),
            TonicWriteResponseExt::Flatbuffer(_) => Err(Status::new(Code::Internal, "logic error")),
        }
    }

    async fn execute_physical_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> std::result::Result<Response<Self::ExecutePhysicalPlanStream>, Status> {
        if let Some(table) = &request.get_ref().table {
            self.hotspot_recorder
                .send_msg_or_log(
                    "inc_remote_plan_reqs",
                    Message::Query(format_hot_key(&table.schema, &table.table)),
                )
                .await
        }

        let record_stream_result = match self.query_dedup.clone() {
            Some(query_dedup) => self
                .dedup_execute_physical_plan_internal(query_dedup, request)
                .await
                .map_err(|e| {
                    error!("Dedup execute physical plan failed, err:{e}");
                    e
                }),
            None => self
                .execute_physical_plan_internal(request)
                .await
                .map_err(|e| {
                    error!("Execute physical plan failed, err:{e}");
                    e
                }),
        };

        record_stream_to_response_stream!(record_stream_result, ExecutePhysicalPlanStream)
    }

    async fn alter_table_schema(
        &self,
        request: Request<AlterTableSchemaRequest>,
    ) -> std::result::Result<Response<AlterTableSchemaResponse>, Status> {
        self.alter_table_schema_internal(request).await
    }

    async fn alter_table_options(
        &self,
        request: Request<AlterTableOptionsRequest>,
    ) -> std::result::Result<Response<AlterTableOptionsResponse>, Status> {
        self.alter_table_options_internal(request).await
    }
}

async fn handle_stream_read(
    ctx: HandlerContext,
    request: ReadRequest,
) -> Result<PartitionedStreams> {
    let table_engine::remote::model::ReadRequest {
        table: table_ident,
        read_request,
    } = request.try_into().box_err().context(ErrWithCause {
        code: StatusCode::BadRequest,
        msg: "fail to convert read request",
    })?;

    let request_id = &read_request.request_id;
    info!(
        "Handle stream read, request_id:{request_id}, table:{table_ident:?}, read_options:{:?}, predicate:{:?} ",
        read_request.opts,
        read_request.predicate,
    );

    let begin = Instant::now();
    let table = find_table_by_identifier(&ctx, &table_ident)?;
    let res = table
        .partitioned_read(read_request.clone())
        .await
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to read table, table:{table_ident:?}"),
        });
    match res {
        Ok(streams) => {
            info!(
                "Handle stream read success, request_id:{request_id}, table:{table_ident:?}, cost:{:?}, read_options:{:?}, predicate:{:?}",
                begin.elapsed(),
                read_request.opts,
                read_request.predicate,
            );

            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .stream_query_succeeded
                .inc();
            Ok(streams)
        }
        Err(e) => {
            error!(
                "Handle stream read failed, request_id:{request_id}, table:{table_ident:?}, cost:{:?}, read_options:{:?}, predicate:{:?}, err:{e}",
                begin.elapsed(),
                read_request.opts,
                read_request.predicate,
            );

            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .stream_query_failed
                .inc();
            Err(e)
        }
    }
}

fn format_hot_key(schema: &str, table: &str) -> String {
    format!("{schema}/{table}")
}

async fn record_write(
    hotspot_recorder: &Arc<HotspotRecorder>,
    request: &table_engine::remote::model::WriteRequest,
) {
    let hot_key = format_hot_key(&request.table.schema, &request.table.table);
    let row_count = request.write_request.row_group.num_rows();
    let field_count = row_count * request.write_request.row_group.schema().num_columns();
    hotspot_recorder
        .send_msg_or_log(
            "inc_write_reqs",
            Message::Write {
                key: hot_key,
                row_count,
                field_count,
            },
        )
        .await;
}

async fn handle_write(ctx: HandlerContext, request: WriteRequestExt<'_>) -> Result<WriteResponse> {
    let (table_ident, rows_payload) = match request {
        WriteRequestExt::Proto(v) => {
            let table_ident: TableIdentifier = v
                .table
                .context(ErrNoCause {
                    code: StatusCode::BadRequest,
                    msg: "missing table ident",
                })?
                .into();

            let rows_payload = v
                .row_group
                .context(ErrNoCause {
                    code: StatusCode::BadRequest,
                    msg: "missing row group payload",
                })?
                .rows
                .context(ErrNoCause {
                    code: StatusCode::BadRequest,
                    msg: "missing rows payload",
                })?;
            (table_ident, RowsPayloadExt::Proto(rows_payload))
        }
        WriteRequestExt::Flatbuffer(v) => {
            let table_ident = v
                .table()
                .context(ErrNoCause {
                    code: StatusCode::BadRequest,
                    msg: "missing table ident",
                })?
                .into();
            let rows_payload = v
                .row_group()
                .context(ErrNoCause {
                    code: StatusCode::BadRequest,
                    msg: "missing row group payload",
                })?
                .contiguous()
                .context(ErrNoCause {
                    code: StatusCode::BadRequest,
                    msg: "missing rows payload",
                })?;
            (table_ident, RowsPayloadExt::Flatbuffer(rows_payload))
        }
    };

    let table = find_table_by_identifier(&ctx, &table_ident)?;
    let write_request = match rows_payload {
        RowsPayloadExt::Proto(v) => match v {
            row_group::Rows::Arrow(_) => {
                // The payload encoded in arrow format won't be accept any more.
                return ErrNoCause {
                    code: StatusCode::BadRequest,
                    msg: "payload encoded in arrow format is not supported anymore",
                }
                .fail();
            }
            row_group::Rows::Contiguous(payload) => {
                let schema = table.schema();
                let row_group = model::WriteRequest::decode_row_group_from_contiguous_payload(
                    ContiguousRowsExt::Proto(&payload),
                    &schema,
                )
                .box_err()
                .context(ErrWithCause {
                    code: StatusCode::BadRequest,
                    msg: "failed to decode row group payload",
                })?;
                model::WriteRequest::new(table_ident, row_group)
            }
        },
        RowsPayloadExt::Flatbuffer(v) => {
            let schema = table.schema();
            let row_group = model::WriteRequest::decode_row_group_from_contiguous_payload(
                ContiguousRowsExt::Flatbuffer(v),
                &schema,
            )
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::BadRequest,
                msg: "failed to decode row group payload",
            })?;
            model::WriteRequest::new(table_ident, row_group)
        }
    };

    // In theory we should record write request we at the beginning of server's
    // handle, but the payload is encoded, so we cannot record until decode payload
    // here.
    record_write(&ctx.hotspot_recorder, &write_request).await;

    let num_rows = write_request.write_request.row_group.num_rows();
    REMOTE_ENGINE_WRITE_BATCH_NUM_ROWS_HISTOGRAM.observe(num_rows as f64);

    let res = table
        .write(write_request.write_request)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to write table, table:{:?}", write_request.table),
        });
    match res {
        Ok(affected_rows) => {
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC.write_succeeded.inc();
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .write_succeeded_row
                .inc_by(affected_rows as u64);
            Ok(WriteResponse {
                header: None,
                affected_rows: affected_rows as u64,
            })
        }
        Err(e) => {
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC.write_failed.inc();
            REMOTE_ENGINE_GRPC_HANDLER_COUNTER_VEC
                .write_failed_row
                .inc_by(num_rows as u64);
            Err(e)
        }
    }
}

async fn handle_get_table_info(
    ctx: HandlerContext,
    request: GetTableInfoRequest,
) -> Result<GetTableInfoResponse> {
    let request: table_engine::remote::model::GetTableInfoRequest =
        request.try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert get table info request",
        })?;

    let schema = find_schema_by_identifier(&ctx, &request.table)?;
    let table = schema
        .table_by_name(&request.table.table)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get table, table:{}", request.table.table),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("table is not found, table:{}", request.table.table),
        })?;

    Ok(GetTableInfoResponse {
        header: None,
        table_info: Some(horaedbproto::remote_engine::TableInfo {
            catalog_name: request.table.catalog,
            schema_name: schema.name().to_string(),
            schema_id: schema.id().as_u32(),
            table_name: table.name().to_string(),
            table_id: table.id().as_u64(),
            table_schema: Some((&table.schema()).into()),
            engine: table.engine_type().to_string(),
            options: table.options(),
            partition_info: table.partition_info().map(Into::into),
        }),
    })
}

fn extract_plan_from_req(request: ExecutePlanRequest) -> Result<(ExecContext, Vec<u8>)> {
    // Build execution context.
    let ctx_in_req = request.context.with_context(|| ErrNoCause {
        code: StatusCode::Internal,
        msg: "execution context not found in physical plan request",
    })?;
    let typed_plan_in_req = request.physical_plan.with_context(|| ErrNoCause {
        code: StatusCode::Internal,
        msg: "plan not found in physical plan request",
    })?;
    // FIXME: return the type from query engine.
    let valid_plan = check_and_extract_plan(typed_plan_in_req, QueryEngineType::Datafusion)?;

    Ok((ctx_in_req, valid_plan))
}

fn create_query_ctx(
    request_id: String,
    default_catalog: String,
    default_schema: String,
    timeout_ms: i64,
    priority: QueryPriority,
) -> QueryContext {
    let request_id = RequestId::from(request_id);
    let deadline = if timeout_ms >= 0 {
        Some(Instant::now() + Duration::from_millis(timeout_ms as u64))
    } else {
        None
    };
    let priority = match priority {
        QueryPriority::Low => Priority::Low,
        QueryPriority::High => Priority::High,
    };

    QueryContext {
        request_id,
        deadline,
        default_catalog,
        default_schema,
        priority,
    }
}

async fn handle_execute_plan(
    ctx: QueryContext,
    physical_plan: PhysicalPlanRef,
    query_engine: QueryEngineRef,
) -> Result<SendableRecordBatchStream> {
    // Execute plan.
    let executor = query_engine.executor();
    executor
        .execute(&ctx, physical_plan)
        .await
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::Internal,
            msg: "failed to execute remote plan",
        })
}

async fn handle_alter_table_schema(
    ctx: HandlerContext,
    request: AlterTableSchemaRequest,
) -> Result<()> {
    let request: table_engine::remote::model::AlterTableSchemaRequest =
        request.try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert alter table schema",
        })?;

    let schema = find_schema_by_identifier(&ctx, &request.table_ident)?;
    let table = schema
        .table_by_name(&request.table_ident.table)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get table, table:{}", request.table_ident.table),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("table is not found, table:{}", request.table_ident.table),
        })?;

    table
        .alter_schema(AlterSchemaRequest {
            schema: request.table_schema,
            pre_schema_version: request.pre_schema_version,
        })
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to alter table schema, table:{}",
                request.table_ident.table
            ),
        })?;
    Ok(())
}

async fn handle_alter_table_options(
    ctx: HandlerContext,
    request: AlterTableOptionsRequest,
) -> Result<()> {
    let request: table_engine::remote::model::AlterTableOptionsRequest =
        request.try_into().box_err().context(ErrWithCause {
            code: StatusCode::BadRequest,
            msg: "fail to convert alter table options",
        })?;

    let schema = find_schema_by_identifier(&ctx, &request.table_ident)?;
    let table = schema
        .table_by_name(&request.table_ident.table)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get table, table:{}", request.table_ident.table),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("table is not found, table:{}", request.table_ident.table),
        })?;

    table
        .alter_options(request.options)
        .await
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to alter table options, table:{}",
                request.table_ident.table
            ),
        })?;
    Ok(())
}

fn check_and_extract_plan(
    typed_plan: execute_plan_request::PhysicalPlan,
    engine_type: QueryEngineType,
) -> Result<Vec<u8>> {
    match (typed_plan, engine_type) {
        (execute_plan_request::PhysicalPlan::Datafusion(plan), QueryEngineType::Datafusion) => {
            Ok(plan)
        }
    }
}

fn find_table_by_identifier(
    ctx: &HandlerContext,
    table_identifier: &TableIdentifier,
) -> Result<TableRef> {
    let schema = find_schema_by_identifier(ctx, table_identifier)?;

    schema
        .table_by_name(&table_identifier.table)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get table, table:{}", table_identifier.table),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("table is not found, table:{}", table_identifier.table),
        })
}

fn find_schema_by_identifier(
    ctx: &HandlerContext,
    table_identifier: &TableIdentifier,
) -> Result<SchemaRef> {
    let catalog = ctx
        .catalog_manager
        .catalog_by_name(&table_identifier.catalog)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!("fail to get catalog, catalog:{}", table_identifier.catalog),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!("catalog is not found, catalog:{}", table_identifier.catalog),
        })?;
    catalog
        .schema_by_name(&table_identifier.schema)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::Internal,
            msg: format!(
                "fail to get schema of table, schema:{}",
                table_identifier.schema
            ),
        })?
        .context(ErrNoCause {
            code: StatusCode::NotFound,
            msg: format!(
                "schema of table is not found, schema:{}",
                table_identifier.schema
            ),
        })
}

fn build_fb_write_response(resp: WriteResponse) -> FlatBufferBytes {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
    let header = resp.header.unwrap();

    let error_message = builder.create_string(header.error.as_str());
    let response_header_args = ResponseHeader::create(
        &mut builder,
        &ResponseHeaderArgs {
            code: header.code,
            error: Some(error_message),
        },
    );

    let root_offset: flatbuffers::WIPOffset<FBWriteResponse<'_>> = FBWriteResponse::create(
        &mut builder,
        &FBWriteResponseArgs {
            header: Some(response_header_args),
            affected_rows: resp.affected_rows,
        },
    );
    FlatBufferBytes::serialize(builder, root_offset)
}
