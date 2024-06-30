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

//! Client for accessing remote table engine

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use arrow_ext::{
    ipc,
    ipc::{CompressOptions, CompressionMethod},
};
use common_types::{record_batch::RecordBatch, schema::RecordSchema};
use fb_util::{
    remote_engine_fb_service_client::RemoteEngineFbServiceClient,
    remote_engine_generated::fbprotocol::WriteResponse,
};
use futures::{Stream, StreamExt};
use generic_error::BoxError;
use horaedbproto::{
    remote_engine::{
        self,
        read_response::Output::{Arrow, Metric},
        remote_engine_service_client::*,
    },
    storage::{arrow_payload, ArrowPayload},
};
use logger::{error, info};
use router::{endpoint::Endpoint, RouterRef};
use runtime::Runtime;
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::{
    remote::model::{
        AlterTableOptionsRequest, AlterTableSchemaRequest, ExecutePlanRequest, GetTableInfoRequest,
        ReadRequest, TableIdentifier, TableInfo, WriteBatchRequest, WriteBatchResult, WriteRequest,
    },
    table::{SchemaId, TableId},
};
use time_ext::ReadableDuration;
use tokio::time::sleep;
use tonic::{transport::Channel, Request, Streaming};

use crate::{cached_router::CachedRouter, config::Config, error::*, status_code};

struct WriteBatchContext {
    table_idents: Vec<TableIdentifier>,
    request: WriteBatchRequest,
    channel: Channel,
}

pub struct Client {
    cached_router: Arc<CachedRouter>,
    io_runtime: Arc<Runtime>,
    pub compression: CompressOptions,
    max_retry: usize,
    retry_interval: ReadableDuration,
}

impl Client {
    pub fn new(config: Config, router: RouterRef, io_runtime: Arc<Runtime>) -> Self {
        let compression = config.compression;
        let max_retry = config.max_retry;
        let retry_interval = config.retry_interval;
        let cached_router = CachedRouter::new(router, config);

        Self {
            cached_router: Arc::new(cached_router),
            io_runtime,
            compression,
            max_retry,
            retry_interval,
        }
    }

    pub async fn read(&self, request: ReadRequest) -> Result<ClientReadRecordBatchStream> {
        // Find the channel from router firstly.
        let route_context = self.cached_router.route(&request.table).await?;

        // Read from remote.
        let table_ident = request.table.clone();
        let record_schema = request.read_request.projected_schema.to_record_schema();
        let mut rpc_client = RemoteEngineServiceClient::<Channel>::new(route_context.channel);
        let request_pb = horaedbproto::remote_engine::ReadRequest::try_from(request)
            .box_err()
            .context(Convert {
                msg: "Failed to convert ReadRequest to pb",
            })?;

        let result = rpc_client
            .read(Request::new(request_pb))
            .await
            .with_context(|| Rpc {
                table_idents: vec![table_ident.clone()],
                msg: "Failed to read from remote engine",
            });

        let response = match result {
            Ok(response) => response,

            Err(e) => {
                // If occurred error, we simply evict the corresponding channel now.
                // TODO: evict according to the type of error.
                self.evict_route_from_cache(&[table_ident]).await;
                return Err(e);
            }
        };

        // When success to get the stream, table has been found in remote, not need to
        // evict cache entry.
        let response = response.into_inner();
        let remote_read_record_batch_stream = ClientReadRecordBatchStream::new(
            route_context.endpoint,
            table_ident,
            response,
            record_schema,
            Default::default(),
        );

        Ok(remote_read_record_batch_stream)
    }

    pub async fn write(&self, request: WriteRequest) -> Result<usize> {
        // Find the channel from router firstly.
        let route_context = self.cached_router.route(&request.table).await?;

        // Write to remote.
        let table_ident = request.table.clone();
        let endpoint = route_context.endpoint.clone();
        let request_fb = request.convert_into_fb().box_err().context(Convert {
            msg: "Failed to convert WriteRequest to fb",
        })?;

        let mut rpc_client = RemoteEngineFbServiceClient::<Channel>::new(route_context.channel);

        let result = rpc_client
            .write(Request::new(request_fb))
            .await
            .with_context(|| Rpc {
                table_idents: vec![table_ident.clone()],
                msg: "Failed to write to remote engine",
            });

        let result = result.and_then(|response| {
            let fb_response = response.into_inner();
            let response = fb_response.deserialize::<WriteResponse>().unwrap();
            if let Some(header) = &response.header()
                && !status_code::is_ok(header.code())
            {
                Server {
                    endpoint,
                    table_idents: vec![table_ident.clone()],
                    code: header.code(),
                    msg: header.error().unwrap().to_string(),
                }
                .fail()
            } else {
                Ok(response.affected_rows() as usize)
            }
        });

        if result.is_err() {
            // If occurred error, we simply evict the corresponding channel now.
            // TODO: evict according to the type of error.
            self.evict_route_from_cache(&[table_ident]).await;
        }

        result
    }

    pub async fn write_batch(&self, requests: Vec<WriteRequest>) -> Result<Vec<WriteBatchResult>> {
        // Find the channels from router firstly.
        let mut write_batch_contexts_by_endpoint = HashMap::new();
        for request in requests {
            let route_context: crate::cached_router::RouteContext =
                self.cached_router.route(&request.table).await?;
            let write_batch_context = write_batch_contexts_by_endpoint
                .entry(route_context.endpoint)
                .or_insert(WriteBatchContext {
                    table_idents: Vec::new(),
                    request: WriteBatchRequest::default(),
                    channel: route_context.channel,
                });
            write_batch_context.table_idents.push(request.table.clone());
            write_batch_context.request.batch.push(request);
        }

        // Merge according to endpoint.
        let mut write_handles = Vec::with_capacity(write_batch_contexts_by_endpoint.len());
        let mut written_tables = Vec::with_capacity(write_batch_contexts_by_endpoint.len());
        for (endpoint, context) in write_batch_contexts_by_endpoint {
            // Write to remote.
            let WriteBatchContext {
                table_idents,
                request,
                channel,
            } = context;

            let batch_request_fb = request.convert_into_fb().box_err().context(Convert {
                msg: "failed to convert request to fb",
            })?;
            let handle = self.io_runtime.spawn(async move {
                let mut rpc_client = RemoteEngineFbServiceClient::<Channel>::new(channel);
                rpc_client
                    .write_batch(Request::new(batch_request_fb))
                    .await
                    .map(|v| (v, endpoint.clone()))
                    .box_err()
            });

            write_handles.push(handle);
            written_tables.push(table_idents);
        }

        let mut results = Vec::with_capacity(write_handles.len());
        for (table_idents, handle) in written_tables.into_iter().zip(write_handles) {
            // If it's runtime error, don't evict entires from route cache.
            let batch_result = match handle.await.box_err() {
                Ok(result) => result,
                Err(e) => {
                    results.push(WriteBatchResult {
                        table_idents,
                        result: Err(e),
                    });
                    continue;
                }
            };

            // Check remote write result then.
            let result = batch_result.and_then(|result| {
                let (response, endpoint) = result;
                let fb_response = response.into_inner();
                let response = fb_response.deserialize::<WriteResponse>().unwrap();
                if let Some(header) = response.header()
                    && !status_code::is_ok(header.code())
                {
                    Server {
                        endpoint,
                        table_idents: table_idents.clone(),
                        code: header.code(),
                        msg: header.error().unwrap().to_string(),
                    }
                    .fail()
                    .box_err()
                } else {
                    Ok(response.affected_rows())
                }
            });

            if result.is_err() {
                // If occurred error, we simply evict the corresponding channel now.
                // TODO: evict according to the type of error.
                self.evict_route_from_cache(&table_idents).await;
            }

            results.push(WriteBatchResult {
                table_idents,
                result,
            });
        }

        Ok(results)
    }

    pub async fn alter_table_schema(&self, request: AlterTableSchemaRequest) -> Result<()> {
        // Find the channel from router firstly.
        let route_context = self.cached_router.route(&request.table_ident).await?;

        let table_ident = request.table_ident.clone();
        let endpoint = route_context.endpoint.clone();
        let request_pb: horaedbproto::remote_engine::AlterTableSchemaRequest = request.into();
        let mut rpc_client = RemoteEngineServiceClient::<Channel>::new(route_context.channel);

        let mut result = Ok(());
        // Alter schema to remote engine with retry.
        // TODO: Define a macro to reuse the retry logic.
        for i in 0..(self.max_retry + 1) {
            let resp = rpc_client
                .alter_table_schema(Request::new(request_pb.clone()))
                .await
                .with_context(|| Rpc {
                    table_idents: vec![table_ident.clone()],
                    msg: "Failed to alter schema to remote engine",
                });

            let resp = resp.and_then(|response| {
                let response = response.into_inner();
                if let Some(header) = &response.header
                    && !status_code::is_ok(header.code)
                {
                    Server {
                        endpoint: endpoint.clone(),
                        table_idents: vec![table_ident.clone()],
                        code: header.code,
                        msg: header.error.clone(),
                    }
                    .fail()
                } else {
                    Ok(())
                }
            });

            if let Err(e) = resp {
                error!(
                    "Failed to alter schema to remote engine,
        table:{table_ident:?}, err:{e}"
                );

                result = Err(e);

                // If occurred error, we simply evict the corresponding channel now.
                // TODO: evict according to the type of error.
                self.evict_route_from_cache(&[table_ident.clone()]).await;

                // Break if it's the last retry.
                if i == self.max_retry {
                    break;
                }

                sleep(self.retry_interval.0).await;
                continue;
            }
            return Ok(());
        }
        result
    }

    pub async fn alter_table_options(&self, request: AlterTableOptionsRequest) -> Result<()> {
        // Find the channel from router firstly.
        let route_context = self.cached_router.route(&request.table_ident).await?;

        let table_ident = request.table_ident.clone();
        let endpoint = route_context.endpoint.clone();
        let request_pb: horaedbproto::remote_engine::AlterTableOptionsRequest = request.into();
        let mut rpc_client = RemoteEngineServiceClient::<Channel>::new(route_context.channel);

        let mut result = Ok(());
        // Alter options to remote engine with retry.
        for i in 0..(self.max_retry + 1) {
            let resp = rpc_client
                .alter_table_options(Request::new(request_pb.clone()))
                .await
                .with_context(|| Rpc {
                    table_idents: vec![table_ident.clone()],
                    msg: "Failed to alter options to remote engine",
                });

            let resp = resp.and_then(|response| {
                let response = response.into_inner();
                if let Some(header) = &response.header
                    && !status_code::is_ok(header.code)
                {
                    Server {
                        endpoint: endpoint.clone(),
                        table_idents: vec![table_ident.clone()],
                        code: header.code,
                        msg: header.error.clone(),
                    }
                    .fail()
                } else {
                    Ok(())
                }
            });

            if let Err(e) = resp {
                error!("Failed to alter options to remote engine, table:{table_ident:?}, err:{e}");

                result = Err(e);

                // If occurred error, we simply evict the corresponding channel now.
                // TODO: evict according to the type of error.
                self.evict_route_from_cache(&[table_ident.clone()]).await;

                // Break if it's the last retry.
                if i == self.max_retry {
                    break;
                }

                sleep(self.retry_interval.0).await;
                continue;
            }
            return Ok(());
        }
        result
    }

    pub async fn get_table_info(&self, request: GetTableInfoRequest) -> Result<TableInfo> {
        // Find the channel from router firstly.
        let route_context = self.cached_router.route(&request.table).await?;
        let table_ident = request.table.clone();
        let endpoint = route_context.endpoint.clone();
        let request_pb = horaedbproto::remote_engine::GetTableInfoRequest::try_from(request)
            .box_err()
            .context(Convert {
                msg: "Failed to convert GetTableInfoRequest to pb",
            })?;

        let mut rpc_client = RemoteEngineServiceClient::<Channel>::new(route_context.channel);

        let result = rpc_client
            .get_table_info(Request::new(request_pb))
            .await
            .with_context(|| Rpc {
                table_idents: vec![table_ident.clone()],
                msg: "Failed to get table info",
            });

        let result = result.and_then(|response| {
            let response = response.into_inner();
            if let Some(header) = &response.header
                && !status_code::is_ok(header.code)
            {
                Server {
                    endpoint: endpoint.clone(),
                    table_idents: vec![table_ident.clone()],
                    code: header.code,
                    msg: header.error.clone(),
                }
                .fail()
            } else {
                Ok(response)
            }
        });

        match result {
            Ok(response) => {
                let table_info = response.table_info.context(Server {
                    endpoint: endpoint.clone(),
                    table_idents: vec![table_ident.clone()],
                    code: status_code::StatusCode::Internal.as_u32(),
                    msg: "Table info is empty",
                })?;

                Ok(TableInfo {
                    catalog_name: table_info.catalog_name,
                    schema_name: table_info.schema_name,
                    schema_id: SchemaId::from(table_info.schema_id),
                    table_name: table_info.table_name,
                    table_id: TableId::from(table_info.table_id),
                    table_schema: table_info
                        .table_schema
                        .map(TryInto::try_into)
                        .transpose()
                        .box_err()
                        .with_context(|| Convert {
                            msg: "Failed to covert table schema",
                        })?
                        .with_context(|| Server {
                            endpoint,
                            table_idents: vec![table_ident],
                            code: status_code::StatusCode::Internal.as_u32(),
                            msg: "Table schema is empty",
                        })?,
                    engine: table_info.engine,
                    options: table_info.options,
                    partition_info: table_info
                        .partition_info
                        .map(TryInto::try_into)
                        .transpose()
                        .box_err()
                        .context(Convert {
                            msg: "Failed to covert partition info",
                        })?,
                })
            }

            Err(e) => {
                // If occurred error, we simply evict the corresponding channel now.
                // TODO: evict according to the type of error.
                self.evict_route_from_cache(&[table_ident]).await;
                Err(e)
            }
        }
    }

    pub async fn execute_physical_plan(
        &self,
        request: ExecutePlanRequest,
    ) -> Result<ClientReadRecordBatchStream> {
        // Find the channel from router firstly.
        let table_ident = request.remote_request.table.clone();
        let route_context = self.cached_router.route(&table_ident).await?;

        // Execute plan from remote.
        let plan_schema = request.plan_schema;
        let mut rpc_client = RemoteEngineServiceClient::<Channel>::new(route_context.channel);
        let request_pb =
            horaedbproto::remote_engine::ExecutePlanRequest::from(request.remote_request);

        let result = rpc_client
            .execute_physical_plan(Request::new(request_pb))
            .await
            .with_context(|| Rpc {
                table_idents: vec![table_ident.clone()],
                msg: "Failed to read from remote engine",
            });

        let response = match result {
            Ok(response) => response,

            Err(e) => {
                // If occurred error, we simply evict the corresponding channel now.
                // TODO: evict according to the type of error.
                self.evict_route_from_cache(&[table_ident]).await;
                return Err(e);
            }
        };

        // When success to get the stream, table has been found in remote, not need to
        // evict cache entry.
        let response = response.into_inner();
        let remote_execute_plan_stream = ClientReadRecordBatchStream::new(
            route_context.endpoint,
            table_ident,
            response,
            plan_schema,
            request.remote_metrics,
        );

        Ok(remote_execute_plan_stream)
    }

    async fn evict_route_from_cache(&self, table_idents: &[TableIdentifier]) {
        info!(
            "Remote engine client evict route from cache, table_ident:{:?}",
            table_idents
        );
        self.cached_router.evict(table_idents).await;
    }
}

pub struct ClientReadRecordBatchStream {
    endpoint: Endpoint,
    pub table_ident: TableIdentifier,
    pub response_stream: Streaming<remote_engine::ReadResponse>,
    pub record_schema: RecordSchema,
    pub remote_metrics: Arc<Mutex<Option<String>>>,
}

impl ClientReadRecordBatchStream {
    pub fn new(
        endpoint: Endpoint,
        table_ident: TableIdentifier,
        response_stream: Streaming<remote_engine::ReadResponse>,
        record_schema: RecordSchema,
        remote_metrics: Arc<Mutex<Option<String>>>,
    ) -> Self {
        Self {
            endpoint,
            table_ident,
            response_stream,
            record_schema,
            remote_metrics,
        }
    }
}

impl Stream for ClientReadRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.response_stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(response))) => {
                // Check header.
                if let Some(header) = response.header
                    && !status_code::is_ok(header.code)
                {
                    return Poll::Ready(Some(
                        Server {
                            endpoint: this.endpoint.clone(),
                            table_idents: vec![this.table_ident.clone()],
                            code: header.code,
                            msg: header.error,
                        }
                        .fail(),
                    ));
                }

                match response.output {
                    None => Poll::Ready(None),
                    Some(v) => match v {
                        Arrow(v) => Poll::Ready(Some(convert_arrow_payload(v))),
                        Metric(v) => {
                            let mut remote_metrics = this.remote_metrics.lock().unwrap();
                            *remote_metrics = Some(v.metric);
                            Poll::Ready(None)
                        }
                    },
                }
            }

            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e).context(Rpc {
                table_idents: vec![this.table_ident.clone()],
                msg: "poll read response",
            }))),

            Poll::Ready(None) => Poll::Ready(None),

            Poll::Pending => Poll::Pending,
        }
    }
}

fn convert_arrow_payload(mut v: ArrowPayload) -> Result<RecordBatch> {
    if v.record_batches.len() != 1 {
        return InvalidRecordBatchNumber {
            batch_num: v.record_batches.len(),
        }
        .fail();
    }
    let compression = match v.compression() {
        arrow_payload::Compression::None => CompressionMethod::None,
        arrow_payload::Compression::Zstd => CompressionMethod::Zstd,
    };

    ipc::decode_record_batches(v.record_batches.swap_remove(0), compression)
        .map_err(|e| Box::new(e) as _)
        .context(Convert {
            msg: "decode read record batch",
        })
        .and_then(|mut record_batch_vec| {
            ensure!(
                record_batch_vec.len() == 1,
                InvalidRecordBatchNumber {
                    batch_num: record_batch_vec.len()
                }
            );
            record_batch_vec
                .swap_remove(0)
                .try_into()
                .map_err(|e| Box::new(e) as _)
                .context(Convert {
                    msg: "convert read record batch",
                })
        })
}
