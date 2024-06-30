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

//! Grpc services

use std::{
    net::{AddrParseError, SocketAddr},
    stringify,
    sync::Arc,
    time::Duration,
};

use cluster::ClusterRef;
use common_types::column_schema;
use fb_util::remote_engine_fb_service_server::RemoteEngineFbServiceServer;
use futures::FutureExt;
use generic_error::GenericError;
use horaedbproto::{
    meta_event::meta_event_service_server::MetaEventServiceServer,
    remote_engine::remote_engine_service_server::RemoteEngineServiceServer,
    storage::storage_service_server::StorageServiceServer,
};
use logger::{info, warn};
use macros::define_result;
use notifier::notifier::RequestNotifiers;
use proxy::{
    auth::with_file::AuthWithFile,
    forward,
    hotspot::HotspotRecorder,
    instance::InstanceRef,
    schema_config_provider::{self},
    Proxy,
};
use runtime::{JoinHandle, Runtime};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::engine::EngineRuntimes;
use tokio::sync::oneshot::{self, Sender};
use tonic::{codegen::InterceptedService, transport::Server};
use wal::manager::OpenedWals;

use self::remote_engine_service::QueryDedup;
use crate::{
    config::QueryDedupConfig,
    grpc::{
        meta_event_service::MetaServiceImpl, remote_engine_service::RemoteEngineServiceImpl,
        storage_service::StorageServiceImpl,
    },
};

mod meta_event_service;
mod metrics;
mod remote_engine_service;
mod storage_service;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Internal error, message:{}, cause:{}", msg, source))]
    Internal { msg: String, source: GenericError },

    #[snafu(display(
        "Failed to keep grpc service, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    FailServe {
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to parse rpc service addr, err:{}.\nBacktrace:\n{}",
        source,
        backtrace
    ))]
    InvalidRpcServeAddr {
        source: AddrParseError,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing meta client config.\nBacktrace:\n{}", backtrace))]
    MissingMetaClientConfig { backtrace: Backtrace },

    #[snafu(display("Missing grpc environment.\nBacktrace:\n{}", backtrace))]
    MissingEnv { backtrace: Backtrace },

    #[snafu(display("Missing runtimes.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing instance.\nBacktrace:\n{}", backtrace))]
    MissingInstance { backtrace: Backtrace },

    #[snafu(display("Missing wals.\nBacktrace:\n{}", backtrace))]
    MissingWals { backtrace: Backtrace },

    #[snafu(display("Missing timeout.\nBacktrace:\n{}", backtrace))]
    MissingTimeout { backtrace: Backtrace },

    #[snafu(display("Missing proxy.\nBacktrace:\n{}", backtrace))]
    MissingProxy { backtrace: Backtrace },

    #[snafu(display("Missing HotspotRecorder.\nBacktrace:\n{}", backtrace))]
    MissingHotspotRecorder { backtrace: Backtrace },

    #[snafu(display("Missing auth.\nBacktrace:\n{}", backtrace))]
    MissingAuth { backtrace: Backtrace },

    #[snafu(display("Catalog name is not utf8.\nBacktrace:\n{}", backtrace))]
    ParseCatalogName {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Schema name is not utf8.\nBacktrace:\n{}", backtrace))]
    ParseSchemaName {
        source: std::string::FromUtf8Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to build forwarder, err:{}", source))]
    BuildForwarder { source: forward::Error },

    #[snafu(display(
        "Fail to build column schema from column:{}, err:{}",
        column_name,
        source
    ))]
    BuildColumnSchema {
        column_name: String,
        source: column_schema::Error,
    },

    #[snafu(display("Invalid column schema, column:{}, err:{}", column_name, source))]
    InvalidColumnSchema {
        column_name: String,
        source: column_schema::Error,
    },

    #[snafu(display("Invalid argument: {}", msg))]
    InvalidArgument { msg: String },

    #[snafu(display("Get schema config failed, err:{}", source))]
    GetSchemaConfig {
        source: schema_config_provider::Error,
    },
}

define_result!(Error);

/// Rpc services manages all grpc services of the server.
pub struct RpcServices {
    serve_addr: SocketAddr,
    rpc_server: InterceptedService<StorageServiceServer<StorageServiceImpl>, AuthWithFile>,
    meta_rpc_server: Option<MetaEventServiceServer<MetaServiceImpl>>,
    remote_engine_server: RemoteEngineServiceServer<RemoteEngineServiceImpl>,
    remote_engine_server_ext: RemoteEngineFbServiceServer<RemoteEngineServiceImpl>,
    runtime: Arc<Runtime>,
    stop_tx: Option<Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl RpcServices {
    pub async fn start(&mut self) -> Result<()> {
        let rpc_server = self.rpc_server.clone();
        let meta_rpc_server = self.meta_rpc_server.clone();
        let remote_engine_server = self.remote_engine_server.clone();
        let remote_engine_server_ext = self.remote_engine_server_ext.clone();
        let serve_addr = self.serve_addr;
        let (stop_tx, stop_rx) = oneshot::channel();
        let join_handle = self.runtime.spawn(async move {
            info!("Grpc server tries to listen on {}", serve_addr);

            let mut router = Server::builder().add_service(rpc_server);

            if let Some(s) = meta_rpc_server {
                info!("Grpc server serves meta rpc service");
                router = router.add_service(s);
            };

            info!("Grpc server serves remote engine rpc service");
            router = router.add_service(remote_engine_server);
            router = router.add_service(remote_engine_server_ext);

            router
                .serve_with_shutdown(serve_addr, stop_rx.map(drop))
                .await
                .unwrap_or_else(|e| {
                    panic!("Grpc server listens failed, err:{e:?}");
                });
        });
        self.join_handle = Some(join_handle);
        self.stop_tx = Some(stop_tx);
        Ok(())
    }

    pub async fn shutdown(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let res = stop_tx.send(());
            warn!("Send stop signal, send_res:{:?}", res);
        }

        if let Some(join_handle) = self.join_handle.take() {
            let join_res = join_handle.await;
            warn!("Finish join with serve task, join_res:{:?}", join_res);
        }
    }
}

pub struct Builder {
    auth: Option<AuthWithFile>,
    endpoint: String,
    timeout: Option<Duration>,
    runtimes: Option<Arc<EngineRuntimes>>,
    instance: Option<InstanceRef>,
    cluster: Option<ClusterRef>,
    opened_wals: Option<OpenedWals>,
    proxy: Option<Arc<Proxy>>,
    query_dedup_config: Option<QueryDedupConfig>,
    hotspot_recorder: Option<Arc<HotspotRecorder>>,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            auth: None,
            endpoint: "0.0.0.0:8381".to_string(),
            timeout: None,
            runtimes: None,
            instance: None,
            cluster: None,
            opened_wals: None,
            proxy: None,
            query_dedup_config: None,
            hotspot_recorder: None,
        }
    }

    pub fn auth(mut self, auth: AuthWithFile) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = endpoint;
        self
    }

    pub fn runtimes(mut self, runtimes: Arc<EngineRuntimes>) -> Self {
        self.runtimes = Some(runtimes);
        self
    }

    pub fn instance(mut self, instance: InstanceRef) -> Self {
        self.instance = Some(instance);
        self
    }

    // Cluster is an optional field for building [RpcServices].
    pub fn cluster(mut self, cluster: Option<ClusterRef>) -> Self {
        self.cluster = cluster;
        self
    }

    pub fn opened_wals(mut self, opened_wals: OpenedWals) -> Self {
        self.opened_wals = Some(opened_wals);
        self
    }

    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn proxy(mut self, proxy: Arc<Proxy>) -> Self {
        self.proxy = Some(proxy);
        self
    }

    pub fn hotspot_recorder(mut self, hotspot_recorder: Arc<HotspotRecorder>) -> Self {
        self.hotspot_recorder = Some(hotspot_recorder);
        self
    }

    pub fn query_dedup(mut self, config: QueryDedupConfig) -> Self {
        self.query_dedup_config = Some(config);
        self
    }
}

impl Builder {
    pub fn build(self) -> Result<RpcServices> {
        let auth = self.auth.context(MissingAuth)?;
        let runtimes = self.runtimes.context(MissingRuntimes)?;
        let instance = self.instance.context(MissingInstance)?;
        let opened_wals = self.opened_wals.context(MissingWals)?;
        let proxy = self.proxy.context(MissingProxy)?;
        let hotspot_recorder = self.hotspot_recorder.context(MissingHotspotRecorder)?;

        let meta_rpc_server = self.cluster.map(|v| {
            let builder = meta_event_service::Builder {
                cluster: v,
                instance: instance.clone(),
                runtime: runtimes.meta_runtime.clone(),
                opened_wals,
            };
            MetaEventServiceServer::new(builder.build())
        });

        let query_dedup = self
            .query_dedup_config
            .map(|v| {
                v.enable.then(|| QueryDedup {
                    config: v.clone(),
                    request_notifiers: Arc::new(RequestNotifiers::default()),
                    physical_plan_notifiers: Arc::new(RequestNotifiers::default()),
                })
            })
            .unwrap_or_default();
        let service = RemoteEngineServiceImpl {
            instance: instance.clone(),
            runtimes: runtimes.clone(),
            query_dedup: query_dedup.clone(),
            hotspot_recorder: hotspot_recorder.clone(),
        };
        let remote_engine_server = RemoteEngineServiceServer::new(service);
        let service_ext = RemoteEngineServiceImpl {
            instance,
            runtimes: runtimes.clone(),
            query_dedup,
            hotspot_recorder,
        };

        let remote_engine_server_ext = RemoteEngineFbServiceServer::new(service_ext);

        let runtime = runtimes.default_runtime.clone();

        let storage_service = StorageServiceImpl {
            proxy,
            runtimes,
            timeout: self.timeout,
        };
        let rpc_server = StorageServiceServer::with_interceptor(storage_service, auth);

        let serve_addr = self.endpoint.parse().context(InvalidRpcServeAddr)?;

        Ok(RpcServices {
            serve_addr,
            rpc_server,
            meta_rpc_server,
            remote_engine_server,
            remote_engine_server_ext,
            runtime,
            stop_tx: None,
            join_handle: None,
        })
    }
}
