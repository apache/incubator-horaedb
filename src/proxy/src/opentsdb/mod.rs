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

//! This module implements [put][1], [query][2] for OpenTSDB
//! [1]: http://opentsdb.net/docs/build/html/api_http/put.html
//! [2]: http://opentsdb.net/docs/build/html/api_http/query/index.html

use std::time::Instant;

use futures::{stream::FuturesOrdered, StreamExt};
use generic_error::BoxError;
use horaedbproto::storage::{
    RequestContext as GrpcRequestContext, WriteRequest as GrpcWriteRequest,
};
use http::StatusCode;
use logger::{debug, info};
use query_frontend::{
    frontend::{Context as SqlContext, Frontend},
    opentsdb::types::QueryRequest,
    provider::CatalogMetaProvider,
};
use snafu::ResultExt;

use self::types::QueryResponse;
use crate::{
    context::RequestContext,
    error::{ErrNoCause, ErrWithCause, Result},
    metrics::HTTP_HANDLER_COUNTER_VEC,
    opentsdb::types::{convert_output_to_response, convert_put_request, PutRequest, PutResponse},
    Context, Proxy,
};

pub mod types;

impl Proxy {
    pub async fn handle_opentsdb_put(
        &self,
        ctx: RequestContext,
        req: PutRequest,
    ) -> Result<PutResponse> {
        let write_table_requests = convert_put_request(req)?;
        let num_rows: usize = write_table_requests
            .iter()
            .map(|req| {
                req.entries
                    .iter()
                    .map(|e| e.field_groups.len())
                    .sum::<usize>()
            })
            .sum();

        let table_request = GrpcWriteRequest {
            context: Some(GrpcRequestContext {
                database: ctx.schema.clone(),
            }),
            table_requests: write_table_requests,
        };
        let proxy_context = Context::new(ctx.timeout, None);

        match self
            .handle_write_internal(proxy_context, table_request)
            .await
        {
            Ok(result) => {
                if result.failed != 0 {
                    HTTP_HANDLER_COUNTER_VEC.write_failed.inc();
                    HTTP_HANDLER_COUNTER_VEC
                        .write_failed_row
                        .inc_by(result.failed as u64);
                    ErrNoCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: format!("fail to write storage, failed rows:{:?}", result.failed),
                    }
                    .fail()?;
                }

                debug!(
                    "OpenTSDB write finished, catalog:{}, schema:{}, result:{result:?}",
                    ctx.catalog, ctx.schema
                );

                Ok(())
            }
            Err(e) => {
                HTTP_HANDLER_COUNTER_VEC.write_failed.inc();
                HTTP_HANDLER_COUNTER_VEC
                    .write_failed_row
                    .inc_by(num_rows as u64);
                Err(e)
            }
        }
    }

    pub async fn handle_opentsdb_query(
        &self,
        ctx: RequestContext,
        req: QueryRequest,
    ) -> Result<Vec<QueryResponse>> {
        let request_id = ctx.request_id;
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);

        info!(
            "Opentsdb query handler try to process request, request_id:{}, request:{:?}",
            request_id, req
        );

        let provider = CatalogMetaProvider {
            manager: self.instance.catalog_manager.clone(),
            default_catalog: &ctx.catalog,
            default_schema: &ctx.schema,
            function_registry: &*self.instance.function_registry,
        };
        let frontend = Frontend::new(provider, self.instance.dyn_config.fronted.clone());
        let sql_ctx = SqlContext::new(request_id.clone(), deadline);

        let opentsdb_plan = frontend
            .opentsdb_query_to_plan(&sql_ctx, req)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Failed to build plan",
            })?;

        for plan in &opentsdb_plan.plans {
            self.instance
                .limiter
                .try_limit(&plan.plan)
                .box_err()
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "Query is blocked",
                })?;
        }

        let mut futures = FuturesOrdered::new();
        for plan in opentsdb_plan.plans {
            let request_id_clone = request_id.clone();
            let one_resp = async {
                let output = self
                    .execute_plan(
                        request_id_clone,
                        &ctx.catalog,
                        &ctx.schema,
                        plan.plan,
                        deadline,
                    )
                    .await?;

                convert_output_to_response(
                    output,
                    plan.metric,
                    plan.field_col_name,
                    plan.timestamp_col_name,
                    plan.tags,
                    plan.aggregated_tags,
                )
            };

            futures.push_back(one_resp);
        }

        let resp = futures.collect::<Vec<_>>().await;
        let resp = resp.into_iter().collect::<Result<Vec<_>>>()?;
        let resp = resp.into_iter().flatten().collect();

        Ok(resp)
    }
}
