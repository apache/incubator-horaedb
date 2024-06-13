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

//! Contains common methods used by the write process.

use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    time::Instant,
};

use bytes::Bytes;
use cluster::config::SchemaConfig;
use common_types::{
    column_schema::ColumnSchema,
    datum::{Datum, DatumKind},
    request_id::RequestId,
    row::{Row, RowGroup},
    schema::Schema,
    time::Timestamp,
};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use generic_error::BoxError;
use horaedbproto::storage::{
    storage_service_client::StorageServiceClient, value, RouteRequest as RouteRequestPb, Value,
    WriteRequest, WriteResponse as WriteResponsePB, WriteSeriesEntry, WriteTableRequest,
};
use http::StatusCode;
use interpreters::interpreter::Output;
use logger::{debug, error, info, warn};
use query_frontend::{
    frontend::{Context as FrontendContext, Frontend},
    plan::{AlterTableOperation, AlterTablePlan, InsertPlan, InsertSource, Plan},
    planner::{build_column_schema, try_get_data_type_from_value},
    provider::CatalogMetaProvider,
};
use router::{endpoint::Endpoint, RouteRequest};
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::table::TableRef;
use tonic::transport::Channel;

use crate::{
    error::{ErrNoCause, ErrWithCause, Internal, InternalNoCause, Result},
    forward::{ForwardResult, ForwarderRef},
    Context, Proxy,
};

type WriteResponseFutures<'a> = Vec<BoxFuture<'a, runtime::Result<Result<WriteResponse>>>>;

struct PlanWithTable {
    plan: Plan,
    table: TableRef,
}

#[derive(Debug)]
pub struct WriteContext {
    pub request_id: RequestId,
    pub deadline: Option<Instant>,
    pub catalog: String,
    pub schema: String,
    pub auto_create_table: bool,
}

#[derive(Debug, Default)]
pub(crate) struct WriteResponse {
    pub success: u32,
    pub failed: u32,
}

impl Proxy {
    pub(crate) async fn handle_write_internal(
        &self,
        ctx: Context,
        req: WriteRequest,
    ) -> Result<WriteResponse> {
        let write_context = req.context.clone();
        let resp = if self.cluster_with_meta {
            self.handle_write_with_meta(ctx, req).await?
        } else {
            self.handle_write_without_meta(ctx, req).await?
        };

        debug!(
            "Handle write finished, write_context:{:?}, resp:{:?}",
            write_context, resp
        );
        Ok(resp)
    }

    // Handle write requests based on horaemeta.
    // 1. Create table via horaemeta if it does not exist.
    // 2. Split write request.
    // 3. Process write.
    async fn handle_write_with_meta(
        &self,
        ctx: Context,
        req: WriteRequest,
    ) -> Result<WriteResponse> {
        let request_id = &ctx.request_id;
        let write_context = req.context.clone().context(ErrNoCause {
            msg: "Missing context",
            code: StatusCode::BAD_REQUEST,
        })?;

        self.handle_auto_create_table_with_meta(request_id.clone(), &write_context.database, &req)
            .await?;

        let (write_request_to_local, write_requests_to_forward) =
            self.split_write_request(req).await?;

        let mut futures = Vec::with_capacity(write_requests_to_forward.len() + 1);

        // Write to remote.
        self.collect_write_to_remote_future(&mut futures, ctx.clone(), write_requests_to_forward)
            .await;

        // Write to local.
        self.collect_write_to_local_future(&mut futures, ctx, write_request_to_local)
            .await;

        self.collect_write_response(futures).await
    }

    // Handle write requests without horaemeta.
    // 1. Split write request.
    // 2. Create table if not exist.
    // 3. Process write.
    async fn handle_write_without_meta(
        &self,
        ctx: Context,
        req: WriteRequest,
    ) -> Result<WriteResponse> {
        let request_id = &ctx.request_id;
        let write_context = req.context.clone().context(ErrNoCause {
            msg: "Missing context",
            code: StatusCode::BAD_REQUEST,
        })?;
        let (write_request_to_local, write_requests_to_forward) =
            self.split_write_request(req).await?;

        let mut futures = Vec::with_capacity(write_requests_to_forward.len() + 1);

        // Write to remote.
        self.collect_write_to_remote_future(&mut futures, ctx.clone(), write_requests_to_forward)
            .await;

        // Create table.
        self.handle_auto_create_table_without_meta(
            request_id.clone(),
            &write_request_to_local,
            &write_context.database,
        )
        .await?;

        // Write to local.
        self.collect_write_to_local_future(&mut futures, ctx, write_request_to_local)
            .await;

        self.collect_write_response(futures).await
    }

    async fn handle_auto_create_table_with_meta(
        &self,
        request_id: RequestId,
        schema: &str,
        req: &WriteRequest,
    ) -> Result<()> {
        if !self.auto_create_table {
            return Ok(());
        }

        let schema_config = self
            .schema_config_provider
            .schema_config(schema)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Fail to fetch schema config, schema_name:{schema}"),
            })?
            .cloned()
            .unwrap_or_default();

        // TODO: Consider whether to build tables concurrently when there are too many
        // tables.
        for write_table_req in &req.table_requests {
            let table_info = self
                .router
                .fetch_table_info(schema, &write_table_req.table)
                .await?;
            if table_info.is_some() {
                continue;
            }
            self.create_table(
                request_id.clone(),
                self.instance.catalog_manager.default_catalog_name(),
                schema,
                write_table_req,
                &schema_config,
                None,
            )
            .await?;
        }
        Ok(())
    }

    async fn handle_auto_create_table_without_meta(
        &self,
        request_id: RequestId,
        write_request: &WriteRequest,
        schema: &str,
    ) -> Result<()> {
        let table_names = write_request
            .table_requests
            .iter()
            .map(|v| v.table.clone())
            .collect::<Vec<String>>();

        let catalog = self.default_catalog_name();
        let schema_config = self
            .schema_config_provider
            .schema_config(schema)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Fail to fetch schema config, schema:{schema}"),
            })?
            .cloned()
            .unwrap_or_default();

        if self.auto_create_table {
            for (idx, table_name) in table_names.iter().enumerate() {
                let table = self.try_get_table(catalog, schema, table_name)?;
                if table.is_none() {
                    self.create_table(
                        request_id.clone(),
                        catalog,
                        schema,
                        &write_request.table_requests[idx],
                        &schema_config,
                        None,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn create_table(
        &self,
        request_id: RequestId,
        catalog: &str,
        schema: &str,
        write_table_req: &WriteTableRequest,
        schema_config: &SchemaConfig,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let provider = CatalogMetaProvider {
            manager: self.instance.catalog_manager.clone(),
            default_catalog: catalog,
            default_schema: schema,
            function_registry: &*self.instance.function_registry,
        };
        let frontend = Frontend::new(provider, self.instance.dyn_config.fronted.clone());
        let ctx = FrontendContext::new(request_id.clone(), deadline);
        let plan = frontend
            .write_req_to_plan(&ctx, schema_config, write_table_req)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!(
                    "Failed to build creating table plan, table:{}",
                    write_table_req.table
                ),
            })?;

        debug!("Execute create table begin, plan:{:?}", plan);

        let output = self
            .execute_plan(request_id, catalog, schema, plan, deadline)
            .await?;

        ensure!(
            matches!(output, Output::AffectedRows(_)),
            ErrNoCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Invalid output type, expect AffectedRows, found Records",
            }
        );
        Ok(())
    }

    async fn split_write_request(
        &self,
        req: WriteRequest,
    ) -> Result<(WriteRequest, HashMap<Endpoint, WriteRequest>)> {
        // Split write request into multiple requests, each request contains table
        // belong to one remote engine.
        let tables = req
            .table_requests
            .iter()
            .map(|table_request| table_request.table.clone())
            .collect();

        // TODO: Make the router can accept an iterator over the tables to avoid the
        // memory allocation here.
        let req_pb = RouteRequestPb {
            context: req.context.clone(),
            tables,
        };
        let request = RouteRequest::new(req_pb, true);
        let route_data = self.router.route(request).await?;

        let forwarded_table_routes = route_data
            .into_iter()
            .filter_map(|router| {
                router
                    .endpoint
                    .map(|endpoint| (router.table, endpoint.into()))
            })
            .filter(|router| !self.forwarder.is_local_endpoint(&router.1))
            .collect::<HashMap<_, _>>();

        // No table need to be forwarded.
        if forwarded_table_routes.is_empty() {
            return Ok((req, HashMap::default()));
        }

        let mut table_requests_to_local = WriteRequest {
            table_requests: Vec::with_capacity(max(
                req.table_requests.len() - forwarded_table_routes.len(),
                0,
            )),
            context: req.context.clone(),
        };

        let mut table_requests_to_forward = HashMap::with_capacity(forwarded_table_routes.len());

        let write_context = req.context;
        for table_request in req.table_requests {
            let route = forwarded_table_routes.get(&table_request.table);
            match route {
                Some(endpoint) => {
                    let table_requests = table_requests_to_forward
                        .entry(endpoint.clone())
                        .or_insert_with(|| WriteRequest {
                            context: write_context.clone(),
                            table_requests: Vec::new(),
                        });
                    table_requests.table_requests.push(table_request);
                }
                _ => {
                    table_requests_to_local.table_requests.push(table_request);
                }
            }
        }
        Ok((table_requests_to_local, table_requests_to_forward))
    }

    async fn collect_write_to_remote_future(
        &self,
        futures: &mut WriteResponseFutures<'_>,
        ctx: Context,
        write_request: HashMap<Endpoint, WriteRequest>,
    ) {
        for (endpoint, table_write_request) in write_request {
            let forwarder = self.forwarder.clone();
            let ctx = ctx.clone();
            let write_handle = self.engine_runtimes.io_runtime.spawn(async move {
                Self::write_to_remote(ctx, forwarder, endpoint, table_write_request).await
            });

            futures.push(write_handle.boxed());
        }
    }

    #[inline]
    async fn collect_write_to_local_future<'a>(
        &'a self,
        futures: &mut WriteResponseFutures<'a>,
        ctx: Context,
        write_request: WriteRequest,
    ) {
        if write_request.table_requests.is_empty() {
            return;
        }

        let local_handle = async move { Ok(self.write_to_local(ctx, write_request).await) };
        futures.push(local_handle.boxed());
    }

    async fn collect_write_response(
        &self,
        futures: Vec<BoxFuture<'_, runtime::Result<Result<WriteResponse>>>>,
    ) -> Result<WriteResponse> {
        let mut futures: FuturesUnordered<_> = futures.into_iter().collect();
        let mut success = 0;
        while let Some(resp) = futures.next().await {
            let resp = resp.box_err().context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to join task",
            })?;
            success += resp?.success;
        }

        Ok(WriteResponse {
            success,
            ..Default::default()
        })
    }

    async fn write_to_remote(
        ctx: Context,
        forwarder: ForwarderRef,
        endpoint: Endpoint,
        table_write_request: WriteRequest,
    ) -> Result<WriteResponse> {
        let do_write = |mut client: StorageServiceClient<Channel>,
                        request: tonic::Request<WriteRequest>,
                        _: &Endpoint| {
            let write = async move {
                client
                    .write(request)
                    .await
                    .map(|resp| resp.into_inner())
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Forwarded write request failed",
                    })
            }
            .boxed();

            Box::new(write) as _
        };

        let forward_result = forwarder
            .forward_with_endpoint(
                endpoint,
                tonic::Request::new(table_write_request),
                ctx.forwarded_from,
                ctx.authorization,
                do_write,
            )
            .await;
        let forward_res = forward_result
            .map_err(|e| {
                error!("Failed to forward sql req but the error is ignored, err:{e}");
                e
            })
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Local response is not expected",
            })?;

        match forward_res {
            ForwardResult::Forwarded(resp) => resp.map(|v: WriteResponsePB| WriteResponse {
                success: v.success,
                failed: v.failed,
            }),
            ForwardResult::Local => InternalNoCause {
                msg: "Local response is not expected".to_string(),
            }
            .fail(),
        }
    }

    async fn write_to_local(&self, ctx: Context, req: WriteRequest) -> Result<WriteResponse> {
        let request_id = ctx.request_id;
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);
        let catalog_name = self.instance.catalog_manager.default_catalog_name();
        let req_ctx = req.context.context(ErrNoCause {
            msg: "Missing context",
            code: StatusCode::BAD_REQUEST,
        })?;
        let schema_name = req_ctx.database;

        debug!(
            "Local write begin, catalog:{catalog_name}, schema:{schema_name}, request_id:{request_id}, first_table:{:?}, num_tables:{}",
            req.table_requests
                .first()
                .map(|m| (&m.table, &m.tag_names, &m.field_names)),
            req.table_requests.len(),
        );

        let write_context = WriteContext {
            request_id: request_id.clone(),
            deadline,
            catalog: catalog_name.to_string(),
            schema: schema_name.clone(),
            auto_create_table: self.auto_create_table,
        };

        let plans = self
            .write_request_to_insert_plan(req.table_requests, write_context)
            .await?;

        let mut success = 0;

        // TODO: concurrently run the insert plan here
        for plan_with_table in plans {
            let PlanWithTable { plan, table } = plan_with_table;

            // check limit first
            // TODO: if one table is blocked, maybe should not lead to failure of whole
            // batch?
            self.instance
                .limiter
                .try_limit(&plan)
                .box_err()
                .context(ErrWithCause {
                    code: StatusCode::INTERNAL_SERVER_ERROR,
                    msg: "table is blocked",
                })?;

            match self
                .execute_insert_plan(
                    request_id.clone(),
                    catalog_name,
                    &schema_name,
                    plan,
                    deadline,
                )
                .await
            {
                Ok(n) => {
                    success += n;
                }
                Err(e) => {
                    // TODO: remove this logic.
                    // Refer to https://github.com/apache/incubator-horaedb/issues/1248.
                    if need_evict_partition_table(e.error_message()) {
                        warn!("Evict partition table:{}", table.name());
                        self.evict_partition_table(table, catalog_name, &schema_name)
                            .await;
                    }
                    return Err(e);
                }
            }
        }

        Ok(WriteResponse {
            success: success as u32,
            ..Default::default()
        })
    }

    async fn write_request_to_insert_plan(
        &self,
        table_requests: Vec<WriteTableRequest>,
        write_context: WriteContext,
    ) -> Result<Vec<PlanWithTable>> {
        let mut plans = Vec::with_capacity(table_requests.len());

        let WriteContext {
            request_id,
            catalog,
            schema,
            deadline,
            auto_create_table,
        } = write_context;
        for write_table_req in table_requests {
            let table_name = &write_table_req.table;
            self.maybe_open_partition_table_if_not_exist(&catalog, &schema, table_name)
                .await?;
            let table = self
                .try_get_table(&catalog, &schema, table_name)?
                .with_context(|| ErrNoCause {
                    code: StatusCode::BAD_REQUEST,
                    msg: format!("Table not found, schema:{schema}, table:{table_name}"),
                })?;

            if auto_create_table {
                // The reasons for making the decision to add columns before writing are as
                // follows:
                // * If judged based on the error message returned, it may cause data that has
                //   already been successfully written to be written again and affect the
                //   accuracy of the data.
                // * Currently, the decision to add columns is made at the request level, not at
                //   the row level, so the cost is relatively small.
                let table_schema = table.schema();
                let columns = find_new_columns(&table_schema, &write_table_req)?;
                if !columns.is_empty() {
                    self.execute_add_columns_plan(
                        request_id.clone(),
                        &catalog,
                        &schema,
                        table.clone(),
                        columns,
                        deadline,
                    )
                    .await?;
                }
            }

            let table_clone = table.clone();
            let plan = match write_table_request_to_insert_plan(table, write_table_req) {
                Err(e) => {
                    // TODO: remove this logic.
                    // Refer to https://github.com/apache/incubator-horaedb/issues/1248.
                    if need_evict_partition_table(e.error_message()) {
                        warn!("Evict partition table:{}", table_clone.name());
                        self.evict_partition_table(table_clone, &catalog, &schema)
                            .await;
                    }
                    return Err(e);
                }
                Ok(v) => v,
            };
            let plan = Plan::Insert(plan);
            let plan_with_table = PlanWithTable {
                plan,
                table: table_clone,
            };

            plans.push(plan_with_table);
        }

        Ok(plans)
    }

    async fn execute_insert_plan(
        &self,
        request_id: RequestId,
        catalog_name: &str,
        schema_name: &str,
        plan: Plan,
        deadline: Option<Instant>,
    ) -> Result<usize> {
        let output = self
            .execute_plan(request_id, catalog_name, schema_name, plan, deadline)
            .await;
        output.and_then(|output| match output {
            Output::AffectedRows(n) => Ok(n),
            Output::Records(_) => ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Invalid output type, expect AffectedRows, found Records",
            }
            .fail(),
        })
    }

    fn try_get_table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> Result<Option<TableRef>> {
        self.instance
            .catalog_manager
            .catalog_by_name(catalog)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find catalog, catalog_name:{catalog}"),
            })?
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Catalog not found, catalog_name:{catalog}"),
            })?
            .schema_by_name(schema)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find schema, schema_name:{schema}"),
            })?
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Schema not found, schema_name:{schema}"),
            })?
            .table_by_name(table_name)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find table, table:{table_name}"),
            })
    }

    async fn execute_add_columns_plan(
        &self,
        request_id: RequestId,
        catalog: &str,
        schema: &str,
        table: TableRef,
        columns: Vec<ColumnSchema>,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let table_name = table.name().to_string();
        info!(
            "Add columns start, request_id:{request_id}, table:{table_name}, columns:{columns:?}"
        );

        let plan = Plan::AlterTable(AlterTablePlan {
            table,
            operations: AlterTableOperation::AddColumn(columns),
        });
        let _ = self
            .execute_plan(request_id.clone(), catalog, schema, plan, deadline)
            .await?;

        info!("Add columns success, request_id:{request_id}, table:{table_name}");
        Ok(())
    }

    async fn evict_partition_table(&self, table: TableRef, catalog_name: &str, schema_name: &str) {
        if table.partition_info().is_some() {
            let catalog = self.get_catalog(catalog_name);
            if catalog.is_err() {
                return;
            }
            let schema = self.get_schema(&catalog.unwrap(), schema_name);
            if schema.is_err() {
                return;
            }
            let _ = self
                .drop_partition_table(
                    schema.unwrap(),
                    catalog_name.to_string(),
                    schema_name.to_string(),
                    table.name().to_string(),
                )
                .await
                .map_err(|e| {
                    warn!("Failed to drop partition table, err:{:?}", e);
                    e
                });
        }
    }
}

fn find_new_columns(
    schema: &Schema,
    write_table_req: &WriteTableRequest,
) -> Result<Vec<ColumnSchema>> {
    let WriteTableRequest {
        table,
        field_names,
        tag_names,
        entries: write_entries,
    } = write_table_req;

    let mut columns: HashMap<_, ColumnSchema> = HashMap::new();
    for write_entry in write_entries {
        // Parse tags.
        for tag in &write_entry.tags {
            let name_index = tag.name_index as usize;
            ensure!(
                name_index < tag_names.len(),
                InternalNoCause {
                    msg: format!(
                        "Tag {tag:?} is not found in tag_names:{tag_names:?}, table:{table}",
                    ),
                }
            );

            let tag_name = &tag_names[name_index];
            build_column(&mut columns, schema, tag_name, &tag.value, true)?;
        }

        // Parse fields.
        for field_group in &write_entry.field_groups {
            for field in &field_group.fields {
                let field_index = field.name_index as usize;
                ensure!(
                    field_index < field_names.len(),
                    InternalNoCause {
                        msg: format!(
                        "Field {field:?} is not found in field_names:{field_names:?}, table:{table}",
                    ),
                    }
                );
                let field_name = &field_names[field.name_index as usize];
                build_column(&mut columns, schema, field_name, &field.value, false)?;
            }
        }
    }

    Ok(columns.into_iter().map(|v| v.1).collect())
}

fn build_column<'a>(
    columns: &mut HashMap<&'a str, ColumnSchema>,
    schema: &Schema,
    name: &'a str,
    value: &Option<Value>,
    is_tag: bool,
) -> Result<()> {
    // Skip adding columns, the following cases:
    // 1. Column already exists.
    // 2. The new column has been added.
    if schema.index_of(name).is_some() || columns.get(name).is_some() {
        return Ok(());
    }

    let column_value = value
        .as_ref()
        .with_context(|| InternalNoCause {
            msg: format!("Column value is needed, column:{name}"),
        })?
        .value
        .as_ref()
        .with_context(|| InternalNoCause {
            msg: format!("Column value type is not supported, column:{name}"),
        })?;

    let data_type = try_get_data_type_from_value(column_value)
        .box_err()
        .context(Internal {
            msg: "Failed to get data type",
        })?;

    let column_schema = build_column_schema(name, data_type, is_tag)
        .box_err()
        .context(Internal {
            msg: "Failed to build column schema",
        })?;
    columns.insert(name, column_schema);
    Ok(())
}

fn write_table_request_to_insert_plan(
    table: TableRef,
    write_table_req: WriteTableRequest,
) -> Result<InsertPlan> {
    let schema = table.schema();

    // TODO: pre-allocate the memory for the row vector.
    let mut total_rows = Vec::new();
    for write_entry in write_table_req.entries {
        let mut rows = write_entry_to_rows(
            &write_table_req.table,
            &schema,
            &write_table_req.tag_names,
            &write_table_req.field_names,
            write_entry,
        )?;
        total_rows.append(&mut rows);
    }
    // The row group builder will checks nullable.
    let row_group = RowGroup::try_new(schema, total_rows)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to build row group, table:{}", table.name()),
        })?;
    Ok(InsertPlan {
        table,
        source: InsertSource::Values { row_group },
        default_value_map: BTreeMap::new(),
    })
}

fn write_entry_to_rows(
    table_name: &str,
    schema: &Schema,
    tag_names: &[String],
    field_names: &[String],
    write_series_entry: WriteSeriesEntry,
) -> Result<Vec<Row>> {
    // Init all columns by null.
    let mut rows = vec![
        Row::from_datums(vec![Datum::Null; schema.num_columns()]);
        write_series_entry.field_groups.len()
    ];

    // Fill tsid by default value.
    if let Some(tsid_idx) = schema.index_of_tsid() {
        let kind = &schema.tsid_column().unwrap().data_type;
        let default_datum = Datum::empty(kind);
        for row in &mut rows {
            row[tsid_idx] = default_datum.clone();
        }
    }

    // Fill tags.
    for tag in write_series_entry.tags {
        let name_index = tag.name_index as usize;
        ensure!(
            name_index < tag_names.len(),
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Tag {tag:?} is not found in tag_names:{tag_names:?}, table:{table_name}",
                ),
            }
        );

        let tag_name = &tag_names[name_index];
        let tag_index_in_schema = schema.index_of(tag_name).with_context(|| ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!("Can't find tag({tag_name}) in schema, table:{table_name}"),
        })?;

        let column_schema = schema.column(tag_index_in_schema);
        ensure!(
            column_schema.is_tag,
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Column({tag_name}) is a field rather than a tag, table:{table_name}"),
            }
        );

        let tag_value = tag
            .value
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Tag({tag_name}) value is needed, table:{table_name}"),
            })?
            .value
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Tag({tag_name}) value type is not supported, table_name:{table_name}"
                ),
            })?;
        for row in &mut rows {
            row[tag_index_in_schema] = convert_proto_value_to_datum(
                table_name,
                tag_name,
                tag_value.clone(),
                column_schema.data_type,
            )?;
        }
    }

    // Fill fields.
    let mut field_name_index: HashMap<String, usize> = HashMap::new();
    for (i, field_group) in write_series_entry.field_groups.into_iter().enumerate() {
        // timestamp
        let timestamp_index_in_schema = schema.timestamp_index();
        rows[i][timestamp_index_in_schema] =
            Datum::Timestamp(Timestamp::new(field_group.timestamp));

        for field in field_group.fields {
            if (field.name_index as usize) < field_names.len() {
                let field_name = &field_names[field.name_index as usize];
                let index_in_schema = if field_name_index.contains_key(field_name) {
                    field_name_index.get(field_name).unwrap().to_owned()
                } else {
                    let index_in_schema =
                        schema.index_of(field_name).with_context(|| ErrNoCause {
                            code: StatusCode::BAD_REQUEST,
                            msg: format!(
                                "Can't find field in schema, table:{table_name}, field_name:{field_name}"
                            ),
                        })?;
                    field_name_index.insert(field_name.to_string(), index_in_schema);
                    index_in_schema
                };
                let column_schema = schema.column(index_in_schema);
                ensure!(
                    !column_schema.is_tag,
                    ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!(
                            "Column {field_name} is a tag rather than a field, table:{table_name}"
                        )
                    }
                );
                let field_value = field
                    .value
                    .with_context(|| ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!("Field({field_name}) is needed, table:{table_name}"),
                    })?
                    .value
                    .with_context(|| ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!(
                            "Field({field_name}) value type is not supported, table:{table_name}"
                        ),
                    })?;

                rows[i][index_in_schema] = convert_proto_value_to_datum(
                    table_name,
                    field_name,
                    field_value,
                    column_schema.data_type,
                )?;
            }
        }
    }

    Ok(rows)
}

/// Convert the `Value_oneof_value` defined in protos into the datum.
fn convert_proto_value_to_datum(
    table_name: &str,
    name: &str,
    value: value::Value,
    data_type: DatumKind,
) -> Result<Datum> {
    match (value, data_type) {
        (value::Value::Float64Value(v), DatumKind::Double) => Ok(Datum::Double(v)),
        (value::Value::StringValue(v), DatumKind::String) => Ok(Datum::String(v.into())),
        (value::Value::Int64Value(v), DatumKind::Int64) => Ok(Datum::Int64(v)),
        (value::Value::Float32Value(v), DatumKind::Float) => Ok(Datum::Float(v)),
        (value::Value::Int32Value(v), DatumKind::Int32) => Ok(Datum::Int32(v)),
        (value::Value::Int16Value(v), DatumKind::Int16) => Ok(Datum::Int16(v as i16)),
        (value::Value::Int8Value(v), DatumKind::Int8) => Ok(Datum::Int8(v as i8)),
        (value::Value::BoolValue(v), DatumKind::Boolean) => Ok(Datum::Boolean(v)),
        (value::Value::Uint64Value(v), DatumKind::UInt64) => Ok(Datum::UInt64(v)),
        (value::Value::Uint32Value(v), DatumKind::UInt32) => Ok(Datum::UInt32(v)),
        (value::Value::Uint16Value(v), DatumKind::UInt16) => Ok(Datum::UInt16(v as u16)),
        (value::Value::Uint8Value(v), DatumKind::UInt8) => Ok(Datum::UInt8(v as u8)),
        (value::Value::TimestampValue(v), DatumKind::Timestamp) => Ok(Datum::Timestamp(Timestamp::new(v))),
        (value::Value::VarbinaryValue(v), DatumKind::Varbinary) => Ok(Datum::Varbinary(Bytes::from(v))),
        (v, _) => ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!(
                "Value type is not same, table:{table_name}, value_name:{name}, schema_type:{data_type:?}, actual_value:{v:?}"
            ),
        }
            .fail(),
    }
}

fn need_evict_partition_table(msg: String) -> bool {
    msg.contains("decode row group payload")
        || msg.contains("Can't find field")
        || msg.contains("Can't find tag")
}

#[cfg(test)]
mod test {
    use common_types::{
        column_schema::{self},
        datum::{Datum, DatumKind},
        row::Row,
        schema::Builder,
        time::Timestamp,
    };
    use horaedbproto::storage::{value, Field, FieldGroup, Tag, Value, WriteSeriesEntry};
    use system_catalog::sys_catalog_table::TIMESTAMP_COLUMN_NAME;

    use super::*;

    const NAME_COL1: &str = "col1";
    const NAME_NEW_COL1: &str = "new_col1";
    const NAME_COL2: &str = "col2";
    const NAME_COL3: &str = "col3";
    const NAME_COL4: &str = "col4";
    const NAME_COL5: &str = "col5";

    #[test]
    fn test_write_entry_to_row_group() {
        let (schema, tag_names, field_names, write_entry) = generate_write_entry();
        let rows =
            write_entry_to_rows("test_table", &schema, &tag_names, &field_names, write_entry)
                .unwrap();
        let row0 = vec![
            Datum::Timestamp(Timestamp::new(1000)),
            Datum::String(NAME_COL1.into()),
            Datum::String(NAME_COL2.into()),
            Datum::Int64(100),
            Datum::Null,
        ];
        let row1 = vec![
            Datum::Timestamp(Timestamp::new(2000)),
            Datum::String(NAME_COL1.into()),
            Datum::String(NAME_COL2.into()),
            Datum::Null,
            Datum::Int64(10),
        ];
        let row2 = vec![
            Datum::Timestamp(Timestamp::new(3000)),
            Datum::String(NAME_COL1.into()),
            Datum::String(NAME_COL2.into()),
            Datum::Null,
            Datum::Int64(10),
        ];

        let expect_rows = vec![
            Row::from_datums(row0),
            Row::from_datums(row1),
            Row::from_datums(row2),
        ];
        assert_eq!(rows, expect_rows);
    }

    #[test]
    fn test_find_new_columns() {
        let write_table_request = generate_write_table_request();
        let schema = build_schema();
        let new_columns = find_new_columns(&schema, &write_table_request)
            .unwrap()
            .into_iter()
            .map(|v| (v.name.clone(), v))
            .collect::<HashMap<_, _>>();

        assert_eq!(new_columns.len(), 2);
        assert!(new_columns.get(NAME_NEW_COL1).is_some());
        assert!(new_columns.get(NAME_NEW_COL1).unwrap().is_tag);
        assert!(new_columns.get(NAME_COL5).is_some());
        assert!(!new_columns.get(NAME_COL5).unwrap().is_tag);
    }

    fn build_schema() -> Schema {
        Builder::new()
            .auto_increment_column_id(true)
            .add_key_column(
                column_schema::Builder::new(
                    TIMESTAMP_COLUMN_NAME.to_string(),
                    DatumKind::Timestamp,
                )
                .build()
                .unwrap(),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new(NAME_COL1.to_string(), DatumKind::String)
                    .is_tag(true)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .add_key_column(
                column_schema::Builder::new(NAME_COL2.to_string(), DatumKind::String)
                    .is_tag(true)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new(NAME_COL3.to_string(), DatumKind::Int64)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .add_normal_column(
                column_schema::Builder::new(NAME_COL4.to_string(), DatumKind::Int64)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .primary_key_indexes(vec![0, 1, 2])
            .build()
            .unwrap()
    }

    fn make_tag(name_index: u32, val: &str) -> Tag {
        Tag {
            name_index,
            value: Some(Value {
                value: Some(value::Value::StringValue(val.to_string())),
            }),
        }
    }

    fn make_field(name_index: u32, val: value::Value) -> Field {
        Field {
            name_index,
            value: Some(Value { value: Some(val) }),
        }
    }

    // tag_names field_names write_entry
    fn generate_write_entry() -> (Schema, Vec<String>, Vec<String>, WriteSeriesEntry) {
        let tag_names = vec![NAME_COL1.to_string(), NAME_COL2.to_string()];
        let field_names = vec![NAME_COL3.to_string(), NAME_COL4.to_string()];

        let tag = make_tag(0, NAME_COL1);
        let tag1 = make_tag(1, NAME_COL2);
        let tags = vec![tag, tag1];

        let field = make_field(0, value::Value::Int64Value(100));
        let field1 = make_field(1, value::Value::Int64Value(10));

        let field_group = FieldGroup {
            timestamp: 1000,
            fields: vec![field],
        };
        let field_group1 = FieldGroup {
            timestamp: 2000,
            fields: vec![field1.clone()],
        };
        let field_group2 = FieldGroup {
            timestamp: 3000,
            fields: vec![field1],
        };

        let write_entry = WriteSeriesEntry {
            tags,
            field_groups: vec![field_group, field_group1, field_group2],
        };

        let schema = build_schema();
        (schema, tag_names, field_names, write_entry)
    }

    fn generate_write_table_request() -> WriteTableRequest {
        let tag1 = make_tag(0, NAME_NEW_COL1);
        let tag2 = make_tag(1, NAME_COL1);
        let tags = vec![tag1, tag2];

        let field1 = make_field(0, value::Value::Int64Value(100));
        let field2 = make_field(1, value::Value::Int64Value(10));

        let field_group1 = FieldGroup {
            timestamp: 1000,
            fields: vec![field1.clone(), field2.clone()],
        };
        let field_group2 = FieldGroup {
            timestamp: 2000,
            fields: vec![field1],
        };
        let field_group3 = FieldGroup {
            timestamp: 3000,
            fields: vec![field2],
        };

        let write_entry = WriteSeriesEntry {
            tags,
            field_groups: vec![field_group1, field_group2, field_group3],
        };

        let tag_names = vec![NAME_NEW_COL1.to_string(), NAME_COL1.to_string()];
        let field_names = vec![NAME_COL3.to_string(), NAME_COL5.to_string()];

        WriteTableRequest {
            table: "test".to_string(),
            tag_names,
            field_names,
            entries: vec![write_entry],
        }
    }
}
