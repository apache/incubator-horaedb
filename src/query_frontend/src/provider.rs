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

//! Adapter to providers in datafusion

use std::{any::Any, borrow::Cow, cell::RefCell, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use catalog::manager::ManagerRef;
use datafusion::{
    catalog::{schema::SchemaProvider, CatalogProvider},
    common::DataFusionError,
    config::ConfigOptions,
    datasource::{DefaultTableSource, TableProvider},
    logical_expr::TableSource,
    physical_plan::{udaf::AggregateUDF, udf::ScalarUDF},
    sql::planner::ContextProvider,
};
use df_operator::{registry::FunctionRegistry, scalar::ScalarUdf, udaf::AggregateUdf};
use macros::define_result;
use snafu::{OptionExt, ResultExt, Snafu};
use table_engine::table::TableRef;

use crate::{
    config::DynamicConfig,
    container::{PlannedTable, TableContainer, TableReference},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to find catalog, name:{}, err:{}", name, source))]
    FindCatalog {
        name: String,
        source: catalog::manager::Error,
    },

    #[snafu(display("Failed to find schema, name:{}, err:{}", name, source))]
    FindSchema {
        name: String,
        source: catalog::Error,
    },

    #[snafu(display("Failed to find catalog, name:{}", name))]
    CatalogNotFound { name: String },

    #[snafu(display("Failed to find schema, name:{}", name))]
    SchemaNotFound { name: String },

    #[snafu(display("Failed to find table, name:{}, err:{}", name, source))]
    FindTable {
        name: String,
        source: Box<catalog::schema::Error>,
    },

    #[snafu(display(
        "Failed to get all tables, catalog_name:{}, schema_name:{}, err:{}",
        catalog_name,
        schema_name,
        source
    ))]
    GetAllTables {
        catalog_name: String,
        schema_name: String,
        source: catalog::schema::Error,
    },

    #[snafu(display("Failed to find udf, err:{}", source))]
    FindUdf {
        source: df_operator::registry::Error,
    },
}

define_result!(Error);

/// Table with catalog and schema
// TODO: we should enable to get catalog and schema from table
// (for example, store catalog id and schema id in table).
// See issue: https://github.com/apache/incubator-horaedb/issues/1157
#[derive(Debug, Clone)]
pub struct ResolvedTable {
    pub catalog: String,
    pub schema: String,
    pub table: TableRef,
}

/// MetaProvider provides meta info needed by Frontend
pub trait MetaProvider {
    /// Default catalog name
    fn default_catalog_name(&self) -> &str;

    /// Default schema name
    fn default_schema_name(&self) -> &str;

    /// Get table meta by table reference
    ///
    /// Note that this function may block current thread. We can't make this
    /// function async as the underlying (aka. datafusion) planner needs a
    /// sync provider.
    fn table(&self, name: TableReference) -> Result<Option<ResolvedTable>>;

    /// Get udf by name.
    fn scalar_udf(&self, name: &str) -> Result<Option<ScalarUdf>>;

    /// Get udaf by name.
    fn aggregate_udf(&self, name: &str) -> Result<Option<AggregateUdf>>;

    /// Return all tables.
    ///
    /// Note that it may incur expensive cost.
    /// Now it is used in `table_names` method in `SchemaProvider`(introduced by
    /// influxql).
    fn all_tables(&self) -> Result<Vec<TableRef>>;
}

/// We use an adapter instead of using [catalog::Manager] directly, because
/// - MetaProvider provides blocking method, but catalog::Manager may provide
/// async method
/// - Other meta data like default catalog and schema are needed
// TODO(yingwen): Maybe support schema searching instead of using a fixed
// default schema
pub struct CatalogMetaProvider<'a> {
    pub manager: ManagerRef,
    pub default_catalog: &'a str,
    pub default_schema: &'a str,
    pub function_registry: &'a (dyn FunctionRegistry + Send + Sync),
}

impl<'a> MetaProvider for CatalogMetaProvider<'a> {
    fn default_catalog_name(&self) -> &str {
        self.default_catalog
    }

    fn default_schema_name(&self) -> &str {
        self.default_schema
    }

    fn table(&self, name: TableReference) -> Result<Option<ResolvedTable>> {
        let resolved = name.resolve(self.default_catalog, self.default_schema);

        let catalog = match self
            .manager
            .catalog_by_name(resolved.catalog.as_ref())
            .with_context(|| FindCatalog {
                name: resolved.catalog.to_string(),
            })? {
            Some(c) => c,
            None => return Ok(None),
        };

        let schema = match catalog
            .schema_by_name(resolved.schema.as_ref())
            .context(FindSchema {
                name: resolved.schema.to_string(),
            })? {
            Some(s) => s,
            None => {
                return SchemaNotFound {
                    name: resolved.schema.to_string(),
                }
                .fail();
            }
        };

        let table = schema
            .table_by_name(resolved.table.as_ref())
            .map_err(Box::new)
            .with_context(|| FindTable {
                name: resolved.table.to_string(),
            })?;

        Ok(table.map(|t| ResolvedTable {
            catalog: resolved.catalog.to_string(),
            schema: resolved.schema.to_string(),
            table: t,
        }))
    }

    fn scalar_udf(&self, name: &str) -> Result<Option<ScalarUdf>> {
        self.function_registry.find_udf(name).context(FindUdf)
    }

    fn aggregate_udf(&self, name: &str) -> Result<Option<AggregateUdf>> {
        self.function_registry.find_udaf(name).context(FindUdf)
    }

    // TODO: after supporting not only default catalog and schema, we should
    // refactor the tables collecting procedure.
    fn all_tables(&self) -> Result<Vec<TableRef>> {
        let catalog = self
            .manager
            .catalog_by_name(self.default_catalog)
            .with_context(|| FindCatalog {
                name: self.default_catalog,
            })?
            .with_context(|| CatalogNotFound {
                name: self.default_catalog,
            })?;

        let schema = catalog
            .schema_by_name(self.default_schema)
            .context(FindSchema {
                name: self.default_schema,
            })?
            .with_context(|| SchemaNotFound {
                name: self.default_schema,
            })?;

        schema.all_tables().with_context(|| GetAllTables {
            catalog_name: self.default_catalog,
            schema_name: self.default_schema,
        })
    }
}

/// An adapter to ContextProvider, not thread safe
pub struct ContextProviderAdapter<'a, P> {
    /// Local cache for TableProvider to avoid create multiple adapter for the
    /// same table, also save all the table needed during planning
    table_cache: RefCell<TableContainer>,
    /// Store the first error MetaProvider returns
    err: RefCell<Option<Error>>,
    meta_provider: &'a P,
    /// Read config for each table.
    config: ConfigOptions,
    /// Hint for logical plan creation.
    dyn_config: &'a DynamicConfig,
}

impl<'a, P: MetaProvider> ContextProviderAdapter<'a, P> {
    /// Create a adapter from meta provider
    // TODO: `read_parallelism` here seems useless, `ContextProviderAdapter` is only
    // used during creating `LogicalPlan`, and `read_parallelism` is used when
    // creating `PhysicalPlan`.
    pub fn new(
        meta_provider: &'a P,
        read_parallelism: usize,
        dyn_config: &'a DynamicConfig,
    ) -> Self {
        let default_catalog = meta_provider.default_catalog_name().to_string();
        let default_schema = meta_provider.default_schema_name().to_string();
        let mut config = ConfigOptions::default();
        config.execution.target_partitions = read_parallelism;

        Self {
            table_cache: RefCell::new(TableContainer::new(default_catalog, default_schema)),
            err: RefCell::new(None),
            meta_provider,
            config,
            dyn_config,
        }
    }

    pub fn clone_with_cleared_cache(&self) -> Self {
        let default_catalog = self.meta_provider.default_catalog_name().to_string();
        let default_schema = self.meta_provider.default_schema_name().to_string();

        Self {
            table_cache: RefCell::new(TableContainer::new(default_catalog, default_schema)),
            err: RefCell::new(None),
            meta_provider: self.meta_provider,
            config: self.config.clone(),
            dyn_config: self.dyn_config,
        }
    }

    /// Consumes the adapter, returning the tables used during planning if no
    /// error occurs, otherwise returning the error
    pub fn try_into_container(self) -> Result<TableContainer> {
        if let Some(e) = self.err.into_inner() {
            return Err(e);
        }

        Ok(self.table_cache.into_inner())
    }

    /// Save error if there is no existing error.
    ///
    /// The datafusion's ContextProvider can't return error, so here we save the
    /// error in the adapter and return None, also let datafusion
    /// return a provider not found error and abort the planning
    /// procedure.
    fn maybe_set_err(&self, err: Error) {
        if self.err.borrow().is_none() {
            *self.err.borrow_mut() = Some(err);
        }
    }

    pub fn table_source(&self, planned_table: PlannedTable) -> Arc<(dyn TableSource + 'static)> {
        let table_provider = planned_table.into_table_provider();

        Arc::new(DefaultTableSource { table_provider })
    }
}

impl<'a, P: MetaProvider> MetaProvider for ContextProviderAdapter<'a, P> {
    fn default_catalog_name(&self) -> &str {
        self.meta_provider.default_catalog_name()
    }

    fn default_schema_name(&self) -> &str {
        self.meta_provider.default_schema_name()
    }

    fn table(&self, name: TableReference) -> Result<Option<ResolvedTable>> {
        self.meta_provider.table(name)
    }

    fn scalar_udf(&self, name: &str) -> Result<Option<ScalarUdf>> {
        self.meta_provider.scalar_udf(name)
    }

    fn aggregate_udf(&self, name: &str) -> Result<Option<AggregateUdf>> {
        self.meta_provider.aggregate_udf(name)
    }

    fn all_tables(&self) -> Result<Vec<TableRef>> {
        self.meta_provider.all_tables()
    }
}

impl<'a, P: MetaProvider> ContextProvider for ContextProviderAdapter<'a, P> {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> std::result::Result<Arc<(dyn TableSource + 'static)>, DataFusionError> {
        // Find in local cache
        if let Some(planned) = self.table_cache.borrow().get(name.clone()) {
            return Ok(self.table_source(planned));
        }

        // Find in meta provider
        // TODO: possible to remove this clone?
        match self.meta_provider.table(name.clone()) {
            Ok(Some(resolved)) => {
                let ResolvedTable {
                    catalog,
                    schema,
                    table,
                } = resolved;

                let enable_dist_query_push_down = self
                    .dyn_config
                    .enable_dist_query_push_down
                    .load(std::sync::atomic::Ordering::Relaxed);

                let planned_table = PlannedTable {
                    catalog,
                    schema,
                    table,
                    enable_dist_query_push_down,
                };

                self.table_cache
                    .borrow_mut()
                    .insert(name, planned_table.clone());
                Ok(self.table_source(planned_table))
            }
            Ok(None) => Err(DataFusionError::Execution(format!(
                "Table is not found, {:?}",
                format_table_reference(name),
            ))),
            Err(e) => {
                let err_msg = format!(
                    "fail to find table, {:?}, err:{}",
                    format_table_reference(name),
                    e
                );
                self.maybe_set_err(e);
                Err(DataFusionError::Execution(err_msg))
            }
        }
    }

    // ScalarUDF is not supported now
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        // We don't cache udf used by the query because now we will register all udf to
        // datafusion's context.
        match self.meta_provider.scalar_udf(name) {
            Ok(Some(udf)) => Some(udf.to_datafusion_udf()),
            Ok(None) => None,
            Err(e) => {
                self.maybe_set_err(e);
                None
            }
        }
    }

    // AggregateUDF is not supported now
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        match self.meta_provider.aggregate_udf(name) {
            Ok(Some(udaf)) => Some(udaf.to_datafusion_udaf()),
            Ok(None) => None,
            Err(e) => {
                self.maybe_set_err(e);
                None
            }
        }
    }

    // TODO: Variable Type is not supported now
    fn get_variable_type(
        &self,
        _variable_names: &[String],
    ) -> Option<common_types::schema::DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.config
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<datafusion::logical_expr::WindowUDF>> {
        None
    }
}

struct SchemaProviderAdapter {
    catalog: String,
    schema: String,
    tables: Arc<TableContainer>,
}

#[async_trait]
impl SchemaProvider for SchemaProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        let _ = self.tables.visit::<_, ()>(|name, table| {
            if name.catalog == self.catalog && name.schema == self.schema {
                names.push(table.name().to_string());
            }
            Ok(())
        });
        names
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let name_ref = TableReference::Full {
            catalog: Cow::from(&self.catalog),
            schema: Cow::from(&self.schema),
            table: Cow::from(name),
        };

        self.tables
            .get(name_ref)
            .map(|resolved| resolved.into_table_provider())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.get(TableReference::parse_str(name)).is_some()
    }
}

#[derive(Default)]
pub struct CatalogProviderAdapter {
    schemas: HashMap<String, Arc<SchemaProviderAdapter>>,
}

impl CatalogProviderAdapter {
    pub fn new_adapters(tables: Arc<TableContainer>) -> HashMap<String, CatalogProviderAdapter> {
        let mut catalog_adapters = HashMap::with_capacity(tables.num_catalogs());
        let _ = tables.visit::<_, ()>(|name, _| {
            // Get or create catalog
            let catalog = match catalog_adapters.get_mut(name.catalog.as_ref()) {
                Some(v) => v,
                None => catalog_adapters
                    .entry(name.catalog.to_string())
                    .or_insert_with(CatalogProviderAdapter::default),
            };
            // Get or create schema
            if catalog.schemas.get(name.schema.as_ref()).is_none() {
                catalog.schemas.insert(
                    name.schema.to_string(),
                    Arc::new(SchemaProviderAdapter {
                        catalog: name.catalog.to_string(),
                        schema: name.schema.to_string(),
                        tables: tables.clone(),
                    }),
                );
            }

            Ok(())
        });

        catalog_adapters
    }
}

impl CatalogProvider for CatalogProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .cloned()
            .map(|v| v as Arc<dyn SchemaProvider>)
    }
}

/// Provide the description string for [`TableReference`] which hasn't derive
/// [`Debug`] or implement [`std::fmt::Display`].
fn format_table_reference(table_ref: TableReference) -> String {
    match table_ref {
        TableReference::Bare { table } => format!("table:{table}"),
        TableReference::Partial { schema, table } => format!("schema:{schema}, table:{table}"),
        TableReference::Full {
            catalog,
            schema,
            table,
        } => format!("catalog:{catalog}, schema:{schema}, table:{table}"),
    }
}

#[cfg(test)]
mod test {
    use crate::{config::DynamicConfig, provider::ContextProviderAdapter, tests::MockMetaProvider};

    #[test]
    fn test_config_options_setting() {
        let provider = MockMetaProvider::default();
        let read_parallelism = 100;
        let dyn_config = DynamicConfig::default();
        let context = ContextProviderAdapter::new(&provider, read_parallelism, &dyn_config);
        assert_eq!(context.config.execution.target_partitions, read_parallelism);
    }
}
