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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::{TableProvider, ViewTable};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use futures::future::try_join_all;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::inspect::MetadataTableType;
use iceberg::spec::{SqlViewRepresentation, ViewRepresentation};
use iceberg::view::View;
use iceberg::{
    Catalog, Error, ErrorKind, NamespaceIdent, Result, Runtime, TableCreation, TableIdent,
};

use crate::catalog::IcebergCatalogProvider;
use crate::table::IcebergTableProvider;
use crate::to_datafusion_error;

/// Represents a [`SchemaProvider`] for the Iceberg [`Catalog`], managing
/// access to table providers within a specific namespace.
#[derive(Debug)]
pub(crate) struct IcebergSchemaProvider {
    /// Reference to the Iceberg catalog
    catalog: Arc<dyn Catalog>,
    /// The namespace this schema represents
    namespace: NamespaceIdent,
    /// A concurrent map where keys are table names
    /// and values are dynamic references to objects implementing the
    /// [`TableProvider`] trait.
    /// Wrapped in Arc to allow sharing across async boundaries in register_table.
    tables: Arc<DashMap<String, Arc<IcebergTableProvider>>>,
    /// A concurrent map where keys are known view names in this namespace.
    view_names: Arc<DashMap<String, ()>>,
    /// A concurrent map where keys are view names and values are lazily planned view providers.
    views: Arc<DashMap<String, Arc<dyn TableProvider>>>,
    /// Propagated to every [`IcebergTableProvider`] created by this provider.
    runtime: Option<Runtime>,
}

impl IcebergSchemaProvider {
    /// Asynchronously tries to construct a new [`IcebergSchemaProvider`]
    /// using the given client to fetch and initialize table providers for
    /// the provided namespace in the Iceberg [`Catalog`].
    pub(crate) async fn try_new(
        client: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
    ) -> Result<Self> {
        Self::try_new_with_runtime(client, namespace, None).await
    }

    /// Like [`Self::try_new`], propagating `runtime` to every child table provider.
    ///
    /// `runtime` is propagated to every [`IcebergTableProvider`] created by
    /// this schema provider.
    pub(crate) async fn try_new_with_runtime(
        client: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        runtime: Option<Runtime>,
    ) -> Result<Self> {
        // TODO:
        // Tables and providers should be cached based on table_name
        // if we have a cache miss; we update our internal cache & check again
        // As of right now; tables might become stale.
        let table_names: Vec<_> = client
            .list_tables(&namespace)
            .await?
            .iter()
            .map(|tbl| tbl.name().to_string())
            .collect();
        let view_names: Vec<_> = match client.list_views(&namespace).await {
            Ok(views) => views.iter().map(|view| view.name().to_string()).collect(),
            Err(err) if err.kind() == ErrorKind::FeatureUnsupported => Vec::new(),
            Err(err) => return Err(err),
        };

        let providers = try_join_all(
            table_names
                .iter()
                .map(|name| {
                    IcebergTableProvider::try_new_with_runtime(
                        client.clone(),
                        namespace.clone(),
                        name,
                        runtime.clone(),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await?;

        let tables = Arc::new(DashMap::new());
        for (name, provider) in table_names.into_iter().zip(providers.into_iter()) {
            tables.insert(name, Arc::new(provider));
        }
        let view_names = Arc::new(
            view_names
                .into_iter()
                .map(|name| (name, ()))
                .collect::<DashMap<_, _>>(),
        );

        Ok(IcebergSchemaProvider {
            catalog: client,
            namespace,
            tables,
            view_names,
            views: Arc::new(DashMap::new()),
            runtime,
        })
    }

    async fn load_view_provider(&self, name: &str) -> DFResult<Arc<dyn TableProvider>> {
        let view_ident = TableIdent::new(self.namespace.clone(), name.to_string());
        let view = self
            .catalog
            .load_view(&view_ident)
            .await
            .map_err(to_datafusion_error)?;
        let sql = select_view_sql(&view).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Iceberg view {} has no SQL representation",
                view.identifier()
            ))
        })?;
        let sql_definition = sql.sql.clone();
        let version = view.current_version();
        let default_catalog = version
            .default_catalog()
            .cloned()
            .unwrap_or_else(|| "datafusion".to_string());
        let default_schema = version.default_namespace().to_string();
        let session_config = SessionConfig::new()
            .with_default_catalog_and_schema(default_catalog.clone(), default_schema)
            .with_create_default_catalog_and_schema(false);
        let session_ctx = SessionContext::new_with_config(session_config);
        let catalog_provider = IcebergCatalogProvider::try_new_with_runtime(
            self.catalog.clone(),
            self.runtime.clone(),
        )
        .await
        .map_err(to_datafusion_error)?;

        session_ctx.register_catalog(default_catalog, Arc::new(catalog_provider));

        let plan = session_ctx
            .state()
            .create_logical_plan(&sql_definition)
            .await?;
        let provider =
            Arc::new(ViewTable::new(plan, Some(sql_definition))) as Arc<dyn TableProvider>;

        Ok(provider)
    }
}

fn select_view_sql(view: &View) -> Option<&SqlViewRepresentation> {
    view.sql_for("datafusion")
        .or_else(|| view.sql_for("spark"))
        .or_else(|| view.sql_for("ansi"))
        .or_else(|| {
            view.current_version()
                .representations()
                .iter()
                .map(|repr| match repr {
                    ViewRepresentation::Sql(sql) => sql,
                })
                .next()
        })
}

#[async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = self
            .tables
            .iter()
            .flat_map(|entry| {
                let table_name = entry.key().clone();
                [table_name.clone()]
                    .into_iter()
                    .chain(
                        MetadataTableType::all_types().map(move |metadata_table_name| {
                            format!("{}${}", table_name, metadata_table_name.as_str())
                        }),
                    )
            })
            .collect::<Vec<_>>();
        names.extend(self.view_names.iter().map(|entry| entry.key().clone()));
        names.sort();
        names.dedup();
        names
    }

    fn table_exist(&self, name: &str) -> bool {
        if let Some((table_name, metadata_table_name)) = name.split_once('$') {
            self.tables.contains_key(table_name)
                && MetadataTableType::try_from(metadata_table_name).is_ok()
        } else {
            self.tables.contains_key(name)
                || self.view_names.contains_key(name)
                || self.views.contains_key(name)
        }
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if let Some((table_name, metadata_table_name)) = name.split_once('$') {
            let metadata_table_type =
                MetadataTableType::try_from(metadata_table_name).map_err(DataFusionError::Plan)?;
            if let Some(table) = self.tables.get(table_name) {
                let metadata_table = table
                    .metadata_table(metadata_table_type)
                    .await
                    .map_err(to_datafusion_error)?;
                return Ok(Some(Arc::new(metadata_table)));
            } else {
                return Ok(None);
            }
        }

        if let Some(table) = self.tables.get(name) {
            return Ok(Some(table.value().clone() as Arc<dyn TableProvider>));
        }

        if let Some(view) = self.views.get(name) {
            return Ok(Some(view.value().clone()));
        }

        if self.view_names.contains_key(name) {
            let view = self.load_view_provider(name).await?;
            self.views.insert(name.to_string(), view.clone());
            return Ok(Some(view));
        }

        Ok(None)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Check if table already exists
        if self.table_exist(name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "Table {name} already exists"
            )));
        }

        // Convert DataFusion schema to Iceberg schema
        // DataFusion schemas don't have field IDs, so we use the function that assigns them automatically
        let df_schema = table.schema();
        let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(df_schema.as_ref())
            .map_err(to_datafusion_error)?;

        // Create the table in the Iceberg catalog
        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .build();

        let catalog = self.catalog.clone();
        let namespace = self.namespace.clone();
        let tables = self.tables.clone();
        let name_clone = name.clone();
        let runtime = self.runtime.clone();

        // Use tokio's spawn_blocking to handle the async work on a blocking thread pool
        let result = tokio::task::spawn_blocking(move || {
            // Create a new runtime handle to execute the async work
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move {
                // Verify the input table is empty - CREATE TABLE only accepts schema definition
                ensure_table_is_empty(&table)
                    .await
                    .map_err(to_datafusion_error)?;

                catalog
                    .create_table(&namespace, table_creation)
                    .await
                    .map_err(to_datafusion_error)?;

                // Create a new table provider using the catalog reference
                let table_provider = IcebergTableProvider::try_new_with_runtime(
                    catalog.clone(),
                    namespace.clone(),
                    name_clone.clone(),
                    runtime,
                )
                .await
                .map_err(to_datafusion_error)?;

                // Store the new table provider
                tables.insert(name_clone, Arc::new(table_provider));

                Ok(None)
            })
        });

        // Block on the spawned task to get the result
        // This is safe because spawn_blocking moves the blocking to a dedicated thread pool
        futures::executor::block_on(result).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create Iceberg table: {e}"))
        })?
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if !self.tables.contains_key(name) {
            if self.view_names.contains_key(name) || self.views.contains_key(name) {
                return Err(DataFusionError::Execution(format!(
                    "Dropping Iceberg view {name} is not supported"
                )));
            }
            return Ok(None);
        }

        let catalog = self.catalog.clone();
        let namespace = self.namespace.clone();
        let tables = self.tables.clone();
        let table_name = name.to_string();

        // Use tokio's spawn_blocking to handle the async work on a blocking thread pool
        let result = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move {
                let table_ident = TableIdent::new(namespace, table_name.clone());

                // Drop the table from the Iceberg catalog
                catalog
                    .drop_table(&table_ident)
                    .await
                    .map_err(to_datafusion_error)?;

                // Remove from local cache and return the removed provider
                let removed = tables
                    .remove(&table_name)
                    .map(|(_, table)| table as Arc<dyn TableProvider>);

                Ok(removed)
            })
        });

        futures::executor::block_on(result)
            .map_err(|e| DataFusionError::Execution(format!("Failed to drop Iceberg table: {e}")))?
    }
}

/// Verifies that a table provider contains no data by scanning with LIMIT 1.
/// Returns an error if the table has any rows.
async fn ensure_table_is_empty(table: &Arc<dyn TableProvider>) -> Result<()> {
    let session_ctx = SessionContext::new();
    let exec_plan = table
        .scan(&session_ctx.state(), None, &[], Some(1))
        .await
        .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to scan table: {e}")))?;

    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            format!("Failed to execute scan: {e}"),
        )
    })?;

    let batches: Vec<_> = stream.collect().await;
    let has_data = batches
        .into_iter()
        .filter_map(|r| r.ok())
        .any(|batch| batch.num_rows() > 0);

    if has_data {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "register_table does not support tables with data.",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::TableType;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{SqlViewRepresentation, ViewRepresentation, ViewRepresentations};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, ViewCreation};
    use tempfile::TempDir;

    use super::*;

    async fn create_test_schema_provider() -> (IcebergSchemaProvider, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let provider =
            IcebergSchemaProvider::try_new_with_runtime(Arc::new(catalog), namespace, None)
                .await
                .unwrap();

        (provider, temp_dir)
    }

    async fn create_test_schema_provider_with_view() -> (IcebergSchemaProvider, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let arrow_schema = ArrowSchema::new(vec![Field::new("foo", DataType::Int32, false)]);
        let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&arrow_schema).unwrap();

        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("tbl1".to_string())
                    .schema(iceberg_schema.clone())
                    .build(),
            )
            .await
            .unwrap();

        catalog
            .create_view(
                &namespace,
                ViewCreation::builder()
                    .name("view1".to_string())
                    .location(format!("{warehouse_path}/test_ns/view1"))
                    .schema(iceberg_schema)
                    .representations(ViewRepresentations::new(vec![ViewRepresentation::Sql(
                        SqlViewRepresentation {
                            sql: "SELECT foo FROM tbl1".to_string(),
                            dialect: "datafusion".to_string(),
                        },
                    )]))
                    .default_namespace(namespace.clone())
                    .default_catalog(Some("datafusion".to_string()))
                    .build(),
            )
            .await
            .unwrap();

        let provider = IcebergSchemaProvider::try_new(Arc::new(catalog), namespace)
            .await
            .unwrap();

        (provider, temp_dir)
    }

    #[tokio::test]
    async fn test_register_table_with_data_fails() {
        let (schema_provider, _temp_dir) = create_test_schema_provider().await;

        // Create a MemTable with data
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])
        .unwrap();

        let mem_table = MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap();

        // Attempt to register the table with data - should fail
        let result = schema_provider.register_table("test_table".to_string(), Arc::new(mem_table));

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("register_table does not support tables with data."),
            "Expected error about tables with data, got: {err}",
        );
    }

    #[tokio::test]
    async fn test_register_empty_table_succeeds() {
        let (schema_provider, _temp_dir) = create_test_schema_provider().await;

        // Create an empty MemTable (schema only, no data rows)
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create an empty batch (0 rows) - MemTable requires at least one partition
        let empty_batch = RecordBatch::new_empty(arrow_schema.clone());
        let mem_table = MemTable::try_new(arrow_schema, vec![vec![empty_batch]]).unwrap();

        // Attempt to register the empty table - should succeed
        let result = schema_provider.register_table("empty_table".to_string(), Arc::new(mem_table));

        assert!(result.is_ok(), "Expected success, got: {result:?}");

        // Verify the table was registered
        assert!(schema_provider.table_exist("empty_table"));
    }

    #[tokio::test]
    async fn test_register_duplicate_table_fails() {
        let (schema_provider, _temp_dir) = create_test_schema_provider().await;

        // Create empty MemTables
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));

        let empty_batch1 = RecordBatch::new_empty(arrow_schema.clone());
        let empty_batch2 = RecordBatch::new_empty(arrow_schema.clone());
        let mem_table1 = MemTable::try_new(arrow_schema.clone(), vec![vec![empty_batch1]]).unwrap();
        let mem_table2 = MemTable::try_new(arrow_schema, vec![vec![empty_batch2]]).unwrap();

        // Register first table - should succeed
        let result1 = schema_provider.register_table("dup_table".to_string(), Arc::new(mem_table1));
        assert!(result1.is_ok());

        // Register second table with same name - should fail
        let result2 = schema_provider.register_table("dup_table".to_string(), Arc::new(mem_table2));
        assert!(result2.is_err());
        let err = result2.unwrap_err();
        assert!(
            err.to_string().contains("already exists"),
            "Expected error about table already existing, got: {err}",
        );
    }

    #[tokio::test]
    async fn test_deregister_table_succeeds() {
        let (schema_provider, _temp_dir) = create_test_schema_provider().await;

        // Create and register an empty table
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));

        let empty_batch = RecordBatch::new_empty(arrow_schema.clone());
        let mem_table = MemTable::try_new(arrow_schema, vec![vec![empty_batch]]).unwrap();

        // Register the table
        let result = schema_provider.register_table("drop_me".to_string(), Arc::new(mem_table));
        assert!(result.is_ok());
        assert!(schema_provider.table_exist("drop_me"));

        // Deregister the table
        let result = schema_provider.deregister_table("drop_me");
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Verify the table no longer exists
        assert!(!schema_provider.table_exist("drop_me"));
    }

    #[tokio::test]
    async fn test_deregister_nonexistent_table_returns_none() {
        let (schema_provider, _temp_dir) = create_test_schema_provider().await;

        // Attempt to deregister a table that doesn't exist
        let result = schema_provider.deregister_table("nonexistent");
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_table_names_include_views() {
        let (schema_provider, _temp_dir) = create_test_schema_provider_with_view().await;

        let table_names = schema_provider.table_names();

        assert!(table_names.contains(&"tbl1".to_string()));
        assert!(table_names.contains(&"view1".to_string()));
    }

    #[tokio::test]
    async fn test_table_loads_iceberg_view_as_datafusion_view() {
        let (schema_provider, _temp_dir) = create_test_schema_provider_with_view().await;

        let view_provider = schema_provider.table("view1").await.unwrap().unwrap();

        assert_eq!(view_provider.table_type(), TableType::View);
        assert_eq!(view_provider.schema().field(0).name(), "foo");
    }

    #[tokio::test]
    async fn test_deregister_view_is_not_supported() {
        let (schema_provider, _temp_dir) = create_test_schema_provider_with_view().await;

        let result = schema_provider.deregister_table("view1");

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not supported"));
    }
}
