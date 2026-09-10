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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{
    AsyncCatalogProvider, AsyncSchemaProvider, CatalogProvider, MemoryCatalogProvider,
};
use datafusion::common::{DataFusionError, Result as DFResult, TableReference, not_impl_err};
use datafusion::datasource::TableProvider;
use datafusion::execution::config::SessionConfig;
use iceberg::inspect::MetadataTableType;
use iceberg::{NamespaceIdent, SessionCatalog, SessionContext};

use crate::table::IcebergTableProvider;
use crate::to_datafusion_error;

/// Resolves an Iceberg [`SessionCatalog`] for one DataFusion query.
///
/// [`Self::resolve`] obtains an `Arc<SessionContext>` directly from the
/// query's [`SessionConfig`] extensions, then loads only the referenced tables
/// under that immutable context. The returned provider retains those table
/// providers only for the query's lifetime. It performs no namespace or table
/// enumeration and adds no persistent metadata cache.
#[derive(Debug)]
pub struct IcebergSessionCatalogProvider {
    catalog: Arc<dyn SessionCatalog>,
}

impl IcebergSessionCatalogProvider {
    /// Creates a reusable resolver over `catalog`.
    pub fn new(catalog: Arc<dyn SessionCatalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl AsyncCatalogProvider for IcebergSessionCatalogProvider {
    async fn schema(&self, _name: &str) -> DFResult<Option<Arc<dyn AsyncSchemaProvider>>> {
        not_impl_err!("Iceberg session catalogs require resolve() with a SessionConfig")
    }

    async fn resolve(
        &self,
        references: &[TableReference],
        config: &SessionConfig,
        catalog_name: &str,
    ) -> DFResult<Arc<dyn CatalogProvider>> {
        let has_matching_reference = references.iter().any(|reference| {
            reference
                .catalog()
                .unwrap_or(&config.options().catalog.default_catalog)
                == catalog_name
        });
        if !has_matching_reference {
            return Ok(Arc::new(MemoryCatalogProvider::new()));
        }

        let context = config.get_extension::<SessionContext>().ok_or_else(|| {
            DataFusionError::Configuration(
                "Iceberg SessionContext is required to resolve a session catalog".to_string(),
            )
        })?;
        let bound = BoundIcebergCatalogProvider {
            catalog: Arc::clone(&self.catalog),
            context,
        };

        AsyncCatalogProvider::resolve(&bound, references, config, catalog_name).await
    }
}

#[derive(Debug)]
struct BoundIcebergCatalogProvider {
    catalog: Arc<dyn SessionCatalog>,
    context: Arc<SessionContext>,
}

#[async_trait]
impl AsyncCatalogProvider for BoundIcebergCatalogProvider {
    async fn schema(&self, name: &str) -> DFResult<Option<Arc<dyn AsyncSchemaProvider>>> {
        Ok(Some(Arc::new(BoundIcebergSchemaProvider {
            catalog: Arc::clone(&self.catalog),
            context: Arc::clone(&self.context),
            namespace: NamespaceIdent::new(name.to_string()),
        })))
    }
}

#[derive(Debug)]
struct BoundIcebergSchemaProvider {
    catalog: Arc<dyn SessionCatalog>,
    context: Arc<SessionContext>,
    namespace: NamespaceIdent,
}

#[async_trait]
impl AsyncSchemaProvider for BoundIcebergSchemaProvider {
    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        load_table_provider(
            Arc::clone(&self.catalog),
            Arc::clone(&self.context),
            self.namespace.clone(),
            name,
        )
        .await
        .map(Some)
    }
}

async fn load_table_provider(
    catalog: Arc<dyn SessionCatalog>,
    context: Arc<SessionContext>,
    namespace: NamespaceIdent,
    name: &str,
) -> DFResult<Arc<dyn TableProvider>> {
    let (table_name, metadata_type) = match name.split_once('$') {
        Some((table_name, metadata_name)) => (
            table_name,
            Some(MetadataTableType::try_from(metadata_name).map_err(DataFusionError::Plan)?),
        ),
        None => (name, None),
    };
    let table = IcebergTableProvider::try_new_session(catalog, context, namespace, table_name)
        .await
        .map_err(to_datafusion_error)?;

    match metadata_type {
        Some(metadata_type) => Ok(Arc::new(
            table
                .metadata_table(metadata_type)
                .await
                .map_err(to_datafusion_error)?,
        )),
        None => Ok(Arc::new(table)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::catalog::AsyncCatalogProvider;
    use datafusion::common::TableReference;
    use datafusion::execution::config::SessionConfig;
    use datafusion::prelude::SessionContext as DataFusionSessionContext;
    use iceberg::SessionContext;
    use iceberg::sensitive::SensitiveString;

    use super::IcebergSessionCatalogProvider;
    use crate::test_utils;

    fn reference(catalog: &str) -> TableReference {
        TableReference::full(catalog, "test_ns", "test_table")
    }

    #[tokio::test]
    async fn resolve_requires_session_context_for_referenced_catalog() {
        let (catalog, _namespace, _table, _temp_dir) = test_utils::create_recording_catalog().await;
        let resolver = IcebergSessionCatalogProvider::new(catalog.clone());

        let error = resolver
            .resolve(&[reference("iceberg")], &SessionConfig::new(), "iceberg")
            .await
            .unwrap_err();

        assert!(error.to_string().contains("SessionContext"));
        assert!(catalog.calls().is_empty());
    }

    #[tokio::test]
    async fn resolve_loads_only_references_with_exact_context() {
        let (catalog, _namespace, _table, _temp_dir) = test_utils::create_recording_catalog().await;
        let resolver = IcebergSessionCatalogProvider::new(catalog.clone());
        let context = Arc::new(
            SessionContext::builder()
                .identity("alice".to_string())
                .properties(HashMap::from([("org".to_string(), "123".to_string())]))
                .credentials(HashMap::from([(
                    "jwt".to_string(),
                    SensitiveString::from("secret".to_string()),
                )]))
                .build(),
        );
        let config = SessionConfig::new().with_extension(Arc::clone(&context));
        assert!(Arc::ptr_eq(
            &context,
            &config.get_extension::<SessionContext>().unwrap()
        ));

        let provider = resolver
            .resolve(&[reference("iceberg")], &config, "iceberg")
            .await
            .unwrap();
        assert_eq!(provider.schema_names(), vec!["test_ns"]);
        assert_eq!(provider.schema("test_ns").unwrap().table_names(), vec![
            "test_table"
        ]);

        let session = DataFusionSessionContext::new_with_config(config);
        session.register_catalog("iceberg", provider);
        session
            .sql("SELECT * FROM iceberg.test_ns.test_table")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            catalog
                .calls()
                .iter()
                .filter(|call| call.operation == "load_table")
                .count(),
            1,
            "scan should reuse the table loaded during resolution",
        );

        session
            .sql("INSERT INTO iceberg.test_ns.test_table VALUES (1, 'test')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let calls = catalog.calls();
        assert_eq!(calls.first().unwrap().operation, "load_table");
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.operation == "load_table")
                .count(),
            2,
            "insert planning should reuse the resolved table; commit should refresh it once",
        );
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.operation == "update_table")
                .count(),
            1,
        );
        assert!(!calls.iter().any(|call| {
            call.operation == "list_namespaces" || call.operation == "list_tables"
        }));
        assert!(calls.iter().all(|call| {
            call.session_id == context.session_id()
                && call.identity.as_deref() == context.identity()
                && call.properties == *context.properties()
                && call.credentials == *context.credentials()
        }));
    }

    #[tokio::test]
    async fn metadata_table_reuses_table_loaded_during_resolution() {
        let (catalog, _namespace, _table, _temp_dir) = test_utils::create_recording_catalog().await;
        let resolver = IcebergSessionCatalogProvider::new(catalog.clone());
        let context = Arc::new(SessionContext::builder().build());
        let config = SessionConfig::new().with_extension(context);

        resolver
            .resolve(
                &[TableReference::full(
                    "iceberg",
                    "test_ns",
                    "test_table$snapshots",
                )],
                &config,
                "iceberg",
            )
            .await
            .unwrap();

        assert_eq!(
            catalog
                .calls()
                .iter()
                .filter(|call| call.operation == "load_table")
                .count(),
            1,
        );
    }

    #[tokio::test]
    async fn concurrent_resolutions_keep_contexts_isolated() {
        let (catalog, _namespace, _table, _temp_dir) = test_utils::create_recording_catalog().await;
        let resolver = IcebergSessionCatalogProvider::new(catalog.clone());
        let alice = Arc::new(
            SessionContext::builder()
                .identity("alice".to_string())
                .build(),
        );
        let bob = Arc::new(
            SessionContext::builder()
                .identity("bob".to_string())
                .build(),
        );
        let references = [reference("iceberg")];
        let alice_config = SessionConfig::new().with_extension(Arc::clone(&alice));
        let bob_config = SessionConfig::new().with_extension(Arc::clone(&bob));

        tokio::try_join!(
            resolver.resolve(&references, &alice_config, "iceberg"),
            resolver.resolve(&references, &bob_config, "iceberg"),
        )
        .unwrap();

        let calls = catalog.calls();
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().any(|call| {
            call.session_id == alice.session_id() && call.identity.as_deref() == Some("alice")
        }));
        assert!(calls.iter().any(|call| {
            call.session_id == bob.session_id() && call.identity.as_deref() == Some("bob")
        }));
    }

    #[tokio::test]
    async fn unrelated_references_do_not_require_context_or_load_tables() {
        let (catalog, _namespace, _table, _temp_dir) = test_utils::create_recording_catalog().await;
        let resolver = IcebergSessionCatalogProvider::new(catalog.clone());

        let provider = resolver
            .resolve(&[reference("other")], &SessionConfig::new(), "iceberg")
            .await
            .unwrap();

        assert!(provider.schema_names().is_empty());
        assert!(catalog.calls().is_empty());
    }
}
