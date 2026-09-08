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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use futures::future::try_join_all;
use iceberg::{Catalog, NamespaceIdent, Result, SessionCatalog, SessionContext};

use crate::catalog_adapter::SessionBindingCatalogAdapter;
use crate::options::IcebergOptions;
use crate::schema_provider::IcebergSchemaProvider;

/// Provides a DataFusion interface to schemas in an Iceberg [`Catalog`] or
/// [`SessionCatalog`].
///
/// Acts as a centralized catalog provider that aggregates
/// multiple [`SchemaProvider`], each associated with distinct namespaces.
#[derive(Debug)]
pub struct IcebergCatalogProvider {
    /// A `HashMap` where keys are namespace names
    /// and values are dynamic references to objects implementing the
    /// [`SchemaProvider`] trait.
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl IcebergCatalogProvider {
    /// Asynchronously constructs an [`IcebergCatalogProvider`] from a
    /// [`Catalog`], fetching and initializing a schema provider for each
    /// namespace.
    ///
    /// This method retrieves the namespace names and collects an initialized
    /// schema provider for each namespace into a `HashMap`.
    pub async fn try_new(catalog: Arc<dyn Catalog>) -> Result<Self> {
        let session_binding_catalog = SessionBindingCatalogAdapter::new_without_context(catalog);
        Self::try_new_with_binding_catalog(Arc::new(session_binding_catalog)).await
    }

    /// Creates an [`IcebergCatalogProvider`] backed by a [`SessionCatalog`].
    ///
    /// Each DataFusion session that has [`crate::IcebergOptions`] configured will
    /// propagate an Iceberg [`SessionContext`] for scans and inserts. Provider
    /// initialization, metadata-table lookup, table registration, and table
    /// deregistration do not receive a DataFusion session; they share one
    /// anonymous fallback context instead.
    ///
    /// Namespace and table discovery is performed once during construction and
    /// shared by all DataFusion sessions. Catalogs with session-dependent
    /// visibility must make the intended discovery set available to the
    /// anonymous fallback; discovery is not repeated per DataFusion session.
    pub async fn try_new_with_session_catalog(catalog: Arc<dyn SessionCatalog>) -> Result<Self> {
        let shared_fallback_context = SessionContext::empty();
        let session_bound = SessionBindingCatalogAdapter::new(shared_fallback_context, catalog);
        Self::try_new_with_binding_catalog(Arc::new(session_bound)).await
    }

    async fn try_new_with_binding_catalog(
        catalog: Arc<SessionBindingCatalogAdapter>,
    ) -> Result<Self> {
        // TODO:
        // Schemas and providers should be cached and evicted based on time
        // As of right now; schemas might become stale.
        let schema_names: Vec<_> = catalog
            .list_namespaces(None)
            .await?
            .iter()
            .flat_map(|ns| ns.as_ref().clone())
            .collect();

        Ok(IcebergCatalogProvider {
            schemas: load_schema_providers(catalog, schema_names).await?,
        })
    }
}

impl CatalogProvider for IcebergCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}

/// Creates providers whose query context is bound before catalog discovery.
///
/// Unlike [`IcebergCatalogProvider::try_new_with_session_catalog`], this factory
/// does not use an anonymous fallback for initial namespace, table, or schema
/// loading. [`Self::for_session`] supplies the caller's [`IcebergOptions`] to
/// those operations as well as subsequent scans, inserts, and metadata lookups.
///
/// Factory construction performs no catalog I/O. Each binding eagerly discovers
/// its own namespaces and tables, sharing only the underlying [`SessionCatalog`]
/// (and its transport), not the discovered providers. Create a new binding for
/// each query or context requiring distinct visibility; there is no cross-binding
/// discovery cache or automatic refresh of discovered names.
#[derive(Debug, Clone)]
pub struct IcebergSessionCatalogProvider {
    inner: Arc<dyn SessionCatalog>,
}

impl IcebergSessionCatalogProvider {
    /// Creates a factory over `inner`. Performs no catalog I/O.
    pub fn new(inner: Arc<dyn SessionCatalog>) -> Self {
        Self { inner }
    }

    /// Resolves an [`iceberg::SessionContext`] from `options`, then builds an
    /// [`IcebergCatalogProvider`] whose namespace and table discovery, and
    /// whose subsequent scan, insert, metadata-lookup, and
    /// register/deregister operations, all use that same context.
    ///
    /// The resolved context is explicitly bound: it stays fixed for the
    /// lifetime of the returned provider and is never overwritten by a
    /// DataFusion session's [`IcebergOptions`], even if that session carries
    /// different or conflicting options. Call this again to create another
    /// binding for another query or context. Each binding receives a fresh
    /// Iceberg session ID, independent of any DataFusion session ID.
    ///
    /// # Errors
    ///
    /// Returns an error if namespace/table discovery or initial schema loading
    /// fails under the supplied context.
    pub async fn for_session(&self, options: IcebergOptions) -> Result<IcebergCatalogProvider> {
        let context = options.to_session_context();
        let session_bound =
            SessionBindingCatalogAdapter::new_explicit(context, Arc::clone(&self.inner));
        IcebergCatalogProvider::try_new_with_binding_catalog(Arc::new(session_bound)).await
    }
}

async fn load_schema_providers(
    catalog: Arc<SessionBindingCatalogAdapter>,
    schema_names: Vec<String>,
) -> Result<HashMap<String, Arc<dyn SchemaProvider>>> {
    let iceberg_providers = try_join_all(
        schema_names
            .iter()
            .map(|name| {
                IcebergSchemaProvider::try_new(
                    Arc::clone(&catalog),
                    NamespaceIdent::new(name.clone()),
                )
            })
            .collect::<Vec<_>>(),
    )
    .await?;

    let provider_map = schema_names
        .into_iter()
        .zip(iceberg_providers)
        .map(|(name, iceberg_provider)| {
            let provider = Arc::new(iceberg_provider) as Arc<dyn SchemaProvider>;
            (name, provider)
        })
        .collect();

    Ok(provider_map)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::CatalogProvider;
    use datafusion::datasource::MemTable;
    use datafusion::execution::config::SessionConfig;
    use datafusion::prelude::SessionContext as DFSessionContext;
    use iceberg::sensitive::SensitiveString;

    use super::*;
    use crate::{IcebergOptions, test_utils};

    #[tokio::test]
    async fn test_session_aware_scan_uses_resolved_context() {
        let (session_catalog, namespace, table_name, _temp_dir) =
            test_utils::create_recording_catalog().await;
        let provider =
            IcebergCatalogProvider::try_new_with_session_catalog(session_catalog.clone())
                .await
                .unwrap();

        let bootstrap_calls = session_catalog.calls();
        assert_eq!(
            bootstrap_calls
                .iter()
                .map(|call| call.operation)
                .collect::<Vec<_>>(),
            vec!["list_namespaces", "list_tables", "load_table"]
        );
        session_catalog.clear_calls();

        let fallback_session_id = bootstrap_calls[0].session_id.as_str();
        assert!(bootstrap_calls.iter().all(|call| {
            call.session_id == fallback_session_id
                && call.identity.is_none()
                && call.properties.is_empty()
                && call.credentials.is_empty()
        }));

        let schema = provider.schema(namespace[0].as_str()).unwrap();
        let table = schema.table(&table_name).await.unwrap().unwrap();

        let first_options = test_utils::iceberg_options();
        let config = SessionConfig::new().with_extension(Arc::clone(&first_options));
        let first_df_context = DFSessionContext::new_with_config(config);

        table
            .scan(&first_df_context.state(), None, &[], None)
            .await
            .unwrap();

        let second_options = Arc::new(IcebergOptions {
            identity: Some("another-user".to_string()),
            properties: HashMap::from([(
                "another-property".to_string(),
                "another-value".to_string(),
            )]),
            credentials: HashMap::from([(
                "another-token".to_string(),
                SensitiveString::from("another-secret".to_string()),
            )]),
        });
        let config = SessionConfig::new().with_extension(Arc::clone(&second_options));
        let second_df_context = DFSessionContext::new_with_config(config);

        table
            .scan(&second_df_context.state(), None, &[], None)
            .await
            .unwrap();

        let calls = session_catalog.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].operation, "load_table");
        assert_eq!(calls[0].session_id, first_df_context.session_id());
        assert_eq!(calls[0].identity, first_options.identity);
        assert_eq!(calls[0].properties, first_options.properties);
        assert_eq!(calls[0].credentials, first_options.credentials);

        assert_eq!(calls[1].operation, "load_table");
        assert_eq!(calls[1].session_id, second_df_context.session_id());
        assert_eq!(calls[1].identity, second_options.identity);
        assert_eq!(calls[1].properties, second_options.properties);
        assert_eq!(calls[1].credentials, second_options.credentials);
    }

    #[tokio::test]
    async fn test_session_aware_scan_without_options_uses_fallback_context() {
        let (session_catalog, namespace, table_name, _temp_dir) =
            test_utils::create_recording_catalog().await;
        let provider =
            IcebergCatalogProvider::try_new_with_session_catalog(session_catalog.clone())
                .await
                .unwrap();
        let fallback_session_id = session_catalog.calls()[0].session_id.clone();
        let schema = provider.schema(namespace[0].as_str()).unwrap();
        let table = schema.table(&table_name).await.unwrap().unwrap();
        session_catalog.clear_calls();

        let df_context = DFSessionContext::new();
        table
            .scan(&df_context.state(), None, &[], None)
            .await
            .unwrap();

        let calls = session_catalog.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operation, "load_table");
        assert_eq!(calls[0].session_id, fallback_session_id);
        assert_ne!(calls[0].session_id, df_context.session_id());
        assert!(calls[0].identity.is_none());
        assert!(calls[0].properties.is_empty());
        assert!(calls[0].credentials.is_empty());
    }

    #[tokio::test]
    async fn test_session_aware_insert_reuses_resolved_context_for_commit() {
        let (session_catalog, namespace, table_name, _temp_dir) =
            test_utils::create_recording_catalog().await;
        let provider =
            IcebergCatalogProvider::try_new_with_session_catalog(session_catalog.clone())
                .await
                .unwrap();
        let schema = provider.schema(namespace[0].as_str()).unwrap();
        let table = schema.table(&table_name).await.unwrap().unwrap();
        session_catalog.clear_calls();

        let config = SessionConfig::new().with_extension(test_utils::iceberg_options());
        let df_context = DFSessionContext::new_with_config(config);
        df_context.register_table("test_table", table).unwrap();
        df_context
            .sql("INSERT INTO test_table VALUES (1, 'test')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let calls = session_catalog.calls();
        let load = calls
            .iter()
            .find(|call| call.operation == "load_table")
            .unwrap();
        let update = calls
            .iter()
            .find(|call| call.operation == "update_table")
            .unwrap();
        assert_eq!(update.session_id, load.session_id);
        assert_eq!(update.identity, load.identity);
        assert_eq!(update.properties, load.properties);
        assert_eq!(update.credentials, load.credentials);
        assert_eq!(load.session_id, df_context.session_id());
        assert_eq!(load.identity, test_utils::iceberg_options().identity);
        assert_eq!(load.properties, test_utils::iceberg_options().properties);
        assert_eq!(load.credentials, test_utils::iceberg_options().credentials);
    }

    #[tokio::test]
    async fn test_sessionless_schema_operations_share_fallback_context() {
        let (session_catalog, namespace, table_name, _temp_dir) =
            test_utils::create_recording_catalog().await;
        let provider =
            IcebergCatalogProvider::try_new_with_session_catalog(session_catalog.clone())
                .await
                .unwrap();
        let schema = provider.schema(namespace[0].as_str()).unwrap();
        session_catalog.clear_calls();

        schema
            .table(&format!("{table_name}$snapshots"))
            .await
            .unwrap()
            .unwrap();

        let metadata_calls = session_catalog.calls();
        assert_eq!(metadata_calls.len(), 1);
        assert_eq!(metadata_calls[0].operation, "load_table");
        let fallback_session_id = metadata_calls[0].session_id.clone();
        assert!(metadata_calls.iter().all(|call| {
            call.session_id == fallback_session_id
                && call.identity.is_none()
                && call.properties.is_empty()
                && call.credentials.is_empty()
        }));
        session_catalog.clear_calls();

        let arrow_schema = schema.table(&table_name).await.unwrap().unwrap().schema();
        let empty_batch = RecordBatch::new_empty(arrow_schema.clone());
        let empty_table = MemTable::try_new(arrow_schema, vec![vec![empty_batch]]).unwrap();
        schema
            .register_table("registered_table".to_string(), Arc::new(empty_table))
            .unwrap();
        schema.deregister_table("registered_table").unwrap();

        let calls = session_catalog.calls();
        assert!(calls.iter().any(|call| call.operation == "create_table"));
        assert!(calls.iter().any(|call| call.operation == "drop_table"));
        assert!(calls.iter().all(|call| {
            call.session_id == fallback_session_id
                && call.identity.is_none()
                && call.properties.is_empty()
                && call.credentials.is_empty()
        }));
    }

    #[tokio::test]
    async fn test_factory_construction_performs_no_catalog_io() {
        let (session_catalog, _temp_dir) = test_utils::create_identity_scoped_catalog().await;

        let _factory = IcebergSessionCatalogProvider::new(session_catalog.clone());

        assert!(
            session_catalog.calls().is_empty(),
            "constructing the factory must not touch the catalog"
        );
    }

    #[tokio::test]
    async fn test_for_session_binds_identity_before_first_discovery_call() {
        let (session_catalog, _temp_dir) = test_utils::create_identity_scoped_catalog().await;
        let factory = IcebergSessionCatalogProvider::new(session_catalog.clone());

        let options = IcebergOptions {
            identity: Some("alice".to_string()),
            properties: HashMap::from([("region".to_string(), "us-east".to_string())]),
            credentials: HashMap::from([(
                "token".to_string(),
                SensitiveString::from("alice-secret".to_string()),
            )]),
        };

        let provider = factory.for_session(options.clone()).await.unwrap();

        let calls = session_catalog.calls();
        assert_eq!(
            calls.iter().map(|call| call.operation).collect::<Vec<_>>(),
            vec!["list_namespaces", "list_tables", "load_table"]
        );
        assert!(calls.iter().all(|call| {
            call.identity == options.identity
                && call.properties == options.properties
                && call.credentials == options.credentials
        }));

        // The bound identity determined what got discovered.
        assert_eq!(provider.schema_names(), vec!["alice_ns".to_string()]);
        let schema = provider.schema("alice_ns").unwrap();
        assert!(schema.table_exist("alice_table"));
    }

    #[tokio::test]
    async fn test_for_session_rejects_anonymous_catalog_by_default_but_allows_binding() {
        let (session_catalog, _temp_dir) = test_utils::create_identity_scoped_catalog().await;
        let factory = IcebergSessionCatalogProvider::new(session_catalog.clone());

        // Anonymous discovery (no identity bound) is rejected by this catalog.
        let error = factory
            .for_session(IcebergOptions::default())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), iceberg::ErrorKind::DataInvalid);
        assert!(
            error
                .to_string()
                .contains("anonymous access is not permitted")
        );

        // An explicitly bound identity can still initialize successfully.
        let provider = factory
            .for_session(IcebergOptions {
                identity: Some("bob".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(provider.schema_names(), vec!["bob_ns".to_string()]);
    }

    #[tokio::test]
    async fn test_two_bindings_do_not_share_discovered_visibility() {
        let (session_catalog, _temp_dir) = test_utils::create_identity_scoped_catalog().await;
        let factory = IcebergSessionCatalogProvider::new(session_catalog.clone());

        let (alice_provider, bob_provider) = tokio::try_join!(
            factory.for_session(IcebergOptions {
                identity: Some("alice".to_string()),
                ..Default::default()
            }),
            factory.for_session(IcebergOptions {
                identity: Some("bob".to_string()),
                ..Default::default()
            }),
        )
        .unwrap();

        assert_eq!(alice_provider.schema_names(), vec!["alice_ns".to_string()]);
        assert_eq!(bob_provider.schema_names(), vec!["bob_ns".to_string()]);

        assert!(alice_provider.schema("bob_ns").is_none());
        assert!(bob_provider.schema("alice_ns").is_none());
    }

    #[tokio::test]
    async fn test_explicit_binding_survives_scan_without_datafusion_options() {
        let (session_catalog, _temp_dir) = test_utils::create_identity_scoped_catalog().await;
        let factory = IcebergSessionCatalogProvider::new(session_catalog.clone());

        let provider = factory
            .for_session(IcebergOptions {
                identity: Some("alice".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let bound_session_id = session_catalog.calls()[0].session_id.clone();
        session_catalog.clear_calls();

        let schema = provider.schema("alice_ns").unwrap();
        let table = schema.table("alice_table").await.unwrap().unwrap();

        // No IcebergOptions extension registered on this DataFusion session.
        let df_context = DFSessionContext::new();
        table
            .scan(&df_context.state(), None, &[], None)
            .await
            .unwrap();

        let calls = session_catalog.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operation, "load_table");
        assert_eq!(calls[0].session_id, bound_session_id);
        assert_eq!(calls[0].identity, Some("alice".to_string()));
    }

    #[tokio::test]
    async fn test_explicit_binding_survives_scan_with_conflicting_datafusion_options() {
        let (session_catalog, _temp_dir) = test_utils::create_identity_scoped_catalog().await;
        let factory = IcebergSessionCatalogProvider::new(session_catalog.clone());

        let provider = factory
            .for_session(IcebergOptions {
                identity: Some("alice".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let bound_session_id = session_catalog.calls()[0].session_id.clone();
        session_catalog.clear_calls();

        let schema = provider.schema("alice_ns").unwrap();
        let table = schema.table("alice_table").await.unwrap().unwrap();

        // A DataFusion session carrying a *different* identity must not
        // override the explicitly bound context.
        let conflicting_options = Arc::new(IcebergOptions {
            identity: Some("bob".to_string()),
            ..Default::default()
        });
        let config = SessionConfig::new().with_extension(conflicting_options);
        let df_context = DFSessionContext::new_with_config(config);
        table
            .scan(&df_context.state(), None, &[], None)
            .await
            .unwrap();

        let calls = session_catalog.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operation, "load_table");
        assert_eq!(calls[0].session_id, bound_session_id);
        assert_ne!(calls[0].session_id, df_context.session_id());
        assert_eq!(
            calls[0].identity,
            Some("alice".to_string()),
            "explicit binding must not be overwritten by conflicting DataFusion IcebergOptions"
        );
    }

    #[tokio::test]
    async fn test_explicit_binding_survives_metadata_lookup_and_sql_commit() {
        let (session_catalog, namespace, table_name, _temp_dir) =
            test_utils::create_recording_catalog().await;
        let factory = IcebergSessionCatalogProvider::new(session_catalog.clone());
        let options = test_utils::iceberg_options();
        let provider = factory.for_session(options.as_ref().clone()).await.unwrap();
        let bound_session_id = session_catalog.calls()[0].session_id.clone();
        session_catalog.clear_calls();

        let schema = provider.schema(namespace[0].as_str()).unwrap();
        schema
            .table(&format!("{table_name}$snapshots"))
            .await
            .unwrap()
            .unwrap();
        let metadata_calls = session_catalog.calls();
        assert_eq!(metadata_calls.len(), 1);
        assert_eq!(metadata_calls[0].operation, "load_table");
        session_catalog.clear_calls();

        let config = SessionConfig::new().with_extension(Arc::new(IcebergOptions {
            identity: Some("conflicting-user".to_string()),
            ..Default::default()
        }));
        let session = DFSessionContext::new_with_config(config);
        session.register_catalog("iceberg", Arc::new(provider));
        session
            .sql("INSERT INTO iceberg.test_ns.test_table VALUES (1, 'test')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let calls = session_catalog.calls();
        assert!(calls.iter().any(|call| call.operation == "load_table"));
        assert!(calls.iter().any(|call| call.operation == "update_table"));
        assert!(metadata_calls.iter().chain(calls.iter()).all(|call| {
            call.session_id == bound_session_id
                && call.identity == options.identity
                && call.properties == options.properties
                && call.credentials == options.credentials
        }));
    }

    #[tokio::test]
    async fn test_binding_uses_one_context_id_throughout_and_distinct_ids_between_bindings() {
        let (session_catalog, _temp_dir) = test_utils::create_identity_scoped_catalog().await;
        let factory = IcebergSessionCatalogProvider::new(session_catalog.clone());

        let first_provider = factory
            .for_session(IcebergOptions {
                identity: Some("alice".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let first_bootstrap_calls = session_catalog.calls();
        session_catalog.clear_calls();

        let _second_provider = factory
            .for_session(IcebergOptions {
                identity: Some("alice".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let second_bootstrap_calls = session_catalog.calls();
        session_catalog.clear_calls();

        let first_session_id = first_bootstrap_calls[0].session_id.clone();
        let second_session_id = second_bootstrap_calls[0].session_id.clone();
        assert!(
            first_bootstrap_calls
                .iter()
                .all(|call| call.session_id == first_session_id)
        );
        assert!(
            second_bootstrap_calls
                .iter()
                .all(|call| call.session_id == second_session_id)
        );
        assert_ne!(
            first_session_id, second_session_id,
            "distinct bindings must get distinct context ids even with identical options"
        );

        // The first binding's context id remains stable across further
        // operations on that binding: scanning re-loads table metadata
        // through the same bound context.
        let schema = first_provider.schema("alice_ns").unwrap();
        let table = schema.table("alice_table").await.unwrap().unwrap();
        let df_context = DFSessionContext::new();
        table
            .scan(&df_context.state(), None, &[], None)
            .await
            .unwrap();
        let calls = session_catalog.calls();
        assert_eq!(calls.last().unwrap().session_id, first_session_id);
    }
}
