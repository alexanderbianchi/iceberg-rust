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

use iceberg::table::Table;
use iceberg::{
    Catalog, NamespaceIdent, Result, SessionCatalog, SessionContext, TableCommit, TableCreation,
    TableIdent,
};

/// Access to an Iceberg catalog, with an optional immutable session context.
///
/// This preserves the existing plain [`Catalog`] API while allowing providers
/// produced by [`crate::IcebergSessionCatalogProvider`] to retain the exact
/// [`SessionContext`] supplied by a DataFusion query.
#[derive(Clone, Debug)]
pub(crate) enum CatalogAccess {
    Plain(Arc<dyn Catalog>),
    Session {
        catalog: Arc<dyn SessionCatalog>,
        context: Arc<SessionContext>,
    },
}

impl From<Arc<dyn Catalog>> for CatalogAccess {
    fn from(catalog: Arc<dyn Catalog>) -> Self {
        Self::Plain(catalog)
    }
}

impl<T: Catalog + 'static> From<Arc<T>> for CatalogAccess {
    fn from(catalog: Arc<T>) -> Self {
        Self::Plain(catalog)
    }
}

#[async_trait::async_trait]
impl Catalog for CatalogAccess {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        match self {
            Self::Plain(catalog) => catalog.list_namespaces(parent).await,
            Self::Session { catalog, context } => catalog.list_namespaces(context, parent).await,
        }
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> Result<iceberg::Namespace> {
        match self {
            Self::Plain(catalog) => catalog.create_namespace(namespace, properties).await,
            Self::Session { catalog, context } => {
                catalog
                    .create_namespace(context, namespace, properties)
                    .await
            }
        }
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<iceberg::Namespace> {
        match self {
            Self::Plain(catalog) => catalog.get_namespace(namespace).await,
            Self::Session { catalog, context } => catalog.get_namespace(context, namespace).await,
        }
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        match self {
            Self::Plain(catalog) => catalog.namespace_exists(namespace).await,
            Self::Session { catalog, context } => {
                catalog.namespace_exists(context, namespace).await
            }
        }
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> Result<()> {
        match self {
            Self::Plain(catalog) => catalog.update_namespace(namespace, properties).await,
            Self::Session { catalog, context } => {
                catalog
                    .update_namespace(context, namespace, properties)
                    .await
            }
        }
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        match self {
            Self::Plain(catalog) => catalog.drop_namespace(namespace).await,
            Self::Session { catalog, context } => catalog.drop_namespace(context, namespace).await,
        }
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        match self {
            Self::Plain(catalog) => catalog.list_tables(namespace).await,
            Self::Session { catalog, context } => catalog.list_tables(context, namespace).await,
        }
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        match self {
            Self::Plain(catalog) => catalog.create_table(namespace, creation).await,
            Self::Session { catalog, context } => {
                catalog.create_table(context, namespace, creation).await
            }
        }
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        match self {
            Self::Plain(catalog) => catalog.load_table(table).await,
            Self::Session { catalog, context } => catalog.load_table(context, table).await,
        }
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        match self {
            Self::Plain(catalog) => catalog.drop_table(table).await,
            Self::Session { catalog, context } => catalog.drop_table(context, table).await,
        }
    }

    async fn purge_table(&self, table: &TableIdent) -> Result<()> {
        match self {
            Self::Plain(catalog) => catalog.purge_table(table).await,
            Self::Session { catalog, context } => catalog.purge_table(context, table).await,
        }
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        match self {
            Self::Plain(catalog) => catalog.table_exists(table).await,
            Self::Session { catalog, context } => catalog.table_exists(context, table).await,
        }
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        match self {
            Self::Plain(catalog) => catalog.rename_table(src, dest).await,
            Self::Session { catalog, context } => catalog.rename_table(context, src, dest).await,
        }
    }

    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        match self {
            Self::Plain(catalog) => catalog.register_table(table, metadata_location).await,
            Self::Session { catalog, context } => {
                catalog
                    .register_table(context, table, metadata_location)
                    .await
            }
        }
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        match self {
            Self::Plain(catalog) => catalog.update_table(commit).await,
            Self::Session { catalog, context } => catalog.update_table(context, commit).await,
        }
    }
}
