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
use iceberg::transaction::Transaction;
use iceberg::{Catalog, Result, SessionCatalog, SessionContext, TableIdent};

/// Iceberg catalog access retained by a DataFusion table provider.
///
/// Unlike a catalog adapter, this type does not implement [`Catalog`]. Each
/// operation dispatches directly to the API selected when the provider was
/// constructed.
#[derive(Clone, Debug)]
pub(crate) enum IcebergCatalogAccess {
    Plain(Arc<dyn Catalog>),
    Session {
        catalog: Arc<dyn SessionCatalog>,
        context: Arc<SessionContext>,
    },
}

impl IcebergCatalogAccess {
    pub(crate) fn plain(catalog: Arc<dyn Catalog>) -> Self {
        Self::Plain(catalog)
    }

    pub(crate) fn session(catalog: Arc<dyn SessionCatalog>, context: Arc<SessionContext>) -> Self {
        Self::Session { catalog, context }
    }

    pub(crate) async fn load_table(&self, ident: &TableIdent) -> Result<Table> {
        match self {
            Self::Plain(catalog) => catalog.load_table(ident).await,
            Self::Session { catalog, context } => catalog.load_table(context, ident).await,
        }
    }

    pub(crate) async fn commit(&self, transaction: Transaction) -> Result<Table> {
        match self {
            Self::Plain(catalog) => transaction.commit(catalog.as_ref()).await,
            Self::Session { catalog, context } => {
                transaction
                    .commit_with_session(catalog.as_ref(), context)
                    .await
            }
        }
    }
}
