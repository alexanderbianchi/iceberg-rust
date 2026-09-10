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

/// The metadata lifetime and commit target for an Iceberg table provider.
#[derive(Debug, Clone)]
pub(crate) enum IcebergTableSource {
    RefreshingCatalog {
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
    },
    ResolvedSession {
        catalog: Arc<dyn SessionCatalog>,
        context: Arc<SessionContext>,
        table: Table,
    },
}

impl IcebergTableSource {
    pub(crate) fn refreshing(catalog: Arc<dyn Catalog>, table_ident: TableIdent) -> Self {
        Self::RefreshingCatalog {
            catalog,
            table_ident,
        }
    }

    pub(crate) fn resolved_session(
        catalog: Arc<dyn SessionCatalog>,
        context: Arc<SessionContext>,
        table: Table,
    ) -> Self {
        Self::ResolvedSession {
            catalog,
            context,
            table,
        }
    }

    pub(crate) async fn table_for_planning(&self) -> Result<Table> {
        match self {
            Self::RefreshingCatalog {
                catalog,
                table_ident,
            } => catalog.load_table(table_ident).await,
            Self::ResolvedSession { table, .. } => Ok(table.clone()),
        }
    }

    pub(crate) async fn commit(&self, transaction: Transaction) -> Result<Table> {
        match self {
            Self::RefreshingCatalog { catalog, .. } => transaction.commit(catalog.as_ref()).await,
            Self::ResolvedSession {
                catalog, context, ..
            } => {
                transaction
                    .commit_with_session(catalog.as_ref(), context)
                    .await
            }
        }
    }
}
