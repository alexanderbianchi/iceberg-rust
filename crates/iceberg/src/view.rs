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

//! View API for Apache Iceberg.

use std::collections::HashMap;

use crate::spec::{
    SchemaRef, SqlViewRepresentation, ViewMetadata, ViewMetadataRef, ViewRepresentation,
    ViewVersionRef,
};
use crate::{Error, ErrorKind, Result, TableIdent};

/// Builder to create a [`View`].
pub struct ViewBuilder {
    metadata_location: Option<String>,
    metadata: Option<ViewMetadataRef>,
    identifier: Option<TableIdent>,
}

impl ViewBuilder {
    pub(crate) fn new() -> Self {
        Self {
            metadata_location: None,
            metadata: None,
            identifier: None,
        }
    }

    /// Sets the view metadata location.
    pub fn metadata_location<T: Into<String>>(mut self, metadata_location: T) -> Self {
        self.metadata_location = Some(metadata_location.into());
        self
    }

    /// Sets the view metadata.
    pub fn metadata<T: Into<ViewMetadataRef>>(mut self, metadata: T) -> Self {
        self.metadata = Some(metadata.into());
        self
    }

    /// Sets the view identifier.
    pub fn identifier(mut self, identifier: TableIdent) -> Self {
        self.identifier = Some(identifier);
        self
    }

    /// Builds the view.
    pub fn build(self) -> Result<View> {
        let Self {
            metadata_location,
            metadata,
            identifier,
        } = self;

        let Some(metadata) = metadata else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "ViewMetadata must be provided with ViewBuilder.metadata()",
            ));
        };

        let Some(identifier) = identifier else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "TableIdent must be provided with ViewBuilder.identifier()",
            ));
        };

        Ok(View {
            metadata_location,
            metadata,
            identifier,
        })
    }
}

/// A logical Iceberg view loaded from a catalog.
#[derive(Debug, Clone)]
pub struct View {
    metadata_location: Option<String>,
    metadata: ViewMetadataRef,
    identifier: TableIdent,
}

impl View {
    /// Returns a builder to build a view.
    pub fn builder() -> ViewBuilder {
        ViewBuilder::new()
    }

    /// Returns the view identifier.
    pub fn identifier(&self) -> &TableIdent {
        &self.identifier
    }

    /// Returns current metadata.
    pub fn metadata(&self) -> &ViewMetadata {
        &self.metadata
    }

    /// Returns current metadata ref.
    pub fn metadata_ref(&self) -> ViewMetadataRef {
        self.metadata.clone()
    }

    /// Returns current metadata location.
    pub fn metadata_location(&self) -> Option<&str> {
        self.metadata_location.as_deref()
    }

    /// Returns current metadata location in a result.
    pub fn metadata_location_result(&self) -> Result<&str> {
        self.metadata_location.as_deref().ok_or(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Metadata location does not exist for view: {}",
                self.identifier
            ),
        ))
    }

    /// Returns the current schema of the view.
    pub fn current_schema(&self) -> SchemaRef {
        self.metadata.current_schema().clone()
    }

    /// Returns the current version of the view.
    pub fn current_version(&self) -> &ViewVersionRef {
        self.metadata.current_version()
    }

    /// Returns view properties.
    pub fn properties(&self) -> &HashMap<String, String> {
        self.metadata.properties()
    }

    /// Returns the view's base location.
    pub fn location(&self) -> &str {
        self.metadata.location()
    }

    /// Resolves the SQL representation for the given dialect.
    pub fn sql_for(&self, dialect: &str) -> Option<&SqlViewRepresentation> {
        let dialect = dialect.to_lowercase();
        self.metadata
            .current_version()
            .representations()
            .iter()
            .find_map(|repr| match repr {
                ViewRepresentation::Sql(sql) if sql.dialect.to_lowercase() == dialect => Some(sql),
                _ => None,
            })
    }
}
