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

use arrow_array::ArrayRef;
use arrow_schema::{DataType, SchemaRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::{add_fallback_field_ids_to_arrow_schema, apply_name_mapping_to_arrow_schema};
use crate::arrow::record_batch_transformer::{ColumnSource, RecordBatchTransformerBuilder};
use crate::metadata_columns::{RESERVED_FIELD_ID_FILE, RESERVED_FIELD_ID_SPEC_ID};
use crate::scan::FileScanTask;
use crate::spec::Datum;
use crate::{Error, ErrorKind, Result};

#[cfg(test)]
mod tests;

/// Resolves a task's columns against metadata supplied by an external decoder.
#[derive(Debug, Clone)]
pub struct FileReadBinding {
    task: Arc<FileScanTask>,
    physical_schema: SchemaRef,
}

/// A physical column or a one-element constant, before engine-specific representation changes.
#[derive(Debug, Clone)]
pub struct BoundReadField {
    source_index: Option<usize>,
    data_type: DataType,
    constant: Option<ArrayRef>,
}

impl BoundReadField {
    /// Returns the top-level physical column index, if this field reads a column.
    pub fn source_index(&self) -> Option<usize> {
        self.source_index
    }

    /// Returns the Iceberg field's Arrow type after schema evolution.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the one-element constant for an absent or metadata field.
    pub fn constant(&self) -> Option<&ArrayRef> {
        self.constant.as_ref()
    }
}

impl FileReadBinding {
    /// Binds a task to a decoder's physical Arrow schema without reading the file.
    pub fn new(task: Arc<FileScanTask>, physical_schema: SchemaRef) -> Result<Self> {
        let missing_ids = physical_schema
            .fields()
            .first()
            .is_some_and(|field| !field.metadata().contains_key(PARQUET_FIELD_ID_META_KEY));
        let physical_schema = if missing_ids {
            match task.name_mapping() {
                Some(mapping) => apply_name_mapping_to_arrow_schema(physical_schema, mapping)?,
                None => add_fallback_field_ids_to_arrow_schema(&physical_schema),
            }
        } else {
            physical_schema
        };
        Ok(Self {
            task,
            physical_schema,
        })
    }

    /// Returns the physical schema with Iceberg field IDs resolved.
    pub fn physical_schema(&self) -> &SchemaRef {
        &self.physical_schema
    }

    /// Resolves a field using the same column rules as the Arrow convenience reader.
    ///
    /// The caller must apply the resulting promotion or constant before evaluating
    /// predicates, not only when constructing output batches.
    pub fn field(&self, field_id: i32) -> Result<BoundReadField> {
        let mut builder = RecordBatchTransformerBuilder::new(self.task.schema_ref(), &[field_id]);
        if let (Some(spec), Some(partition)) = (self.task.partition_spec(), self.task.partition()) {
            builder = builder.with_partition(Arc::clone(spec), partition.clone())?;
        }
        if field_id == RESERVED_FIELD_ID_FILE {
            builder = builder.with_constant(field_id, Datum::string(self.task.data_file_path()));
        }
        if field_id == RESERVED_FIELD_ID_SPEC_ID {
            let spec = self
                .task
                .partition_spec()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Partition spec is missing"))?;
            builder = builder.with_constant(field_id, Datum::int(spec.spec_id()));
        }
        let transformer = builder.build();
        let operations = transformer.bind_columns(&self.physical_schema)?;
        let operation = &operations[0];
        let (source_index, data_type) = match operation {
            ColumnSource::PassThrough { source_index } => (
                Some(*source_index),
                self.physical_schema
                    .field(*source_index)
                    .data_type()
                    .clone(),
            ),
            ColumnSource::Promote {
                source_index,
                target_type,
            } => (Some(*source_index), target_type.clone()),
            ColumnSource::Add { target_type, .. }
            | ColumnSource::ConstantLookup { target_type, .. } => (None, target_type.clone()),
            ColumnSource::Coalesce { .. } => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "Coalesced metadata fields require a row-lineage binding",
                ));
            }
        };
        let constant = if source_index.is_none() {
            transformer
                .transform_columns(&[], &operations, 1)?
                .into_iter()
                .next()
        } else {
            None
        };
        Ok(BoundReadField {
            source_index,
            data_type,
            constant,
        })
    }
}
