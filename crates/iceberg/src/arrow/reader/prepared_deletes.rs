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

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::SchemaRef as ArrowSchemaRef;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowSelection, RowSelector};
use tokio::sync::OnceCell;

use super::ArrowReader;
use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::arrow::delete_file_loader::DeleteFileLoader;
use crate::arrow::schema_to_arrow_schema;
use crate::delete_vector::DeleteVector;
use crate::encryption::{EncryptedInputFile, StandardKeyMetadata};
use crate::expr::{Bind, Predicate};
use crate::io::FileIO;
use crate::scan::{FileScanTask, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, DataFileFormat, SchemaRef};
use crate::{Error, ErrorKind, Result};

#[cfg(test)]
mod tests;

type DeleteCache =
    HashMap<(String, Option<u64>), (FileScanTaskDeleteFile, Arc<OnceCell<LoadedDeletes>>)>;

/// Scan-scoped delete preparation using a caller-supplied Parquet delete reader.
pub struct DeleteReadContext {
    loader: Arc<dyn DeleteFileLoader + Send + Sync>,
    file_io: FileIO,
    schema: SchemaRef,
    cache: Mutex<DeleteCache>,
}

impl fmt::Debug for DeleteReadContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeleteReadContext")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
enum LoadedDeletes {
    Positions(HashMap<String, DeleteVector>),
    Equality(Predicate),
}

/// Mandatory deletes prepared for one data file, independently of query predicates.
#[derive(Debug)]
pub struct PreparedDeletes {
    positions: DeleteVector,
    equality: Predicate,
    schema: SchemaRef,
    equality_ids: Vec<i32>,
    case_sensitive: bool,
}

/// An equality-delete keep-mask evaluator over its logical key schema.
pub struct EqualityDeleteFilter {
    schema: ArrowSchemaRef,
    field_ids: Vec<i32>,
    predicate: Mutex<Box<dyn ArrowPredicate>>,
}

impl fmt::Debug for EqualityDeleteFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EqualityDeleteFilter")
            .field("field_ids", &self.field_ids)
            .finish_non_exhaustive()
    }
}

impl DeleteReadContext {
    /// Creates a cache for one read schema; clones of the context should be shared across partitions.
    pub fn new(
        loader: Arc<dyn DeleteFileLoader + Send + Sync>,
        file_io: FileIO,
        schema: SchemaRef,
    ) -> Self {
        Self {
            loader,
            file_io,
            schema,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Prepares only the deletes applicable to this task, retrying failed or cancelled loads.
    pub async fn prepare(&self, task: &FileScanTask) -> Result<PreparedDeletes> {
        if task.schema() != self.schema.as_ref() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Delete context and task read schemas differ",
            ));
        }
        let mut positions = DeleteVector::default();
        let mut equality = Predicate::AlwaysTrue;
        let mut equality_ids = HashSet::new();
        for delete in task.deletes() {
            let cell = {
                let mut cache = self
                    .cache
                    .lock()
                    .map_err(|_| Error::new(ErrorKind::Unexpected, "Delete cache lock poisoned"))?;
                let key = (delete.file_path().to_owned(), delete.content_offset());
                let (cached_task, cell) = cache
                    .entry(key)
                    .or_insert_with(|| (delete.clone(), Arc::new(OnceCell::new())));
                if cached_task != delete {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "Conflicting metadata for a cached delete file",
                    ));
                }
                Arc::clone(cell)
            };
            let loaded = cell.get_or_try_init(|| self.load(delete)).await?;
            match loaded {
                LoadedDeletes::Positions(files) => {
                    if let Some(vector) = files.get(task.data_file_path()) {
                        for position in vector.iter() {
                            positions.insert(position);
                        }
                    }
                }
                LoadedDeletes::Equality(predicate) => {
                    equality = equality.and(predicate.clone());
                    equality_ids.extend(delete.equality_ids().unwrap_or_default());
                }
            }
        }
        let mut equality_ids: Vec<_> = equality_ids.into_iter().collect();
        equality_ids.sort_unstable();
        Ok(PreparedDeletes {
            positions,
            equality,
            schema: self.schema.clone(),
            equality_ids,
            case_sensitive: task.case_sensitive(),
        })
    }

    async fn load(&self, task: &FileScanTaskDeleteFile) -> Result<LoadedDeletes> {
        if task.file_format() == DataFileFormat::Puffin {
            let (start, len, path, count) =
                CachingDeleteFileLoader::validate_deletion_vector_task(task)?;
            let input = self.file_io.new_input(task.file_path())?;
            let reader = match task.key_metadata() {
                Some(metadata) => {
                    EncryptedInputFile::new(input, StandardKeyMetadata::decode(metadata)?)
                        .reader()
                        .await?
                }
                None => input.reader().await?,
            };
            let vector = DeleteVector::deserialize(&reader.read(start..start + len).await?)?;
            CachingDeleteFileLoader::validate_deletion_vector_cardinality(
                &vector,
                count,
                task.file_path(),
            )?;
            return Ok(LoadedDeletes::Positions(HashMap::from([(path, vector)])));
        }
        if task.file_format() != DataFileFormat::Parquet {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Unsupported delete file format",
            ));
        }
        let stream = self
            .loader
            .read_delete_file(task, self.schema.clone())
            .await?;
        match task.file_type() {
            DataContentType::PositionDeletes => Ok(LoadedDeletes::Positions(
                CachingDeleteFileLoader::parse_positional_deletes_record_batch_stream(stream)
                    .await?,
            )),
            DataContentType::EqualityDeletes => {
                let ids = task.equality_ids().ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Missing equality delete IDs")
                })?;
                Ok(LoadedDeletes::Equality(
                    CachingDeleteFileLoader::parse_equality_deletes_record_batch_stream(
                        stream,
                        ids.iter().copied().collect(),
                    )
                    .await?,
                ))
            }
            DataContentType::Data => {
                Err(Error::new(ErrorKind::DataInvalid, "Expected a delete file"))
            }
        }
    }
}

impl PreparedDeletes {
    /// Combines positional deletes with Iceberg byte-split ownership using decoder-supplied metadata.
    pub fn row_selection(
        &self,
        task: &FileScanTask,
        metadata: &Arc<parquet::file::metadata::ParquetMetaData>,
    ) -> Result<Option<RowSelection>> {
        let rows = u64::try_from(metadata.file_metadata().num_rows()).map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "Negative Parquet row count").with_source(e)
        })?;
        let deletes = self.position_selection(rows)?;
        if task.start() == 0 && (task.length() == 0 || task.length() == task.file_size_in_bytes()) {
            return Ok(deletes);
        }
        task.start()
            .checked_add(task.length())
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "File split range overflow"))?;
        let groups =
            ArrowReader::filter_row_groups_by_byte_range(metadata, task.start(), task.length())?;
        let selectors = metadata
            .row_groups()
            .iter()
            .enumerate()
            .map(|(index, group)| {
                let rows = usize::try_from(group.num_rows()).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Row group size exceeds address space",
                    )
                    .with_source(e)
                })?;
                Ok(if groups.binary_search(&index).is_ok() {
                    RowSelector::select(rows)
                } else {
                    RowSelector::skip(rows)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let split = RowSelection::from(selectors);
        Ok(Some(match deletes {
            Some(deletes) => split.intersection(&deletes),
            None => split,
        }))
    }

    /// Returns a keep selection in whole-file coordinates, independent of splits and query pruning.
    pub fn position_selection(&self, record_count: u64) -> Result<Option<RowSelection>> {
        if self.positions.len() == 0 {
            return Ok(None);
        }
        let mut selectors = Vec::new();
        let mut next = 0;
        for position in self.positions.iter() {
            if position >= record_count {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Delete position is outside the data file",
                ));
            }
            let keep = usize::try_from(position - next).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Row selection exceeds address space",
                )
                .with_source(e)
            })?;
            selectors.push(RowSelector::select(keep));
            selectors.push(RowSelector::skip(1));
            next = position + 1;
        }
        selectors.push(RowSelector::select(
            usize::try_from(record_count - next).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Row selection exceeds address space",
                )
                .with_source(e)
            })?,
        ));
        Ok(Some(RowSelection::from(selectors)))
    }

    /// Returns whether this task has positional deletes.
    pub fn has_position_deletes(&self) -> bool {
        self.positions.len() != 0
    }

    /// Creates a fresh evaluator that keeps rows not matched by an equality delete.
    ///
    /// Input keys must already have Iceberg schema evolution and defaults applied.
    pub fn equality_filter(&self) -> Result<Option<EqualityDeleteFilter>> {
        if self.equality == Predicate::AlwaysTrue {
            return Ok(None);
        }
        let full_schema = schema_to_arrow_schema(&self.schema)?;
        let mut indices = Vec::new();
        for id in &self.equality_ids {
            let index = full_schema
                .fields()
                .iter()
                .position(|field| {
                    field
                        .metadata()
                        .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                        .and_then(|v| v.parse::<i32>().ok())
                        == Some(*id)
                })
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::FeatureUnsupported,
                        "Nested equality-delete keys are not supported by the Arrow evaluator",
                    )
                })?;
            indices.push(index);
        }
        let schema = Arc::new(full_schema.project(&indices)?);
        let parquet_schema = ArrowSchemaConverter::new().convert(&schema)?;
        let predicate = self
            .equality
            .clone()
            .bind(self.schema.clone(), self.case_sensitive)?;
        let (ids, map) =
            ArrowReader::build_field_id_set_and_map(&parquet_schema, &schema, &predicate, false)?;
        let mut predicates =
            ArrowReader::get_row_filter(&predicate, &parquet_schema, &ids, &map)?.into_predicates();
        let predicate = predicates.pop().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "Missing equality-delete evaluator")
        })?;
        let (indices, field_ids): (Vec<_>, Vec<_>) = self
            .equality_ids
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, id)| ids.contains(id))
            .unzip();
        let schema = Arc::new(schema.project(&indices)?);
        Ok(Some(EqualityDeleteFilter {
            schema,
            field_ids,
            predicate: Mutex::new(predicate),
        }))
    }
}

impl EqualityDeleteFilter {
    /// Returns the field IDs in the evaluator's input-column order.
    pub fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }

    /// Returns the evolved key schema expected by this evaluator.
    pub fn schema(&self) -> &ArrowSchemaRef {
        &self.schema
    }

    /// Evaluates a batch of evolved keys, returning true for rows to retain.
    pub fn evaluate(&self, batch: RecordBatch) -> Result<BooleanArray> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Equality-delete input schema does not match its binding",
            ));
        }
        Ok(self
            .predicate
            .lock()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Equality-delete evaluator lock poisoned",
                )
            })?
            .evaluate(batch)?)
    }
}
