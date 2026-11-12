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

use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::Int64Array;
use futures::FutureExt;

use super::*;
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{NestedField, PrimitiveType, Schema, Type};

struct Loader {
    calls: AtomicUsize,
    batch: RecordBatch,
    first: FirstLoad,
}

enum FirstLoad {
    Success,
    Error,
    Pending,
}

#[async_trait::async_trait]
impl DeleteFileLoader for Loader {
    async fn read_delete_file(
        &self,
        _: &FileScanTaskDeleteFile,
        _: SchemaRef,
    ) -> Result<ArrowRecordBatchStream> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            match self.first {
                FirstLoad::Error => return Err(Error::new(ErrorKind::Unexpected, "load failed")),
                FirstLoad::Pending => futures::future::pending::<()>().await,
                FirstLoad::Success => (),
            }
        }
        Ok(Box::pin(futures::stream::iter([Ok(self.batch.clone())])))
    }
}

fn fixture(first: FirstLoad) -> (Arc<Loader>, DeleteReadContext, FileScanTask) {
    let schema = Arc::new(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "key",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap(),
    );
    let batch = RecordBatch::try_new(Arc::new(schema_to_arrow_schema(&schema).unwrap()), vec![
        Arc::new(Int64Array::from(vec![Some(1), None])),
    ])
    .unwrap();
    let loader = Arc::new(Loader {
        calls: AtomicUsize::new(0),
        batch,
        first,
    });
    let context = DeleteReadContext::new(loader.clone(), FileIO::new_with_fs(), schema.clone());
    let delete = FileScanTaskDeleteFile::builder()
        .with_file_path("eq.parquet".to_owned())
        .with_file_size_in_bytes(1)
        .with_file_type(DataContentType::EqualityDeletes)
        .with_file_format(DataFileFormat::Parquet)
        .with_partition_spec_id(0)
        .with_equality_ids(Some(vec![1]))
        .build()
        .unwrap();
    let task = FileScanTask::builder()
        .with_data_file_path("data.parquet".to_owned())
        .with_data_file_format(DataFileFormat::Parquet)
        .with_file_size_in_bytes(1)
        .with_start(0)
        .with_length(1)
        .with_schema(schema)
        .with_project_field_ids(vec![1])
        .with_case_sensitive(true)
        .with_deletes(vec![delete])
        .build()
        .unwrap();
    (loader, context, task)
}

#[tokio::test]
async fn shared_preparation_preserves_null_delete_semantics() {
    let (loader, context, task) = fixture(FirstLoad::Success);
    let (a, b) = tokio::join!(context.prepare(&task), context.prepare(&task));
    for prepared in [a.unwrap(), b.unwrap()] {
        let filter = prepared.equality_filter().unwrap().unwrap();
        let batch = RecordBatch::try_new(filter.schema().clone(), vec![Arc::new(
            Int64Array::from(vec![Some(1), None, Some(2)]),
        )])
        .unwrap();
        assert_eq!(
            filter.evaluate(batch).unwrap(),
            BooleanArray::from(vec![false, false, true])
        );
    }
    assert_eq!(loader.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_load_can_be_retried() {
    let (loader, context, task) = fixture(FirstLoad::Error);
    assert!(context.prepare(&task).await.is_err());
    context.prepare(&task).await.unwrap();
    assert_eq!(loader.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancelled_load_can_be_retried() {
    let (loader, context, task) = fixture(FirstLoad::Pending);
    assert!(context.prepare(&task).now_or_never().is_none());
    context.prepare(&task).await.unwrap();
    assert_eq!(loader.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn equality_filter_projects_only_referenced_keys() {
    use crate::expr::Reference;
    use crate::spec::Datum;
    let schema = Arc::new(
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "unused",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "key",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap(),
    );
    let prepared = PreparedDeletes {
        positions: DeleteVector::default(),
        equality: Reference::new("key").not_equal_to(Datum::long(1)),
        schema,
        equality_ids: vec![1, 2],
        case_sensitive: true,
    };
    let filter = prepared.equality_filter().unwrap().unwrap();
    assert_eq!(filter.field_ids(), &[2]);
    let batch = RecordBatch::try_new(filter.schema().clone(), vec![Arc::new(Int64Array::from(
        vec![1, 2],
    ))])
    .unwrap();
    assert_eq!(
        filter.evaluate(batch).unwrap(),
        BooleanArray::from(vec![false, true])
    );
}

#[test]
fn position_selection_preserves_file_coordinates_and_rejects_out_of_range() {
    let (_, _, task) = fixture(FirstLoad::Success);
    let mut positions = DeleteVector::default();
    positions.insert(1);
    positions.insert(3);
    let prepared = PreparedDeletes {
        positions,
        equality: Predicate::AlwaysTrue,
        schema: task.schema_ref(),
        equality_ids: vec![],
        case_sensitive: true,
    };
    let expected = RowSelection::from(vec![
        RowSelector::select(1),
        RowSelector::skip(1),
        RowSelector::select(1),
        RowSelector::skip(1),
        RowSelector::select(1),
    ]);
    assert_eq!(prepared.position_selection(5).unwrap(), Some(expected));
    assert!(prepared.position_selection(3).is_err());
}
