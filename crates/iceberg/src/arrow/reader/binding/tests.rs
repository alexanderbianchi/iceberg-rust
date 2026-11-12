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

use arrow_array::Int32Array;
use arrow_schema::{Field, Schema};

use super::*;
use crate::spec::{
    DataFileFormat, Literal, NestedField, PrimitiveType, Schema as IcebergSchema, Type,
};

fn task() -> Arc<FileScanTask> {
    let schema = Arc::new(
        IcebergSchema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "renamed",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(
                    NestedField::optional(2, "added", Type::Primitive(PrimitiveType::Int))
                        .with_initial_default(Literal::int(7)),
                ),
            ])
            .build()
            .unwrap(),
    );
    Arc::new(
        FileScanTask::builder()
            .with_data_file_path("data.parquet".to_owned())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_file_size_in_bytes(1)
            .with_start(0)
            .with_length(1)
            .with_schema(schema)
            .with_project_field_ids(vec![1, 2])
            .with_case_sensitive(true)
            .build()
            .unwrap(),
    )
}

#[test]
fn binds_renamed_promoted_and_defaulted_fields_before_decoding() {
    let field = Field::new("old_name", DataType::Int32, false).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_owned(),
        "1".to_owned(),
    )]));
    let binding = FileReadBinding::new(task(), Arc::new(Schema::new(vec![field]))).unwrap();
    let renamed = binding.field(1).unwrap();
    assert_eq!(
        (
            renamed.source_index(),
            renamed.data_type(),
            renamed.constant().is_none()
        ),
        (Some(0), &DataType::Int64, true)
    );
    let added = binding.field(2).unwrap();
    let expected: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    assert_eq!(
        (added.source_index(), added.constant()),
        (None, Some(&expected))
    );
}

#[test]
fn idless_schema_uses_positional_fallback() {
    let physical = Arc::new(Schema::new(vec![Field::new(
        "old_name",
        DataType::Int64,
        false,
    )]));
    let binding = FileReadBinding::new(task(), physical).unwrap();
    assert_eq!(binding.field(1).unwrap().source_index(), Some(0));
}

#[test]
fn name_mapping_overrides_positional_fallback_before_decoding() {
    use crate::spec::{MappedField, NameMapping};
    let mapping = Arc::new(NameMapping::new(vec![MappedField::new(
        Some(1),
        vec!["old_name".to_owned()],
        vec![],
    )]));
    let task = Arc::new(
        FileScanTask::builder()
            .with_data_file_path("data.parquet".to_owned())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_file_size_in_bytes(1)
            .with_start(0)
            .with_length(1)
            .with_schema(task().schema_ref())
            .with_project_field_ids(vec![1, 2])
            .with_case_sensitive(true)
            .with_name_mapping(Some(mapping))
            .build()
            .unwrap(),
    );
    let physical = Arc::new(Schema::new(vec![
        Field::new("unrelated", DataType::Int32, true),
        Field::new("old_name", DataType::Int64, false),
    ]));
    let binding = FileReadBinding::new(task, physical).unwrap();
    assert_eq!(binding.field(1).unwrap().source_index(), Some(1));
}

#[test]
fn missing_required_field_is_rejected_before_decoding() {
    let field = Field::new("renamed", DataType::Int64, false).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_owned(),
        "99".to_owned(),
    )]));
    let binding = FileReadBinding::new(task(), Arc::new(Schema::new(vec![field]))).unwrap();
    assert!(
        binding
            .field(1)
            .unwrap_err()
            .to_string()
            .contains("Missing required field")
    );
}
