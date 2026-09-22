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

use arrow_array::{Array, Date32Array, Decimal128Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use vortex::array::VortexSessionExecute;
use vortex::file::OpenOptionsSessionExt;

use super::*;

fn batch(rows: usize) -> RecordBatch {
    let days = [
        Some(-100_000),
        Some(-1),
        Some(0),
        Some(1),
        Some(20_000),
        None,
    ];
    let amounts = [
        Some(-999_999_999_999_999_i128),
        Some(-1),
        Some(0),
        Some(1),
        Some(999_999_999_999_999),
        None,
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, true),
        Field::new("amount", DataType::Decimal128(15, 2), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from_iter(
                (0..rows).map(|i| days[i % days.len()]),
            )),
            Arc::new(
                Decimal128Array::from_iter((0..rows).map(|i| amounts[i % amounts.len()]))
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}

fn assert_roundtrip(rows: usize) -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("input.parquet");
    let output = directory.path().join("output.vortex");
    let expected = batch(rows);
    let mut writer = ArrowWriter::try_new(File::create(&input)?, expected.schema(), None)?;
    writer.write(&expected)?;
    writer.close()?;
    assert_eq!(convert(&input, &output)?.0, rows as u64);

    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = session.open_options().open_buffer(std::fs::read(output)?)?;
    assert_eq!(
        file.dtype(),
        &session.arrow().from_arrow_schema(expected.schema().as_ref())?
    );
    let target = Field::new(
        "",
        DataType::Struct(expected.schema().fields().clone()),
        false,
    );
    let mut ctx = session.create_execution_ctx();
    let mut offset = 0;
    for chunk in file.scan()?.into_array_iter(&runtime)? {
        let chunk = chunk?;
        let length = chunk.len();
        let actual = session
            .arrow()
            .execute_arrow(chunk, Some(&target), &mut ctx)?;
        let wanted = StructArray::from(expected.slice(offset, length));
        assert_eq!(actual.to_data(), wanted.to_data());
        offset += length;
    }
    assert_eq!(offset, rows);
    Ok(())
}

#[test]
fn preserves_dates_exact_decimals_nulls_and_multiple_batches() -> Result<(), Box<dyn Error>> {
    assert_roundtrip(BATCH_ROWS + 17)
}

#[test]
fn preserves_empty_schema() -> Result<(), Box<dyn Error>> {
    assert_roundtrip(0)
}
