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

// SPDX-License-Identifier: Apache-2.0
// Keep source values and encoded ALP arrays separate: the former are the oracle.
use std::{
    env,
    fs::{self, File},
    ops::Range,
    path::PathBuf,
    sync::Arc,
};
use vortex::{
    VortexSessionDefault,
    array::{
        ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute,
        arrays::{PrimitiveArray, StructArray},
        validity::Validity,
    },
    encodings::alp::{ALP, ALPArray, ALPArrayExt, ALPArraySlotsExt, Exponents, alp_encode},
    file::WriteOptionsSessionExt,
    io::{
        runtime::{BlockingRuntime, current::CurrentThreadRuntime},
        session::RuntimeSessionExt,
    },
    layout::layouts::{flat::writer::FlatLayoutStrategy, table::TableStrategy},
    session::VortexSession,
};

const ROWS: usize = 4105;
const SPECIAL: [usize; 11] = [1, 4, 5, 31, 1023, 1024, 1025, 3072, 4095, 4096, 4102];
const BITS32: [u32; 11] = [
    0x7fc01234, 0xffc05678, 0x7f800000, 0xff800000, 0x80000000, 0, 1, 0x80000001, 0x3eaaaaab,
    0x7f7fffff, 0x00800000,
];
const BITS64: [u64; 11] = [
    0x7ff8000000001234,
    0xfff8000000005678,
    0x7ff0000000000000,
    0xfff0000000000000,
    0x8000000000000000,
    0,
    1,
    0x8000000000000001,
    0x3fd5555555555555,
    0x7fefffffffffffff,
    0x0010000000000000,
];

fn valid(row: usize) -> bool {
    row % 17 != 0 || SPECIAL.contains(&row)
}
fn value32(row: usize) -> f32 {
    SPECIAL
        .iter()
        .position(|&r| r == row)
        .map(|i| f32::from_bits(BITS32[i]))
        .unwrap_or(row as f32 - 2000.0)
}
fn value64(row: usize) -> f64 {
    SPECIAL
        .iter()
        .position(|&r| r == row)
        .map(|i| f64::from_bits(BITS64[i]))
        .unwrap_or(row as f64 - 2000.0)
}

fn sliced(array: &ALPArray, range: Range<usize>) -> Result<ALPArray, Box<dyn std::error::Error>> {
    // The same construction as Rust's ALP slice kernel, without a lazy slice wrapper.
    Ok(ALP::new(
        array.encoded().slice(range.clone())?,
        array.exponents(),
        array
            .patches()
            .map(|p| p.slice(range))
            .transpose()?
            .flatten(),
    ))
}

fn check(
    array: &ALPArray,
    name: &str,
    start: usize,
    ctx: &mut ExecutionCtx,
) -> Result<(), Box<dyn std::error::Error>> {
    let decoded = array.clone().into_array().execute::<PrimitiveArray>(ctx)?;
    let validity = decoded.validity()?;
    for i in 0..array.len() {
        let row = start + i;
        let expected_valid =
            !name.ends_with("all_null") && (!name.ends_with("nullable") || valid(row));
        assert_eq!(
            validity.execute_is_valid(i, ctx)?,
            expected_valid,
            "{name} row {row} validity"
        );
        if expected_valid {
            if name.starts_with("f32") {
                assert_eq!(
                    decoded.as_slice::<f32>()[i].to_bits(),
                    value32(row).to_bits(),
                    "{name} row {row}"
                );
            } else {
                assert_eq!(
                    decoded.as_slice::<f64>()[i].to_bits(),
                    value64(row).to_bits(),
                    "{name} row {row}"
                );
            }
        }
    }
    if let Some(patches) = array.patches() {
        let offsets = patches
            .chunk_offsets()
            .as_ref()
            .expect("real chunked patches")
            .clone()
            .execute::<PrimitiveArray>(ctx)?;
        let indices = patches.indices().clone().execute::<PrimitiveArray>(ctx)?;
        assert_eq!(patches.offset(), start);
        if start == 0 {
            assert_eq!(offsets.as_slice::<u64>(), &[0, 5, 6, 6, 8]);
            assert_eq!(
                indices.as_slice::<u64>(),
                &[1, 4, 5, 31, 1023, 1025, 3072, 4095, 4096, 4102]
            );
        }
        if start == 5 {
            assert_eq!(patches.offset_within_chunk(), Some(2));
        }
        if start == 1025 {
            assert_eq!(patches.offset_within_chunk(), Some(0));
        }
        println!(
            "  {name}: patches={} offset={} within={:?} chunk_offsets={:?}",
            patches.num_patches(),
            patches.offset(),
            patches.offset_within_chunk(),
            offsets.as_slice::<u64>()
        );
    } else {
        assert!(array.is_empty() || name.ends_with("all_null"));
        println!("  {name}: no patches, rows={}", array.len());
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = PathBuf::from(
        env::args_os()
            .nth(1)
            .or_else(|| env::var_os("VORTEX_TESTDATA_DIR"))
            .expect("output directory argument or VORTEX_TESTDATA_DIR"),
    );
    fs::create_dir_all(&out)?;
    let rt = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(rt.handle());
    let mut ctx = session.create_execution_ctx();
    let mut fields: Vec<(&str, ALPArray)> = Vec::new();
    for (name, original) in [
        (
            "f32_required",
            PrimitiveArray::from_iter((0..ROWS).map(value32)),
        ),
        (
            "f32_nullable",
            PrimitiveArray::from_option_iter((0..ROWS).map(|i| valid(i).then(|| value32(i)))),
        ),
        (
            "f32_all_null",
            PrimitiveArray::from_option_iter((0..ROWS).map(|_| None::<f32>)),
        ),
        (
            "f64_required",
            PrimitiveArray::from_iter((0..ROWS).map(value64)),
        ),
        (
            "f64_nullable",
            PrimitiveArray::from_option_iter((0..ROWS).map(|i| valid(i).then(|| value64(i)))),
        ),
        (
            "f64_all_null",
            PrimitiveArray::from_option_iter((0..ROWS).map(|_| None::<f64>)),
        ),
    ] {
        fields.push((
            name,
            alp_encode(original.as_view(), Some(Exponents { e: 0, f: 0 }), &mut ctx)?,
        ));
    }
    for (suffix, range) in [
        ("", 0..ROWS),
        ("_slice", 5..4100),
        ("_late_slice", 1025..4100),
        ("_boundary", 1023..1026),
        ("_empty", 5..5),
    ] {
        let name = format!("alp_patches{suffix}.vortex");
        println!("{name}: source rows {range:?}");
        let mut names = vec!["row_id"];
        let mut arrays: Vec<ArrayRef> =
            vec![PrimitiveArray::from_iter(range.clone().map(|i| i as i64)).into_array()];
        for (field_name, full) in &fields {
            let array = if range == (0..ROWS) {
                full.clone()
            } else {
                sliced(full, range.clone())?
            };
            check(&array, field_name, range.start, &mut ctx)?;
            names.push(field_name);
            arrays.push(array.into_array());
        }
        let array = StructArray::try_new(names.into(), arrays, range.len(), Validity::NonNullable)?;
        let flat = Arc::new(FlatLayoutStrategy::default());
        let strategy = Arc::new(TableStrategy::new(flat.clone(), flat));
        let mut writer = session
            .write_options()
            .with_strategy(strategy)
            .blocking(&rt)
            .writer(File::create(out.join(name))?, array.dtype().clone());
        writer.push(array.into_array())?;
        writer.finish()?;
    }
    Ok(())
}
