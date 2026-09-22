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
// Copyright 2026 the vortex-go contributors.
// Independently chosen source data; canonical input arrays remain the value oracle.
use std::{
    env,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};
use vortex::{
    VortexSessionDefault,
    array::{
        ArrayRef, IntoArray, VortexSessionExecute,
        arrays::{
            BoolArray, ExtensionArray, PrimitiveArray, StructArray, TemporalArray, VarBinArray,
            VarBinViewArray, struct_::StructArrayExt,
        },
        dtype::{DType, Nullability, PType},
        extension::datetime::TimeUnit,
        patches::Patches,
        validity::Validity,
    },
    editions::{EditionSessionExt, PREVIEW_2026_06_0},
    encodings::{
        alp::{ALP, ALPArrayExt, Exponents},
        datetime_parts::DateTimeParts,
        fastlanes::Delta,
    },
    file::{OpenOptionsSessionExt, WriteOptionsSessionExt},
    io::{
        runtime::{BlockingRuntime, current::CurrentThreadRuntime},
        session::RuntimeSessionExt,
    },
    layout::layouts::{flat::writer::FlatLayoutStrategy, table::TableStrategy},
    session::VortexSession,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const SCALAR_ROWS: usize = 6149;
const NULLABLE_ROWS: usize = 2057;
const TEMPORAL_ROWS: usize = 3079;
const ALP_ROWS: usize = 3091;
const PATCH_ROWS: [usize; 10] = [2, 7, 19, 509, 1022, 1024, 1537, 2047, 2048, 3089];
const PATCH_BITS32: [u32; 10] = [
    0x80000000, 0x7f800000, 0xff800000, 0x7fc02468, 0xffc01357, 0x00000001, 0x80000001, 0x7f7fffff,
    0x00800000, 0x3e4ccccd,
];
const PATCH_BITS64: [u64; 10] = [
    0x8000000000000000,
    0x7ff0000000000000,
    0xfff0000000000000,
    0x7ff8000000002468,
    0xfff8000000001357,
    0x0000000000000001,
    0x8000000000000001,
    0x7fefffffffffffff,
    0x0010000000000000,
    0x3fc999999999999a,
];

struct Fixture {
    name: &'static str,
    names: Vec<&'static str>,
    // Preserve canonical sources separately from explicitly encoded fields.
    source: Vec<ArrayRef>,
    stored: Vec<ArrayRef>,
    preserve_encoding: bool,
}

fn scalars() -> Fixture {
    let labels = [
        "",
        "cedar",
        "fjord",
        "東京駅",
        "naïve",
        "🪁",
        "a\0z",
        "independent strings exceed twelve bytes",
    ];
    let source = vec![
        PrimitiveArray::from_iter((0..SCALAR_ROWS).map(|r| r as i64 * 29 - 80000)).into_array(),
        PrimitiveArray::from_iter((0..SCALAR_ROWS).map(|r| ((r * 37) % 10007) as i32 - 5003))
            .into_array(),
        PrimitiveArray::from_iter((0..SCALAR_ROWS).map(|r| 9007199254740993_i64 + r as i64 * 101))
            .into_array(),
        PrimitiveArray::from_iter((0..SCALAR_ROWS).map(|r| ((r % 1801) as f64 - 900.0) / 8.0))
            .into_array(),
        BoolArray::from_iter((0..SCALAR_ROWS).map(|r| r % 7 < 3)).into_array(),
        VarBinArray::from_iter(
            (0..SCALAR_ROWS).map(|r| Some(labels[r % labels.len()])),
            DType::Utf8(Nullability::NonNullable),
        )
        .into_array(),
        VarBinArray::from_iter(
            (0..SCALAR_ROWS).map(|r| Some(format!("reading/{r:05}/sector-{}", r % 23))),
            DType::Utf8(Nullability::NonNullable),
        )
        .into_array(),
    ];
    Fixture {
        name: "owned_scalars",
        names: vec![
            "sequence", "small", "wide", "measure", "enabled", "label", "note",
        ],
        stored: source.clone(),
        source,
        preserve_encoding: false,
    }
}

fn nullable() -> Fixture {
    let source =
        vec![
            PrimitiveArray::from_option_iter((0..NULLABLE_ROWS).map(|r| {
                (r % 13 != 4 && !(1000..1017).contains(&r)).then_some(r as i32 * 19 - 7000)
            }))
            .into_array(),
            VarBinArray::from_iter(
                (0..NULLABLE_ROWS).map(|r| (r % 17 != 9).then(|| format!("item-{r:04}-δ"))),
                DType::Utf8(Nullability::Nullable),
            )
            .into_array(),
        ];
    Fixture {
        name: "owned_nullable",
        names: vec!["maybe_int", "maybe_str"],
        stored: source.clone(),
        source,
        preserve_encoding: false,
    }
}

fn temporal_delta(session: &VortexSession) -> Result<Fixture> {
    let integers = [
        PrimitiveArray::from_iter((0..TEMPORAL_ROWS).map(|r| 1024_u16 + r as u16 * 3)),
        PrimitiveArray::from_iter((0..TEMPORAL_ROWS).map(|r| 4000000_u32 + r as u32 * 17)),
        PrimitiveArray::from_iter((0..TEMPORAL_ROWS).map(|r| (1_u64 << 63) + 37 + r as u64 * 1009)),
    ];
    let mut source = integers
        .iter()
        .cloned()
        .map(IntoArray::into_array)
        .collect::<Vec<_>>();
    let mut ctx = session.create_execution_ctx();
    let mut stored = integers
        .iter()
        .map(|a| Delta::try_from_primitive_array(a, &mut ctx).map(IntoArray::into_array))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for (unit, start, step) in [
        (TimeUnit::Nanoseconds, 1653456789012345678_i64, 7000003_i64),
        (TimeUnit::Milliseconds, -987654321_i64, 13003_i64),
    ] {
        let storage =
            PrimitiveArray::from_iter((0..TEMPORAL_ROWS).map(|r| start + r as i64 * step));
        let timestamp = TemporalArray::new_timestamp(storage.into_array(), unit, None);
        source.push(timestamp.clone().into_array());
        stored.push(DateTimeParts::try_from_temporal(timestamp, &mut ctx)?.into_array());
    }
    Ok(Fixture {
        name: "owned_temporal_delta",
        names: vec![
            "u16_delta",
            "u32_delta",
            "u64_delta",
            "clock_ns",
            "clock_ms",
        ],
        source,
        stored,
        preserve_encoding: true,
    })
}

fn float_valid(row: usize) -> bool {
    row % 23 != 11 && !(1800..1831).contains(&row)
}

fn float32(row: usize) -> f32 {
    match PATCH_ROWS.iter().position(|&r| r == row) {
        Some(index) => f32::from_bits(PATCH_BITS32[index]),
        None => (row as i32 * 3 - 4600) as f32,
    }
}

fn float64(row: usize) -> f64 {
    match PATCH_ROWS.iter().position(|&r| r == row) {
        Some(index) => f64::from_bits(PATCH_BITS64[index]),
        None => (row as i64 * 3 - 4600) as f64,
    }
}

fn alp_flat() -> Result<Fixture> {
    let mut source = Vec::new();
    let mut stored = Vec::new();
    for nullable in [false, true] {
        let source32 = if nullable {
            PrimitiveArray::from_option_iter(
                (0..ALP_ROWS).map(|r| float_valid(r).then(|| float32(r))),
            )
        } else {
            PrimitiveArray::from_iter((0..ALP_ROWS).map(float32))
        };
        let source64 = if nullable {
            PrimitiveArray::from_option_iter(
                (0..ALP_ROWS).map(|r| float_valid(r).then(|| float64(r))),
            )
        } else {
            PrimitiveArray::from_iter((0..ALP_ROWS).map(float64))
        };
        source.push(source32.into_array());
        source.push(source64.into_array());
        // Integers are only a storage representation; exceptional values live in flat patches.
        let encoded32 = if nullable {
            PrimitiveArray::from_option_iter((0..ALP_ROWS).map(|r| {
                float_valid(r).then_some(if PATCH_ROWS.contains(&r) {
                    0
                } else {
                    r as i32 * 3 - 4600
                })
            }))
        } else {
            PrimitiveArray::from_iter((0..ALP_ROWS).map(|r| {
                if PATCH_ROWS.contains(&r) {
                    0
                } else {
                    r as i32 * 3 - 4600
                }
            }))
        };
        let encoded64 = if nullable {
            PrimitiveArray::from_option_iter((0..ALP_ROWS).map(|r| {
                float_valid(r).then_some(if PATCH_ROWS.contains(&r) {
                    0
                } else {
                    r as i64 * 3 - 4600
                })
            }))
        } else {
            PrimitiveArray::from_iter((0..ALP_ROWS).map(|r| {
                if PATCH_ROWS.contains(&r) {
                    0
                } else {
                    r as i64 * 3 - 4600
                }
            }))
        };
        let indices = PrimitiveArray::from_iter(PATCH_ROWS.map(|r| r as u64)).into_array();
        let patch_values32 = if nullable {
            PrimitiveArray::from_option_iter(PATCH_BITS32.map(|bits| Some(f32::from_bits(bits))))
        } else {
            PrimitiveArray::from_iter(PATCH_BITS32.map(f32::from_bits))
        };
        let patch_values64 = if nullable {
            PrimitiveArray::from_option_iter(PATCH_BITS64.map(|bits| Some(f64::from_bits(bits))))
        } else {
            PrimitiveArray::from_iter(PATCH_BITS64.map(f64::from_bits))
        };
        for (encoded, values) in [
            (encoded32.into_array(), patch_values32.into_array()),
            (encoded64.into_array(), patch_values64.into_array()),
        ] {
            let patches = Patches::new(ALP_ROWS, 0, indices.clone(), values, None)?;
            assert!(patches.chunk_offsets().is_none());
            assert!(patches.offset_within_chunk().is_none());
            stored
                .push(ALP::try_new(encoded, Exponents { e: 0, f: 0 }, Some(patches))?.into_array());
        }
    }
    Ok(Fixture {
        name: "owned_alp_flat",
        names: vec!["single", "double", "nullable_single", "nullable_double"],
        source,
        stored,
        preserve_encoding: true,
    })
}

fn verify(
    path: &Path,
    fixture: &Fixture,
    session: &VortexSession,
    rt: &CurrentThreadRuntime,
) -> Result<()> {
    let file = session.open_options().open_buffer(fs::read(path)?)?;
    assert_eq!(file.row_count() as usize, fixture.source[0].len());
    let mut ctx = session.create_execution_ctx();
    let mut offset = 0;
    for batch in file.scan()?.into_array_iter(rt)? {
        let batch = batch?.execute::<StructArray>(&mut ctx)?;
        for (column, name) in fixture.names.iter().enumerate() {
            let field = batch.unmasked_field(column).clone();
            let original = &fixture.source[column];
            assert_eq!(field.dtype(), original.dtype(), "{name} dtype");
            if fixture.name == "owned_alp_flat" {
                // Inspect the reopened encoding before execution can replace it with primitives.
                let alp = field.as_opt::<ALP>().expect("writer must preserve ALP");
                let patches = alp
                    .patches()
                    .expect("flat patches must survive serialization");
                assert!(
                    patches.chunk_offsets().is_none(),
                    "legacy flat patches have no chunk offsets"
                );
                assert!(patches.offset_within_chunk().is_none());
                assert_eq!(patches.offset(), 0);
                assert_eq!(patches.num_patches(), PATCH_ROWS.len());
                let indices = patches
                    .indices()
                    .clone()
                    .execute::<PrimitiveArray>(&mut ctx)?;
                assert_eq!(indices.as_slice::<u64>(), &PATCH_ROWS.map(|r| r as u64));
            }
            if fixture.name == "owned_temporal_delta" && column < 3 {
                assert!(
                    field.is::<Delta>(),
                    "writer must preserve explicit unsigned delta"
                );
            }
            if fixture.name == "owned_temporal_delta" && column >= 3 {
                assert!(
                    field.is::<DateTimeParts>(),
                    "writer must preserve datetimeparts"
                );
            }
            let decoded = match original.dtype() {
                DType::Bool(_) => field.execute::<BoolArray>(&mut ctx)?.into_array(),
                DType::Utf8(_) => field.execute::<VarBinViewArray>(&mut ctx)?.into_array(),
                DType::Extension(_) => field.execute::<ExtensionArray>(&mut ctx)?.into_array(),
                _ => field.execute::<PrimitiveArray>(&mut ctx)?.into_array(),
            };
            for row in 0..batch.len() {
                let got = decoded.execute_scalar(row, &mut ctx)?;
                let want = original.execute_scalar(offset + row, &mut ctx)?;
                assert_eq!(
                    got.is_valid(),
                    want.is_valid(),
                    "{name} row {} validity",
                    offset + row
                );
                if want.is_null() {
                    continue;
                }
                match original.dtype() {
                    DType::Primitive(PType::F32, _) => assert_eq!(
                        got.as_primitive().as_::<f32>().unwrap().to_bits(),
                        want.as_primitive().as_::<f32>().unwrap().to_bits(),
                        "{name} row {} bits",
                        offset + row,
                    ),
                    DType::Primitive(PType::F64, _) => assert_eq!(
                        got.as_primitive().as_::<f64>().unwrap().to_bits(),
                        want.as_primitive().as_::<f64>().unwrap().to_bits(),
                        "{name} row {} bits",
                        offset + row,
                    ),
                    _ => assert_eq!(
                        got.value(),
                        want.value(),
                        "{name} row {} value",
                        offset + row
                    ),
                }
            }
        }
        offset += batch.len();
    }
    assert_eq!(offset, fixture.source[0].len());
    Ok(())
}

fn main() -> Result<()> {
    let out = PathBuf::from(
        env::args_os()
            .nth(1)
            .or_else(|| env::var_os("VORTEX_TESTDATA_DIR"))
            .expect("output directory argument or VORTEX_TESTDATA_DIR"),
    );
    fs::create_dir_all(&out)?;
    let rt = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(rt.handle());
    session.enable_edition(PREVIEW_2026_06_0)?;
    for fixture in [
        scalars(),
        nullable(),
        temporal_delta(&session)?,
        alp_flat()?,
    ] {
        let rows = fixture.source[0].len();
        let table = StructArray::try_new(
            fixture.names.clone().into(),
            fixture.stored.clone(),
            rows,
            Validity::NonNullable,
        )?;
        let path = out.join(format!("{}.vortex", fixture.name));
        let options = if fixture.preserve_encoding {
            let flat = Arc::new(FlatLayoutStrategy::default());
            session
                .write_options()
                .with_strategy(Arc::new(TableStrategy::new(flat.clone(), flat)))
        } else {
            session.write_options()
        };
        let mut writer = options
            .blocking(&rt)
            .writer(File::create(&path)?, table.dtype().clone());
        writer.push(table.into_array())?;
        let summary = writer.finish()?;
        assert_eq!(summary.row_count() as usize, rows);
        verify(&path, &fixture, &session, &rt)?;
        println!(
            "{}: {rows} rows, {} bytes; reopened source values, IEEE bits, nulls and required encodings verified",
            path.display(),
            summary.size()
        );
    }
    Ok(())
}
