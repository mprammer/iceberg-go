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

// Ordinary canonical inputs: never override compression, layout, or statistics.
use std::{
    env,
    fs::{self, File},
    ops::Range,
    path::{Path, PathBuf},
};
use vortex::{
    VortexSessionDefault,
    array::{
        ArrayRef, IntoArray, VortexSessionExecute,
        arrays::{
            BoolArray, ChunkedArray, PrimitiveArray, StructArray, VarBinArray, VarBinViewArray,
            struct_::StructArrayExt,
        },
        dtype::{DType, Nullability},
        validity::Validity,
    },
    file::{OpenOptionsSessionExt, WriteOptionsSessionExt},
    io::{
        runtime::{BlockingRuntime, current::CurrentThreadRuntime},
        session::RuntimeSessionExt,
    },
    session::VortexSession,
};

const ROWS: usize = 8193;
const NAMES: [&str; 8] = ["row_id", "flag", "v32", "v64", "f32", "f64", "text", "blob"];
const CHUNKS: [usize; 8] = [997, 1021, 2053, 683, 4093, 1237, 509, 1543];
const SPECIAL32: [u32; 10] = [
    0x80000000, 0, 0x7fc01234, 0xffc05678, 0x7f800000, 0xff800000, 1, 0x80000001, 0x7f7fffff,
    0x00800000,
];
const SPECIAL64: [u64; 10] = [
    0x8000000000000000,
    0,
    0x7ff8000000001234,
    0xfff8000000005678,
    0x7ff0000000000000,
    0xfff0000000000000,
    1,
    0x8000000000000001,
    0x7fefffffffffffff,
    0x0010000000000000,
];

fn mode(case: usize, row: usize) -> usize {
    if case == 3 { (row / 2048) % 3 } else { case }
}
fn valid(case: usize, col: usize, row: usize) -> bool {
    col == 0 || (!(case == 3 && (3072..4096).contains(&row)) && (row + col * 3) % 29 != 0)
}
fn hash(row: usize) -> u64 {
    let mut x = (row as u64).wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
fn flag(case: usize, row: usize) -> bool {
    match mode(case, row) {
        0 => true,
        1 | 4 => (row / 127) % 2 == 0,
        _ => hash(row) & 1 != 0,
    }
}
fn value32(case: usize, row: usize) -> i32 {
    match mode(case, row) {
        0 => -77,
        1 => row as i32 - 4096,
        4 => match row % 1009 {
            0 => i32::MIN,
            1 => i32::MAX,
            _ => 1000 + (row % 17) as i32,
        },
        _ => match row % 257 {
            0 => i32::MIN,
            1 => i32::MAX,
            _ => hash(row) as i32,
        },
    }
}
fn value64(case: usize, row: usize) -> i64 {
    match mode(case, row) {
        0 => (1_i64 << 53) + 1,
        1 => i64::MIN + row as i64 * 17,
        4 => match row % 1009 {
            0 => i64::MIN,
            1 => i64::MAX,
            _ => (1_i64 << 45) + (row % 31) as i64,
        },
        _ => match row % 257 {
            0 => i64::MIN,
            1 => i64::MAX,
            _ => hash(row) as i64,
        },
    }
}
fn float32(case: usize, row: usize) -> f32 {
    match mode(case, row) {
        0 => 1.25,
        1 => ((row % 2001) as f32 - 1000.0) / 10.0,
        4 => f32::from_bits(if row % 1021 < SPECIAL32.len() {
            SPECIAL32[row % 1021]
        } else {
            0x3f800000 | (hash(row) as u32 & 0x007fffff)
        }),
        _ => f32::from_bits(if row % 257 < SPECIAL32.len() {
            SPECIAL32[row % 257]
        } else {
            hash(row) as u32 & 0xff7fffff
        }),
    }
}
fn float64(case: usize, row: usize) -> f64 {
    match mode(case, row) {
        0 => -3.5,
        1 => ((row % 2001) as f64 - 1000.0) / 10.0,
        4 => f64::from_bits(if row % 1021 < SPECIAL64.len() {
            SPECIAL64[row % 1021]
        } else {
            0x3ff0000000000000 | (hash(row) & 0x000fffffffffffff)
        }),
        _ => f64::from_bits(if row % 257 < SPECIAL64.len() {
            SPECIAL64[row % 257]
        } else {
            hash(row) & 0xffefffffffffffff
        }),
    }
}
fn text(case: usize, row: usize) -> String {
    match mode(case, row) {
        0 => "constant string longer than twelve bytes".into(),
        1 | 4 => [
            "",
            "a",
            "iceberg",
            "vortex dictionary entry",
            "東京",
            "éclair",
            "🙂",
            "a\0b",
        ][row % 8]
            .into(),
        _ => match row % 4 {
            0 => String::new(),
            1 => format!("s{row}"),
            2 => format!("row={row:05};hash={:016x};東京;", hash(row)),
            _ => format!(
                "{}:{row}:{:016x}",
                "shared long prefix/".repeat(12),
                hash(row)
            ),
        },
    }
}
fn blob(case: usize, row: usize) -> Vec<u8> {
    match mode(case, row) {
        0 => vec![0, 255, 128, 1, 0, 7],
        1 | 4 => vec![0, (row % 7) as u8, 255],
        _ => {
            let h = hash(row);
            (0..[0, 3, 19, 131][row % 4])
                .map(|i| ((h >> ((i % 8) * 8)) as u8).wrapping_add(i as u8))
                .collect()
        }
    }
}
fn column(case: usize, col: usize, rows: Range<usize>) -> ArrayRef {
    match col {
        0 => PrimitiveArray::from_iter(rows.map(|r| r as i64)).into_array(),
        1 => BoolArray::from_iter(rows.map(|r| valid(case, col, r).then(|| flag(case, r))))
            .into_array(),
        2 => PrimitiveArray::from_option_iter(
            rows.map(|r| valid(case, col, r).then(|| value32(case, r))),
        )
        .into_array(),
        3 => PrimitiveArray::from_option_iter(
            rows.map(|r| valid(case, col, r).then(|| value64(case, r))),
        )
        .into_array(),
        4 => PrimitiveArray::from_option_iter(
            rows.map(|r| valid(case, col, r).then(|| float32(case, r))),
        )
        .into_array(),
        5 => PrimitiveArray::from_option_iter(
            rows.map(|r| valid(case, col, r).then(|| float64(case, r))),
        )
        .into_array(),
        6 => VarBinArray::from_iter(
            rows.map(|r| valid(case, col, r).then(|| text(case, r))),
            DType::Utf8(Nullability::Nullable),
        )
        .into_array(),
        7 => VarBinArray::from_iter(
            rows.map(|r| valid(case, col, r).then(|| blob(case, r))),
            DType::Binary(Nullability::Nullable),
        )
        .into_array(),
        _ => unreachable!(),
    }
}
fn check(
    path: &Path,
    case: usize,
    session: &VortexSession,
    rt: &CurrentThreadRuntime,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = session.open_options().open_buffer(fs::read(path)?)?;
    let mut ctx = session.create_execution_ctx();
    let mut offset = 0;
    for chunk in file.scan()?.into_array_iter(rt)? {
        let chunk = chunk?.execute::<StructArray>(&mut ctx)?;
        for col in 0..NAMES.len() {
            let source = column(case, col, offset..offset + chunk.len());
            // Canonicalize once so each scalar access does not decompress a child.
            let field = chunk.unmasked_field(col).clone();
            let decoded = match col {
                1 => field.execute::<BoolArray>(&mut ctx)?.into_array(),
                6 | 7 => field.execute::<VarBinViewArray>(&mut ctx)?.into_array(),
                _ => field.execute::<PrimitiveArray>(&mut ctx)?.into_array(),
            };
            for i in 0..chunk.len() {
                let got = decoded.execute_scalar(i, &mut ctx)?;
                let want = source.execute_scalar(i, &mut ctx)?;
                assert_eq!(
                    got.is_valid(),
                    want.is_valid(),
                    "{} row {} null",
                    NAMES[col],
                    offset + i
                );
                if want.is_null() {
                    continue;
                }
                match col {
                    4 => assert_eq!(
                        got.as_primitive().as_::<f32>().unwrap().to_bits(),
                        want.as_primitive().as_::<f32>().unwrap().to_bits(),
                        "f32 row {}",
                        offset + i
                    ),
                    5 => assert_eq!(
                        got.as_primitive().as_::<f64>().unwrap().to_bits(),
                        want.as_primitive().as_::<f64>().unwrap().to_bits(),
                        "f64 row {}",
                        offset + i
                    ),
                    _ => assert_eq!(
                        got.value(),
                        want.value(),
                        "{} row {}",
                        NAMES[col],
                        offset + i
                    ),
                }
            }
        }
        offset += chunk.len();
    }
    assert_eq!(offset, ROWS);
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
    for (case, name) in ["repeated", "progression", "broad", "chunked", "precision"]
        .iter()
        .enumerate()
    {
        let mut columns = Vec::new();
        for col in 0..NAMES.len() {
            columns.push(if case == 3 {
                let parts = (0..ROWS)
                    .step_by(CHUNKS[col])
                    .map(|start| column(case, col, start..ROWS.min(start + CHUNKS[col])))
                    .collect::<Vec<_>>();
                println!(
                    "{name} {} input chunks: {:?}",
                    NAMES[col],
                    parts.iter().map(|a| a.len()).collect::<Vec<_>>()
                );
                ChunkedArray::try_new(parts.clone(), parts[0].dtype().clone())?.into_array()
            } else {
                column(case, col, 0..ROWS)
            });
        }
        let array = StructArray::try_new(NAMES.into(), columns, ROWS, Validity::NonNullable)?;
        let path = out.join(format!("default_{name}.vortex"));
        let mut writer = session
            .write_options()
            .blocking(&rt)
            .writer(File::create(&path)?, array.dtype().clone());
        writer.push(array.into_array())?;
        let summary = writer.finish()?;
        assert_eq!(summary.row_count(), ROWS as u64);
        println!(
            "{}: {} rows, {} bytes",
            path.display(),
            summary.row_count(),
            summary.size()
        );
        check(&path, case, &session, &rt)?;
        println!("  all original values, float bits, and nulls verified by Rust");
    }
    Ok(())
}
