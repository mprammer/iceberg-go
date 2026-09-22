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
// Generate valid Zstd files whose legacy metadata omits per-frame value counts.
use std::{
    env,
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
};
use vortex::{
    VortexSessionDefault,
    array::{
        ArrayRef, IntoArray, VortexSessionExecute,
        arrays::{PrimitiveArray, StructArray, VarBinViewArray},
        dtype::{DType, Nullability},
        validity::Validity,
    },
    encodings::zstd::{Zstd, ZstdArray, ZstdData},
    file::WriteOptionsSessionExt,
    io::{
        runtime::{BlockingRuntime, current::CurrentThreadRuntime},
        session::RuntimeSessionExt,
    },
    layout::layouts::{flat::writer::FlatLayoutStrategy, table::TableStrategy},
    session::VortexSession,
};

fn omit_counts(array: ZstdArray, mixed: bool) -> Result<ArrayRef, Box<dyn std::error::Error>> {
    let mut parts = array.data().clone().into_parts(array.validity()?);
    for (i, frame) in parts.metadata.frames.iter_mut().enumerate() {
        if !mixed || i % 2 == 0 {
            frame.n_values = 0;
        }
    }
    Ok(Zstd::try_new(
        array.dtype().clone(),
        ZstdData::new(parts.dictionary, parts.frames, parts.metadata, parts.n_rows),
        parts.validity,
    )?
    .into_array())
}

fn text(row: usize) -> String {
    if row % 13 == 0 {
        String::new()
    } else {
        format!("value-{row:04}-abcdefghijklmnopqrstuv-\u{03bb}")
    }
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
    for n in [1051, 0] {
        let mut fields: Vec<(&str, ArrayRef)> = Vec::new();
        let ints = PrimitiveArray::from_option_iter(
            (0..n).map(|i| (i % 11 >= 3).then_some(i as i64 * 19 - 1000)),
        );
        fields.push((
            "i64_nullable",
            omit_counts(Zstd::from_primitive(&ints, 3, 31, &mut ctx)?, false)?,
        ));
        fields.push((
            "i64_mixed",
            omit_counts(Zstd::from_primitive(&ints, 3, 31, &mut ctx)?, true)?,
        ));
        let ints = PrimitiveArray::from_iter((0..n).map(|i| i as i64 * 19 - 1000));
        fields.push((
            "i64_required",
            omit_counts(Zstd::from_primitive(&ints, 3, 31, &mut ctx)?, false)?,
        ));
        let floats = PrimitiveArray::from_option_iter((0..n).map(|i| {
            (i % 11 >= 3).then_some(f64::from_bits(
                [
                    0x8000000000000000,
                    0x7ff8000000001234,
                    0x7ff0000000000000,
                    0xfff0000000000000,
                    1,
                ][i % 5],
            ))
        }));
        fields.push((
            "f64_nullable",
            omit_counts(Zstd::from_primitive(&floats, 3, 31, &mut ctx)?, false)?,
        ));
        let strings = VarBinViewArray::from_iter(
            (0..n).map(|i| (i % 11 >= 3).then(|| text(i))),
            DType::Utf8(Nullability::Nullable),
        );
        fields.push((
            "utf8_nullable",
            omit_counts(
                Zstd::from_var_bin_view(&strings, 3, n.max(1), &mut ctx)?,
                false,
            )?,
        ));
        let strings = VarBinViewArray::from_iter(
            (0..n).map(|i| Some(text(i))),
            DType::Utf8(Nullability::NonNullable),
        );
        fields.push((
            "utf8_required",
            omit_counts(
                Zstd::from_var_bin_view(&strings, 3, n.max(1), &mut ctx)?,
                false,
            )?,
        ));
        let binary = VarBinViewArray::from_iter(
            (0..n).map(|i| (i % 11 >= 3).then(|| vec![0, 255, (i % 256) as u8])),
            DType::Binary(Nullability::Nullable),
        );
        fields.push((
            "binary_nullable",
            omit_counts(
                Zstd::from_var_bin_view(&binary, 3, n.max(1), &mut ctx)?,
                false,
            )?,
        ));
        let ints = PrimitiveArray::from_option_iter((0..n).map(|_| None::<i64>));
        fields.push((
            "i64_all_null",
            omit_counts(Zstd::from_primitive(&ints, 3, 31, &mut ctx)?, false)?,
        ));
        let strings = VarBinViewArray::from_iter(
            (0..n).map(|_| None::<String>),
            DType::Utf8(Nullability::Nullable),
        );
        fields.push((
            "utf8_all_null",
            omit_counts(
                Zstd::from_var_bin_view(&strings, 3, n.max(1), &mut ctx)?,
                false,
            )?,
        ));
        let (names, arrays): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
        let array = StructArray::try_new(names.into(), arrays, n, Validity::NonNullable)?;
        let flat = Arc::new(FlatLayoutStrategy::default());
        let strategy = Arc::new(TableStrategy::new(flat.clone(), flat));
        let name = if n == 0 {
            "zstd_legacy_empty.vortex"
        } else {
            "zstd_legacy.vortex"
        };
        let mut writer = session
            .write_options()
            .with_strategy(strategy)
            .blocking(&rt)
            .writer(File::create(out.join(name))?, array.dtype().clone());
        writer.push(array.into_array())?;
        writer.finish()?;
        println!("{name}: {n} rows");
    }
    Ok(())
}
