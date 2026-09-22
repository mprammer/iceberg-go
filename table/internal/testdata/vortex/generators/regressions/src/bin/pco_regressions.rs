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
// Real Vortex Pco arrays produced by the pinned Rust implementation. Flat leaves
// preserve Pco encoding; the Go tests assert it before comparing every row.

use std::{
    env,
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
};
use vortex::{
    VortexSessionDefault,
    array::{
        IntoArray, VortexSessionExecute,
        arrays::{PrimitiveArray, StructArray},
        validity::Validity,
    },
    encodings::pco::Pco,
    file::WriteOptionsSessionExt,
    io::{
        runtime::{BlockingRuntime, current::CurrentThreadRuntime},
        session::RuntimeSessionExt,
    },
    layout::layouts::{flat::writer::FlatLayoutStrategy, table::TableStrategy},
    session::VortexSession,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = PathBuf::from(
        env::args_os()
            .nth(1)
            .or_else(|| env::var_os("VORTEX_TESTDATA_DIR"))
            .expect("output directory argument or VORTEX_TESTDATA_DIR"),
    );
    fs::create_dir_all(&out)?;
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut ctx = session.create_execution_ctx();
    let n = 8000;
    let mut names = Vec::new();
    let mut values = Vec::new();
    macro_rules! field {
        ($name:expr, $arr:expr) => {{
            let a = $arr;
            let p = Pco::from_primitive(a.as_view(), 8, 256, &mut ctx)?;
            names.push($name);
            values.push(p.into_array());
        }};
    }
    field!(
        "i64_nullable",
        PrimitiveArray::from_option_iter((0..n).map(|i| if i % 3 == 0 {
            None
        } else {
            Some((i * 7) as i64)
        }))
    );
    field!(
        "i64_all_null",
        PrimitiveArray::from_option_iter((0..n).map(|_| None::<i64>))
    );
    field!(
        "i64_all_valid",
        PrimitiveArray::from_option_iter((0..n).map(|i| Some((i * 7) as i64)))
    );
    field!(
        "i64_first",
        PrimitiveArray::from_option_iter((0..n).map(|i| if i == 0 {
            Some(i64::MIN)
        } else {
            None
        }))
    );
    field!(
        "i64_last",
        PrimitiveArray::from_option_iter((0..n).map(|i| if i == n - 1 {
            Some(i64::MAX)
        } else {
            None
        }))
    );
    field!(
        "i64_null_runs",
        PrimitiveArray::from_option_iter((0..n).map(|i| {
            if i < 257 || (510..800).contains(&i) || i == n - 1 {
                None
            } else {
                Some(-(i as i64) * 7)
            }
        }))
    );
    field!(
        "f64_positive",
        PrimitiveArray::from_iter((0..n).map(|i| ((i * 17) % 77) as f64 * 0.1))
    );
    field!(
        "f64_negative",
        PrimitiveArray::from_iter((0..n).map(|i| ((i * 17) % 77) as f64 * -0.1))
    );
    field!(
        "f64_nullable",
        PrimitiveArray::from_option_iter((0..n).map(|i| if i % 3 == 0 {
            None
        } else {
            Some(((i * 17) % 77) as f64 * 0.1)
        }))
    );
    field!(
        "f64_large",
        PrimitiveArray::from_iter((0..n).map(|i| 1e13 + ((i * 17) % 77) as f64 * 0.1))
    );
    field!(
        "f32_positive",
        PrimitiveArray::from_iter((0..n).map(|i| ((i * 17) % 77) as f32 * 0.1))
    );
    field!(
        "f32_negative",
        PrimitiveArray::from_iter((0..n).map(|i| ((i * 17) % 77) as f32 * -0.1))
    );
    field!(
        "f32_nullable",
        PrimitiveArray::from_option_iter((0..n).map(|i| if i % 3 == 0 {
            None
        } else {
            Some(((i * 17) % 77) as f32 * 0.1)
        }))
    );
    let st = StructArray::try_new(names.into(), values, n, Validity::NonNullable)?;
    let flat = Arc::new(FlatLayoutStrategy::default());
    let strategy = Arc::new(TableStrategy::new(flat.clone(), flat));
    let path = out.join("pco_regressions.vortex");
    let file = File::create(&path)?;
    let mut writer = session
        .write_options()
        .with_strategy(strategy)
        .blocking(&runtime)
        .writer(file, st.dtype().clone());
    writer.push(st.into_array())?;
    let summary = writer.finish()?;
    println!(
        "{} rows, {} bytes: {}",
        summary.row_count(),
        summary.size(),
        path.display()
    );
    Ok(())
}
