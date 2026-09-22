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

use std::fs::File;

use vortex::VortexSessionDefault;
use vortex::array::IntoArray;
use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
use vortex::array::validity::Validity;
use vortex::error::VortexResult;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

fn chunk(start: i32, rows: usize) -> VortexResult<StructArray> {
    let ids = PrimitiveArray::from_iter(start..start + rows as i32);
    let mut state = start as u64 + 0x9e3779b97f4a7c15;
    let mut text = || -> String {
        (0..128)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"[state as usize % 62] as char
            })
            .collect()
    };
    let left = VarBinViewArray::from_iter_str((0..rows).map(|_| text()));
    let right = VarBinViewArray::from_iter_str((0..rows).map(|_| text()));
    let score = PrimitiveArray::from_iter((start..start + rows as i32).map(|i| i as f64 * 1.5));
    StructArray::try_new(
        ["id", "blob1", "blob2", "score"].into(),
        vec![ids.into_array(), left.into_array(), right.into_array(), score.into_array()],
        rows,
        Validity::NonNullable,
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).ok_or("expected output .vortex path")?;
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let first = chunk(0, 12500)?;
    let file = File::create(&path)?;
    let mut writer = session.write_options().blocking(&runtime).writer(file, first.dtype().clone());
    writer.push(first.into_array())?;
    for idx in 1..8 {
        writer.push(chunk(idx * 12500, 12500)?.into_array())?;
    }
    let summary = writer.finish()?;
    println!("{} rows, {} bytes: {}", summary.row_count(), summary.size(), path);
    Ok(())
}
