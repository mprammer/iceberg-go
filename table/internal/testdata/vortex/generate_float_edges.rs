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

use vortex::array::arrays::{PrimitiveArray, StructArray};
use vortex::array::validity::Validity;
use vortex::array::IntoArray;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).expect("usage: generate_float_edges OUTPUT");
    let values64 = [
        Some(f64::from_bits(0xfff8000000000000)),
        Some(f64::from_bits(0x7ff8000000000000)),
        Some(f64::NEG_INFINITY), Some(-1.0), Some(-0.0), Some(0.0), Some(1.0),
        Some(f64::INFINITY), Some(f64::from_bits(0x7ff8000000000001)), None,
    ];
    let values32 = [
        Some(f32::from_bits(0xffc00000)), Some(f32::from_bits(0x7fc00000)),
        Some(f32::NEG_INFINITY), Some(-1.0), Some(-0.0), Some(0.0), Some(1.0),
        Some(f32::INFINITY), Some(f32::from_bits(0x7fc00001)), None,
    ];
    let arr = StructArray::try_new(
        ["id", "value64", "value32"].into(),
        vec![
            PrimitiveArray::from_iter(0..values64.len() as i32).into_array(),
            PrimitiveArray::from_option_iter(values64).into_array(),
            PrimitiveArray::from_option_iter(values32).into_array(),
        ],
        values64.len(), Validity::NonNullable,
    )?;
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut writer = session.write_options().blocking(&runtime)
        .writer(File::create(path)?, arr.dtype().clone());
    writer.push(arr.into_array())?;
    writer.finish()?;
    Ok(())
}
