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
    let path = std::env::args().nth(1).expect("usage: generate_empty OUTPUT");
    let arr = StructArray::try_new(
        ["id", "value"].into(),
        vec![
            PrimitiveArray::from_iter(std::iter::empty::<i32>()).into_array(),
            PrimitiveArray::from_iter(std::iter::empty::<f64>()).into_array(),
        ],
        0, Validity::NonNullable,
    )?;
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let writer = session.write_options().blocking(&runtime)
        .writer(File::create(path)?, arr.dtype().clone());
    // No arrays are pushed: scalar columns have empty chunked layouts.
    writer.finish()?;
    Ok(())
}
