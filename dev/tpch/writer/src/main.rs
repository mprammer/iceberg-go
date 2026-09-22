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

use std::error::Error;
use std::fs::File;
use std::path::Path;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::NamedTempFile;
use vortex::VortexSessionDefault;
use vortex::arrow::ArrowSessionExt;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

const REVISION: &str = "01f23e7f7b0a496a44929afbe8c6f10eca427625";
const BATCH_ROWS: usize = 65_536;

fn convert(input: &Path, output: &Path) -> Result<(u64, u64), Box<dyn Error>> {
    if output.exists() && input.canonicalize()? == output.canonicalize()? {
        return Err("input and output must be different files".into());
    }
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(input)?)?.with_batch_size(BATCH_ROWS);
    let schema = builder.schema().clone();
    let reader = builder.build()?;
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let dtype = session.arrow().from_arrow_schema(schema.as_ref())?;
    let directory = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = NamedTempFile::new_in(directory)?;
    let mut writer = session
        .write_options()
        .blocking(&runtime)
        .writer(temporary.as_file_mut(), dtype);
    let mut rows = 0;
    for batch in reader {
        let batch = batch?;
        rows += batch.num_rows() as u64;
        // Import Arrow's logical types directly: dates remain dates and decimals
        // retain their integer values, precision, and scale. Use default compression.
        writer.push(session.arrow().from_arrow_record_batch(batch, schema.as_ref())?)?;
    }
    let summary = writer.finish()?;
    if summary.row_count() != rows {
        return Err(format!(
            "writer produced {} rows, expected {rows}",
            summary.row_count()
        )
        .into());
    }
    temporary.persist(output)?;
    Ok((summary.row_count(), summary.size()))
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    if arguments.len() == 1 && arguments[0] == "--version" {
        println!("iceberg-tpch-vortex-writer Vortex {REVISION}");
        return Ok(());
    }
    if arguments.len() != 2 {
        return Err("usage: iceberg-tpch-vortex-writer INPUT.parquet OUTPUT.vortex".into());
    }
    let (rows, bytes) = convert(Path::new(&arguments[0]), Path::new(&arguments[1]))?;
    println!(
        "{rows} rows, {bytes} bytes: {}",
        Path::new(&arguments[1]).display()
    );
    Ok(())
}

#[cfg(test)]
mod tests;
