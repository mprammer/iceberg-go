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

use std::{
    env,
    fs::{self, File},
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
};
use vortex::{
    VortexSessionDefault,
    array::{
        IntoArray,
        arrays::{PrimitiveArray, StructArray},
        validity::Validity,
    },
    file::WriteOptionsSessionExt,
    io::{
        runtime::{BlockingRuntime, current::CurrentThreadRuntime},
        session::RuntimeSessionExt,
    },
    layout::{
        LayoutStrategy,
        layouts::{
            chunked::writer::ChunkedLayoutStrategy,
            flat::writer::FlatLayoutStrategy,
            repartition::{RepartitionStrategy, RepartitionWriterOptions},
            table::TableStrategy,
            zoned::writer::{ZonedLayoutOptions, ZonedStrategy},
        },
    },
    session::VortexSession,
};

const ZONE: usize = 1024;
const ROWS: usize = 4 * ZONE + 3;

fn valid(row: usize) -> bool {
    row / ZONE != 1 && ![2, 2068, 3101, 4097].contains(&row)
}

fn value32(row: usize) -> i32 {
    let within = (row % ZONE) as i32;
    match row / ZONE {
        0 => i32::MIN + within,
        1 => 0,
        2 => 7,
        3 => 1000 + within,
        4 => i32::MAX - 2 + within,
        _ => unreachable!(),
    }
}

fn value64(row: usize) -> i64 {
    let within = (row % ZONE) as i64;
    match row / ZONE {
        0 => i64::MIN + within,
        1 => 0,
        2 => 7,
        3 => (1_i64 << 53) + 1 + within,
        4 => i64::MAX - 2 + within,
        _ => unreachable!(),
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
    // Push one canonical primitive chunk per zone. The data child keeps those
    // physical boundaries, so a pruned zone really avoids its data segments.
    let chunks = (0..ROWS)
        .step_by(ZONE)
        .map(|start| {
            let end = ROWS.min(start + ZONE);
            let rows = start..end;
            let values32: Vec<_> = rows.clone().filter(|&r| valid(r)).map(value32).collect();
            let values64: Vec<_> = rows.clone().filter(|&r| valid(r)).map(value64).collect();
            println!(
                "zone {}: rows={start}..{end}, null_count={}, i32={:?}..{:?}, i64={:?}..{:?}",
                start / ZONE,
                rows.len() - values64.len(),
                values32.iter().min(),
                values32.iter().max(),
                values64.iter().min(),
                values64.iter().max()
            );
            StructArray::try_new(
                ["row_id", "v32", "v64"].into(),
                vec![
                    PrimitiveArray::from_iter(rows.clone().map(|r| r as i64)).into_array(),
                    PrimitiveArray::from_option_iter(
                        rows.clone().map(|r| valid(r).then(|| value32(r))),
                    )
                    .into_array(),
                    PrimitiveArray::from_option_iter(
                        rows.clone().map(|r| valid(r).then(|| value64(r))),
                    )
                    .into_array(),
                ],
                rows.len(),
                Validity::NonNullable,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let flat_stats: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
    let chunked_stats: Arc<dyn LayoutStrategy> = Arc::new(RepartitionStrategy::new(
        ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()),
        RepartitionWriterOptions {
            block_size_minimum: 0,
            block_len_multiple: 2,
            block_size_target: None,
            canonicalize: true,
        },
    ));
    for (name, stats) in [
        ("zoned_ints.vortex", flat_stats),
        ("zoned_ints_chunked_stats.vortex", chunked_stats),
    ] {
        let zoned = Arc::new(ZonedStrategy::new(
            ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()),
            stats,
            ZonedLayoutOptions {
                block_size: NonZeroUsize::new(ZONE).unwrap(),
                ..Default::default()
            },
        ));
        let strategy = Arc::new(TableStrategy::new(
            Arc::new(FlatLayoutStrategy::default()),
            zoned,
        ));
        let mut writer = session
            .write_options()
            .with_strategy(strategy)
            .blocking(&rt)
            .writer(File::create(out.join(name))?, chunks[0].dtype().clone());
        for chunk in &chunks {
            writer.push(chunk.clone().into_array())?;
        }
        let summary = writer.finish()?;
        assert_eq!(summary.row_count(), ROWS as u64);
        println!(
            "{name}: {} rows, {} bytes",
            summary.row_count(),
            summary.size()
        );
    }
    Ok(())
}
