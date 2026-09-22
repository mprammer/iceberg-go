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
// Generates independent wrapped-Pco fixtures, including exact source bit patterns.
// Run with an explicit output directory; the Go tests need no Rust installation.

use std::{env, fs, path::PathBuf};

use pco::{
    ChunkConfig, DeltaSpec, ModeSpec, PagingSpec,
    data_types::Number,
    metadata::{DeltaEncoding, Mode},
    wrapped::{FileCompressor, FileDecompressor},
};
use serde_json::{Value, json};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

trait RawBytes: Number {
    fn raw_bytes(self) -> Vec<u8>;
}
macro_rules! raw_bytes {
    ($($t:ty),*) => { $(impl RawBytes for $t {
        fn raw_bytes(self) -> Vec<u8> { self.to_le_bytes().to_vec() }
    })* };
}
raw_bytes!(u16, u32, u64, i16, i32, i64, f32, f64);

fn fixture<T: RawBytes>(
    name: &str,
    ty: &str,
    nums: &[T],
    expected: Vec<u8>,
    mode: ModeSpec,
    delta: DeltaSpec,
) -> Value {
    let config = ChunkConfig::default()
        .with_compression_level(8)
        .with_mode_spec(mode.clone())
        .with_delta_spec(delta.clone())
        // Pages shorter than delta state, exact ANS batch sizes, and tails.
        .with_paging_spec(PagingSpec::Exact(
            if matches!(delta, DeltaSpec::TryConv1(_)) {
                vec![6, 7, 249, 255, 256, 254]
            } else {
                vec![1, 2, 3, 255, 256, 257, 253]
            },
        ));
    let fc = FileCompressor::default();
    let mut header = Vec::new();
    fc.write_header(&mut header).unwrap();
    let mut cc = fc.chunk_compressor(nums, &config).unwrap();
    let wanted_mode = match mode {
        ModeSpec::Classic => 0,
        ModeSpec::TryIntMult(_) => 1,
        ModeSpec::TryFloatMult(_) => 2,
        ModeSpec::TryFloatQuant(_) => 3,
        ModeSpec::TryDict => 4,
        _ => panic!("explicit modes only"),
    };
    let actual_mode = match &cc.meta().mode {
        Mode::Classic => 0,
        Mode::IntMult(_) => 1,
        Mode::FloatMult(_) => 2,
        Mode::FloatQuant(_) => 3,
        Mode::Dict(_) => 4,
        _ => panic!("unknown mode"),
    };
    assert_eq!(
        actual_mode, wanted_mode,
        "{name}: compressor fell back from requested mode"
    );
    let wanted_delta = match delta {
        DeltaSpec::NoOp => 0,
        DeltaSpec::TryConsecutive(_) => 1,
        DeltaSpec::TryLookback => 2,
        DeltaSpec::TryConv1(_) => 3,
        _ => panic!("explicit deltas only"),
    };
    let actual_delta = match &cc.meta().delta_encoding {
        DeltaEncoding::NoOp => 0,
        DeltaEncoding::Consecutive { .. } => 1,
        DeltaEncoding::Lookback { .. } => 2,
        DeltaEncoding::Conv1(_) => 3,
        _ => panic!("unknown delta"),
    };
    assert_eq!(
        actual_delta, wanted_delta,
        "{name}: compressor fell back from requested delta"
    );
    let mut meta = Vec::new();
    cc.write_meta(&mut meta).unwrap();
    let (fd, _) = FileDecompressor::new(header.as_slice()).unwrap();
    let (mut cd, _) = fd.chunk_decompressor::<T, _>(meta.as_slice()).unwrap();
    let mut pages = Vec::new();
    let mut offset = 0;
    for (i, n) in cc.n_per_page().into_iter().enumerate() {
        let mut data = Vec::new();
        cc.write_page(i, &mut data).unwrap();
        let mut decoded = nums[offset..offset + n].to_vec();
        let mut pd = cd.page_decompressor(data.as_slice(), n).unwrap();
        assert!(pd.read(&mut decoded).unwrap().finished);
        let decoded_bytes: Vec<u8> = decoded.iter().flat_map(|x| x.raw_bytes()).collect();
        let width = std::mem::size_of::<T>();
        assert_eq!(
            decoded_bytes,
            expected[offset * width..(offset + n) * width],
            "{name}: Rust round-trip failed"
        );
        offset += n;
        pages.push(json!({"n": n, "data": hex(&data)}));
    }
    json!({"name": name, "type": ty, "mode": actual_mode, "delta": actual_delta,
        "header": hex(&header), "meta": hex(&meta), "pages": pages, "expected": hex(&expected)})
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = PathBuf::from(
        env::args_os()
            .nth(1)
            .or_else(|| env::var_os("VORTEX_TESTDATA_DIR"))
            .expect("output directory argument or VORTEX_TESTDATA_DIR"),
    );
    fs::create_dir_all(&out)?;
    let mut cases = Vec::new();
    let n = 1027;
    macro_rules! integers {
        ($t:ty, $tag:literal) => {{
            let edges: &[$t] = &[<$t>::MIN, <$t>::MAX, 0, 1, 7, 256 as $t, 257 as $t];
            for (mode_name, mode) in [
                ("classic", ModeSpec::Classic),
                ("mult", ModeSpec::TryIntMult(7)),
                ("dict", ModeSpec::TryDict),
            ] {
                let nums: Vec<$t> = (0..n)
                    .map(|i| {
                        if i < edges.len() {
                            edges[i]
                        } else {
                            (((i * 17) % 77) as $t).wrapping_mul(7)
                        }
                    })
                    .collect();
                let expected: Vec<u8> = nums.iter().flat_map(|x| x.to_le_bytes()).collect();
                cases.push(fixture(
                    &format!("{}_{}", $tag, mode_name),
                    $tag,
                    &nums,
                    expected,
                    mode,
                    DeltaSpec::NoOp,
                ));
            }
        }};
    }
    integers!(u16, "u16");
    integers!(u32, "u32");
    integers!(u64, "u64");
    integers!(i16, "i16");
    integers!(i32, "i32");
    integers!(i64, "i64");

    macro_rules! floats {
        ($t:ty, $bits:ty, $tag:literal, $precision:expr) => {{
            let gpi = (1_u64 << $precision) as $t;
            let edges: &[$t] = &[
                0.0,
                -0.0,
                1.0,
                -1.0,
                0.1,
                -0.1,
                gpi - 1.0,
                gpi,
                gpi + 2.0,
                -gpi,
                -(gpi + 2.0),
                <$t>::MAX,
                <$t>::MIN,
                <$t>::MIN_POSITIVE,
                -<$t>::MIN_POSITIVE,
                <$t>::from_bits(1),
                -<$t>::from_bits(1),
                <$t>::INFINITY,
                <$t>::NEG_INFINITY,
                <$t>::from_bits(<$t>::NAN.to_bits() | 0x123),
                -<$t>::from_bits(<$t>::NAN.to_bits() | 0x456),
                <$t>::from_bits(<$t>::INFINITY.to_bits() | 1),
            ];
            for (mode_name, mode) in [
                ("classic", ModeSpec::Classic),
                ("mult_small", ModeSpec::TryFloatMult(0.1)),
                ("mult_negative", ModeSpec::TryFloatMult(-0.1)),
                ("mult_large", ModeSpec::TryFloatMult(10.0)),
                ("quant_1", ModeSpec::TryFloatQuant(1)),
                ("quant_max", ModeSpec::TryFloatQuant($precision - 1)),
                ("dict", ModeSpec::TryDict),
            ] {
                let base = match mode {
                    ModeSpec::TryFloatMult(base) => base as $t,
                    _ => 0.125,
                };
                let nums: Vec<$t> = (0..n)
                    .map(|i| {
                        if i < edges.len() {
                            edges[i]
                        } else {
                            (((i * 17) % 77) as $t - 38.0) * base
                        }
                    })
                    .collect();
                let expected: Vec<u8> = nums.iter().flat_map(|x| x.to_le_bytes()).collect();
                cases.push(fixture(
                    &format!("{}_{}", $tag, mode_name),
                    $tag,
                    &nums,
                    expected,
                    mode,
                    DeltaSpec::NoOp,
                ));
            }
            for (delta_name, delta) in [
                ("consecutive_1", DeltaSpec::TryConsecutive(1)),
                ("consecutive_2", DeltaSpec::TryConsecutive(2)),
                ("consecutive_7", DeltaSpec::TryConsecutive(7)),
                ("lookback", DeltaSpec::TryLookback),
                ("conv1_6", DeltaSpec::TryConv1(6)),
            ] {
                if std::mem::size_of::<$t>() > 4 && matches!(delta, DeltaSpec::TryConv1(_)) {
                    continue;
                }
                for (mode_name, mode) in [
                    ("classic", ModeSpec::Classic),
                    ("mult", ModeSpec::TryFloatMult(0.1)),
                    ("quant", ModeSpec::TryFloatQuant(5)),
                ] {
                    // Smooth primary values with independently varying ULP adjustments.
                    let nums: Vec<$t> = (0..n)
                        .map(|i| {
                            let v = ((i as $t) - 500.0) * 0.1;
                            <$t>::from_bits(v.to_bits().wrapping_add((i * 31 % 3) as $bits))
                        })
                        .collect();
                    let expected: Vec<u8> = nums.iter().flat_map(|x| x.to_le_bytes()).collect();
                    cases.push(fixture(
                        &format!("{}_{}_{}", $tag, mode_name, delta_name),
                        $tag,
                        &nums,
                        expected,
                        mode,
                        delta.clone(),
                    ));
                }
            }
        }};
    }
    floats!(f32, u32, "f32", 24);
    floats!(f64, u64, "f64", 53);
    let path = out.join("pco_modes.json");
    fs::write(
        &path,
        serde_json::to_vec_pretty(&json!({"pco_version": "1.0.3", "cases": cases}))?,
    )?;
    println!("wrote {} cases to {}", cases.len(), path.display());
    Ok(())
}
