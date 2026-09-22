<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Vortex reader fixtures

These files are written by the Rust Vortex implementation, independently of the
standalone Go decoder. The pinned writer revision is
`46a8d39c032b1e9f8ae28a13efe8abc9ebad556f`, matching the FFI build script.

- `simple.parquet` and `simple.vortex` contain the same 1,000-row reference data
  used by shared reader conformance tests.
- `float_edges.vortex` contains ten rows with both floating-point widths:
  negative and positive NaNs, negative infinity, -1, negative and positive zero,
  1, positive infinity, a second positive NaN payload, and null. The explicit bit
  patterns in `generate_float_edges.rs` make comparisons and membership tests
  sensitive to signed-zero and NaN-payload differences.
- `empty.vortex` contains a valid zero-row scalar schema, testing the direct
  reader independently of Iceberg's existing zero-record AddFiles restriction.
  Its writer is `generate_empty.rs`.

## Regenerate edge fixtures

After building the pinned Rust checkout with
`internal/vortexffi/build-lib.sh /tmp/iceberg-vortex-ffi`, run these commands from
the Iceberg-Go repository root. Use the same Cargo environment as the build.

```sh
repo_dir="$PWD"
build_dir=/tmp/iceberg-vortex-ffi
export CARGO_HOME="$build_dir/cargo-home"
export CARGO_TARGET_DIR="$build_dir/target"
cp table/internal/testdata/vortex/generate_float_edges.rs \
   "$build_dir/source/vortex-ffi/examples/iceberg_go_float_edges.rs"
cargo run --locked --manifest-path "$build_dir/source/Cargo.toml" \
  -p vortex-ffi --example iceberg_go_float_edges -- \
  "$repo_dir/table/internal/testdata/vortex/float_edges.vortex"
cp table/internal/testdata/vortex/generate_empty.rs \
   "$build_dir/source/vortex-ffi/examples/iceberg_go_empty.rs"
cargo run --locked --manifest-path "$build_dir/source/Cargo.toml" \
  -p vortex-ffi --example iceberg_go_empty -- \
  "$repo_dir/table/internal/testdata/vortex/empty.vortex"
```

The tests read the checked-in files; ordinary Go testing requires no Rust tools.
Run `go test ./table -run 'TestVortex(FloatPredicateSemantics|PromotedIntegerPredicates)'`
and repeat with `-tags vortex` and the documented library link flags to compare
both readers. The full floating-point predicate is always evaluated by Arrow;
the row-lineage scan used as the test oracle disables backend pushdown.

## Registration fixtures

`unicode_string.vortex` and `unicode_binary.vortex` each contain one required
`value` equal to the UTF-8 bytes of `éabc`, typed as string and binary respectively.
`dotted.vortex` contains one required string field named `a.b` with value `actual`.
These fixtures preserve the Unicode partition-promotion and ambiguous-name
registration regressions without a private Go writer.

Regenerate them through the pinned converter in `dev/tpch/writer`:

```sh
python table/internal/testdata/vortex/generate_registration.py \
  --writer /path/to/iceberg-tpch-vortex-writer table/internal/testdata/vortex
```

The script requires PyArrow and verifies the converter's Rust source pin before
writing. Ordinary Go tests read the checked-in output and require no Python or Rust.

## Current Rust regression corpus

`rust/` contains independent ALP, PCO, legacy-Zstd, zoned-statistics, date/decimal,
and default-writer fixtures. Their current source pin is
`46a8d39c032b1e9f8ae28a13efe8abc9ebad556f`. The Apache-licensed generators and
locked dependencies are in `generators/regressions`.

```sh
table/internal/testdata/vortex/generate.sh
```

The script regenerates the current codec fixtures using the pinned Rust source,
with `VORTEX_RUST_SOURCE` optionally naming an existing checkout and
`CARGO_TARGET_DIR` selecting a persistent build cache. `date_decimal.vortex` is
regenerated separately by `dev/tpch/writer/generate-types.py`; copy its
`boundary.vortex` output here. It contains eight exact Date32 and Decimal128(15,2)
rows, including signed limits, negative values, zero, and nulls.

Five `default_*.vortex` files each contain 8,193 rows of all supported scalar
types. The ordinary Rust writer chooses compression and layouts. Go tests compare
every valid value and null to independent source formulas, including float bits,
full scans, and individual column projections. `zoned_ints*.vortex` contain 4,099
rows over five physical zones, including an all-null zone and int64 values above
2^53. Integration checks compare values and physical row positions and require
fewer source reads and bytes when integer predicates prune zones. Physical wire
format and malformed-codec checks belong to the standalone reader's own tests.

## Independently generated scalar fixtures

`rust/owned_nullable.vortex` and `rust/owned_alp_flat.vortex` are generated by
`generators/regressions/src/bin/independent_corpus.rs`, using fresh source
arrays and the current pinned upstream Rust writer. They cover nullable
int32-to-int64 promotion and required/nullable flat-patch ALP float values.
Both native and FFI tests compare to original formulas and exact IEEE bits.
No archived inputs from a third-party Go reader are included.

## Generator ownership

`mprammer/vortex-go/dev/fixtures` is the canonical owner of the shared PCO,
Zstd, ALP, zoned-statistics, default-writer, and independent-corpus generators. The copies here are
synchronized with the standalone source revision pinned in `go.mod`
for Iceberg's combined native/FFI fixture run. Update the standalone
owner first, then synchronize the corresponding sources, Cargo.toml, and
Cargo.lock when updating the `go.mod` reader pin. Iceberg-only registration and
large date/decimal generators remain owned by Iceberg.
