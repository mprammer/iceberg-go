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

# Reading Vortex data files

Iceberg-Go's Vortex integration is read-only. Both backends use the same Iceberg
schema mapping, delete handling, residual filtering, and file registration path.
Backend selection is application configuration; files and manifests still use
one `VORTEX` format identifier.

`VORTEX` is an experimental extension to this checkout's Iceberg file-format
support. Interoperation requires consumers that understand that format identifier.

## Select a reader

The default reader is pure Go:

```go
import "github.com/apache/iceberg-go/vortex"

ctx := vortex.WithBackend(context.Background(), vortex.Native)
scan := tbl.Scan(
    table.WithSelectedFields("name"),
    table.WithRowFilter(iceberg.LessThan(iceberg.Reference("id"), int32(100))),
)
result, err := scan.ToArrowTable(ctx)
// Handle err, then release result when finished.
```

Use `vortex.FFI` with the same context API to select the Rust-backed reader.
Pass that context to `AddFiles` too when registering existing files. Selection
is scoped to the context, so different callers can use different readers without
changing process-global state or table metadata.

`vortex.AvailableBackends()` reports compiled backends. Selecting an unknown
backend, or FFI in a binary that does not include it, returns an error on open.
The FFI backend is enabled by `-tags vortex` with cgo enabled.

## Reader contract and scope

Both readers open data through the caller's `iceio.IO` and its `io.ReaderAt`,
using the configured object-store credentials and endpoints. The shared adapter
owns the file until the backend closes. It verifies the exact physical row count,
resolves Iceberg field IDs through the file schema/name mapping, and exposes Arrow
record batches in file order.

The native backend currently accepts scalar schemas containing boolean, int32,
int64, float32, float64, UTF-8, binary, Date32 (days), and Decimal128 fields
(precision 1–38, scale 0–precision). It preserves nulls and reports unsupported
types, layouts, and encodings as errors. It is not a general replacement for every
Rust Vortex feature. Nested schemas and logical extensions other than Date32
remain outside this MVP. The FFI reader delegates decoding to Rust; the shared
adapter projects at the top-level field boundary and normalizes Arrow view types.

The [default-writer corpus](table/internal/testdata/vortex/README.md#current-rust-regression-corpus)
uses the pinned Rust writer's default compression and layout settings for five
scalar input distributions. Tests check every original value and null through
both readers, including exact float bits, and exercise full-table and individual
column projections. This records compatibility with the tested writer revision
and selected encodings; it does not cover arbitrary future writer choices.

Native ALP decoding supports float32 and float64 exceptions with both legacy flat
and current chunked patch metadata, including sliced arrays. It applies absolute
patch indices directly during full decoding and preserves exceptional values' bits,
including signed zero and NaN payloads. The bundled FFI patch also preserves the
patch offset needed to read these sliced arrays correctly. Native ALPRD decoding
also preserves float bits and nulls. Sparse dictionary codes, ZigZag integers,
nullable FastLanes run-length indices, and patched bitpacked integer outliers
are covered by the default-writer corpus.

Readers check cancellation before emitting batches and around storage reads. A
blocking `ReaderAt` supplied by a caller must itself support interruption to stop
an already-running read promptly.

The native reader fetches data during iteration and releases consumed chunk
caches. Each source read is at most 64 KiB; metadata and encoded data segments
are limited to 64 MiB and 256 MiB respectively. Canonical decoding has a 512 MiB
allocation budget per array tree. These are bounds on individual operations,
not a total process-memory limit: active column chunks, dictionaries, and Arrow
output batches can coexist.

## Filtering

The division of responsibility matches the existing Parquet scan pipeline:

1. Iceberg uses partition and manifest/file metrics during planning.
2. The selected backend may prune data while reading.
3. Iceberg applies deletes and the residual row predicate to Arrow batches, then
   returns the selected output columns and enforces the result limit.

The native backend pushes integer comparisons joined by AND to the reader's
min/max zone pruning. Other predicates remain in Iceberg. Unsupported or missing
zone statistics keep the data. The native reader compares integers exactly;
it never rounds int64 bounds through float64. It reads both legacy statistics
and version-1 `vortex.zoned` integer min/max and null-count aggregates. Unknown
aggregate functions, options, or statistics metadata disable pruning for that
column. Physical chunks are skipped only when every overlapping zone is ruled out.

The FFI backend can translate more predicates into Vortex expressions; Rust can
both prune and filter rows. Iceberg still applies the complete residual predicate.
Untranslatable expression parts are handled conservatively. Predicates involving
missing fields with non-null initial defaults disable early pruning.

Floating-point comparisons stay in Arrow: Vortex's comparison semantics differ
for signed zero and NaN, so directly pushing these comparisons could discard
matching rows. Floating-point set membership uses a separate translation;
null and NaN predicates fall back wherever their semantics are not established.
Projection supports int32-to-int64 and float32-to-float64 promotion. Predicates
on those promoted physical columns return `ErrNotImplemented` when reading a
Vortex file because the shared Iceberg residual cannot preserve their logical
comparison type. Existing Iceberg planning limitations also apply: older
4-byte column bounds can fail evaluation after promotion to a 64-bit type.

Both backends preserve original file positions while pruning, so positional
deletes, deletion vectors, and synthesized row IDs can use the same pipeline
ordering as Parquet. The native reader supplies original batch offsets; the FFI
reader projects physical row indices alongside filtered data and removes that
helper column before returning user data. Position metadata is checked against
the physical file row count and batch length. Each position-dependent pipeline
step reads the same positions independently. A result limit is never pushed
below Iceberg's delete or residual-filter steps.

This matches Parquet's ability to prune while applying positional deletes; it
does not promise identical pruning granularity or support for every predicate
that Parquet statistics can handle. Unsupported Vortex predicates retain the
existing conservative fallback to Iceberg's residual filter.

## Registering files and statistics

`AddFiles` and `FileToDataFile` record the real `VORTEX` format and verified row
count. Both backends stream the required columns without filters to compute
file-wide value, null and NaN counts and lower/upper bounds for top-level boolean,
integer, floating-point, string and binary fields. Bounds exclude nulls and NaNs;
floating-point bounds distinguish negative and positive zero. The existing
`write.metadata.metrics.default` and per-column metrics settings control which
metrics are stored, including string/binary truncation. These manifest metrics
participate in Iceberg's file pruning.

Registration performs a data scan when metrics or partition sources require it;
it is not a footer-only operation. Unsupported nested/logical field metrics,
compressed column sizes and split offsets are omitted. A field missing from the
physical file does not receive fabricated column metrics. Registration and scans
use the table name mapping to resolve aliases and reused names; an in-file field
ID takes precedence over that mapping.

Partitioned registration verifies that every row produces the same partition
tuple, using full source values independently of stored metrics and their
truncation. Applicable identity, truncate, bucket and void transforms are
supported for the scalar types above. Mixed partitions, including a mixture of
null and non-null partition values, are rejected. Missing optional partition
sources without an initial default use null. A missing source with a non-null
initial default returns `ErrNotImplemented`. Unsupported partition sources and
integer truncation that overflows the existing Iceberg transform are rejected.
Existing public registration rejects zero-row files;
the partition collector also rejects an empty file with a non-void partition.

Schema-default serialization, schema evolution, partition projection, and metrics
planning retain the existing Iceberg behavior. This integration does not repair
those shared paths. In particular, top-level names containing dots receive
counts-only metrics under the existing metrics planner.

Writing Vortex data files is not implemented.

## Immediate MVP priorities

1. Verify both readers against representative output from the pinned current Rust
   writer using its default compression settings, and close scalar decoding gaps.
2. Export verified file-wide registration metrics and support partitioned imports
   when partition values can be established correctly.
3. Validate clean downstream builds and the CI workflow, and provide a runnable
   registration and scan example for both backends.
4. Preserve original file positions while pruning with positional deletes,
   deletion vectors, and synthesized row lineage. Both backends now carry these
   positions through the adapter; differential tests cover delete application,
   projection, result limits, and avoided data reads against Parquet.

Nested schemas, logical extensions other than Date32, additional pushed predicates,
and writing Vortex data files through Iceberg-Go remain outside these immediate
priorities.

The [TPC-H correctness harness](dev/tpch/README.md) is a runnable registration,
commit, reload, and scan example for Parquet and both Vortex backends. It compares
all eight generated tables exactly and executes the 22 queries in DuckDB over
exported Arrow data. It does not push SQL predicates into Iceberg-Go.

## Building the FFI backend

The FFI uses an explicitly pinned Rust revision plus the checked-in ReaderAt,
row-index, and projection extensions. An arbitrary system `libvortex_ffi` is not
an interchangeable dependency. Build the compatible library using:

```sh
internal/vortexffi/build-lib.sh /tmp/iceberg-vortex-ffi
```

See [the binding README](internal/vortexffi/README.md) for prerequisites, output
paths, link flags, and the pin/patch update procedure. The default pure-Go build
requires none of these Rust tools or libraries.

## Native reader source

The native reader is the independent Go module
[`github.com/mprammer/vortex-go`](https://github.com/mprammer/vortex-go), consumed
through its public Arrow record-reader API. Iceberg owns field mapping,
registration metrics, expression translation, delete application, and the shared
scan semantics. The reader owns Vortex parsing, codecs, layouts, and conservative
integer min/max pruning. Physical batch offsets flow through the adapter so
positional deletes and row lineage retain their original file positions.

The development reader repository is currently private. Fetching this pin
requires a GitHub account with read access and `GOPRIVATE=github.com/mprammer/vortex-go`.
For a developer whose GitHub SSH access is already configured, this command
uses that access for the module download without changing global Git settings:

```sh
GOPRIVATE=github.com/mprammer/vortex-go \
GIT_CONFIG_COUNT=1 \
GIT_CONFIG_KEY_0=url.git@github.com:.insteadOf \
GIT_CONFIG_VALUE_0=https://github.com/ \
GOWORK=off go mod download github.com/mprammer/vortex-go
```

Ordinary public CI cannot fetch a private dependency with its default token.
Making the reader public or configuring an approved cross-repository credential
is a prerequisite for upstream distribution; that decision is pending.

The integration has no bundled native reader or writer. Rust-generated fixtures
exercise both native and FFI backends against independent original-value oracles.

## Validation

The shared conformance suite runs the same Rust-written fixture through every
compiled backend. It covers registration, projection, per-row nulls, filter-only
columns, missing defaults, empty results, limits, positional/equality deletes,
and row lineage. Internal tests cover reader lifecycle, cancellation, storage
errors, and zero-column projections. The FFI and native packages have additional
backend regression tests.

```sh
# Pure-Go integration, without cgo.
CGO_ENABLED=0 go test ./table/... ./vortex -run Vortex -count=1

# Both backends, after setting the library flags from the binding README.
go test -tags vortex ./table/... ./vortex -run Vortex -count=1

# All binding regressions, without filtering out their test names.
go test -tags vortex ./internal/vortexffi -count=1

# Standalone reader regressions (from its own repository).
go test ./...
```

See the [binding README](internal/vortexffi/README.md) for generating the large
fixture and running the I/O and lifecycle tests with the race detector. The
[edge-case fixture sources](table/internal/testdata/vortex/README.md) document
the independent Rust inputs used by the floating-point and empty-file tests.
