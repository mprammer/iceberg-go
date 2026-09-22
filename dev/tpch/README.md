<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# TPC-H through Iceberg-Go

## Live SQL measurements

`live.py` executes the **unchanged 22 queries** from DuckDB's pinned `tpch`
extension. DuckDB requests Arrow streams from Iceberg-Go while each SQL query
runs. Each table occurrence gets a fresh scanner process; repeated aliases and
subqueries can scan the same table independently. No original table is imported
into DuckDB or written to an intermediate Arrow file before execution.

Use the prepared dataset layout described below and committed catalogs from
`scan -register-only` or a previous `perf.py` run. Select release builds for the
FFI scanner. With the Python dependencies from `requirements.txt` installed:

```sh
.cache/tpch/venv/bin/python dev/tpch/live.py \
  --dataset /path/on/disk/dataset \
  --catalogs /path/on/disk/committed-catalogs \
  --output /path/on/disk/new-live-results \
  --native-scanner .cache/tpch/scan-native \
  --ffi-scanner .cache/tpch/scan-ffi \
  --ffi-library .cache/tpch/perf-ffi/target/release/libvortex_ffi.so \
  --runs 5 --threads 4 --memory 8GB
```

The catalogs directory contains `parquet/`, `ffi/`, and `native/`, each with the
eight committed tables in namespace `tpch`. Their manifests must reference the
files in the supplied dataset: the Go scanner checks file identity. Rebuild the
scanner after adding `-stream`, `-columns`, and `-receipt` support.

Timing covers SQL execution, live scanner startup and metadata loading, projected
Iceberg source reads, Arrow IPC transport through pipes, SQL operators, result
fetching, scanner cleanup, DuckDB JSON profiling, and scan-lifecycle evidence
writes. It excludes registration, source generation, reference queries, and
correctness comparison. The source tables are never materialized
before a query; ordinary query execution may buffer joins or spill to disk.

DuckDB passes column selections into Iceberg-Go. Filters supplied by DuckDB are
evaluated by PyArrow identically for all three readers; remaining predicates run
in DuckDB. This bridge does **not** translate those predicates into Iceberg-Go
filters, so the results do not measure format-specific predicate pruning. It
adds common IPC/process overhead and is a benchmark adapter, not an in-process
production SQL connector. DuckDB's Arrow source also lacks row-count statistics,
so join planning uses unknown/default cardinality estimates; retained profiles
make that limitation visible. The same adapter and planner limitations apply to
all three readers.

One full warm-up suite precedes the five measured suites. Backend order rotates.
Every query result schema and duplicate-preserving row multiset is checked
against the unchanged SQL over the original DuckDB-generated tables; result
ordering is not separately checked. Decimal results are exact and floating
aggregates use the existing small tolerance. The report provides per-query and suite medians,
ranges, and every sample. Query profiles, result sets, and per-scan receipts
record requested columns, filters, rows, bytes, completion, and cancellation.
The runner verifies query text against `tpch_queries()` and records its hash.

This adapter deliberately pins DuckDB 1.4.4 and PyArrow 21.0.0. It uses the
Python client's [Dataset scanner dispatch](https://github.com/duckdb/duckdb-python/blob/v1.4.4/src/duckdb_py/arrow/arrow_array_stream.cpp),
which is not a general public custom-Dataset extension API. Changing versions
requires rerunning the protocol/lifecycle tests and the full query smoke test.
The runner re-executes under its selected CPU affinity before library thread
pools initialize. Its results describe a warm-cache, shared-machine experiment,
not an official TPC-H benchmark.

## Export correctness harness

This harness checks all eight TPC-H tables and all 22 queries through Iceberg-Go's
Parquet, Vortex FFI, and Vortex native readers. It uses a local Hadoop catalog;
no catalog service is needed. The default scale factor is 0.01 (60,175 lineitem
rows). Dates and `DECIMAL(15,2)` values retain their logical types and exact values.

This is an integration correctness workload, **not a TPC-H performance benchmark**.
DuckDB executes SQL after Iceberg-Go scans have been exported to Arrow IPC.
DuckDB's SQL predicates are not automatically translated into Iceberg-Go filters.
Three additional fixed cases call Iceberg-Go's filter API directly: an order-key
range with filter-only columns, an empty order-key match, and Q6's date/decimal
conjunction. Their exported row multisets must match independent SQL predicates
on the original data for every backend. The report
separates registration, scan plus IPC export, and SQL execution durations; it does
not measure isolated reader throughput. Delete files and schema evolution are
covered by the Go integration tests, not by this generated workload.

## Run

Run from the repository root. Keep the working directory on disk, not a tmpfs.
Python 3.11 or newer is required. Dependencies and generated data live under the ignored `.cache` directory.
The Rust converter uses the same pinned Vortex revision and patches as the FFI
reader, with the Rust default compression strategy.

```sh
uv venv --python 3.12 .cache/tpch/venv
uv pip install --python .cache/tpch/venv/bin/python -r dev/tpch/requirements.txt
# DuckDB's tpch extension is needed once per DuckDB installation/version.
.cache/tpch/venv/bin/python -c 'import duckdb; duckdb.connect().execute("INSTALL tpch")'

VORTEX_BUILD_PROFILE=dev bash dev/tpch/writer/build.sh .cache/tpch/writer
CGO_ENABLED=0 go build -o .cache/tpch/scan-native ./dev/tpch/scan
```

To build the FFI scanner, first build a compatible library following
[the FFI instructions](../../internal/vortexffi/README.md), or reuse an existing
build from this checkout's pinned revision and patches. For a new build:

```sh
VORTEX_BUILD_PROFILE=dev bash internal/vortexffi/build-lib.sh .cache/tpch/ffi
export CGO_LDFLAGS="-L$PWD/.cache/tpch/ffi/target/debug"
export LD_LIBRARY_PATH="$PWD/.cache/tpch/ffi/target/debug${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
CGO_ENABLED=1 go build -tags vortex -o .cache/tpch/scan-ffi ./dev/tpch/scan

.cache/tpch/venv/bin/python dev/tpch/run.py \
  --scanner .cache/tpch/scan-ffi \
  --writer .cache/tpch/writer/target/debug/iceberg-tpch-vortex-writer
```

The FFI-enabled scanner includes both Vortex backends; backend selection is
explicit for every registration and scan. To check a binary built without cgo:

```sh
.cache/tpch/venv/bin/python dev/tpch/run.py \
  --scanner .cache/tpch/scan-native \
  --writer .cache/tpch/writer/target/debug/iceberg-tpch-vortex-writer \
  --backends parquet native
```

`--backends parquet` needs no Rust converter or library. Other useful options are
`--scale 0.01` and `--work-dir /path/on/disk/tpch`. Every invocation creates a fresh
`run-*` directory and preserves its data, catalog, Arrow exports, logs, query
text, reference answers, and `report.json`. Nothing is reused from an earlier run.
The command exits nonzero if any requested backend fails; unavailable backends
are failures, not skips. The library path must remain set when running the FFI binary.

## What passes mean

1. DuckDB generates the data and supplies the 22 query texts. Its version and the
   query-text hash are recorded. Parquet and Vortex derive from the same generated
   rows; the original DuckDB tables remain the independent value oracle.
2. For each backend and table, the Go scanner creates a table, registers its file
   with `AddFiles`, commits, reloads durable metadata, and checks the manifest's
   file format and row count. The public scan API writes Arrow IPC.
3. Every exported table must have the expected logical schema and match the
   original row multiset **exactly**, including duplicate multiplicity. No float
   or decimal tolerance applies to this check.
4. DuckDB executes each query against the exported tables and compares result
   schemas and row multisets with the original tables' answers. Row order is not
   compared: scans may deliver batches in different orders, and SQL ties need
   not be ordered identically. Decimals, dates, integers, strings, and nulls are
   exact. Only floating-point SQL results permit `rel_tol=1e-12, abs_tol=1e-9`.
   SQL runs with one DuckDB thread for repeatability.

At tiny scale some query results may be empty. All 22 must still execute, and
exact whole-table comparisons provide coverage independent of query selectivity.
The fixed filtered cases check results, including conservative residual fallback;
they do not claim that every predicate was pushed down or that bytes were saved.
The Go position-sensitive pruning tests separately check avoided data reads.

This checks reader integration against the same DuckDB engine, not DuckDB's own
SQL implementation against an independently certified TPC answer set.

Native support added for this workload is Date32 (`vortex.date` in days) and
Decimal128 (precision 1–38 with Iceberg-compatible scale). Other logical types
and unsupported decimal representations continue to fail explicitly.

## Scan/export microbenchmarks

`perf.py` runs on Linux and consumes a prepared dataset on disk. Prepare all
eight tables from one DuckDB `dbgen` invocation, retaining the original tables
in `reference.duckdb`. Export each table to one Parquet file, derive its Iceberg
schema with `run.iceberg_schema`, and convert that file with the pinned Rust
writer. Keep the files unchanged throughout the measurements. The input layout
is:

```text
dataset/
  reference.duckdb          # Original eight generated tables
  queries.json              # [[1, "SQL"], ..., [22, "SQL"]] from tpch_queries()
  dataset.json              # {"scale_factor": 10, "tables": {"region": {"rows": 5}, ...}}
  source/
    region.parquet          # Repeat these three files for all eight tables
    region.vortex
    region.schema.json
```

`dataset.json` must supply the exact row count for every table. Include the
generator version, writer revision and file sizes in this metadata for later
comparison. Dataset preparation and conversion happen before the measured run;
`perf.py` does not generate them, and `run.py` does not produce this layout.

Build the writer and FFI library with the **release** profile for measurements;
the earlier correctness examples use the development profile. Use fresh build
directories for the commands below:

```sh
VORTEX_BUILD_PROFILE=release bash dev/tpch/writer/build.sh .cache/tpch/perf-writer
VORTEX_BUILD_PROFILE=release CARGO_TARGET_DIR="$PWD/.cache/tpch/perf-ffi/target" \
  bash internal/vortexffi/build-lib.sh .cache/tpch/perf-ffi
CGO_ENABLED=0 go build -o .cache/tpch/scan-native ./dev/tpch/scan
CGO_ENABLED=1 CGO_LDFLAGS="-L$PWD/.cache/tpch/perf-ffi/target/release" \
  go build -tags vortex -o .cache/tpch/scan-ffi ./dev/tpch/scan

.cache/tpch/venv/bin/python dev/tpch/perf.py \
  --dataset /path/on/disk/dataset \
  --output /path/on/disk/new-results \
  --native-scanner .cache/tpch/scan-native \
  --ffi-scanner .cache/tpch/scan-ffi \
  --ffi-library .cache/tpch/perf-ffi/target/release/libvortex_ffi.so \
  --runs 5 --threads 4 --memory 8GB
```

The output directory must be new. The runner selects and verifies the supplied
FFI library, registers each table once per backend, then reuses its committed
catalog. One warm-up round precedes five measured rounds. Each round scans all
eight tables and the three explicit filter cases; measured rounds also execute
and check all 22 queries after importing the exports into DuckDB. Full exports
are compared exactly against the source Parquet in storage order, using bounded
Arrow batches. Filtered outputs use exact multiset comparison against the
original DuckDB tables.

The report separates registration, table loading, scan plus IPC export, process
wall/CPU time, verification, DuckDB import and SQL execution. IPC writes are
buffered without `fsync`; these timings measure the integration path. SQL runs
on imported DuckDB tables and does not measure format-specific query execution.
Summaries give all samples, median, minimum and maximum of each round's total
full-scan time, with filter timings reported separately. Scanner peak RSS comes
from Linux `VmHWM`; the separate `wait4` value can include inherited parent RSS.

Scanner processes run sequentially, with rotating backend order. The default
four allowed CPUs form a common resource budget: native decoding is serial,
the FFI uses its default caller-driven runtime, and Parquet decodes columns in
parallel. `--threads` also sets DuckDB threads and `GOMAXPROCS`. Finish builds
before running. The runner does not evict OS caches, and verification rereads
inputs; results describe this warm-cache workflow.

Keep source files, output and spill directories off tmpfs. `--memory` sets each
DuckDB connection's memory limit; scanners use the Go soft memory limit
`GOMEMLIMIT=4GiB`, which does not cap Rust allocations or total process memory.
Successful exports and candidate databases are removed after checking; logs,
catalogs, reference answers and `report.json` remain, along with the unchanged
source dataset.

## Harness checks

```sh
.cache/tpch/venv/bin/python -m unittest discover -s dev/tpch -p 'test_*.py'
go test ./dev/tpch/scan
```
