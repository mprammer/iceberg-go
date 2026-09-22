#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""TPC-H integration correctness through committed Iceberg-Go table scans.

This is not a TPC-H benchmark: DuckDB executes SQL after full scan exports.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path
import subprocess
import tempfile
import time

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc

TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")
TPCDS_TABLES = (
    "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
    "customer_address", "customer_demographics", "date_dim", "household_demographics",
    "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
    "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
)
# The suites this harness knows how to drive. `queries` is the DuckDB table
# function that supplies the unchanged query text, and `count` is how many
# queries it must yield; both are checked against the loaded extension rather
# than trusted from a prepared dataset.
BENCHMARKS = {
    "tpch": {"tables": TABLES, "extension": "tpch", "queries": "tpch_queries()", "count": 22},
    "tpcds": {"tables": TPCDS_TABLES, "extension": "tpcds", "queries": "tpcds_queries()", "count": 99},
}
BACKENDS = ("parquet", "ffi", "native")


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, default=str) + "\n")


def iceberg_schema(schema):
    fields = []
    for field_id, field in enumerate(schema, 1):
        dtype = field.type
        if pa.types.is_int64(dtype):
            kind = "long"
        elif pa.types.is_int32(dtype):
            kind = "int"
        elif pa.types.is_string(dtype) or pa.types.is_string_view(dtype) or pa.types.is_large_string(dtype):
            kind = "string"
        elif pa.types.is_date32(dtype):
            kind = "date"
        elif pa.types.is_decimal128(dtype):
            kind = f"decimal({dtype.precision},{dtype.scale})"
        else:
            raise ValueError(f"unexpected benchmark type: {field}")
        fields.append({"id": field_id, "name": field.name, "required": False, "type": kind})
    return {"type": "struct", "schema-id": 0, "fields": fields}


def compare_rows(expected, actual):
    """Compare bags, retaining duplicate multiplicity and exact decimal values.

    Only SQL floating-point results allow a small accumulation tolerance. Input
    table checks below use SQL EXCEPT ALL with exact types and no tolerance.
    """
    if len(expected) != len(actual):
        raise AssertionError(f"row count: expected {len(expected)}, got {len(actual)}")
    key = lambda row: tuple((value is not None, value) for value in row)
    for index, (left, right) in enumerate(zip(sorted(expected, key=key), sorted(actual, key=key))):
        if len(left) != len(right):
            raise AssertionError(f"column count at row {index}")
        for column, (want, got) in enumerate(zip(left, right)):
            if isinstance(want, float) and isinstance(got, float):
                equal = math.isfinite(want) and math.isfinite(got) and math.isclose(want, got, rel_tol=1e-12, abs_tol=1e-9)
            else:
                equal = type(want) is type(got) and want == got
            if not equal:
                raise AssertionError(f"row {index}, column {column}: expected {want!r}, got {got!r}")


def command(argv, log):
    result = subprocess.run([str(arg) for arg in argv], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log.write_text(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(f"{argv[0]} exited {result.returncode}; see {log}\n{result.stderr[-2000:]}")
    return result.stdout


def connection():
    conn = duckdb.connect()
    # Keep SQL accumulation and execution repeatable; no benchmark claims.
    conn.execute("SET threads=1")
    return conn


# Independent SQL oracles for the fixed Iceberg expressions in scan/main.go.
# These check exact filtered row bags and filter-only column projection; the
# existing Go zoned-file tests separately prove that data reads are avoided.
FILTERED_SCANS = (
    ("orders-key-range", "orders", "SELECT o_custkey, o_orderstatus FROM orders WHERE o_orderkey >= 5000 AND o_orderkey < 10000"),
    ("orders-empty", "orders", "SELECT o_custkey, o_orderstatus FROM orders WHERE o_orderkey = -1"),
    ("lineitem-q6", "lineitem", "SELECT l_extendedprice, l_discount FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24.00"),
)


def check_filtered_scans(args, source, directory, backend, reference, candidate, detail):
    detail["filtered_scans"] = {}
    extension = "parquet" if backend == "parquet" else "vortex"
    for case, name, query in FILTERED_SCANS:
        arrow_path = directory / f"{case}.arrows"
        output = command([
            args.scanner, "-input", source / f"{name}.{extension}",
            "-schema", source / f"{name}.schema.json", "-warehouse", directory / f"warehouse-{case}",
            "-table", name, "-backend", "ffi" if backend == "ffi" else "native",
            "-output", arrow_path, "-scan-case", case,
        ], directory / f"{case}.scan.log")
        receipt = json.loads(output)
        with pa.memory_map(str(arrow_path), "r") as mapped:
            scanned = ipc.open_stream(mapped).read_all()
            candidate.register("scan_input", scanned)
            candidate.execute("CREATE OR REPLACE TABLE filtered_result AS SELECT * FROM scan_input")
            candidate.unregister("scan_input")
        expected = reference.execute(query).fetch_arrow_table()
        candidate.register("expected_input", expected)
        got_schema = candidate.execute("SELECT * FROM filtered_result LIMIT 0").fetch_arrow_table().schema
        if [(f.name, str(f.type)) for f in got_schema] != [(f.name, str(f.type)) for f in expected.schema]:
            raise AssertionError(f"{case}: filtered schema mismatch")
        mismatch = candidate.execute(
            "SELECT * FROM ((SELECT * FROM filtered_result EXCEPT ALL SELECT * FROM expected_input) "
            "UNION ALL (SELECT * FROM expected_input EXCEPT ALL SELECT * FROM filtered_result)) LIMIT 1"
        ).fetchall()
        candidate.unregister("expected_input")
        if mismatch or receipt["rows"] != expected.num_rows:
            raise AssertionError(f"{case}: filtered row multiset mismatch: {mismatch!r}")
        receipt["exact_rows_match"] = True
        detail["filtered_scans"][case] = receipt
        print(f"{backend}: {case} exact filtered rows match ({receipt['rows']})", flush=True)


def execute(args, root, report):
    source = root / "source"
    source.mkdir()
    with connection() as reference:
        reference.execute("LOAD tpch")
        reference.execute(f"CALL dbgen(sf={args.scale!r})")
        queries = reference.execute("SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr").fetchall()
        if [number for number, _ in queries] != list(range(1, 23)):
            raise AssertionError("TPC-H extension did not supply all 22 queries")
        write_json(root / "queries.json", queries)
        report["query_sha256"] = hashlib.sha256((root / "queries.json").read_bytes()).hexdigest()
        report["tables"] = {}
        for name in TABLES:
            schema = reference.execute(f"SELECT * FROM {name} LIMIT 0").fetch_arrow_table().schema
            write_json(source / f"{name}.schema.json", iceberg_schema(schema))
            parquet = source / f"{name}.parquet"
            reference.execute(f"COPY {name} TO {sql_string(parquet)} (FORMAT PARQUET)")
            report["tables"][name] = {"rows": reference.execute(f"SELECT count(*) FROM {name}").fetchone()[0]}
            if any(backend != "parquet" for backend in args.backends):
                command([args.writer, parquet, source / f"{name}.vortex"], source / f"{name}.writer.log")
            print(f"generated {name}: {report['tables'][name]['rows']} rows", flush=True)
        answers = {}
        for number, query in queries:
            result = reference.execute(query)
            answers[number] = ([(col[0], str(col[1])) for col in result.description], result.fetchall())
        write_json(root / "reference-results.json", answers)
        report["backends"] = {}
        for backend in args.backends:
            detail = {"status": "running", "tables": {}, "queries": {}}
            report["backends"][backend] = detail
            directory = root / backend
            directory.mkdir()
            try:
                with connection() as candidate:
                    for name in TABLES:
                        extension = "parquet" if backend == "parquet" else "vortex"
                        arrow_path = directory / f"{name}.arrows"
                        output = command([
                            args.scanner, "-input", source / f"{name}.{extension}",
                            "-schema", source / f"{name}.schema.json", "-warehouse", directory / "warehouse",
                            "-table", name, "-backend", "ffi" if backend == "ffi" else "native",
                            "-output", arrow_path,
                        ], directory / f"{name}.scan.log")
                        receipt = json.loads(output)
                        with pa.memory_map(str(arrow_path), "r") as mapped:
                            scanned = ipc.open_stream(mapped).read_all()
                            candidate.register("scan_input", scanned)
                            candidate.execute(f"CREATE TABLE {name} AS SELECT * FROM scan_input")
                            candidate.unregister("scan_input")
                        # Independently compare the exported rows to the generator,
                        # not another adapter; EXCEPT ALL detects duplicates/loss.
                        original = reference.execute(f"SELECT * FROM {name}").fetch_arrow_table()
                        candidate.register("expected_input", original)
                        wanted_types = [(f.name, str(f.type)) for f in original.schema]
                        got_schema = candidate.execute(f"SELECT * FROM {name} LIMIT 0").fetch_arrow_table().schema
                        if wanted_types != [(f.name, str(f.type)) for f in got_schema]:
                            raise AssertionError(f"{name}: exported logical schema differs")
                        mismatch = candidate.execute(
                            f"SELECT * FROM ((SELECT * FROM {name} EXCEPT ALL SELECT * FROM expected_input) "
                            f"UNION ALL (SELECT * FROM expected_input EXCEPT ALL SELECT * FROM {name})) LIMIT 1"
                        ).fetchall()
                        candidate.unregister("expected_input")
                        if mismatch or receipt["rows"] != report["tables"][name]["rows"]:
                            raise AssertionError(f"{name}: input row multiset mismatch: {mismatch!r}")
                        receipt["exact_rows_match"] = True
                        detail["tables"][name] = receipt
                        print(f"{backend}: {name} exact rows match", flush=True)
                    for number, query in queries:
                        started = time.monotonic()
                        result = candidate.execute(query)
                        columns = [(col[0], str(col[1])) for col in result.description]
                        actual = result.fetchall()
                        elapsed = time.monotonic() - started
                        expected_columns, expected_rows = answers[number]
                        if columns != expected_columns:
                            raise AssertionError(f"Q{number}: result schema mismatch")
                        try:
                            compare_rows(expected_rows, actual)
                        except AssertionError as error:
                            raise AssertionError(f"Q{number}: {error}") from error
                        detail["queries"][str(number)] = {"match": True, "rows": len(actual), "sql_seconds": elapsed}
                    check_filtered_scans(args, source, directory, backend, reference, candidate, detail)
                    detail["status"] = "passed"
                    print(f"{backend}: all 22 queries match", flush=True)
            except Exception as error:
                detail["status"] = "failed"
                detail["error"] = str(error)
                print(f"{backend}: FAILED: {error}", flush=True)
            write_json(root / "report.json", report)
    report["status"] = "passed" if all(item["status"] == "passed" for item in report["backends"].values()) else "failed"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scanner", type=Path, required=True, help="compiled dev/tpch/scan binary")
    parser.add_argument("--writer", type=Path, help="pinned Parquet-to-Vortex converter (required for Vortex)")
    parser.add_argument("--scale", type=float, default=0.01)
    parser.add_argument("--backends", nargs="+", choices=BACKENDS, default=list(BACKENDS))
    parser.add_argument("--work-dir", type=Path, default=Path(".cache/tpch"), help="on-disk parent for a fresh run directory")
    args = parser.parse_args()
    if not math.isfinite(args.scale) or args.scale <= 0:
        parser.error("--scale must be finite and positive")
    if len(set(args.backends)) != len(args.backends):
        parser.error("duplicate backends")
    if any(backend != "parquet" for backend in args.backends) and args.writer is None:
        parser.error("--writer is required for Vortex backends")
    args.scanner = args.scanner.resolve(strict=True)
    if args.writer is not None:
        args.writer = args.writer.resolve(strict=True)
    args.work_dir.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="run-", dir=args.work_dir.resolve()))
    report = {"status": "running", "scale_factor": args.scale, "duckdb": duckdb.__version__, "pyarrow": pa.__version__,
              "scanner": str(args.scanner), "writer": str(args.writer), "requested_backends": args.backends,
              "scope": "full and explicitly filtered Iceberg scans; SQL runs after Arrow export; no SQL-engine predicate translation"}
    print(f"artifacts: {root}", flush=True)
    try:
        with args.scanner.open("rb") as binary:
            report["scanner_sha256"] = hashlib.file_digest(binary, "sha256").hexdigest()
        if args.writer is not None:
            report["writer_version"] = command([args.writer, "--version"], root / "writer-version.log").strip()
            with args.writer.open("rb") as binary:
                report["writer_sha256"] = hashlib.file_digest(binary, "sha256").hexdigest()
        execute(args, root, report)
    except Exception as error:
        report["status"] = "failed"
        report["error"] = str(error)
    finally:
        write_json(root / "report.json", report)
    print(f"{report['status']}: {root / 'report.json'}", flush=True)
    if "error" in report:
        print(report["error"], flush=True)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
