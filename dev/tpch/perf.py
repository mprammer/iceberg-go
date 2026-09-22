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

"""Repeated Iceberg scans over a prepared TPC-H dataset (not a TPC benchmark)."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from run import TABLES, FILTERED_SCANS, compare_rows, sql_string, write_json


def digest(path):
    with Path(path).open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def run_process(argv, directory, label, env, cores):
    stdout = directory / f'{label}.stdout'
    stderr = directory / f'{label}.stderr'
    started = time.monotonic()
    with stdout.open('wb') as out, stderr.open('wb') as err:
        process = subprocess.Popen(['taskset', '-c', cores, *map(str, argv)], stdout=out, stderr=err, env=env)
        try:
            _, status, usage = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
        except BaseException:
            process.kill()
            process.wait()
            raise
    result = {'process_wall_seconds': time.monotonic() - started,
              'user_seconds': usage.ru_utime, 'system_seconds': usage.ru_stime,
              'wait4_peak_rss_kib': usage.ru_maxrss, 'exit_code': process.returncode}
    result['average_cpu_cores'] = (usage.ru_utime + usage.ru_stime) / result['process_wall_seconds']
    write_json(directory / f'{label}.process.json', result)
    if process.returncode:
        raise RuntimeError(f'{label}: exit {process.returncode}; {stderr}\n{stderr.read_text()[-4000:]}')
    result.update(json.loads(stdout.read_text()))
    return result


def logical_schema(schema):
    fields = []
    for field in schema:
        dtype = field.type
        if pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
            dtype = pa.string()
        elif pa.types.is_large_binary(dtype) or pa.types.is_binary_view(dtype):
            dtype = pa.binary()
        fields.append(pa.field(field.name, dtype, nullable=True))
    return pa.schema(fields)


def compare_streams(expected, actual, expected_schema, actual_schema):
    target = logical_schema(expected_schema)
    if target != logical_schema(actual_schema):
        raise AssertionError(f'logical schema mismatch: {target} vs {actual_schema}')
    left = right = None
    li = ri = 0
    count = 0
    while True:
        if left is None or li == left.num_rows:
            left = next(expected, None)
            li = 0
            while left is not None and left.num_rows == 0:
                left = next(expected, None)
        if right is None or ri == right.num_rows:
            right = next(actual, None)
            ri = 0
            while right is not None and right.num_rows == 0:
                right = next(actual, None)
        if left is None or right is None:
            if left is not None or right is not None:
                raise AssertionError(f'streams end at different row counts near {count}')
            return count
        length = min(left.num_rows - li, right.num_rows - ri)
        for i, field in enumerate(target):
            a, b = left.column(i).slice(li, length), right.column(i).slice(ri, length)
            if a.type != field.type:
                a = pc.cast(a, field.type, safe=True)
            if b.type != field.type:
                b = pc.cast(b, field.type, safe=True)
            if not a.equals(b):
                raise AssertionError(f'exact row mismatch in {field.name} at or after row {count}')
        count += length
        li += length
        ri += length


def verify_full(parquet_path, ipc_path):
    with pq.ParquetFile(parquet_path) as source, pa.memory_map(str(ipc_path), 'r') as mapped:
        with ipc.open_stream(mapped) as scanned:
            return compare_streams(iter(source.iter_batches(batch_size=65536)), iter(scanned), source.schema_arrow, scanned.schema)


def connect(path, temp, memory, threads, read_only=False):
    conn = duckdb.connect(str(path), read_only=read_only)
    conn.execute(f'SET threads={threads}')
    conn.execute(f'SET memory_limit={sql_string(memory)}')
    conn.execute(f'SET temp_directory={sql_string(temp)}')
    return conn


def import_ipc(conn, name, path):
    with pa.memory_map(str(path), 'r') as mapped:
        with ipc.open_stream(mapped) as reader:
            conn.register('scan_input', reader)
            conn.execute(f'CREATE OR REPLACE TABLE {name} AS SELECT * FROM scan_input')
            conn.unregister('scan_input')


def verify_filtered(reference, candidate, query, expected_rows):
    # These three result sets are small relative to SF10; compare exact SQL bags,
    # preserving decimal types/multiplicity and not requiring identical row order.
    original = reference.execute(query).fetch_arrow_table()
    candidate.register('expected_input', original)
    try:
        got = candidate.execute('SELECT * FROM filtered_result LIMIT 0').fetch_arrow_table().schema
        if logical_schema(got) != logical_schema(original.schema):
            raise AssertionError('filtered schema mismatch')
        mismatch = candidate.execute(
            'SELECT * FROM ((SELECT * FROM filtered_result EXCEPT ALL SELECT * FROM expected_input) '
            'UNION ALL (SELECT * FROM expected_input EXCEPT ALL SELECT * FROM filtered_result)) LIMIT 1').fetchall()
        if mismatch or expected_rows != original.num_rows:
            raise AssertionError(f'filtered multiset mismatch: {mismatch}')
    finally:
        candidate.unregister('expected_input')


def summary(report):
    result = {}
    for backend in report['backends']:
        rounds = [r for r in report['rounds'] if r['backend'] == backend and r['round'] > 0 and r['status'] == 'passed']
        if not rounds:
            continue
        def distribution(values):
            return {'median': statistics.median(values), 'min': min(values), 'max': max(values), 'samples': values}
        result[backend] = {
            'completed_runs': len(rounds),
            'full_scan_ipc_seconds': distribution([sum(t['scan_ipc_seconds'] for t in r['tables'].values()) for r in rounds]),
            'full_scan_process_seconds': distribution([sum(t['process_wall_seconds'] for t in r['tables'].values()) for r in rounds]),
            'full_scan_cpu_seconds': distribution([sum(t['user_seconds'] + t['system_seconds'] for t in r['tables'].values()) for r in rounds]),
            'peak_scan_rss_kib': distribution([max(t['peak_rss_kib'] for t in r['tables'].values()) for r in rounds]),
            'duckdb_import_seconds': distribution([sum(t['duckdb_import_seconds'] for t in r['tables'].values()) for r in rounds]),
            'duckdb_queries_seconds': distribution([sum(t['seconds'] for t in r['queries'].values()) for r in rounds]),
            'filtered_scan_ipc_seconds': {case: distribution([r['filtered_scans'][case]['scan_ipc_seconds'] for r in rounds]) for case, _, _ in FILTERED_SCANS},
            'tables': {name: distribution([r['tables'][name]['scan_ipc_seconds'] for r in rounds]) for name in TABLES},
        }
    return result


def execute(args, report):
    source = args.dataset / 'source'
    root = args.output
    queries = json.loads((args.dataset / 'queries.json').read_text())
    if [number for number, _ in queries] != list(range(1, 23)):
        raise ValueError('queries.json must contain exactly Q1 through Q22 in order')
    report['queries_sha256'] = digest(args.dataset / 'queries.json')
    data = json.loads((args.dataset / 'dataset.json').read_text())
    report['dataset'] = data
    env = os.environ.copy()
    env.update(GOMAXPROCS=str(args.threads), GOMEMLIMIT='4GiB', RAYON_NUM_THREADS=str(args.threads))
    if 'ffi' in args.backends:
        library = args.ffi_library.resolve()
        env['LD_LIBRARY_PATH'] = str(library.parent) + (':' + env['LD_LIBRARY_PATH'] if env.get('LD_LIBRARY_PATH') else '')
        linkage = subprocess.check_output(['ldd', str(args.ffi_scanner.resolve())], env=env, text=True)
        resolved = [line.split('=>', 1)[1].strip().split()[0] for line in linkage.splitlines() if 'libvortex_ffi.so =>' in line]
        if len(resolved) != 1 or Path(resolved[0]).resolve() != library:
            raise RuntimeError(f'FFI library resolution mismatch: {linkage}')
        report['ffi_linkage'] = linkage
    cores = ','.join(map(str, args.cores))
    def scanner(backend):
        return args.ffi_scanner if backend == 'ffi' else args.native_scanner
    def scan_args(backend, name):
        return [scanner(backend), '-input', source / f'{name}.{"parquet" if backend == "parquet" else "vortex"}',
                '-schema', source / f'{name}.schema.json', '-warehouse', root / 'catalogs' / backend,
                '-table', name, '-backend', 'ffi' if backend == 'ffi' else 'native']
    report['registration'] = {}
    for backend in args.backends:
        report['registration'][backend] = {}
        directory = root / 'registration' / backend
        directory.mkdir(parents=True)
        for name in TABLES:
            print(f'register {backend}/{name}', flush=True)
            receipt = run_process([*scan_args(backend, name), '-register-only'], directory, name, env, cores)
            if receipt['source_rows'] != data['tables'][name]['rows']:
                raise AssertionError(f'{backend}/{name}: registration count mismatch')
            report['registration'][backend][name] = receipt
            write_json(root / 'report.json', report)
    report['reference_queries'] = {}
    answers = {}
    with connect(args.dataset / 'reference.duckdb', root / 'reference-tmp', args.memory, args.threads, read_only=True) as reference:
        for number, query in queries:
            started = time.monotonic()
            cursor = reference.execute(query)
            answers[number] = ([(d[0], str(d[1])) for d in cursor.description], cursor.fetchall())
            report['reference_queries'][str(number)] = {'seconds': time.monotonic()-started, 'rows': len(answers[number][1])}
            print(f'reference Q{number}: {report["reference_queries"][str(number)]["seconds"]:.2f}s', flush=True)
        write_json(root / 'reference-answers.json', answers)
        # Round zero warms each backend outside the five measured rounds. Rotate
        # order for subsequent rounds and keep complete raw receipts.
        for round_number in range(args.runs + 1):
            offset = max(0, round_number-1) % len(args.backends)
            order = args.backends[offset:] + args.backends[:offset]
            for backend in order:
                directory = root / f'round-{round_number}' / backend
                directory.mkdir(parents=True)
                detail = {'round': round_number, 'backend': backend, 'status': 'running', 'order': order,
                          'host_load_start': os.getloadavg(), 'tables': {}, 'filtered_scans': {}, 'queries': {}}
                report['rounds'].append(detail)
                print(f'ROUND {round_number} {backend} start', flush=True)
                with connect(directory / 'candidate.duckdb', directory / 'spill', args.memory, args.threads) as candidate:
                    for name in TABLES:
                        arrow_path = directory / f'{name}.arrows'
                        receipt = run_process([*scan_args(backend, name), '-reuse', '-output', arrow_path], directory, name, env, cores)
                        receipt['ipc_bytes'] = arrow_path.stat().st_size
                        start = time.monotonic()
                        rows = verify_full(source / f'{name}.parquet', arrow_path)
                        if rows != receipt['rows'] or rows != data['tables'][name]['rows']:
                            raise AssertionError(f'{backend}/{name}: full row count mismatch')
                        receipt['exact_rows_match'] = True
                        receipt['verification_seconds'] = time.monotonic()-start
                        start = time.monotonic()
                        import_ipc(candidate, name, arrow_path)
                        receipt['duckdb_import_seconds'] = time.monotonic()-start
                        detail['tables'][name] = receipt
                        arrow_path.unlink()
                        print(f'ROUND {round_number} {backend}/{name}: scan+IPC {receipt["scan_ipc_seconds"]:.3f}s CPU {receipt["user_seconds"]+receipt["system_seconds"]:.3f}s RSS {receipt["peak_rss_kib"]/1024:.1f}MiB exact', flush=True)
                        write_json(root / 'report.json', report)
                    for case, name, query in FILTERED_SCANS:
                        arrow_path = directory / f'{case}.arrows'
                        receipt = run_process([*scan_args(backend, name), '-reuse', '-output', arrow_path, '-scan-case', case], directory, case, env, cores)
                        receipt['ipc_bytes'] = arrow_path.stat().st_size
                        start = time.monotonic()
                        import_ipc(candidate, 'filtered_result', arrow_path)
                        verify_filtered(reference, candidate, query, receipt['rows'])
                        receipt['verification_import_seconds'] = time.monotonic()-start
                        receipt['exact_rows_match'] = True
                        detail['filtered_scans'][case] = receipt
                        arrow_path.unlink()
                        print(f'ROUND {round_number} {backend}/{case}: {receipt["scan_ipc_seconds"]:.3f}s {receipt["rows"]} rows exact', flush=True)
                    candidate.execute('DROP TABLE filtered_result')
                    candidate.execute('CHECKPOINT')
                    if round_number:
                        for number, query in queries:
                            start = time.monotonic()
                            cursor = candidate.execute(query)
                            columns = [(d[0], str(d[1])) for d in cursor.description]
                            actual = cursor.fetchall()
                            elapsed = time.monotonic()-start
                            expected_columns, expected = answers[number]
                            if columns != expected_columns:
                                raise AssertionError(f'{backend}/Q{number}: result schema mismatch')
                            try:
                                compare_rows(expected, actual)
                            except AssertionError as error:
                                raise AssertionError(f'{backend}/Q{number}: {error}') from error
                            detail['queries'][str(number)] = {'seconds': elapsed, 'rows': len(actual), 'match': True}
                        print(f'ROUND {round_number} {backend}: 22 queries match, SQL {sum(q["seconds"] for q in detail["queries"].values()):.2f}s', flush=True)
                detail['status'] = 'passed'
                detail['host_load_end'] = os.getloadavg()
                # Candidate data is reproducible from retained sources; discard
                # only this run's own generated database after all checks pass.
                (directory / 'candidate.duckdb').unlink()
                report['summary'] = summary(report)
                write_json(root / 'report.json', report)
    report['status'] = 'passed'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--native-scanner', type=Path, required=True)
    parser.add_argument('--ffi-scanner', type=Path, required=True)
    parser.add_argument('--ffi-library', type=Path, required=True)
    parser.add_argument('--runs', type=int, default=5)
    parser.add_argument('--backends', nargs='+', choices=['parquet','ffi','native'], default=['parquet','ffi','native'])
    parser.add_argument('--threads', type=int, default=4)
    parser.add_argument('--memory', default='8GB')
    args = parser.parse_args()
    if args.runs < 1 or args.threads < 1 or len(set(args.backends)) != len(args.backends):
        parser.error('positive runs/threads and unique backends are required')
    args.cores = sorted(os.sched_getaffinity(0))[:args.threads]
    if len(args.cores) != args.threads:
        parser.error('not enough allowed CPUs')
    args.output.mkdir(parents=True, exist_ok=False)
    os.sched_setaffinity(0, args.cores)
    report = {'status':'running', 'runs':args.runs, 'backends':args.backends, 'rounds':[],
              'machine':platform.platform(),'duckdb':duckdb.__version__,'pyarrow':pa.__version__,
              'cores':args.cores,'duckdb_memory_limit':args.memory,'go_memory_limit':'4GiB',
              'cache_policy':'one full warm-up round; no OS cache eviction; input re-read by verification; rotating backend order',
              'metric':'scan + Arrow IPC export; buffered output, no fsync; SQL/import/verification reported separately',
              'parallelism':'common CPU budget; native serial decoding, default FFI caller-driven runtime, Parquet parallel column decoding',
              'artifacts':{str(p):digest(p) for p in [args.native_scanner,args.ffi_scanner,args.ffi_library]}}
    try:
        execute(args, report)
    except BaseException as error:
        report['status']='failed'
        report['error']=repr(error)
        for detail in report['rounds']:
            if detail['status'] == 'running':
                detail['status'] = 'failed'
                detail['error'] = repr(error)
        raise
    finally:
        report['summary']=summary(report)
        write_json(args.output/'report.json',report)
        print(f'{report["status"]}: {args.output / "report.json"}',flush=True)


if __name__ == '__main__':
    main()
