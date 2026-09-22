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

"""Run unchanged TPC-H or TPC-DS SQL with Iceberg-Go scans during DuckDB execution."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import pickle
import subprocess
import sys
import threading
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from run import BENCHMARKS, compare_rows, sql_string
from live_source import IcebergDataset, LiveScanManager


def digest(path):
    with Path(path).open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, default=str) + '\n')
    temporary.replace(path)


def distribution(values):
    return {'median': statistics.median(values), 'min': min(values), 'max': max(values), 'samples': values}


def summarize(report):
    result = {}
    measured = {backend: [r for r in report['rounds']
                          if r['backend'] == backend and r['round'] > 0 and r['status'] == 'passed']
                for backend in report['backends']}
    # A query counts only where every backend completed it in every measured
    # round. An abandoned query is dropped from all of them, so the suite
    # totals stay comparable rather than quietly omitting work from one side.
    comparable = [n for n in report['query_numbers']
                  if all(rounds and all(r['queries'].get(str(n), {}).get('status') == 'completed' for r in rounds)
                         for rounds in measured.values())]
    report['abandoned_queries'] = [n for n in report['query_numbers'] if n not in comparable]
    for backend, rounds in measured.items():
        if rounds:
            result[backend] = {
                'completed_runs': len(rounds),
                'compared_queries': len(comparable),
                'suite_seconds': distribution([sum(r['queries'][str(n)]['seconds'] for n in comparable) for r in rounds]),
                'queries': {str(n): distribution([r['queries'][str(n)]['seconds'] for r in rounds]) for n in comparable},
            }
    return result


def iceberg_arrow_schema(schema):
    """The Arrow schema an Iceberg scan of this file yields.

    Iceberg has a single string type, so a corpus written with Arrow's
    string_view or large_string reads back as plain utf8 through every backend.
    The IPC field check compares against this, not the physical Parquet
    spelling, which would otherwise reject a scan that is perfectly correct.
    """
    fields = []
    for field in schema:
        kind = field.type
        if pa.types.is_string_view(kind) or pa.types.is_large_string(kind):
            kind = pa.string()
        fields.append(field.with_type(kind))
    return pa.schema(fields)


def connect(path, temp, args, read_only=False):
    connection = duckdb.connect(str(path), read_only=read_only)
    connection.execute(f'SET threads={args.threads}')
    connection.execute(f'SET memory_limit={sql_string(args.memory)}')
    connection.execute(f'SET temp_directory={sql_string(temp)}')
    # This bridge gives DuckDB no row counts, so joins are planned on default
    # cardinality estimates and a heavy query can pick a plan that spills
    # without bound. The cap makes that fail quickly instead of occupying the
    # machine; it applies identically to every backend.
    connection.execute(f'SET max_temp_directory_size={sql_string(args.max_spill)}')
    return connection


def execute(args, report):
    if duckdb.__version__ != '1.4.4' or pa.__version__ != '21.0.0':
        raise RuntimeError('live Dataset callback protocol is tested with DuckDB 1.4.4 and PyArrow 21.0.0; use requirements.txt')
    suite = BENCHMARKS[args.benchmark]
    tables = suite['tables']
    source = args.dataset / 'source'
    queries_path = args.dataset / 'queries.json'
    queries = json.loads(queries_path.read_text())
    expected_numbers = list(range(1, suite['count'] + 1))
    if [n for n, _ in queries] != expected_numbers:
        raise ValueError(f"queries.json must contain unchanged Q1 through Q{suite['count']} in order")
    report['query_numbers'] = expected_numbers
    report['query_sha256'] = digest(queries_path)
    report['dataset'] = json.loads((args.dataset / 'dataset.json').read_text())
    # Prefer the schema the dataset recorded: a runner reading over the network
    # has no local Parquet footer to open.
    schemas = {}
    for name in tables:
        recorded = source / f'{name}.arrow-schema'
        if recorded.exists():
            schemas[name] = pa.ipc.read_schema(pa.BufferReader(recorded.read_bytes()))
        else:
            schemas[name] = iceberg_arrow_schema(pq.read_schema(source / f'{name}.parquet'))
    dataset_tables = report['dataset']['tables']

    def location(name, fmt):
        # --data-prefix wins, so one dataset can be run against several copies of
        # the same corpus: a bucket one day and a local disk the next. Otherwise
        # the dataset's own URI is used, and failing that the file beside it.
        if args.data_prefix:
            prefix = args.data_prefix.rstrip('/')
            return (f'{prefix}/{name}.{fmt}' if '://' in prefix
                    else Path(prefix) / f'{name}.{fmt}')
        return dataset_tables[name].get(f'{fmt}_uri') or source / f'{name}.{fmt}'
    report['schemas'] = {name: str(schema) for name, schema in schemas.items()}
    env = os.environ.copy()
    env.update(GOMAXPROCS=str(args.threads), GOMEMLIMIT='4GiB', RAYON_NUM_THREADS=str(args.threads))
    if 'ffi' in args.backends:
        library = args.ffi_library.resolve()
        env['LD_LIBRARY_PATH'] = str(library.parent) + (':' + env['LD_LIBRARY_PATH'] if env.get('LD_LIBRARY_PATH') else '')
        linkage = subprocess.check_output(['ldd', str(args.ffi_scanner.resolve())], env=env, text=True)
        resolved = [line.split('=>',1)[1].strip().split()[0] for line in linkage.splitlines() if 'libvortex_ffi.so =>' in line]
        if len(resolved) != 1 or Path(resolved[0]).resolve() != library:
            raise RuntimeError(f'wrong FFI library: {linkage}')
        report['ffi_linkage'] = linkage
    answers = {}
    stored = args.dataset / 'reference-answers.pickle'
    if stored.exists():
        # Answers computed where the corpus is local. Running the oracle from
        # object storage would re-read every table per query for no benefit:
        # the expected results do not depend on where the benchmark runs.
        answers = pickle.loads(stored.read_bytes())
        if sorted(answers) != expected_numbers:
            raise ValueError(f'{stored} does not answer exactly Q1..Q{suite["count"]}')
        report['reference'] = f'precomputed: {stored}'
    else:
        with connect(args.dataset / 'reference.duckdb', args.output / 'reference-spill', args, read_only=True) as reference:
            reference.execute(f"LOAD {suite['extension']}")
            canonical = reference.execute(
                f"SELECT query_nr, query FROM {suite['queries']} ORDER BY query_nr").fetchall()
            if queries != [list(pair) for pair in canonical]:
                raise AssertionError(
                    f"query text differs from the pinned DuckDB {suite['extension']} extension")
            for number, sql in queries:
                start = time.perf_counter()
                cursor = reference.execute(sql)
                columns = [(field[0], str(field[1])) for field in cursor.description]
                rows = cursor.fetchall()
                answers[number] = (columns, rows)
                report['reference_queries'][str(number)] = {'seconds':time.perf_counter()-start, 'rows':len(rows)}
            save(args.output / 'reference-answers.json', answers)
    # Catalogs were already committed by scan -register-only. No AddFiles,
    # original-table import, or data cache construction occurs in this runner.
    for round_number in range(args.runs + 1):
        offset = max(round_number-1,0) % len(args.backends)
        order = args.backends[offset:] + args.backends[:offset]
        for backend in order:
            directory = args.output / f'round-{round_number}' / backend
            directory.mkdir(parents=True)
            detail = {'round':round_number,'backend':backend,'order':order,'status':'running',
                      'host_load_start':os.getloadavg(),'queries':{}}
            report['rounds'].append(detail)
            manager = LiveScanManager(log_dir=directory / 'scans', env=env)
            scanner = args.ffi_scanner if backend == 'ffi' else args.native_scanner
            with connect(':memory:', directory / 'spill', args) as candidate:
                for name in tables:
                    dataset = IcebergDataset(manager, schema=schemas[name], scanner=scanner,
                        input_path=location(name, 'parquet' if backend == 'parquet' else 'vortex'),
                        schema_path=source / f'{name}.schema.json', warehouse=(f'{args.catalogs}/{backend}' if '://' in str(args.catalogs)
                                   else args.catalogs / backend),
                        table=name, backend='ffi' if backend == 'ffi' else 'native')
                    candidate.register(name, dataset)
                for number, sql in queries:
                    profile_path = directory / f'q{number:02d}.profile.json'
                    candidate.execute("PRAGMA enable_profiling='json'")
                    candidate.execute(f'PRAGMA profiling_output={sql_string(profile_path)}')
                    started = time.perf_counter()
                    watchdog, expired = None, threading.Event()
                    if args.query_timeout:
                        def abandon(connection=candidate, flag=expired):
                            # Record that the limit is what stopped the query:
                            # Timer.finished cannot say so, being set by a
                            # cancel just as much as by firing.
                            flag.set()
                            connection.interrupt()
                        watchdog = threading.Timer(args.query_timeout, abandon)
                        watchdog.start()
                    try:
                        with manager.query(f'q{number:02d}') as query_run:
                            cursor = candidate.execute(sql)
                            columns = [(field[0], str(field[1])) for field in cursor.description]
                            rows = cursor.fetchall()
                    except Exception as error:
                        elapsed = time.perf_counter()-started
                        if not expired.is_set():
                            raise
                        detail['queries'][str(number)] = {'seconds':elapsed,'status':'abandoned',
                                                          'error':repr(error)}
                        print(f'ROUND {round_number} {backend} Q{number:02d}: abandoned after {elapsed:.1f}s', flush=True)
                        save(args.output / 'report.json', report)
                        continue
                    finally:
                        if watchdog is not None:
                            watchdog.cancel()
                    elapsed = time.perf_counter()-started
                    # Comparison and JSON result persistence stay outside the timer.
                    expected_columns, expected_rows = answers[number]
                    if columns != expected_columns:
                        raise AssertionError(f'{backend}/Q{number}: result schema mismatch {columns} vs {expected_columns}')
                    compare_rows(expected_rows, rows)
                    if not query_run.events:
                        raise AssertionError(f'{backend}/Q{number}: no live Iceberg scans observed')
                    receipt = {'seconds':elapsed,'rows':len(rows),'match':True,'status':'completed',
                               'scans':query_run.events,'profile':str(profile_path)}
                    save(directory / f'q{number:02d}.result.json', {'columns':columns,'rows':rows})
                    detail['queries'][str(number)] = receipt
                    print(f'ROUND {round_number} {backend} Q{number:02d}: {elapsed:.3f}s {len(query_run.events)} live scans, match', flush=True)
                    save(args.output / 'report.json', report)
            detail['status'] = 'passed'
            detail['host_load_end'] = os.getloadavg()
            report['summary'] = summarize(report)
            save(args.output / 'report.json', report)
            done = [q for q in detail['queries'].values() if q.get('status') == 'completed']
            print(f'ROUND {round_number} {backend} suite: {sum(q["seconds"] for q in done):.3f}s '
                  f'{len(done)}/{len(queries)} match, {len(detail["queries"])-len(done)} abandoned',flush=True)
    report['status'] = 'passed'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--benchmark',choices=sorted(BENCHMARKS),default='tpch')
    parser.add_argument('--dataset',type=Path,required=True)
    # Kept as text: argparse would collapse the // of a gs:// URI into a
    # relative path before anything could inspect it.
    parser.add_argument('--catalogs',required=True)
    parser.add_argument('--data-prefix',
                        help='directory or URI prefix holding <table>.<format>, overriding '
                             'the location the dataset recorded')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--native-scanner',type=Path,required=True)
    parser.add_argument('--ffi-scanner',type=Path,required=True)
    parser.add_argument('--ffi-library',type=Path,required=True)
    parser.add_argument('--runs',type=int,default=5)
    parser.add_argument('--backends',nargs='+',choices=['parquet','ffi','native'],default=['parquet','ffi','native'])
    parser.add_argument('--threads',type=int,default=4)
    parser.add_argument('--memory',default='8GB')
    parser.add_argument('--max-spill',default='64GB')
    parser.add_argument('--query-timeout',type=float,default=0,
                        help='abandon a query after this many seconds; 0 disables')
    args=parser.parse_args()
    if args.runs<1 or args.threads<1 or len(set(args.backends))!=len(args.backends):
        parser.error('positive runs/threads and unique backends required')
    cores=sorted(os.sched_getaffinity(0))[:args.threads]
    if len(cores)!=args.threads:
        parser.error('not enough allowed CPUs')
    # DuckDB/Arrow create native pools at import. Re-exec under the selected
    # mask so every thread, including pre-existing library workers, inherits it.
    if set(os.sched_getaffinity(0)) != set(cores):
        os.sched_setaffinity(0, cores)
        os.execv(sys.executable, [sys.executable, *sys.argv])
    for key in ['dataset','catalogs','output','native_scanner','ffi_scanner','ffi_library']:
        value = getattr(args,key)
        # A catalog may live in object storage; resolving that string would
        # turn gs://bucket/x into a path under the working directory.
        setattr(args,key,str(value) if '://' in str(value) else Path(value).resolve())
    args.output.mkdir(parents=True,exist_ok=False)
    os.sched_setaffinity(0,cores)
    pa.set_cpu_count(args.threads)
    pa.set_io_thread_count(args.threads)
    affinities = set()
    for task in Path('/proc/self/task').iterdir():
        try:
            affinities.add(tuple(sorted(os.sched_getaffinity(int(task.name)))))
        except ProcessLookupError:
            pass
    if any(not set(mask).issubset(cores) for mask in affinities):
        raise RuntimeError(f'native workers exceed CPU allowance: {affinities}')
    report={'native_thread_affinities':sorted(affinities),'status':'running','runs':args.runs,'backends':args.backends,'rounds':[],'reference_queries':{},'query_numbers':[],
            'machine':platform.platform(),'duckdb':duckdb.__version__,'pyarrow':pa.__version__,'cores':cores,
            'duckdb_memory_limit':args.memory,'go_memory_limit':'4GiB',
            'metric':'unchanged SQL execute + fetchall + live scan lifecycle; correctness comparison excluded',
            'scan_path':'DuckDB -> Python Arrow Dataset callback -> fresh Iceberg-Go projected scan -> IPC pipe -> Arrow filter -> DuckDB operators',
            'pushdown':'projection into Iceberg-Go; DuckDB-provided filters evaluated by PyArrow identically for all backends; no predicate translation to Go',
            'cache_policy':'one complete warm-up; no OS cache eviction; rotating backend order; no source table materialization',
            'artifacts':{str(p):digest(p) for p in [args.native_scanner,args.ffi_scanner,args.ffi_library]}}
    try:
        execute(args,report)
    except BaseException as error:
        report['status']='failed';report['error']=repr(error)
        for detail in report['rounds']:
            if detail['status']=='running':detail['status']='failed';detail['error']=repr(error)
        raise
    finally:
        report['summary']=summarize(report)
        save(args.output/'report.json',report)
        print(f'{report["status"]}: {args.output / "report.json"}',flush=True)


if __name__=='__main__':
    main()
