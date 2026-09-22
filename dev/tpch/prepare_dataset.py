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

"""Assemble a live.py dataset from an already-generated Parquet/Vortex corpus.

The corpus is used where it lies: `source/` holds symlinks, not copies, so a
dataset costs kilobytes next to a hundred gigabytes of tables. `reference.duckdb`
is likewise a set of views over the Parquet files rather than a second physical
copy, so the oracle reads exactly the bytes the Parquet backend reads.
"""
import argparse
import json
import pickle
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from run import BENCHMARKS, iceberg_schema, sql_string


def resolve(corpus, table, suffix):
    """Find <corpus>/<prefix><table>/<suffix>/*.<suffix>, as the generator lays it out."""
    directory = corpus / table / suffix
    if not directory.is_dir():
        raise FileNotFoundError(f'{directory} is missing')
    files = sorted(p for p in directory.glob(f'*.{suffix}') if not p.name.startswith('.'))
    if len(files) != 1:
        raise ValueError(f'expected exactly one .{suffix} in {directory}, found {len(files)}')
    return files[0].resolve()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--benchmark', choices=sorted(BENCHMARKS), required=True)
    parser.add_argument('--corpus', type=Path, required=True,
                        help='directory holding one <prefix><table>/ per table')
    parser.add_argument('--prefix', default='',
                        help='name prefix in front of each table directory')
    parser.add_argument('--scale', type=float, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--data-uri-prefix',
                        help='object-store prefix holding <table>.parquet and <table>.vortex; '
                             'the scanners then read from there instead of the local corpus')
    parser.add_argument('--with-answers', action='store_true',
                        help='run every query against the reference and store the results, so a '
                             'runner without the corpus need not execute the oracle itself')
    args = parser.parse_args()

    suite = BENCHMARKS[args.benchmark]
    source = args.output / 'source'
    source.mkdir(parents=True, exist_ok=False)

    connection = duckdb.connect(str(args.output / 'reference.duckdb'))
    connection.execute(f"INSTALL {suite['extension']}")
    connection.execute(f"LOAD {suite['extension']}")

    tables = {}
    for name in suite['tables']:
        parquet = resolve(args.corpus, f'{args.prefix}{name}', 'parquet')
        vortex = resolve(args.corpus, f'{args.prefix}{name}', 'vortex')
        (source / f'{name}.parquet').symlink_to(parquet)
        (source / f'{name}.vortex').symlink_to(vortex)
        metadata = pq.read_metadata(parquet)
        schema = pq.read_schema(parquet)
        (source / f'{name}.schema.json').write_text(
            json.dumps(iceberg_schema(schema), indent=2) + '\n')
        connection.execute(
            f'CREATE VIEW {name} AS SELECT * FROM read_parquet({sql_string(parquet)})')
        # Record the Arrow schema a scan yields, so a runner that reaches the
        # data only over the network never has to open a Parquet footer to
        # learn it. Iceberg has one string type, so string_view narrows to utf8.
        normalized = pa.schema([
            field.with_type(pa.string())
            if pa.types.is_string_view(field.type) or pa.types.is_large_string(field.type)
            else field
            for field in schema
        ])
        (source / f'{name}.arrow-schema').write_bytes(normalized.serialize())
        tables[name] = {
            'rows': metadata.num_rows,
            'parquet_bytes': parquet.stat().st_size,
            'vortex_bytes': vortex.stat().st_size,
            'parquet': str(parquet),
            'vortex': str(vortex),
        }
        if args.data_uri_prefix:
            prefix = args.data_uri_prefix.rstrip('/')
            tables[name]['parquet_uri'] = f'{prefix}/{name}.parquet'
            tables[name]['vortex_uri'] = f'{prefix}/{name}.vortex'
        print(f'{name}: {metadata.num_rows} rows', flush=True)

    queries = connection.execute(
        f"SELECT query_nr, query FROM {suite['queries']} ORDER BY query_nr").fetchall()
    if [n for n, _ in queries] != list(range(1, suite['count'] + 1)):
        raise ValueError(f"expected Q1..Q{suite['count']} from {suite['queries']}")

    if args.with_answers:
        # Pickle, not JSON: the comparison is exact on decimals, dates and
        # nulls, and JSON would flatten them to strings. Written and read by
        # the same pinned DuckDB and Python, and never fetched from elsewhere.
        answers = {}
        for number, sql in queries:
            cursor = connection.execute(sql)
            columns = [(field[0], str(field[1])) for field in cursor.description]
            answers[number] = (columns, cursor.fetchall())
            print(f'answered Q{number}: {len(answers[number][1])} rows', flush=True)
        (args.output / 'reference-answers.pickle').write_bytes(pickle.dumps(answers))
    connection.close()

    (args.output / 'queries.json').write_text(
        json.dumps([[n, q] for n, q in queries], indent=2) + '\n')
    (args.output / 'dataset.json').write_text(json.dumps({
        'benchmark': args.benchmark,
        'scale_factor': args.scale,
        'corpus': str(args.corpus.resolve()),
        'reference': 'DuckDB views over the corpus Parquet files; no second physical copy',
        'data_uri_prefix': args.data_uri_prefix,
        'answers': 'reference-answers.pickle' if args.with_answers else None,
        'tables': tables,
    }, indent=2) + '\n')
    print(f'dataset: {args.output}')


if __name__ == '__main__':
    main()
