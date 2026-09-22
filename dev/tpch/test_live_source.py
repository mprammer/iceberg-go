# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

import os
from pathlib import Path
import sys
import tempfile
import unittest

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds

from live_source import IcebergDataset, LiveScanManager


CHILD = '''
import argparse, json, sys
from pathlib import Path
import pyarrow as pa
import pyarrow.ipc as ipc
p=argparse.ArgumentParser()
p.add_argument('-columns'); p.add_argument('-receipt'); p.add_argument('-table')
args,_=p.parse_known_args()
tables={
 'part':pa.table({'p_partkey':[1],'p_brand':['Brand#23'],'p_container':['MED BOX']}),
 'lineitem':pa.table({'l_extendedprice':[10,20,30],'l_quantity':[1,20,30],'l_partkey':[1,1,1]}),
 'small':pa.table({'a':[1,2,3],'b':[10,20,30]}),
 'bad':pa.table({'a':[1,2,3],'b':[10,20,30]}),
 'large':pa.table({'a':list(range(65536)),'b':list(range(65536))}),
}
table=tables[args.table]
selected=json.loads(args.columns)
# Match Iceberg's table-order projection, regardless of requested order.
table=table.select([n for n in table.schema.names if n in selected])
rows=0
with ipc.new_stream(sys.stdout.buffer,table.schema) as writer:
    while True:
        for batch in table.to_batches(max_chunksize=1024):
            writer.write_batch(batch); rows+=batch.num_rows
        if args.table!='large': break
if args.table=='bad':
    print('intentional producer failure',file=sys.stderr)
    raise SystemExit(3)
Path(args.receipt).write_text(json.dumps({'rows':rows}))
'''

Q17 = """SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem, part
WHERE p_partkey = l_partkey
  AND p_brand = 'Brand#23'
  AND p_container = 'MED BOX'
  AND l_quantity < (
    SELECT 0.2 * avg(l_quantity)
    FROM lineitem
    WHERE l_partkey = p_partkey
  );"""


class LiveSourceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.child = self.root / 'scanner'
        self.child.write_text(f'#!{sys.executable}\n' + CHILD)
        self.child.chmod(0o700)
        env = dict(os.environ, OPENBLAS_NUM_THREADS='1', PYTHONDONTWRITEBYTECODE='1')
        self.manager = LiveScanManager(self.root / 'logs', env)

    def tearDown(self):
        self.temp.cleanup()

    def dataset(self, name, schema=None):
        schema = schema or pa.schema([('a', pa.int64()), ('b', pa.int64())])
        return IcebergDataset(
            self.manager, schema=schema, scanner=self.child,
            input_path=self.root / 'source', schema_path=self.root / 'schema',
            warehouse=self.root / 'warehouse', table=name, backend='native',
        )

    def assert_reaped(self, run):
        for event in run.events:
            with self.assertRaises(ProcessLookupError):
                os.kill(event['pid'], 0)

    def test_repeated_q17_references_and_executions(self):
        part = self.dataset('part', pa.schema([
            ('p_partkey', pa.int64()), ('p_brand', pa.string()), ('p_container', pa.string()),
        ]))
        lineitem = self.dataset('lineitem', pa.schema([
            ('l_extendedprice', pa.int64()), ('l_quantity', pa.int64()), ('l_partkey', pa.int64()),
        ]))
        with duckdb.connect(config={'threads': 2}) as conn:
            conn.register('part', part)
            conn.register('lineitem', lineitem)
            self.assertFalse(self.manager.log_dir.exists())
            pids = set()
            for n in range(2):
                with self.manager.query(f'q17-{n}') as run:
                    self.assertEqual(conn.execute(Q17).fetchall(), [(10 / 7,)])
                self.assertEqual(run.status, 'passed')
                self.assertEqual([e['table'] for e in run.events].count('lineitem'), 2)
                self.assertTrue(all(e['rows_read'] > 0 and e['ipc_bytes_read'] > 0 for e in run.events))
                self.assertTrue(all(e['exit_code'] == 0 for e in run.events))
                new_pids = {e['pid'] for e in run.events}
                self.assertTrue(pids.isdisjoint(new_pids))
                pids.update(new_pids)
                self.assert_reaped(run)

    def test_projection_order_filter_binding_and_zero_columns(self):
        dataset = self.dataset('small')
        with self.manager.query('projection') as run:
            got = dataset.scanner(columns=['b', 'a']).to_table()
            self.assertEqual(got.column_names, ['b', 'a'])
            self.assertEqual(got.to_pydict(), {'b': [10, 20, 30], 'a': [1, 2, 3]})
        self.assertFalse(run.events[0]['full_schema_fallback'])
        with self.manager.query('filter-only') as run:
            got = dataset.scanner(columns=['a'], filter=ds.field('b') > 15).to_table()
            self.assertEqual(got.to_pydict(), {'a': [2, 3]})
        self.assertTrue(run.events[0]['full_schema_fallback'])
        with self.manager.query('zero-columns') as run:
            batches = dataset.scanner(columns=[]).to_reader()
            self.assertEqual(sum(b.num_rows for b in batches), 3)
        self.assertEqual(run.events[0]['scan_columns'], ['a'])
        self.assert_reaped(run)

    def test_nonzero_exit_after_valid_ipc_fails_query(self):
        with duckdb.connect(config={'threads': 1}) as conn:
            conn.register('bad', self.dataset('bad'))
            with self.assertRaises(Exception):
                with self.manager.query('producer-failure') as run:
                    conn.execute('SELECT sum(a) FROM bad').fetchall()
        self.assertEqual(run.status, 'failed')
        self.assertEqual(run.events[0]['exit_code'], 3)
        self.assertFalse(run.events[0]['intentionally_cancelled'])
        self.assert_reaped(run)

    def test_early_limit_terminates_and_reaps_producer(self):
        with duckdb.connect(config={'threads': 1}) as conn:
            conn.register('large', self.dataset('large'))
            with self.manager.query('early-limit') as run:
                self.assertEqual(conn.execute('SELECT a FROM large LIMIT 1').fetchall(), [(0,)])
        self.assertEqual(run.status, 'passed')
        self.assertTrue(run.events[0]['intentionally_cancelled'])
        self.assertGreater(run.events[0]['ipc_bytes_read'], 0)
        self.assertLess(run.events[0]['rows_read'], 1_000_000)
        self.assert_reaped(run)

    def test_exception_cleans_unconsumed_stream(self):
        with self.assertRaisesRegex(RuntimeError, 'caller failed'):
            with self.manager.query('exception') as run:
                scanner = self.dataset('large').scanner(columns=['a'])
                self.assertIsNotNone(scanner)
                raise RuntimeError('caller failed')
        self.assertEqual(run.status, 'failed')
        self.assertTrue(run.events[0]['intentionally_cancelled'])
        self.assert_reaped(run)

    def test_caught_reader_failure_still_fails_context(self):
        wrong_schema = pa.schema([('a', pa.string()), ('b', pa.int64())])
        with self.assertRaisesRegex(RuntimeError, 'incompatible IPC field a'):
            with self.manager.query('caught-reader-failure') as run:
                with self.assertRaisesRegex(ValueError, 'incompatible IPC field a'):
                    self.dataset('small', wrong_schema).scanner(columns=['a'])
        self.assertEqual(run.status, 'failed')
        self.assertEqual(run.events[0]['status'], 'failed')
        self.assert_reaped(run)


if __name__ == '__main__':
    unittest.main()
