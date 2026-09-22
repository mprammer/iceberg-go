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

"""Fresh Iceberg-Go Arrow streams for DuckDB 1.4.4 / PyArrow 21 datasets.

DuckDB calls Dataset.scanner separately for each physical scan, including
repeated aliases. This deliberately depends on the pinned Python client's
Dataset dispatch; it is not a general PyArrow Dataset implementation.
"""

from contextlib import contextmanager
import io
import json
import os
from pathlib import Path
import re
import subprocess
import threading
import time

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.ipc as ipc


class _CountingPipe(io.RawIOBase):
    def __init__(self, pipe, event):
        super().__init__()
        self.pipe = pipe
        self.event = event

    def readable(self):
        return True

    def read(self, size=-1):
        if size < 0:
            raise ValueError("live IPC reads must have a bounded size")
        data = self.pipe.read(size)
        self.event["ipc_bytes_read"] += len(data)
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)

    def close(self):
        if not self.closed:
            self.pipe.close()
        super().close()


class _ScanProcess:
    def __init__(self, event, argv, env, stderr_path, receipt_path):
        self.event = event
        self.receipt_path = receipt_path
        self.started = time.monotonic()
        self.exhausted = False
        self.cancelled = False
        self.ipc_reader = None
        self.batch_reader = None
        self.pipe = None
        self.process = None
        self.stderr = stderr_path.open("wb")
        try:
            self.process = subprocess.Popen(
                list(map(str, argv)), stdout=subprocess.PIPE, stderr=self.stderr,
                env=env, bufsize=0,
            )
            # Buffering fills bounded IPC reads across short OS pipe reads.
            buffered = io.BufferedReader(self.process.stdout, buffer_size=65536)
            self.pipe = _CountingPipe(buffered, event)
            event["pid"] = self.process.pid
        except BaseException:
            self.stderr.close()
            raise

    def reader(self, expected_schema, scan_columns):
        try:
            self.ipc_reader = ipc.open_stream(self.pipe)
            actual = self.ipc_reader.schema
            for name in scan_columns:
                if name not in actual.names or actual.field(name).type != expected_schema.field(name).type:
                    raise ValueError(f"{self.event['table']}: incompatible IPC field {name}")
            self.batch_reader = pa.RecordBatchReader.from_batches(actual, self.batches())
            return self.batch_reader
        except Exception as error:
            self.event["status"] = "failed"
            self.event["error"] = str(error)
            raise

    def read_receipt(self, required):
        if not self.receipt_path.exists():
            if required:
                raise RuntimeError(f"missing scanner receipt: {self.receipt_path}")
            return
        receipt = json.loads(self.receipt_path.read_text())
        self.event["receipt"] = receipt
        if self.exhausted and receipt["rows"] != self.event["rows_read"]:
            raise RuntimeError(f"{self.event['table']}: IPC rows differ from scanner receipt")

    def batches(self):
        try:
            for batch in self.ipc_reader:
                self.event["batches_read"] += 1
                self.event["rows_read"] += batch.num_rows
                yield batch
            self.exhausted = True
            code = self.process.wait(timeout=5)
            self.event["exit_code"] = code
            if code and not self.cancelled:
                raise RuntimeError(
                    f"{self.event['table']}: scanner exited {code}; see {self.event['stderr_path']}"
                )
            if not self.cancelled:
                self.read_receipt(required=True)
                self.event["status"] = "completed"
        except Exception as error:
            if not self.cancelled:
                self.event["status"] = "failed"
                self.event["error"] = str(error)
                raise

    def close(self):
        """Stop unfinished producers before closing pipes; reap every child."""
        error = None
        try:
            if self.process is not None:
                code = self.process.poll()
                if code is None:
                    # A successful SQL query may stop reading after LIMIT or a
                    # join short-circuit. Explicitly terminate that producer.
                    self.cancelled = True
                    self.event["intentionally_cancelled"] = True
                    self.process.terminate()
                    try:
                        code = self.process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        self.process.kill()
                        code = self.process.wait(timeout=2)
                self.event["exit_code"] = code
                if code and not self.cancelled:
                    error = RuntimeError(
                        f"{self.event['table']}: scanner exited {code}; see {self.event['stderr_path']}"
                    )
                elif not self.cancelled:
                    self.read_receipt(required=True)
        except Exception as caught:
            error = caught
        finally:
            # Keep ownership until query cleanup even when Arrow releases its
            # stream early. Closing a pipe first would give the producer SIGPIPE.
            for resource in (self.batch_reader, self.ipc_reader, self.pipe, self.stderr):
                if resource is not None:
                    try:
                        resource.close()
                    except Exception as caught:
                        if error is None and not self.cancelled:
                            error = caught
            self.event["process_wall_seconds"] = time.monotonic() - self.started
            self.event["stream_exhausted"] = self.exhausted
            if error is not None:
                self.event["status"] = "failed"
                self.event["error"] = str(error)
            elif self.event["status"] != "failed":
                self.event["status"] = "cancelled" if self.cancelled else "completed"
        if error is not None:
            raise error


class QueryScans:
    def __init__(self, label, directory):
        self.label = label
        self.directory = directory
        self.events = []
        self.status = "running"
        self.error = None
        self._processes = []


class LiveScanManager:
    """Own all scanner subprocesses for one timed SQL query at a time."""

    def __init__(self, log_dir, env=None):
        self.log_dir = Path(log_dir)
        self.env = dict(os.environ if env is None else env)
        self._active = None
        self._lock = threading.RLock()

    @contextmanager
    def query(self, label):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", label) or label in (".", ".."):
            raise ValueError("query label must be a simple directory name")
        with self._lock:
            if self._active is not None:
                raise RuntimeError("overlapping query contexts are not supported")
            directory = self.log_dir / label
            directory.mkdir(parents=True, exist_ok=False)
            run = self._active = QueryScans(label, directory)
        body_error = None
        try:
            yield run
        except BaseException as error:
            body_error = error
            run.error = repr(error)
            raise
        finally:
            cleanup_errors = []
            for process in run._processes:
                try:
                    process.close()
                except Exception as error:
                    cleanup_errors.append(error)
            if not cleanup_errors:
                cleanup_errors.extend(
                    RuntimeError(event.get("error", "scanner failed"))
                    for event in run.events if event["status"] == "failed"
                )
            with self._lock:
                self._active = None
            if body_error is not None or cleanup_errors:
                run.status = "failed"
                run.error = run.error or str(cleanup_errors[0])
            else:
                run.status = "passed"
            (directory / "scans.json").write_text(json.dumps(
                {"query": label, "status": run.status, "error": run.error, "scans": run.events},
                indent=2,
            ) + "\n")
            if body_error is None and cleanup_errors:
                raise cleanup_errors[0]

    def start(self, dataset, requested_columns, scan_columns, filter, fallback):
        with self._lock:
            run = self._active
            if run is None:
                raise RuntimeError("a live scan must run inside manager.query()")
            number = len(run.events)
            stem = f"scan-{number:03d}-{dataset.table}"
            stderr_path = run.directory / f"{stem}.stderr"
            receipt_path = run.directory / f"{stem}.receipt.json"
            event = {
                "scan_id": number, "table": dataset.table, "backend": dataset.backend,
                "requested_columns": requested_columns, "scan_columns": scan_columns,
                "filter": None if filter is None else str(filter),
                "full_schema_fallback": fallback, "status": "running",
                "ipc_bytes_read": 0, "rows_read": 0, "batches_read": 0,
                "intentionally_cancelled": False, "stderr_path": str(stderr_path),
                "receipt_path": str(receipt_path),
            }
            run.events.append(event)
            argv = [dataset.scanner_path, "-input", str(dataset.input_path),
                    "-schema", dataset.schema_path, "-warehouse", dataset.warehouse,
                    "-table", dataset.table, "-backend", dataset.backend,
                    "-reuse", "-stream", "-columns", json.dumps(scan_columns),
                    "-receipt", receipt_path]
            process = _ScanProcess(event, argv, self.env, stderr_path, receipt_path)
            run._processes.append(process)
            return process


class IcebergDataset(ds.Dataset):
    """Pinned DuckDB Dataset adapter; schema access never starts a scan."""

    def __init__(self, manager, *, schema, scanner, input_path, schema_path,
                 warehouse, table, backend):
        self.manager = manager
        self._schema = schema
        self.scanner_path = Path(scanner).resolve()
        # A local path is resolved so the scanner's committed-file check sees
        # the same string the catalog recorded; an object-store URI is already
        # absolute and must be passed through untouched.
        self.input_path = str(input_path) if '://' in str(input_path) else Path(input_path).resolve()
        self.schema_path = Path(schema_path).resolve()
        # Same as input_path: pathlib collapses the // in a URI and then
        # resolves what is left against the working directory.
        self.warehouse = str(warehouse) if '://' in str(warehouse) else Path(warehouse).resolve()
        self.table = table
        self.backend = backend
        if not schema.names or len(set(schema.names)) != len(schema.names):
            raise ValueError("source schema must have unique, nonempty fields")

    @property
    def schema(self):
        return self._schema

    def scanner(self, columns=None, filter=None, **kwargs):
        if kwargs:
            raise TypeError(f"unexpected DuckDB scanner options: {sorted(kwargs)}")
        requested = None if columns is None else list(columns)
        scan_columns = list(self.schema.names if columns is None else dict.fromkeys(columns))
        if not scan_columns:
            # COUNT(*) can request zero output columns. Read one physical
            # column to preserve row counts, then let Arrow project it away.
            scan_columns = [self.schema.names[0]]
        candidate_schema = pa.schema([self.schema.field(name) for name in scan_columns])
        fallback = False
        if filter is not None:
            # Ask Arrow to bind the expression, rather than parsing its text.
            # Missing filter-only columns require a conservative full projection.
            try:
                ds.dataset(pa.Table.from_batches([], schema=candidate_schema)).scanner(filter=filter)
            except pa.ArrowInvalid:
                ds.dataset(pa.Table.from_batches([], schema=self.schema)).scanner(filter=filter)
                scan_columns = list(self.schema.names)
                fallback = True
        process = self.manager.start(self, requested, scan_columns, filter, fallback)
        reader = process.reader(self.schema, scan_columns)
        # DuckDB treats the supplied Arrow filter as enforced. Always apply it
        # here; identical residual filtering is used for every storage backend.
        return ds.Scanner.from_batches(
            reader, columns=requested, filter=filter, batch_size=65536,
            # With use_threads=False, Arrow 21 can synchronously drain its
            # source before an early LIMIT returns. Its threaded execution
            # preserves streaming/backpressure and lets query cleanup cancel.
            batch_readahead=1, fragment_readahead=1, use_threads=True,
        )
