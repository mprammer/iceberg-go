#!/usr/bin/env python3
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
"""Generate small Iceberg registration fixtures with the pinned Rust converter."""

import argparse
from pathlib import Path
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writer", type=Path, required=True)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    writer = args.writer.resolve()
    version = subprocess.check_output([str(writer), "--version"], text=True)
    revision = "46a8d39c032b1e9f8ae28a13efe8abc9ebad556f"
    if revision not in version:
        raise ValueError(f"expected pinned Vortex writer {revision}, got {version.strip()}")
    args.output.mkdir(parents=True, exist_ok=True)
    fixtures = [
        ("unicode_string", "value", pa.string(), "éabc"),
        ("unicode_binary", "value", pa.binary(), "éabc".encode("utf-8")),
        ("dotted", "a.b", pa.string(), "actual"),
    ]
    with tempfile.TemporaryDirectory(prefix="iceberg-vortex-registration-") as staging:
        for name, field, dtype, value in fixtures:
            schema = pa.schema([pa.field(field, dtype, nullable=False)])
            table = pa.Table.from_arrays([pa.array([value], type=dtype)], schema=schema)
            parquet = Path(staging) / f"{name}.parquet"
            pq.write_table(table, parquet)
            subprocess.run([str(writer), str(parquet), str(args.output / f"{name}.vortex")], check=True)


if __name__ == "__main__":
    main()
