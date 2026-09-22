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
"""Generate exact date/decimal reference fixtures through the pinned writer.

Requires the same PyArrow installation as the TPC-H harness.
"""

import argparse
from decimal import Decimal
import json
from pathlib import Path
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq


def generate(writer: Path, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    version = subprocess.check_output([str(writer), "--version"], text=True).strip()
    days = [-100_000, -1, 0, 1, 20_000, None, 2_147_483_647, -2_147_483_648]
    amounts = [
        -999_999_999_999_999,
        -1,
        0,
        1,
        999_999_999_999_999,
        None,
        123_456_789_012_345,
        -123_456_789_012_345,
    ]
    schema = pa.schema([
        pa.field("row_id", pa.int64(), nullable=False),
        pa.field("date", pa.date32()),
        pa.field("amount", pa.decimal128(15, 2)),
    ])
    for case, rows in [("boundary", 8), ("repeated", 8193), ("chunked", 70013)]:
        if case == "chunked":
            date_values = [None if i % 29 == 0 else i - 35_000 for i in range(rows)]
            unscaled = [None if i % 31 == 0 else (i - 35_000) * 170_001 for i in range(rows)]
        else:
            date_values = [days[i % len(days)] for i in range(rows)]
            unscaled = [amounts[i % len(amounts)] for i in range(rows)]
        table = pa.Table.from_arrays([
            pa.array(range(rows), type=pa.int64()),
            pa.array(date_values, type=pa.date32()),
            pa.array(
                [None if value is None else Decimal(value).scaleb(-2) for value in unscaled],
                type=pa.decimal128(15, 2),
            ),
        ], schema=schema)
        parquet_path = output / f"{case}.parquet"
        pq.write_table(table, parquet_path, row_group_size=4093)
        subprocess.run([str(writer), str(parquet_path), str(output / f"{case}.vortex")], check=True)
        (output / f"{case}.json").write_text(json.dumps({
            "writer": version,
            "rows": rows,
            "date_days": date_values,
            "amount_unscaled": unscaled,
        }) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writer", type=Path, required=True)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    generate(args.writer.resolve(), args.output)


if __name__ == "__main__":
    main()
