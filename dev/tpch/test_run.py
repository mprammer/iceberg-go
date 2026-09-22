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

import datetime
from decimal import Decimal
import unittest

import run


class ResultComparisonTest(unittest.TestCase):
    def test_unordered_rows_preserve_exact_types_and_nulls(self):
        rows = [(None, Decimal("-1.23"), datetime.date(1960, 1, 1)),
                (2, Decimal("1234567890123.45"), datetime.date(1995, 3, 15))]
        run.compare_rows(rows, list(reversed(rows)))

    def test_same_count_different_duplicate_multiplicity_fails(self):
        with self.assertRaises(AssertionError):
            run.compare_rows([(1,), (1,), (2,)], [(1,), (2,), (2,)])

    def test_decimal_and_date_changes_fail(self):
        for expected, actual in [
            (Decimal("9999999999999.99"), Decimal("9999999999999.98")),
            (Decimal("1.00"), 1.0),
            (datetime.date(1995, 3, 15), datetime.date(1995, 3, 16)),
            (None, 0),
        ]:
            with self.subTest(expected=expected, actual=actual):
                with self.assertRaises(AssertionError):
                    run.compare_rows([(expected,)], [(actual,)])

    def test_sql_float_tolerance_is_bounded(self):
        run.compare_rows([(1.0,)], [(1.0 + 1e-13,)])
        for changed in (1.01, float("nan"), float("inf")):
            with self.subTest(changed=changed):
                with self.assertRaises(AssertionError):
                    run.compare_rows([(1.0,)], [(changed,)])


if __name__ == "__main__":
    unittest.main()
