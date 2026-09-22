// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/substrait"
	"github.com/stretchr/testify/require"
)

// Evaluate the normal Arrow residual over a complete, unfiltered read. No
// backend receives the predicate, so this remains independent of pushdown.
func vortexResidualIDs(t *testing.T, ctx context.Context, tbl *table.Table, filter iceberg.BooleanExpression) []int32 {
	t.Helper()
	bound, err := iceberg.BindExpr(tbl.Schema(), filter, true)
	require.NoError(t, err)
	extensions, expression, err := substrait.ConvertExpr(tbl.Schema(), bound, true)
	require.NoError(t, err)
	ctx = exprs.WithExtensionIDSet(ctx, exprs.NewExtensionSetDefault(*extensions))
	unfiltered := readVortexScan(t, ctx, tbl.Scan())
	records := array.NewTableReader(unfiltered, 127)
	defer records.Release()
	var ids []int32
	for records.Next() {
		record := records.RecordBatch()
		input := compute.NewDatumWithoutOwning(record)
		mask, err := exprs.ExecuteScalarExpression(ctx, record.Schema(), expression, input)
		require.NoError(t, err)
		filtered, err := compute.Filter(ctx, input, mask, *compute.DefaultFilterOptions())
		mask.Release()
		require.NoError(t, err)
		result := filtered.(*compute.RecordDatum).Value
		column := result.Column(result.Schema().FieldIndices("id")[0]).(*array.Int32)
		ids = append(ids, column.Int32Values()...)
		filtered.Release()
	}
	require.NoError(t, records.Err())

	return ids
}
