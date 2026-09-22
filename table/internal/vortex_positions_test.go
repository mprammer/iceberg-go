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

package internal

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

func TestVortexPhysicalPositionValidation(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		spans                       []RowGroupSpan
		rows, fileRows, previousEnd int64
		valid                       bool
	}{
		{"sparse", []RowGroupSpan{{3, 2}, {10, 3}}, 5, 20, 0, true},
		{"empty_batch", nil, 0, 20, 8, true},
		{"maximum_position", []RowGroupSpan{{math.MaxInt64 - 1, 1}}, 1, math.MaxInt64, 0, true},
		{"negative_position", []RowGroupSpan{{-1, 1}}, 1, 20, 0, false},
		{"zero_span", []RowGroupSpan{{3, 0}}, 0, 20, 0, false},
		{"negative_span", []RowGroupSpan{{3, -1}}, 1, 20, 0, false},
		{"missing_rows", []RowGroupSpan{{3, 1}}, 2, 20, 0, false},
		{"extra_rows", []RowGroupSpan{{3, 3}}, 2, 20, 0, false},
		{"past_file", []RowGroupSpan{{19, 2}}, 2, 20, 0, false},
		{"overflow", []RowGroupSpan{{math.MaxInt64 - 1, 3}}, 3, math.MaxInt64, 0, false},
		{"overlap", []RowGroupSpan{{3, 2}, {4, 1}}, 3, 20, 0, false},
		{"unordered", []RowGroupSpan{{10, 1}, {3, 2}}, 3, 20, 0, false},
		{"previous_batch_overlap", []RowGroupSpan{{3, 1}}, 1, 20, 4, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateVortexRowPositions(tc.spans, tc.rows, tc.fileRows, tc.previousEnd)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			}
		})
	}
}

type positionedTestReader struct {
	array.RecordReader
	spans [][]RowGroupSpan
	batch int
}

func (r *positionedTestReader) Next() bool {
	if !r.RecordReader.Next() {
		return false
	}
	r.batch++

	return true
}
func (r *positionedTestReader) RowPositions() []RowGroupSpan { return r.spans[r.batch-1] }

func TestVortexPositionReaderRejectsCrossBatchReordering(t *testing.T) {
	schema := arrow.NewSchema(nil, nil)
	batch := array.NewRecordBatch(schema, nil, 2)
	defer batch.Release()
	inner, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch, batch})
	require.NoError(t, err)
	positioned := &positionedTestReader{RecordReader: inner, spans: [][]RowGroupSpan{{{5, 2}}, {{6, 2}}}}
	reader, err := newVortexPositionReader(positioned, 10)
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next())
	require.False(t, reader.Next())
	require.ErrorIs(t, reader.Err(), iceberg.ErrInvalidArgument)
	require.False(t, reader.Next())
}

func TestVortexEmptyProjectionPreservesPhysicalPositions(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			reader, err := vortexFormat{}.Open(ctx, iceio.LocalFS{}, vortexFixture)
			require.NoError(t, err)
			defer reader.Close()
			schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32})
			bound, err := iceberg.BindExpr(schema, iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(990)), true)
			require.NoError(t, err)
			for _, filter := range []iceberg.BooleanExpression{iceberg.AlwaysTrue{}, bound} {
				_, cols, err := reader.PrunedSchema(map[int]struct{}{}, fixtureNameMapping())
				require.NoError(t, err)
				records, err := reader.GetRecords(ctx, cols, &VortexScanFilter{Expr: filter, FileSchema: schema, NeedPositions: true})
				require.NoError(t, err)
				positioned, ok := records.(RowPositionedRecordReader)
				require.True(t, ok)
				var rows, end int64
				for records.Next() {
					batch := records.RecordBatch()
					require.Zero(t, batch.NumCols())
					end, err = validateVortexRowPositions(positioned.RowPositions(), batch.NumRows(), 1000, end)
					require.NoError(t, err)
					rows += batch.NumRows()
				}
				require.NoError(t, records.Err())
				require.EqualValues(t, 1000, end)
				if filter.Equals(iceberg.AlwaysTrue{}) {
					require.EqualValues(t, 1000, rows)
				} else if backend == vortex.FFI {
					require.EqualValues(t, 10, rows, "the empty projection must not bypass its pushed predicate")
				} else {
					require.GreaterOrEqual(t, rows, int64(10), "native returns conservative pruning candidates")
				}
				records.Release()
			}
		})
	}
}
