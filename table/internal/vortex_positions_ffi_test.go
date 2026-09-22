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

//go:build vortex && cgo

package internal

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestFFIVortexPhysicalPositions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "__iceberg_vortex_row_position", Type: arrow.PrimitiveTypes.Int64},
		{Name: "__iceberg_vortex_row_position_", Type: arrow.PrimitiveTypes.Uint64},
	}, nil)
	values := array.NewInt64Builder(mem)
	values.AppendValues([]int64{50, 60, 90, 100, 110}, nil)
	data := values.NewArray()
	values.Release()
	indices := array.NewUint64Builder(mem)
	indices.AppendValues([]uint64{5, 6, 9, 10, 11}, nil)
	positions := indices.NewArray()
	indices.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{data, positions}, 5)
	data.Release()
	positions.Release()
	inner, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch})
	require.NoError(t, err)
	batch.Release()
	stripped := newFFIVortexPositionReader(inner)
	validated, err := newVortexPositionReader(stripped, 20)
	require.NoError(t, err)
	require.True(t, validated.Next())
	require.Equal(t, []RowGroupSpan{{5, 2}, {9, 3}}, validated.(RowPositionedRecordReader).RowPositions())
	out := validated.RecordBatch()
	out.Retain()
	require.EqualValues(t, 1, out.NumCols())
	require.Equal(t, "__iceberg_vortex_row_position", out.Schema().Field(0).Name, "only the private trailing column is removed")
	require.False(t, validated.Next())
	require.NoError(t, validated.Err())
	validated.Release()
	require.Equal(t, []int64{50, 60, 90, 100, 110}, out.Column(0).(*array.Int64).Int64Values())
	out.Release()
}

func TestFFIVortexRejectsMalformedPhysicalIndices(t *testing.T) {
	for _, tc := range []struct {
		name    string
		indices []uint64
		valid   []bool
	}{
		{"null", []uint64{1, 2}, []bool{true, false}},
		{"int64_overflow", []uint64{math.MaxInt64 + 1}, nil},
		{"past_file", []uint64{20}, nil},
		{"duplicates", []uint64{1, 1}, nil},
		{"out_of_order", []uint64{3, 2}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			schema := arrow.NewSchema([]arrow.Field{{Name: "_private", Type: arrow.PrimitiveTypes.Uint64, Nullable: true}}, nil)
			builder := array.NewUint64Builder(mem)
			builder.AppendValues(tc.indices, tc.valid)
			indices := builder.NewArray()
			builder.Release()
			batch := array.NewRecordBatch(schema, []arrow.Array{indices}, int64(len(tc.indices)))
			indices.Release()
			inner, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch})
			require.NoError(t, err)
			batch.Release()
			reader, err := newVortexPositionReader(newFFIVortexPositionReader(inner), 20)
			require.NoError(t, err)
			require.False(t, reader.Next())
			require.ErrorIs(t, reader.Err(), iceberg.ErrInvalidArgument)
			reader.Release()
		})
	}
}
