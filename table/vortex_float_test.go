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
	"math"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

func vortexFloatTable(t *testing.T, ctx context.Context) *table.Table {
	t.Helper()

	location := t.TempDir()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "value64", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 3, Name: "value32", Type: iceberg.PrimitiveTypes.Float32})
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		location, iceberg.Properties{table.PropertyFormatVersion: "3"})
	require.NoError(t, err)
	tbl := table.New(table.Identifier{"default", "float_edges"}, meta,
		filepath.Join(location, "metadata", "v1.metadata.json"),
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &mockedCatalog{meta})
	path, err := filepath.Abs("internal/testdata/vortex/float_edges.vortex")
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(ctx, []string{path}, nil, false))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	return tbl
}

func testVortexFloatPredicates[T float32 | float64](t *testing.T, ctx context.Context,
	tbl *table.Table, field string, nan, otherNaN T,
) {
	t.Helper()

	ref := iceberg.Reference(field)
	tests := []struct {
		name string
		expr iceberg.BooleanExpression
	}{
		{"null", iceberg.IsNull(ref)},
		{"nonnull", iceberg.NotNull(ref)},
		{"nan", iceberg.IsNaN(ref)},
		{"not_nan", iceberg.NotNaN(ref)},
		{"in_zero_one", iceberg.IsIn(ref, T(0), T(1))},
		{"not_in_zero_one", iceberg.NotIn(ref, T(0), T(1))},
		{"in_nan_one", iceberg.IsIn(ref, nan, T(1))},
		{"not_in_nan_one", iceberg.NotIn(ref, nan, T(1))},
		{"and", iceberg.NewAnd(iceberg.EqualTo(ref, T(0)), iceberg.GreaterThan(iceberg.Reference("id"), int32(3)))},
		{"or", iceberg.NewOr(iceberg.EqualTo(ref, T(0)), iceberg.LessThan(iceberg.Reference("id"), int32(2)))},
		{"not_and", iceberg.NewNot(iceberg.NewAnd(iceberg.EqualTo(ref, T(0)), iceberg.GreaterThan(iceberg.Reference("id"), int32(3))))},
		{"not_or", iceberg.NewNot(iceberg.NewOr(iceberg.EqualTo(ref, T(0)), iceberg.LessThan(iceberg.Reference("id"), int32(2))))},
	}
	for _, literal := range []struct {
		name  string
		value T
	}{
		{"zero", T(0)},
		{"negative_zero", T(math.Copysign(0, -1))},
		{"nan", nan},
		{"other_nan", otherNaN},
		{"infinity", T(math.Inf(1))},
		{"negative_infinity", T(math.Inf(-1))},
		{"one", T(1)},
		{"negative_one", T(-1)},
	} {
		for _, op := range []struct {
			name string
			expr iceberg.BooleanExpression
		}{
			{"equal", iceberg.EqualTo(ref, literal.value)},
			{"not_equal", iceberg.NotEqualTo(ref, literal.value)},
			{"less", iceberg.LessThan(ref, literal.value)},
			{"less_equal", iceberg.LessThanEqual(ref, literal.value)},
			{"greater", iceberg.GreaterThan(ref, literal.value)},
			{"greater_equal", iceberg.GreaterThanEqual(ref, literal.value)},
		} {
			tests = append(tests, struct {
				name string
				expr iceberg.BooleanExpression
			}{op.name + "/" + literal.name, op.expr})
		}
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Evaluate the predicate over an unfiltered read so the oracle cannot
			// drop the same rows as an incorrect backend predicate translation.
			want := vortexResidualIDs(t, ctx, tbl, tc.expr)
			out := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("id"), table.WithRowFilter(tc.expr)))
			requireVortexIDs(t, out, want)
			switch tc.name {
			case "equal/zero", "equal/negative_zero":
				require.Equal(t, []int32{4, 5}, want)
			case "nan":
				require.Equal(t, []int32{0, 1, 8}, want)
			case "null":
				require.Equal(t, []int32{9}, want)
			}
		})
	}
}

func TestVortexFloatPredicateSemantics(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			tbl := vortexFloatTable(t, ctx)
			t.Run("float64", func(t *testing.T) {
				testVortexFloatPredicates(t, ctx, tbl, "value64",
					math.Float64frombits(0x7ff8000000000000), math.Float64frombits(0x7ff8000000000001))
			})
			t.Run("float32", func(t *testing.T) {
				testVortexFloatPredicates(t, ctx, tbl, "value32",
					math.Float32frombits(0x7fc00000), math.Float32frombits(0x7fc00001))
			})
		})
	}
}
