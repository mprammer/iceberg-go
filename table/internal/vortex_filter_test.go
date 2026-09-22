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
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal/vortexffi"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

// fixtureIcebergSchema is the fixture's schema on the Iceberg side, which the
// converter resolves field paths against.
func fixtureIcebergSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "score", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 4, Name: "flag", Type: iceberg.PrimitiveTypes.Bool},
	)
}

// bindFilter binds an unbound Iceberg expression to the fixture schema, as the
// scanner does before handing a filter to the reader.
func bindFilter(t *testing.T, expr iceberg.BooleanExpression) iceberg.BooleanExpression {
	t.Helper()

	bound, err := iceberg.BindExpr(fixtureIcebergSchema(), expr, true)
	require.NoError(t, err)

	return bound
}

// countPushedDown scans the fixture with the filter pushed into Vortex and
// returns how many rows Vortex emitted.
//
// This deliberately goes through the reader rather than a table scan: the
// scanner re-applies the row filter in Go afterwards, which would produce the
// right answer even if nothing were pushed down at all. Reading here means the
// row count reflects only what Vortex itself decided to return.
func countPushedDown(t *testing.T, expr iceberg.BooleanExpression) int {
	t.Helper()

	rdr := openFixture(t, vortex.WithBackend(t.Context(), vortex.FFI))

	recRdr, err := rdr.GetRecords(context.Background(), nil,
		&VortexScanFilter{Expr: bindFilter(t, expr), FileSchema: fixtureIcebergSchema()})
	require.NoError(t, err)
	defer recRdr.Release()

	rows := 0
	for recRdr.Next() {
		rows += int(recRdr.RecordBatch().NumRows())
	}
	require.NoError(t, recRdr.Err())

	return rows
}

// TestVortexPushdownIsHonored is the test that would catch pushdown silently not
// happening. Each expected count is what the predicate selects out of the
// fixture's 1000 rows; if the filter never reached Vortex, every case would come
// back as 1000.
func TestVortexPushdownIsHonored(t *testing.T) {
	tests := []struct {
		name string
		expr iceberg.BooleanExpression
		want int
	}{
		{
			name: "less than on int32",
			expr: iceberg.LessThan(iceberg.Reference("id"), int32(100)),
			want: 100,
		},
		{
			name: "greater equal on int32",
			expr: iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(900)),
			want: 100,
		},
		{
			name: "equality on int32",
			expr: iceberg.EqualTo(iceberg.Reference("id"), int32(42)),
			want: 1,
		},
		{
			name: "conjunction narrows further",
			expr: iceberg.NewAnd(
				iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(100)),
				iceberg.LessThan(iceberg.Reference("id"), int32(200))),
			want: 100,
		},
		{
			name: "disjunction unions both arms",
			expr: iceberg.NewOr(
				iceberg.LessThan(iceberg.Reference("id"), int32(10)),
				iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(995))),
			want: 15,
		},
		{
			// name is NULL exactly where id % 7 == 0: 143 of 1000 rows.
			name: "is null on a nullable string",
			expr: iceberg.IsNull(iceberg.Reference("name")),
			want: 143,
		},
		{
			name: "not null on a nullable string",
			expr: iceberg.NotNull(iceberg.Reference("name")),
			want: 857,
		},
		{
			// score is NULL where id % 5 == 0: 200 of 1000 rows.
			name: "is null on a nullable double",
			expr: iceberg.IsNull(iceberg.Reference("score")),
			want: 200,
		},
		{
			name: "equality on string",
			expr: iceberg.EqualTo(iceberg.Reference("name"), "name-1"),
			want: 1,
		},
		{
			name: "in on int32 expands to ored equalities",
			expr: iceberg.IsIn(iceberg.Reference("id"), int32(1), int32(2), int32(3)),
			want: 3,
		},
		{
			name: "equality on bool",
			expr: iceberg.EqualTo(iceberg.Reference("flag"), true),
			want: 334,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, countPushedDown(t, tt.expr))
		})
	}
}

// A pushed-down filter must never drop a row the predicate accepts. Comparing
// against an unfiltered scan of the same column makes that concrete rather than
// trusting a hand-computed count.
func TestVortexPushdownAgreesWithUnfilteredScan(t *testing.T) {
	unfiltered := 0
	rdr := openFixture(t, vortex.WithBackend(t.Context(), vortex.FFI))
	recRdr, err := rdr.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	for recRdr.Next() {
		unfiltered += int(recRdr.RecordBatch().NumRows())
	}
	require.NoError(t, recRdr.Err())
	recRdr.Release()

	below := countPushedDown(t, iceberg.LessThan(iceberg.Reference("id"), int32(400)))
	atOrAbove := countPushedDown(t, iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(400)))

	require.Equal(t, unfiltered, below+atOrAbove,
		"a partition of the predicate space must cover every row exactly once")
}

// Predicates Vortex cannot express must be reported as untranslatable so the
// caller scans without a filter, rather than being pushed down as something
// weaker that would silently drop rows.
func TestVortexUnconvertiblePredicates(t *testing.T) {
	tests := []struct {
		name string
		expr iceberg.BooleanExpression
	}{
		{
			// No LIKE or starts_with in the C FFI expression API.
			name: "starts with",
			expr: iceberg.StartsWith(iceberg.Reference("name"), "name-1"),
		},
		{
			// An OR with one untranslatable arm has to be abandoned whole:
			// keeping only the translatable arm would exclude matching rows.
			name: "or with an untranslatable arm",
			expr: iceberg.NewOr(
				iceberg.LessThan(iceberg.Reference("id"), int32(10)),
				iceberg.StartsWith(iceberg.Reference("name"), "name-9")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arena := vortexffi.NewExprArena()
			defer arena.Close()

			expr, err := BuildVortexFilter(arena, fixtureIcebergSchema(), bindFilter(t, tt.expr))
			require.NoError(t, err)
			require.Nil(t, expr, "expected no pushdown for an untranslatable predicate")
		})
	}
}

// An AND may keep just its translatable side: the untranslatable half is
// re-applied in Go, so narrowing by the half Vortex understands is still
// correct and still saves work.
func TestVortexPushdownKeepsTranslatableSideOfAnd(t *testing.T) {
	expr := iceberg.NewAnd(
		iceberg.LessThan(iceberg.Reference("id"), int32(100)),
		iceberg.StartsWith(iceberg.Reference("name"), "name-9"))

	arena := vortexffi.NewExprArena()
	defer arena.Close()

	built, err := BuildVortexFilter(arena, fixtureIcebergSchema(), bindFilter(t, expr))
	require.NoError(t, err)
	require.NotNil(t, built, "the id < 100 side should still be pushed down")

	// The surviving half alone selects 100 rows; the StartsWith is left for the
	// scanner to apply.
	require.Equal(t, 100, countPushedDown(t, expr))
}

func TestVortexNegationBeforePartialPushdown(t *testing.T) {
	// The fixture has no NaNs, so the unsupported predicate is always false.
	// Weakening the inner AND before negation would incorrectly drop ids < 100.
	idFilter := iceberg.LessThan(iceberg.Reference("id"), int32(100))
	unsupported := iceberg.IsNaN(iceberg.Reference("score"))
	for _, tc := range []struct {
		name       string
		expr       iceberg.BooleanExpression
		pushesDown bool
		rows       int
	}{
		{"not_and", iceberg.NewNot(iceberg.NewAnd(idFilter, unsupported)), false, 1000},
		{"not_or", iceberg.NewNot(iceberg.NewOr(idFilter, unsupported)), true, 900},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arena := vortexffi.NewExprArena()
			defer arena.Close()
			expr, err := BuildVortexFilter(arena, fixtureIcebergSchema(), bindFilter(t, tc.expr))
			require.NoError(t, err)
			require.Equal(t, tc.pushesDown, expr != nil)
			require.Equal(t, tc.rows, countPushedDown(t, tc.expr))
		})
	}
}

func TestVortexTransformIsNotPhysicalReference(t *testing.T) {
	term, err := iceberg.NewUnboundTransform(iceberg.TruncateTransform{Width: 10},
		iceberg.Reference("id")).Bind(fixtureIcebergSchema(), true)
	require.NoError(t, err)
	arena := vortexffi.NewExprArena()
	defer arena.Close()
	converter := &vortexFilterConverter{arena: arena, sc: fixtureIcebergSchema()}

	// truncate(id, 10) = 0 includes ids 0..9. Comparing the physical id to 0
	// would silently discard ids 1..9, so the converter must reject this term.
	converted := converter.VisitEqual(term, iceberg.Int32Literal(0))
	require.Equal(t, convUnconvertible, converted.kind)
	require.Nil(t, converted.expr)
}
