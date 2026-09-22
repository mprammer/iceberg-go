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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func registeredVortexTable(t *testing.T, ctx context.Context, version string) (*table.Table, string) {
	t.Helper()

	location, dataPath := stageVortexFixture(t)
	tx := newVortexTableAtVersion(t, location, version).NewTransaction()
	require.NoError(t, tx.AddFiles(ctx, []string{dataPath}, nil, false))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	return tbl, dataPath
}

func readVortexScan(t *testing.T, ctx context.Context, scan *table.Scan) arrow.Table {
	t.Helper()

	out, err := scan.ToArrowTable(ctx)
	require.NoError(t, err)
	t.Cleanup(out.Release)

	return out
}

func vortexIDsWhere(keep func(int32) bool) []int32 {
	out := make([]int32, 0)
	for id := range int32(vortexE2ERows) {
		if keep(id) {
			out = append(out, id)
		}
	}

	return out
}

func requireVortexIDs(t *testing.T, out arrow.Table, want []int32) {
	t.Helper()

	require.EqualValues(t, len(want), out.NumRows())
	indices := out.Schema().FieldIndices("id")
	require.Len(t, indices, 1)
	got := chunkedInt32Values(t, out.Column(indices[0]).Data())
	// Arrow represents an empty result using no chunks.
	if len(want) == 0 {
		require.Empty(t, got)
	} else {
		require.Equal(t, want, got)
	}
}

func testVortexFilterOnlyColumns(t *testing.T, ctx context.Context) {
	tbl, _ := registeredVortexTable(t, ctx, "2")
	for _, tc := range []struct {
		name   string
		filter iceberg.BooleanExpression
		keep   func(int32) bool
	}{
		{"null_name", iceberg.IsNull(iceberg.Reference("name")), func(id int32) bool { return id%7 == 0 }},
		{"nonnull_score", iceberg.NotNull(iceberg.Reference("score")), func(id int32) bool { return id%5 != 0 }},
		{"boolean", iceberg.EqualTo(iceberg.Reference("flag"), true), func(id int32) bool { return id%3 == 0 }},
		{"no_matches", iceberg.GreaterThan(iceberg.Reference("id"), int32(1000)), func(int32) bool { return false }},
		{"null_boolean", iceberg.IsNull(iceberg.Reference("flag")), func(int32) bool { return false }},
		{"negated_partial_conjunction", iceberg.NewNot(iceberg.NewAnd(
			iceberg.LessThan(iceberg.Reference("id"), int32(100)),
			iceberg.IsNaN(iceberg.Reference("score")))), func(int32) bool { return true }},
		{"negated_partial_disjunction", iceberg.NewNot(iceberg.NewOr(
			iceberg.LessThan(iceberg.Reference("id"), int32(100)),
			iceberg.IsNaN(iceberg.Reference("score")))), func(id int32) bool { return id >= 100 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := readVortexScan(t, ctx, tbl.Scan(
				table.WithSelectedFields("id"), table.WithRowFilter(tc.filter)))
			require.EqualValues(t, 1, out.NumCols(), "filter-only fields must not leak into the result")
			requireVortexIDs(t, out, vortexIDsWhere(tc.keep))
		})
	}
}

func testVortexMissingInitialDefault(t *testing.T, ctx context.Context) {
	tbl, _ := registeredVortexTable(t, ctx, "3")
	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).
		AddColumn([]string{"priority"}, iceberg.PrimitiveTypes.Int32, "", false, iceberg.Int32Literal(7)).
		Commit())
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	// priority is physically absent. Translating it to null before evaluating
	// the default would discard every row for both equality and IS NOT NULL.
	for _, tc := range []struct {
		name   string
		filter iceberg.BooleanExpression
		keep   func(int32) bool
	}{
		{"equal", iceberg.EqualTo(iceberg.Reference("priority"), int32(7)), func(int32) bool { return true }},
		{"nonnull_and_tail", iceberg.NewAnd(
			iceberg.NotNull(iceberg.Reference("priority")),
			iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(990))), func(id int32) bool { return id >= 990 }},
		{"null", iceberg.IsNull(iceberg.Reference("priority")), func(int32) bool { return false }},
		{"unequal", iceberg.EqualTo(iceberg.Reference("priority"), int32(8)), func(int32) bool { return false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := readVortexScan(t, ctx, tbl.Scan(
				table.WithSelectedFields("id"), table.WithRowFilter(tc.filter)))
			require.EqualValues(t, 1, out.NumCols())
			requireVortexIDs(t, out, vortexIDsWhere(tc.keep))
		})
	}

	t.Run("materialized_default", func(t *testing.T) {
		out := readVortexScan(t, ctx, tbl.Scan(
			table.WithSelectedFields("id", "priority"),
			table.WithRowFilter(iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(995)))))
		requireVortexIDs(t, out, []int32{995, 996, 997, 998, 999})
		require.Equal(t, []int32{7, 7, 7, 7, 7}, chunkedInt32Values(t, out.Column(1).Data()))
		require.Zero(t, out.Column(1).NullN())
	})
}

func testVortexLimitAfterResidual(t *testing.T, ctx context.Context) {
	tbl, _ := registeredVortexTable(t, ctx, "3")
	for _, lineage := range []bool{false, true} {
		t.Run(fmt.Sprintf("lineage_%t", lineage), func(t *testing.T) {
			opts := []table.ScanOption{
				table.WithSelectedFields("id"),
				table.WithRowFilter(iceberg.NewAnd(
					iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(990)),
					iceberg.EqualTo(iceberg.Reference("flag"), true))),
			}
			if lineage {
				opts = append(opts, table.WithRowLineage())
			}
			// The limit must apply after the exact residual and preserve original
			// positions for synthesized lineage, including when pruning is active.
			out := readVortexScan(t, ctx, tbl.Scan(opts...).UseRowLimit(3))
			requireVortexIDs(t, out, []int32{990, 993, 996})
			if lineage {
				rowID := out.Schema().FieldIndices(iceberg.RowIDColumnName)
				require.Len(t, rowID, 1)
				require.Equal(t, []int64{990, 993, 996}, chunkedInt64Values(t, out.Column(rowID[0]).Data()))
			}
		})
	}
}

func addVortexDeleteFile(t *testing.T, ctx context.Context, tbl *table.Table,
	content iceberg.ManifestEntryContent, schema *arrow.Schema, jsonRows string, count int64, equalityIDs []int,
) *table.Table {
	t.Helper()

	path := filepath.Join(tbl.Location(), "data", "delete.parquet")
	writeParquetFile(t, path, schema, jsonRows)
	info, err := os.Stat(path)
	require.NoError(t, err)
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, content,
		path, iceberg.ParquetFile, nil, nil, nil, count, info.Size())
	require.NoError(t, err)
	if len(equalityIDs) != 0 {
		builder.EqualityFieldIDs(equalityIDs)
	}

	tx := tbl.NewTransaction()
	delta := tx.NewRowDelta(nil)
	delta.AddDeletes(builder.Build())
	require.NoError(t, delta.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	return tbl
}

func testVortexPositionDeletes(t *testing.T, ctx context.Context) {
	tbl, dataPath := registeredVortexTable(t, ctx, "3")
	schema, err := table.SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, true, false)
	require.NoError(t, err)
	tbl = addVortexDeleteFile(t, ctx, tbl, iceberg.EntryContentPosDeletes, schema,
		fmt.Sprintf(`[{"file_path":%q,"pos":0},{"file_path":%q,"pos":990},{"file_path":%q,"pos":992}]`,
			dataPath, dataPath, dataPath), 3, nil)

	for _, lineage := range []bool{false, true} {
		t.Run(fmt.Sprintf("lineage_%t", lineage), func(t *testing.T) {
			opts := []table.ScanOption{
				table.WithSelectedFields("id"),
				table.WithRowFilter(iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(990))),
			}
			if lineage {
				opts = append(opts, table.WithRowLineage())
			}
			scan := tbl.Scan(opts...)
			tasks, err := scan.PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			require.Len(t, tasks[0].DeleteFiles, 1, "the fixture must plan the positional delete")

			out := readVortexScan(t, ctx, scan)
			requireVortexIDs(t, out, []int32{991, 993, 994, 995, 996, 997, 998, 999})
			if lineage {
				rowID := out.Schema().FieldIndices(iceberg.RowIDColumnName)
				require.Len(t, rowID, 1)
				require.Equal(t, []int64{991, 993, 994, 995, 996, 997, 998, 999},
					chunkedInt64Values(t, out.Column(rowID[0]).Data()))
			}

			limited := readVortexScan(t, ctx, scan.UseRowLimit(3))
			requireVortexIDs(t, limited, []int32{991, 993, 994})
		})
	}
}

func testVortexEqualityDeletes(t *testing.T, ctx context.Context) {
	t.Run("unselected_key", func(t *testing.T) {
		tbl, _ := registeredVortexTable(t, ctx, "3")
		schema, err := table.SchemaToArrowSchema(iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32}), nil, true, false)
		require.NoError(t, err)
		tbl = addVortexDeleteFile(t, ctx, tbl, iceberg.EntryContentEqDeletes, schema,
			`[{"id":0},{"id":991},{"id":999}]`, 3, []int{1})

		// Neither the projection nor its filter references the equality key.
		scan := tbl.Scan(table.WithSelectedFields("flag"),
			table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("flag"), true)))
		tasks, err := scan.PlanFiles(ctx)
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		require.Len(t, tasks[0].EqualityDeleteFiles, 1)
		out := readVortexScan(t, ctx, scan)
		require.EqualValues(t, 332, out.NumRows()) // 334 true flags, minus ids 0 and 999.
		require.EqualValues(t, 1, out.NumCols())
		require.Equal(t, "flag", out.Schema().Field(0).Name)
		for _, chunk := range out.Column(0).Data().Chunks() {
			flags, ok := chunk.(*array.Boolean)
			require.True(t, ok)
			for i := range flags.Len() {
				require.False(t, flags.IsNull(i))
				require.True(t, flags.Value(i))
			}
		}

		lineage := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("id"), table.WithRowLineage(),
			table.WithRowFilter(iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(990)))).UseRowLimit(3))
		requireVortexIDs(t, lineage, []int32{990, 992, 993})
		rowID := lineage.Schema().FieldIndices(iceberg.RowIDColumnName)
		require.Len(t, rowID, 1)
		require.Equal(t, []int64{990, 992, 993}, chunkedInt64Values(t, lineage.Column(rowID[0]).Data()))
	})

	t.Run("null_key", func(t *testing.T) {
		tbl, _ := registeredVortexTable(t, ctx, "2")
		schema, err := table.SchemaToArrowSchema(iceberg.NewSchema(0,
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String}), nil, true, false)
		require.NoError(t, err)
		tbl = addVortexDeleteFile(t, ctx, tbl, iceberg.EntryContentEqDeletes, schema,
			`[{"name":null}]`, 1, []int{2})
		out := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("id")))
		requireVortexIDs(t, out, vortexIDsWhere(func(id int32) bool { return id%7 != 0 }))
		require.EqualValues(t, 1, out.NumCols())
	})
}

func testVortexPartitionedRegistration(t *testing.T, ctx context.Context) {
	location, dataPath := stageVortexFixture(t)
	// Every fixture row belongs to one truncation partition. Registration
	// verifies the transformed values before publishing the file.
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id_trunc",
		Transform: iceberg.TruncateTransform{Width: 1000},
	})
	tx := newVortexTableWithSpec(t, location, "2", &spec).NewTransaction()
	require.NoError(t, tx.AddFiles(ctx, []string{dataPath}, nil, false))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)
	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, map[int]any{1000: int32(0)}, tasks[0].File.Partition())
	out := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("id")))
	requireVortexIDs(t, out, vortexIDsWhere(func(int32) bool { return true }))
}

func testVortexNullableNegativePredicates(t *testing.T, ctx context.Context) {
	tbl, _ := registeredVortexTable(t, ctx, "3")
	for _, tc := range []struct {
		name   string
		filter iceberg.BooleanExpression
	}{
		{"not_equal_name", iceberg.NotEqualTo(iceberg.Reference("name"), "name-1")},
		{"not_in_name", iceberg.NotIn(iceberg.Reference("name"), "name-1", "name-2")},
		{"not_equal_score", iceberg.NotEqualTo(iceberg.Reference("score"), float64(1.5))},
		{"not_in_score", iceberg.NotIn(iceberg.Reference("score"), float64(1.5), float64(3))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := []table.ScanOption{table.WithSelectedFields("id"), table.WithRowFilter(tc.filter)}
			// Read all rows before applying Arrow's residual, independently of
			// Vortex predicate pushdown and row-lineage projection.
			want := vortexResidualIDs(t, ctx, tbl, tc.filter)

			out := readVortexScan(t, ctx, tbl.Scan(opts...))
			require.EqualValues(t, 1, out.NumCols())
			requireVortexIDs(t, out, want)
		})
	}
}
