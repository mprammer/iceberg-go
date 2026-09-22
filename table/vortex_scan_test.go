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
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

// These tests take a Vortex file written by the Vortex Rust reference
// implementation all the way through Iceberg: register it in a table with
// AddFiles, then scan the table and check the values that come back. They are
// the end-to-end counterpart to the reader unit tests in table/internal.
//
// Fixture contents (see table/internal/vortex_files_test.go):
//
//	1000 rows, id 0..999, sum(id) == 499500,
//	857 non-null `name`, 800 non-null `score`, 334 true `flag`.
const (
	vortexE2ERows        = 1000
	vortexE2ESumID       = int64(499500)
	vortexE2ENonNullName = 857
)

func TestVortexConformance(t *testing.T) {
	backends := vortex.AvailableBackends()
	require.Contains(t, backends, vortex.Native, "the native backend must be available without cgo")
	for _, backend := range backends {
		t.Run(string(backend), func(t *testing.T) {
			for _, tc := range []struct {
				name string
				run  func(*testing.T, context.Context)
			}{
				{"AddFilesAndScan", testVortexAddFilesAndScan},
				{"Projection", testVortexScanWithProjection},
				{"RowFilter", testVortexScanWithRowFilter},
				{"RowLineage", testVortexRowLineageWithRowFilter},
				{"FilterOnlyColumns", testVortexFilterOnlyColumns},
				{"MissingInitialDefault", testVortexMissingInitialDefault},
				{"LimitAfterResidual", testVortexLimitAfterResidual},
				{"PositionDeletes", testVortexPositionDeletes},
				{"EqualityDeletes", testVortexEqualityDeletes},
				{"PartitionedRegistration", testVortexPartitionedRegistration},
				{"NullableNegativePredicates", testVortexNullableNegativePredicates},
			} {
				t.Run(tc.name, func(t *testing.T) {
					tc.run(t, vortex.WithBackend(t.Context(), backend))
				})
			}
		})
	}
}

// vortexTestSchema mirrors the fixture. Field IDs here are what the file's
// columns get matched to by name, since Vortex carries no in-file field IDs.
func vortexTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "score", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 4, Name: "flag", Type: iceberg.PrimitiveTypes.Bool},
	)
}

// stageVortexFixture copies the fixture into a fresh warehouse directory and
// returns the warehouse location and the file's path inside it.
func stageVortexFixture(t *testing.T) (location, filePath string) {
	t.Helper()

	src, err := os.ReadFile(filepath.Join("internal", "testdata", "vortex", "simple.vortex"))
	require.NoError(t, err)

	location = t.TempDir()
	filePath = filepath.Join(location, "data", "simple.vortex")
	require.NoError(t, os.MkdirAll(filepath.Dir(filePath), 0o755))
	require.NoError(t, os.WriteFile(filePath, src, 0o644))

	return location, filePath
}

func newVortexTable(t *testing.T, location string) *table.Table {
	t.Helper()

	return newVortexTableAtVersion(t, location, "2")
}

func newVortexTableAtVersion(t *testing.T, location, formatVersion string) *table.Table {
	t.Helper()

	return newVortexTableWithSpec(t, location, formatVersion, iceberg.UnpartitionedSpec)
}

func newVortexTableWithSpec(t *testing.T, location, formatVersion string, spec *iceberg.PartitionSpec) *table.Table {
	t.Helper()

	meta, err := table.NewMetadata(vortexTestSchema(), spec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: formatVersion})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"default", "vortex_table"},
		meta,
		filepath.Join(location, "metadata", "v1.metadata.json"),
		func(ctx context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&mockedCatalog{meta},
	)
}

func testVortexAddFilesAndScan(t *testing.T, ctx context.Context) {
	location, filePath := stageVortexFixture(t)
	tbl := newVortexTable(t, location)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(ctx, []string{filePath}, nil, false))

	staged, err := tx.StagedTable()
	require.NoError(t, err)

	// The manifest entry must record VORTEX, not the Parquet default.
	snap := staged.CurrentSnapshot()
	require.NotNil(t, snap)
	require.Equal(t, "1", snap.Summary.Properties["added-data-files"])
	require.Equal(t, "1000", snap.Summary.Properties["added-records"])

	scan, err := tx.Scan()
	require.NoError(t, err)

	tasks, err := scan.PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, iceberg.VortexFile, tasks[0].File.FileFormat())

	arrTbl, err := scan.ToArrowTable(ctx)
	require.NoError(t, err)
	defer arrTbl.Release()

	require.EqualValues(t, vortexE2ERows, arrTbl.NumRows())
	require.EqualValues(t, 4, arrTbl.NumCols())

	// Values, not just shape: check the sum and the null count survive the
	// whole path from the Vortex file to the Arrow table.
	var (
		sumID     int64
		nonNullNm int
	)
	idCol := arrTbl.Column(0).Data()
	for _, chunk := range idCol.Chunks() {
		ids, ok := chunk.(*array.Int32)
		require.True(t, ok)
		for i := range ids.Len() {
			sumID += int64(ids.Value(i))
		}
	}
	nameCol := arrTbl.Column(1).Data()
	for _, chunk := range nameCol.Chunks() {
		nonNullNm += chunk.Len() - chunk.NullN()
	}

	require.Equal(t, vortexE2ESumID, sumID)
	require.Equal(t, vortexE2ENonNullName, nonNullNm)

	// Counts alone miss reordered rows or shifted validity bits. Check each
	// physical position against the fixture's independent generation rules.
	records := array.NewTableReader(arrTbl, 127)
	defer records.Release()
	position := 0
	for records.Next() {
		record := records.RecordBatch()
		ids := record.Column(0).(*array.Int32)
		names := record.Column(1).(*array.String)
		scores := record.Column(2).(*array.Float64)
		flags := record.Column(3).(*array.Boolean)
		for i := range ids.Len() {
			require.False(t, ids.IsNull(i), "position %d", position)
			require.EqualValues(t, position, ids.Value(i))
			require.Equal(t, position%7 == 0, names.IsNull(i), "name at position %d", position)
			require.Equal(t, position%5 == 0, scores.IsNull(i), "score at position %d", position)
			require.False(t, flags.IsNull(i), "position %d", position)
			require.Equal(t, position%3 == 0, flags.Value(i), "flag at position %d", position)
			position++
		}
	}
	require.NoError(t, records.Err())
	require.Equal(t, vortexE2ERows, position)
}

// A projected scan must read only the selected columns out of the Vortex file
// while still producing the right values.
func testVortexScanWithProjection(t *testing.T, ctx context.Context) {
	location, filePath := stageVortexFixture(t)
	tbl := newVortexTable(t, location)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(ctx, []string{filePath}, nil, false))

	scan, err := tx.Scan(table.WithSelectedFields("id", "score"))
	require.NoError(t, err)

	arrTbl, err := scan.ToArrowTable(ctx)
	require.NoError(t, err)
	defer arrTbl.Release()

	require.EqualValues(t, 2, arrTbl.NumCols())
	require.EqualValues(t, vortexE2ERows, arrTbl.NumRows())
	require.Equal(t, "id", arrTbl.Schema().Field(0).Name)
	require.Equal(t, "score", arrTbl.Schema().Field(1).Name)
	requireVortexIDs(t, arrTbl, vortexIDsWhere(func(int32) bool { return true }))
	require.Equal(t, 200, arrTbl.Column(1).NullN())

	for _, tc := range []struct {
		column string
		nulls  int
	}{
		{"id", 0}, {"name", 143}, {"score", 200}, {"flag", 0},
	} {
		t.Run(tc.column, func(t *testing.T) {
			scan, err := tx.Scan(table.WithSelectedFields(tc.column))
			require.NoError(t, err)
			out := readVortexScan(t, ctx, scan)
			require.EqualValues(t, vortexE2ERows, out.NumRows())
			require.EqualValues(t, 1, out.NumCols())
			require.Equal(t, tc.column, out.Schema().Field(0).Name)
			require.Equal(t, tc.nulls, out.Column(0).NullN())
		})
	}
}

// A row filter must apply correctly to a Vortex file. Manifest-level pruning
// cannot help here because the reader publishes no column bounds, so this
// exercises the in-file predicate path.
func testVortexScanWithRowFilter(t *testing.T, ctx context.Context) {
	location, filePath := stageVortexFixture(t)
	tbl := newVortexTable(t, location)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(ctx, []string{filePath}, nil, false))

	scan, err := tx.Scan(table.WithRowFilter(
		iceberg.LessThan(iceberg.Reference("id"), int32(100))))
	require.NoError(t, err)

	arrTbl, err := scan.ToArrowTable(ctx)
	require.NoError(t, err)
	defer arrTbl.Release()

	require.EqualValues(t, 100, arrTbl.NumRows())
}

// Filtering may leave non-contiguous physical positions. Synthesized _row_id
// must use those positions rather than dense indexes among surviving rows.
func testVortexRowLineageWithRowFilter(t *testing.T, ctx context.Context) {
	location, filePath := stageVortexFixture(t)
	tbl := newVortexTableAtVersion(t, location, "3")

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(ctx, []string{filePath}, nil, false))

	scan, err := tx.Scan(
		table.WithRowLineage(),
		table.WithRowFilter(iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(990))))
	require.NoError(t, err)

	arrTbl, err := scan.ToArrowTable(ctx)
	require.NoError(t, err)
	defer arrTbl.Release()

	require.EqualValues(t, 10, arrTbl.NumRows())

	idIdx := arrTbl.Schema().FieldIndices("id")
	require.NotEmpty(t, idIdx)
	rowIDIdx := arrTbl.Schema().FieldIndices(iceberg.RowIDColumnName)
	require.NotEmpty(t, rowIDIdx, "_row_id must be projected under WithRowLineage")

	ids := chunkedInt32Values(t, arrTbl.Column(idIdx[0]).Data())
	rowIDs := chunkedInt64Values(t, arrTbl.Column(rowIDIdx[0]).Data())
	require.Len(t, rowIDs, 10)

	// id == file position in this fixture, so _row_id must track id exactly.
	// A dense 0..9 here would mean rows were dropped before positions were read.
	for i, id := range ids {
		require.EqualValues(t, id, rowIDs[i],
			"_row_id must be the row's original file position, not its index among survivors")
	}
	require.EqualValues(t, 990, rowIDs[0])
}

func chunkedInt32Values(t *testing.T, col *arrow.Chunked) []int32 {
	t.Helper()

	var out []int32
	for _, chunk := range col.Chunks() {
		arr, ok := chunk.(*array.Int32)
		require.True(t, ok, "expected int32, got %T", chunk)
		out = append(out, arr.Int32Values()...)
	}

	return out
}

func chunkedInt64Values(t *testing.T, col *arrow.Chunked) []int64 {
	t.Helper()

	var out []int64
	for _, chunk := range col.Chunks() {
		arr, ok := chunk.(*array.Int64)
		require.True(t, ok, "expected int64, got %T", chunk)
		out = append(out, arr.Int64Values()...)
	}

	return out
}

func TestVortexAddFilesAfterRemovingLastPartition(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			location, dataPath := stageVortexFixture(t)
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_trunc", Transform: iceberg.TruncateTransform{Width: 1000}})
			tbl := newVortexTableWithSpec(t, location, "2", &spec)
			tx := tbl.NewTransaction()
			require.NoError(t, tx.UpdateSpec(false).RemoveField("id_trunc").Commit())
			var err error
			tbl, err = tx.Commit(ctx)
			require.NoError(t, err)
			evolved := tbl.Spec()
			require.True(t, evolved.IsUnpartitioned())
			require.Greater(t, evolved.ID(), 0)
			tx = tbl.NewTransaction()
			require.NoError(t, tx.AddFiles(ctx, []string{dataPath}, nil, false))
			tbl, err = tx.Commit(ctx)
			require.NoError(t, err)
			tasks, err := tbl.Scan().PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			require.EqualValues(t, evolved.ID(), tasks[0].File.SpecID())
			out := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("id")))
			requireVortexIDs(t, out, vortexIDsWhere(func(int32) bool { return true }))
		})
	}
}

func TestVortexV1VoidPartitionRegistration(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			location, dataPath := stageVortexFixture(t)

			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_trunc", Transform: iceberg.TruncateTransform{Width: 1000}})
			tbl := newVortexTableWithSpec(t, location, "1", &spec)
			tx := tbl.NewTransaction()
			require.NoError(t, tx.UpdateSpec(false).RemoveField("id_trunc").Commit())
			var err error
			tbl, err = tx.Commit(ctx)
			require.NoError(t, err)
			evolved := tbl.Spec()
			require.True(t, evolved.IsUnpartitioned())
			require.Equal(t, 1, evolved.NumFields())
			require.IsType(t, iceberg.VoidTransform{}, evolved.Field(0).Transform)
			tx = tbl.NewTransaction()
			require.NoError(t, tx.AddFiles(ctx, []string{dataPath}, nil, false))
			tbl, err = tx.Commit(ctx)
			require.NoError(t, err)
			tasks, err := tbl.Scan().PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			df := tasks[0].File
			require.Equal(t, iceberg.VortexFile, df.FileFormat())
			require.EqualValues(t, evolved.ID(), df.SpecID())
			require.EqualValues(t, 1000, df.Count())
			require.Contains(t, df.Partition(), evolved.Field(0).FieldID)
			require.Nil(t, df.Partition()[evolved.Field(0).FieldID])
			out := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("id")))
			requireVortexIDs(t, out, vortexIDsWhere(func(int32) bool { return true }))
		})
	}
}
