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
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

// The fixture was written by the Vortex Rust reference implementation
// (`vx convert`) from a DuckDB-written Parquet file, so these tests read
// across implementations rather than round-tripping our own writer. Its
// contents, and the reference values every assertion below uses:
//
//	1000 rows: id INT32 0..999 (no nulls)
//	           name  STRING, NULL where id % 7 == 0  -> 857 non-null
//	           score DOUBLE, NULL where id % 5 == 0  -> 800 non-null
//	           flag  BOOL,   id % 3 == 0             (no nulls)
//	sum(id) == 499500
const (
	vortexFixture = "testdata/vortex/simple.vortex"

	fixtureRows         = 1000
	fixtureSumID        = int64(499500)
	fixtureNonNullName  = 857
	fixtureNonNullScore = 800
)

// fixtureNameMapping stands in for the in-file field IDs that Vortex does not
// yet carry, which is how the reader resolves Iceberg field IDs today.
func fixtureNameMapping() iceberg.NameMapping {
	id := func(i int) *int { return &i }

	return iceberg.NameMapping{
		{Names: []string{"id"}, FieldID: id(1)},
		{Names: []string{"name"}, FieldID: id(2)},
		{Names: []string{"score"}, FieldID: id(3)},
		{Names: []string{"flag"}, FieldID: id(4)},
	}
}

func fieldNames(sc *arrow.Schema) []string {
	names := make([]string, 0, len(sc.Fields()))
	for _, f := range sc.Fields() {
		names = append(names, f.Name)
	}

	return names
}

func openFixture(t *testing.T, ctx context.Context) FileReader {
	t.Helper()

	fs, err := iceio.LoadFS(ctx, nil, vortexFixture)
	require.NoError(t, err)

	rdr, err := vortexFormat{}.Open(ctx, fs, vortexFixture)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rdr.Close()) })

	return rdr
}

func testVortexSchema(t *testing.T, ctx context.Context) {
	rdr := openFixture(t, ctx)

	sc, err := rdr.Schema()
	require.NoError(t, err)

	require.Equal(t, []string{"id", "name", "score", "flag"}, fieldNames(sc))
	require.Equal(t, arrow.INT32, sc.Field(0).Type.ID())
	require.Equal(t, arrow.FLOAT64, sc.Field(2).Type.ID())
	require.Equal(t, arrow.BOOL, sc.Field(3).Type.ID())
}

// Vortex hands Arrow its string columns as string_view. Iceberg's
// Arrow-to-Iceberg schema conversion has no case for view types and panics on
// one, so the reader must not let them escape: everything downstream of here
// assumes contiguous string and binary layouts.
func testVortexNormalizesViewTypes(t *testing.T, ctx context.Context) {
	rdr := openFixture(t, ctx)

	sc, err := rdr.Schema()
	require.NoError(t, err)
	require.Equal(t, arrow.STRING, sc.Field(1).Type.ID(), "string_view must not reach callers")

	recRdr, err := rdr.GetRecords(ctx, nil, nil)
	require.NoError(t, err)
	defer recRdr.Release()

	require.Equal(t, arrow.STRING, recRdr.Schema().Field(1).Type.ID())
	require.True(t, recRdr.Next())
	require.IsType(t, &array.String{}, recRdr.RecordBatch().Column(1))
}

func testVortexSourceFileSize(t *testing.T, ctx context.Context) {
	rdr := openFixture(t, ctx)
	require.Positive(t, rdr.SourceFileSize())
}

func testVortexMetadataRowCount(t *testing.T, ctx context.Context) {
	rdr := openFixture(t, ctx)

	meta, ok := rdr.Metadata().(*VortexMetadata)
	require.True(t, ok)
	require.EqualValues(t, fixtureRows, meta.RowCount)
	require.True(t, meta.ExactCount)
}

// Check nulls independently of the row count: replacing a null with a zero
// value changes predicates and equality-delete matching.
func testVortexFullScanNullFidelity(t *testing.T, ctx context.Context) {
	rdr := openFixture(t, ctx)

	recRdr, err := rdr.GetRecords(ctx, nil, nil)
	require.NoError(t, err)
	defer recRdr.Release()

	var (
		rows       int
		sumID      int64
		nonNullNm  int
		nonNullScr int
		trueFlags  int
	)

	for recRdr.Next() {
		batch := recRdr.RecordBatch()
		rows += int(batch.NumRows())

		ids, ok := batch.Column(0).(*array.Int32)
		require.True(t, ok)
		for i := range ids.Len() {
			require.False(t, ids.IsNull(i), "id must never be null")
			sumID += int64(ids.Value(i))
		}

		names, ok := batch.Column(1).(*array.String)
		require.True(t, ok)
		nonNullNm += names.Len() - names.NullN()

		scores, ok := batch.Column(2).(*array.Float64)
		require.True(t, ok)
		nonNullScr += scores.Len() - scores.NullN()

		flags, ok := batch.Column(3).(*array.Boolean)
		require.True(t, ok)
		for i := range flags.Len() {
			if flags.Value(i) {
				trueFlags++
			}
		}
	}
	require.NoError(t, recRdr.Err())

	require.Equal(t, fixtureRows, rows)
	require.Equal(t, fixtureSumID, sumID)
	require.Equal(t, fixtureNonNullName, nonNullNm, "nulls in `name` were not preserved")
	require.Equal(t, fixtureNonNullScore, nonNullScr, "nulls in `score` were not preserved")
	// id % 3 == 0 over 0..999.
	require.Equal(t, 334, trueFlags)
}

func testVortexPrunedSchemaProjectsByFieldID(t *testing.T, ctx context.Context) {
	rdr := openFixture(t, ctx)

	// Field IDs 1 and 3 are `id` and `score`.
	sc, cols, err := rdr.PrunedSchema(map[int]struct{}{1: {}, 3: {}}, fixtureNameMapping())
	require.NoError(t, err)

	require.Equal(t, []string{"id", "score"}, fieldNames(sc))
	require.Equal(t, []int{0, 2}, cols)

	// The returned schema must carry field IDs so that conversion back to an
	// Iceberg schema takes its by-ID path.
	for i, want := range []string{"1", "3"} {
		got, ok := sc.Field(i).Metadata.GetValue("PARQUET:field_id")
		require.True(t, ok, "field %d is missing PARQUET:field_id", i)
		require.Equal(t, want, got)
	}
}

func testVortexProjectedScanReadsOnlySelectedColumns(t *testing.T, ctx context.Context) {
	rdr := openFixture(t, ctx)

	projectedSchema, cols, err := rdr.PrunedSchema(map[int]struct{}{1: {}, 3: {}}, fixtureNameMapping())
	require.NoError(t, err)

	recRdr, err := rdr.GetRecords(ctx, cols, nil)
	require.NoError(t, err)
	defer recRdr.Release()

	require.Equal(t, []string{"id", "score"}, fieldNames(recRdr.Schema()))
	require.Equal(t, projectedSchema.Fields(), recRdr.Schema().Fields())

	var (
		rows       int
		sumID      int64
		nonNullScr int
	)
	for recRdr.Next() {
		batch := recRdr.RecordBatch()
		require.EqualValues(t, 2, batch.NumCols())
		require.Equal(t, projectedSchema.Fields(), batch.Schema().Fields())
		rows += int(batch.NumRows())

		ids, ok := batch.Column(0).(*array.Int32)
		require.True(t, ok)
		for i := range ids.Len() {
			sumID += int64(ids.Value(i))
		}

		scores, ok := batch.Column(1).(*array.Float64)
		require.True(t, ok)
		nonNullScr += scores.Len() - scores.NullN()
	}
	require.NoError(t, recRdr.Err())

	require.Equal(t, fixtureRows, rows)
	require.Equal(t, fixtureSumID, sumID)
	require.Equal(t, fixtureNonNullScore, nonNullScr)
}

func testVortexReadTable(t *testing.T, ctx context.Context) {
	rdr := openFixture(t, ctx)

	tbl, err := rdr.ReadTable(ctx)
	require.NoError(t, err)
	defer tbl.Release()

	require.EqualValues(t, fixtureRows, tbl.NumRows())
	require.EqualValues(t, 4, tbl.NumCols())
}

// A tester is Parquet's row-group pruning hook. Vortex prunes inside its own
// scan, so being handed one means the caller has mis-wired the format and the
// reader must say so rather than quietly dropping the pruning.
func testVortexGetRecordsRejectsTester(t *testing.T, ctx context.Context) {
	rdr := openFixture(t, ctx)

	_, err := rdr.GetRecords(ctx, nil, &ParquetRowGroupTester{})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

// Registering an existing Vortex file must work (AddFiles goes through these
// two), while producing one must not silently appear to.
func testVortexRegistrationMetadata(t *testing.T, ctx context.Context) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "nested", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 3, Name: "inner", Type: iceberg.PrimitiveTypes.String},
			},
		}},
	)

	mapping, err := vortexFormat{}.PathToIDMapping(sc)
	require.NoError(t, err)
	require.Equal(t, map[string]int{"id": 1, "nested": 2, "nested.inner": 3}, mapping)

	rdr := openFixture(t, ctx)
	stats := vortexFormat{}.DataFileStatsFromMeta(rdr.Metadata(), nil, nil, nil, nil)
	require.NotNil(t, stats)
	require.EqualValues(t, fixtureRows, stats.RecordCount)
	// No bounds are available from the file, so none are claimed.
	require.Empty(t, stats.ColAggs)
}

func testVortexWritePathIsUnsupported(t *testing.T, ctx context.Context) {
	_, err := vortexFormat{}.NewFileWriter(ctx, nil, nil, WriteFileInfo{}, nil)
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)

	_, err = vortexFormat{}.WriteDataFile(ctx, nil, nil, WriteFileInfo{}, nil)
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
}

func testVortexFormatDispatch(t *testing.T, ctx context.Context) {
	require.NotNil(t, GetFileFormat(iceberg.VortexFile))
	require.NotNil(t, FormatFromFileName("data.vortex"))
}

func TestVortexFileReader(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			for _, tc := range []struct {
				name string
				run  func(*testing.T, context.Context)
			}{
				{"Schema", testVortexSchema},
				{"NormalizesViewTypes", testVortexNormalizesViewTypes},
				{"SourceFileSize", testVortexSourceFileSize},
				{"MetadataRowCount", testVortexMetadataRowCount},
				{"FullScanNullFidelity", testVortexFullScanNullFidelity},
				{"SourceParquetValues", testVortexMatchesSourceParquet},
				{"PrunedSchemaProjectsByFieldID", testVortexPrunedSchemaProjectsByFieldID},
				{"ProjectedScanReadsOnlySelectedColumns", testVortexProjectedScanReadsOnlySelectedColumns},
				{"ReadTable", testVortexReadTable},
				{"GetRecordsRejectsTester", testVortexGetRecordsRejectsTester},
				{"RegistrationMetadata", testVortexRegistrationMetadata},
				{"WritePathIsUnsupported", testVortexWritePathIsUnsupported},
				{"FormatDispatch", testVortexFormatDispatch},
				{"CountOnlyProjection", testVortexCountOnlyProjection},
				{"ReaderLifecycle", testVortexReaderLifecycle},
				{"Cancellation", testVortexCancellation},
				{"ReadFailure", testVortexReadFailure},
			} {
				t.Run(tc.name, func(t *testing.T) {
					tc.run(t, vortex.WithBackend(t.Context(), backend))
				})
			}
		})
	}
}

func testVortexMatchesSourceParquet(t *testing.T, ctx context.Context) {
	// This is the Parquet input used to generate the Rust-written Vortex
	// fixture. Compare every value and validity bit, regardless of batching.
	parquet, err := GetFileFormat(iceberg.ParquetFile).Open(ctx, iceio.LocalFS{}, "testdata/vortex/simple.parquet")
	require.NoError(t, err)
	defer parquet.Close()
	want, err := parquet.ReadTable(ctx)
	require.NoError(t, err)
	defer want.Release()

	rdr := openFixture(t, ctx)
	got, err := rdr.ReadTable(ctx)
	require.NoError(t, err)
	defer got.Release()
	require.Equal(t, want.NumRows(), got.NumRows())
	require.Equal(t, want.NumCols(), got.NumCols())
	for i := range int(want.NumCols()) {
		require.Equal(t, want.Schema().Field(i).Name, got.Schema().Field(i).Name)
		require.True(t, array.ChunkedEqual(want.Column(i).Data(), got.Column(i).Data()),
			"Vortex column %s differs from its source Parquet values", want.Schema().Field(i).Name)
	}
}
