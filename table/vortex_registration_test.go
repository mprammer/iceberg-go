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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

type vortexRegistrationMetrics struct {
	values, nulls, nans map[int]int64
	lower, upper        map[int][]byte
}

func vortexRegistrationBound(value any) []byte {
	switch v := value.(type) {
	case bool:
		if v {
			return []byte{1}
		}

		return []byte{0}
	case int32:
		return binary.LittleEndian.AppendUint32(nil, uint32(v))
	case int64:
		return binary.LittleEndian.AppendUint64(nil, uint64(v))
	case float32:
		return binary.LittleEndian.AppendUint32(nil, math.Float32bits(v))
	case float64:
		return binary.LittleEndian.AppendUint64(nil, math.Float64bits(v))
	case string:
		return []byte(v)
	case []byte:
		return v
	default:
		panic(fmt.Sprintf("unexpected oracle scalar %T", value))
	}
}

func requireVortexRegistrationMap[V any](t *testing.T, want, got map[int]V) {
	t.Helper()
	require.Len(t, got, len(want))
	for id, value := range want {
		require.Contains(t, got, id)
		require.Equal(t, value, got[id], "field ID %d", id)
	}
}

func requireVortexRegistrationMetrics(t *testing.T, df iceberg.DataFile, rows int64, want vortexRegistrationMetrics) {
	t.Helper()
	require.Equal(t, iceberg.VortexFile, df.FileFormat())
	require.Equal(t, rows, df.Count())
	require.Positive(t, df.FileSizeBytes())
	require.Empty(t, df.ColumnSizes(), "compressed layout bytes are not verified physical column sizes")
	require.Empty(t, df.SplitOffsets(), "registration does not invent split offsets")
	requireVortexRegistrationMap(t, want.values, df.ValueCounts())
	requireVortexRegistrationMap(t, want.nulls, df.NullValueCounts())
	requireVortexRegistrationMap(t, want.nans, df.NaNValueCounts())
	requireVortexRegistrationMap(t, want.lower, df.LowerBoundValues())
	requireVortexRegistrationMap(t, want.upper, df.UpperBoundValues())
}

func newVortexRegistrationTable(t *testing.T, schema *iceberg.Schema, spec *iceberg.PartitionSpec, props iceberg.Properties) *table.Table {
	t.Helper()
	location := t.TempDir()
	properties := iceberg.Properties{table.PropertyFormatVersion: "3"}
	for key, value := range props {
		properties[key] = value
	}
	meta, err := table.NewMetadata(schema, spec, table.UnsortedSortOrder, location, properties)
	require.NoError(t, err)

	return table.New(table.Identifier{"default", "registration"}, meta,
		filepath.Join(location, "metadata", "v1.metadata.json"),
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &mockedCatalog{meta})
}

func commitVortexRegistration(t *testing.T, ctx context.Context, tbl *table.Table, path string) (*table.Table, iceberg.DataFile) {
	t.Helper()
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(ctx, []string{path}, nil, false))
	committed, err := tx.Commit(ctx)
	require.NoError(t, err)
	snapshot := committed.CurrentSnapshot()
	require.NotNil(t, snapshot)
	manifests, err := snapshot.Manifests(iceio.LocalFS{})
	require.NoError(t, err)
	var files []iceberg.DataFile
	for _, manifest := range manifests {
		for entry, err := range manifest.Entries(iceio.LocalFS{}, true) {
			require.NoError(t, err)
			files = append(files, entry.DataFile())
		}
	}
	require.Len(t, files, 1, "read the committed data-file entry from its manifest")
	require.Equal(t, path, files[0].FilePath())

	return committed, files[0]
}

func vortexRegistrationScalarSchema() *iceberg.Schema {
	names := []string{"row_id", "flag", "v32", "v64", "f32", "f64", "text", "blob"}
	types := []iceberg.Type{
		iceberg.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.String, iceberg.PrimitiveTypes.Binary,
	}
	fields := make([]iceberg.NestedField, len(names))
	for i, name := range names {
		fields[i] = iceberg.NestedField{ID: i + 1, Name: name, Type: types[i], Required: i == 0}
	}

	return iceberg.NewSchema(0, fields...)
}

func vortexRegistrationHash(row int) uint64 {
	x := uint64(row) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb

	return x ^ (x >> 31)
}

// These byte values and validity formulas come from default_writer.rs inputs,
// not from either reader or from the metrics collector being tested.
func vortexRegistrationBytes(mode, col, row int) []byte {
	h := vortexRegistrationHash(row)
	if col == 6 {
		switch mode {
		case 0:
			return []byte("constant string longer than twelve bytes")
		case 1, 4:
			return []byte([...]string{"", "a", "iceberg", "vortex dictionary entry", "東京", "éclair", "🙂", "a\x00b"}[row%8])
		default:
			switch row % 4 {
			case 0:
				return []byte{}
			case 1:
				return fmt.Appendf(nil, "s%d", row)
			case 2:
				return fmt.Appendf(nil, "row=%05d;hash=%016x;東京;", row, h)
			default:
				return fmt.Appendf(nil, "%s:%d:%016x", strings.Repeat("shared long prefix/", 12), row, h)
			}
		}
	}
	switch mode {
	case 0:
		return []byte{0, 255, 128, 1, 0, 7}
	case 1, 4:
		return []byte{0, byte(row % 7), 255}
	default:
		out := make([]byte, [...]int{0, 3, 19, 131}[row%4])
		for i := range out {
			out[i] = byte(h>>((i%8)*8)) + byte(i)
		}

		return out
	}
}

func vortexRegistrationDefaultMetrics(kind int) vortexRegistrationMetrics {
	want := vortexRegistrationMetrics{values: map[int]int64{}, nulls: map[int]int64{}, nans: map[int]int64{5: 0, 6: 0}, lower: map[int][]byte{}, upper: map[int][]byte{}}
	for col := range 8 {
		id := col + 1
		want.values[id] = 8193
		want.nulls[id] = 0
		var low, high []byte
		seen := false
		for row := range 8193 {
			valid := col == 0 || ((kind != 3 || row < 3072 || row >= 4096) && (row+col*3)%29 != 0)
			if !valid {
				want.nulls[id]++

				continue
			}
			mode := kind
			if kind == 3 {
				mode = (row / 2048) % 3
			}
			if (col == 4 || col == 5) && (mode == 2 || mode == 4) {
				period := 257
				if mode == 4 {
					period = 1021
				}
				if row%period == 2 || row%period == 3 {
					want.nans[id]++
				}
			}
			if col >= 6 {
				value := vortexRegistrationBytes(mode, col, row)
				if !seen || bytes.Compare(value, low) < 0 {
					low = value
				}
				if !seen || bytes.Compare(value, high) > 0 {
					high = value
				}
				seen = true
			}
		}
		if col >= 6 {
			// The shared DataFile builder conservatively omits empty bounds.
			if len(low) > 0 {
				want.lower[id] = low
			}
			if len(high) > 0 {
				want.upper[id] = high
			}
		}
	}
	lows := []any{int64(0), false, int32(math.MinInt32), int64(math.MinInt64), float32(math.Inf(-1)), math.Inf(-1)}
	highs := []any{int64(8192), true, int32(math.MaxInt32), int64(math.MaxInt64), float32(math.Inf(1)), math.Inf(1)}
	switch kind {
	case 0:
		lows = []any{int64(0), true, int32(-77), int64(1<<53 + 1), float32(1.25), float64(-3.5)}
		highs = []any{int64(8192), true, int32(-77), int64(1<<53 + 1), float32(1.25), float64(-3.5)}
	case 1:
		lows = []any{int64(0), false, int32(-4096), int64(math.MinInt64), float32(-100), float64(-100)}
		highs = []any{int64(8192), true, int32(4096), int64(math.MinInt64) + 8192*17, float32(100), float64(100)}
	}
	for col := range 6 {
		want.lower[col+1] = vortexRegistrationBound(lows[col])
		want.upper[col+1] = vortexRegistrationBound(highs[col])
	}

	return want
}

func TestVortexRegistrationDefaultMetrics(t *testing.T) {
	schema := vortexRegistrationScalarSchema()
	props := iceberg.Properties{table.DefaultWriteMetricsModeKey: "full"}
	for _, backend := range vortex.AvailableBackends() {
		for kind, name := range []string{"repeated", "progression", "broad", "chunked", "precision"} {
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				ctx := vortex.WithBackend(t.Context(), backend)
				path, err := filepath.Abs("internal/testdata/vortex/rust/default_" + name + ".vortex")
				require.NoError(t, err)
				want := vortexRegistrationDefaultMetrics(kind)
				direct, err := table.FileToDataFile(ctx, iceio.LocalFS{}, path, schema, *iceberg.UnpartitionedSpec, 0, props)
				require.NoError(t, err)
				requireVortexRegistrationMetrics(t, direct, 8193, want)
				tbl, committed := commitVortexRegistration(t, ctx, newVortexRegistrationTable(t, schema, iceberg.UnpartitionedSpec, props), path)
				requireVortexRegistrationMetrics(t, committed, 8193, want)
				tasks, err := tbl.Scan(table.WithRowFilter(iceberg.GreaterThan(iceberg.Reference("row_id"), int64(8192)))).PlanFiles(ctx)
				require.NoError(t, err)
				require.Empty(t, tasks, "verified manifest bounds exclude the whole file")
			})
		}
	}
}

func vortexRegistrationSimpleMetrics() vortexRegistrationMetrics {
	return vortexRegistrationMetrics{
		values: map[int]int64{1: 1000, 2: 1000, 3: 1000, 4: 1000},
		nulls:  map[int]int64{1: 0, 2: 143, 3: 200, 4: 0}, nans: map[int]int64{3: 0},
		lower: map[int][]byte{1: vortexRegistrationBound(int32(0)), 2: []byte("name-1"), 3: vortexRegistrationBound(float64(1.5)), 4: {0}},
		upper: map[int][]byte{1: vortexRegistrationBound(int32(999)), 2: []byte("name-999"), 3: vortexRegistrationBound(float64(1498.5)), 4: {1}},
	}
}

func TestVortexRegistrationMetricModes(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		for _, mode := range []string{"none", "counts", "full", "truncate(2)", "overrides"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				ctx := vortex.WithBackend(t.Context(), backend)
				_, path := stageVortexFixture(t)
				props := iceberg.Properties{table.DefaultWriteMetricsModeKey: mode}
				want := vortexRegistrationSimpleMetrics()
				switch mode {
				case "none":
					want = vortexRegistrationMetrics{}
				case "counts":
					want.lower = nil
					want.upper = nil
				case "truncate(2)":
					want.lower[2] = []byte("na")
					want.upper[2] = []byte("nb")
				case "overrides":
					props[table.DefaultWriteMetricsModeKey] = "counts"
					props[table.MetricsModeColumnConfPrefix+".name"] = "full"
					props[table.MetricsModeColumnConfPrefix+".score"] = "none"
					delete(want.values, 3)
					delete(want.nulls, 3)
					want.nans = nil
					want.lower = map[int][]byte{2: []byte("name-1")}
					want.upper = map[int][]byte{2: []byte("name-999")}
				}
				df, err := table.FileToDataFile(ctx, iceio.LocalFS{}, path, vortexTestSchema(), *iceberg.UnpartitionedSpec, 0, props)
				require.NoError(t, err)
				requireVortexRegistrationMetrics(t, df, 1000, want)
				tbl, committed := commitVortexRegistration(t, ctx, newVortexRegistrationTable(t, vortexTestSchema(), iceberg.UnpartitionedSpec, props), path)
				requireVortexRegistrationMetrics(t, committed, 1000, want)
				tasks, err := tbl.Scan(table.WithRowFilter(iceberg.GreaterThan(iceberg.Reference("id"), int32(999)))).PlanFiles(ctx)
				require.NoError(t, err)
				if mode == "full" || mode == "truncate(2)" {
					require.Empty(t, tasks)
				} else {
					require.Len(t, tasks, 1, "without id bounds the planner retains the file")
				}
			})
		}
		t.Run(string(backend)+"/binary_truncation", func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			props := iceberg.Properties{table.DefaultWriteMetricsModeKey: "none", table.MetricsModeColumnConfPrefix + ".text": "truncate(3)", table.MetricsModeColumnConfPrefix + ".blob": "truncate(2)"}
			path := "internal/testdata/vortex/rust/default_repeated.vortex"
			df, err := table.FileToDataFile(ctx, iceio.LocalFS{}, path, vortexRegistrationScalarSchema(), *iceberg.UnpartitionedSpec, 0, props)
			require.NoError(t, err)
			original := vortexRegistrationDefaultMetrics(0)
			want := vortexRegistrationMetrics{values: map[int]int64{7: 8193, 8: 8193}, nulls: map[int]int64{7: original.nulls[7], 8: original.nulls[8]}, lower: map[int][]byte{7: []byte("con"), 8: {0, 255}}, upper: map[int][]byte{7: []byte("coo"), 8: {1, 255}}}
			requireVortexRegistrationMetrics(t, df, 8193, want)
		})
	}
}

func TestVortexRegistrationFloatAndEmptyMetrics(t *testing.T) {
	floats := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "value64", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 3, Name: "value32", Type: iceberg.PrimitiveTypes.Float32})
	empty := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Float64})
	props := iceberg.Properties{table.DefaultWriteMetricsModeKey: "full"}
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend)+"/float_edges", func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			path, err := filepath.Abs("internal/testdata/vortex/float_edges.vortex")
			require.NoError(t, err)
			df, err := table.FileToDataFile(ctx, iceio.LocalFS{}, path, floats, *iceberg.UnpartitionedSpec, 0, props)
			require.NoError(t, err)
			// The ten source rows contain three NaNs, six ordered values, and one null.
			want := vortexRegistrationMetrics{
				values: map[int]int64{1: 10, 2: 10, 3: 10}, nulls: map[int]int64{1: 0, 2: 1, 3: 1}, nans: map[int]int64{2: 3, 3: 3},
				lower: map[int][]byte{1: vortexRegistrationBound(int32(0)), 2: vortexRegistrationBound(math.Inf(-1)), 3: vortexRegistrationBound(float32(math.Inf(-1)))},
				upper: map[int][]byte{1: vortexRegistrationBound(int32(9)), 2: vortexRegistrationBound(math.Inf(1)), 3: vortexRegistrationBound(float32(math.Inf(1)))},
			}
			requireVortexRegistrationMetrics(t, df, 10, want)
			_, committed := commitVortexRegistration(t, ctx, newVortexRegistrationTable(t, floats, iceberg.UnpartitionedSpec, props), path)
			requireVortexRegistrationMetrics(t, committed, 10, want)
		})
		t.Run(string(backend)+"/empty", func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			path := "internal/testdata/vortex/empty.vortex"
			_, err := table.FileToDataFile(ctx, iceio.LocalFS{}, path, empty, *iceberg.UnpartitionedSpec, 0, props)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.ErrorContains(t, err, "record count must be greater than 0")
			tx := newVortexRegistrationTable(t, empty, iceberg.UnpartitionedSpec, props).NewTransaction()
			require.Error(t, tx.AddFiles(ctx, []string{path}, nil, false))
			staged, stageErr := tx.StagedTable()
			require.NoError(t, stageErr)
			require.Nil(t, staged.CurrentSnapshot(), "existing zero-row registration policy")
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_trunc", Transform: iceberg.TruncateTransform{Width: 1000}})
			_, err = table.FileToDataFile(ctx, iceio.LocalFS{}, path, empty, spec, 0, props)
			require.Error(t, err, "no row establishes a non-void partition")
		})
	}
}

func TestVortexRegistrationPartitions(t *testing.T) {
	unsupported, err := iceberg.ParseTransform("future_partition_transform")
	require.NoError(t, err)
	for _, backend := range vortex.AvailableBackends() {
		for _, mode := range []string{"none", "truncate(2)"} {
			for _, tc := range []struct {
				name      string
				source    int
				transform iceberg.Transform
				value     any
				reject    bool
			}{
				{"truncate", 1, iceberg.TruncateTransform{Width: 1000}, int32(0), false},
				{"bucket_one", 1, iceberg.BucketTransform{NumBuckets: 1}, int32(0), false},
				{"void", 2, iceberg.VoidTransform{}, nil, false},
				{"identity_mixed", 1, iceberg.IdentityTransform{}, nil, true},
				{"bucket_mixed", 1, iceberg.BucketTransform{NumBuckets: 2}, nil, true},
				{"null_and_prefix", 2, iceberg.TruncateTransform{Width: 5}, nil, true},
				{"unsupported_transform", 1, unsupported, nil, true},
			} {
				t.Run(string(backend)+"/"+mode+"/"+tc.name, func(t *testing.T) {
					ctx := vortex.WithBackend(t.Context(), backend)
					location, path := stageVortexFixture(t)
					spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{tc.source}, FieldID: 1000, Name: "partition", Transform: tc.transform})
					tbl := newVortexTableWithSpec(t, location, "3", &spec)
					tx := tbl.NewTransaction()
					require.NoError(t, tx.SetProperties(iceberg.Properties{table.DefaultWriteMetricsModeKey: mode}))
					err := tx.AddFiles(ctx, []string{path}, nil, false)
					if tc.reject {
						require.Error(t, err)
						staged, stageErr := tx.StagedTable()
						require.NoError(t, stageErr)
						require.Nil(t, staged.CurrentSnapshot(), "partition validation precedes the data snapshot")

						return
					}
					require.NoError(t, err)
					committed, err := tx.Commit(ctx)
					require.NoError(t, err)
					tasks, err := committed.Scan().PlanFiles(ctx)
					require.NoError(t, err)
					require.Len(t, tasks, 1)
					require.Equal(t, map[int]any{1000: tc.value}, tasks[0].File.Partition())
					out := readVortexScan(t, ctx, committed.Scan(table.WithSelectedFields("id")))
					requireVortexIDs(t, out, vortexIDsWhere(func(int32) bool { return true }))
				})
			}
		}
	}
}

func TestVortexRegistrationMissingPartitionSource(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		for _, tc := range []struct {
			name             string
			initial, write   any
			required, reject bool
			want             any
		}{
			{"initial_default", int32(17), int32(99), false, true, nil},
			{"initial_default_long", "complete partition value longer than two runes", "different write default", false, true, nil},
			{"null", nil, nil, false, false, nil},
			{"write_default_is_not_read_default", nil, int32(99), false, false, nil},
			{"required_write_default", nil, int32(99), true, true, nil},
		} {
			t.Run(string(backend)+"/"+tc.name, func(t *testing.T) {
				ctx := vortex.WithBackend(t.Context(), backend)
				_, path := stageVortexFixture(t)
				fieldType := iceberg.Type(iceberg.PrimitiveTypes.Int32)
				if tc.name == "initial_default_long" {
					fieldType = iceberg.PrimitiveTypes.String
				}
				fields := append(vortexTestSchema().Fields(), iceberg.NestedField{ID: 5, Name: "missing", Type: fieldType, Required: tc.required, InitialDefault: tc.initial, WriteDefault: tc.write})
				schema := iceberg.NewSchema(0, fields...)
				spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{5}, FieldID: 1000, Name: "missing_partition", Transform: iceberg.IdentityTransform{}})
				props := iceberg.Properties{table.DefaultWriteMetricsModeKey: "none"}
				if tc.name == "initial_default_long" {
					props[table.DefaultWriteMetricsModeKey] = "truncate(2)"
				}
				tbl := newVortexRegistrationTable(t, schema, &spec, props)
				tx := tbl.NewTransaction()
				err := tx.AddFiles(ctx, []string{path}, nil, false)
				if tc.reject {
					require.Error(t, err)
					if tc.initial != nil {
						require.ErrorIs(t, err, iceberg.ErrNotImplemented)
					}
					staged, stageErr := tx.StagedTable()
					require.NoError(t, stageErr)
					require.Nil(t, staged.CurrentSnapshot())

					return
				}
				require.NoError(t, err)
				committed, err := tx.Commit(ctx)
				require.NoError(t, err)
				tasks, err := committed.Scan().PlanFiles(ctx)
				require.NoError(t, err)
				require.Len(t, tasks, 1)
				require.Equal(t, map[int]any{1000: tc.want}, tasks[0].File.Partition())
				requireVortexDefaultValues(t, readVortexScan(t, ctx, committed.Scan(table.WithSelectedFields("missing"))), tc.want)
				var predicate iceberg.BooleanExpression = iceberg.IsNull(iceberg.Reference("missing"))

				for range 2 {
					scan := committed.Scan(table.WithSelectedFields("missing"), table.WithRowFilter(predicate))
					tasks, err := scan.PlanFiles(ctx)
					require.NoError(t, err)
					require.Len(t, tasks, 1)
					out := readVortexScan(t, ctx, scan)
					require.EqualValues(t, 1000, out.NumRows())
					requireVortexDefaultValues(t, out, tc.want)
					committed = reloadVortexMetadata(t, committed)
				}
			})
		}
	}
}

func TestVortexRegistrationMultiFileAtomicity(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			location, path := stageVortexFixture(t)
			bad := filepath.Join(location, "data", "broken.vortex")
			require.NoError(t, os.WriteFile(bad, []byte("invalid Vortex footer"), 0o600))
			tx := newVortexTable(t, location).NewTransaction()
			require.Error(t, tx.AddFiles(ctx, []string{path, bad}, nil, false, table.WithAddFilesConcurrency(1)))
			staged, err := tx.StagedTable()
			require.NoError(t, err)
			require.Nil(t, staged.CurrentSnapshot(), "a later invalid file must not publish earlier files")
			require.NoError(t, tx.AddFiles(ctx, []string{path}, nil, false))
			committed, err := tx.Commit(ctx)
			require.NoError(t, err)
			tasks, err := committed.Scan().PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			require.Equal(t, path, tasks[0].File.FilePath())
			require.EqualValues(t, 1000, tasks[0].File.Count())
		})
	}
}

// Numeric metrics use table field IDs/types even when schema order and physical
// widths differ. The same normalized value supplies the partition transform.
func TestVortexRegistrationPromotedAndRemappedFields(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 33, Name: "score", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 101, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "flag", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 8, Name: "name", Type: iceberg.PrimitiveTypes.String})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{101}, FieldID: 1000, Name: "id_trunc", Transform: iceberg.TruncateTransform{Width: 1000}})
	props := iceberg.Properties{table.DefaultWriteMetricsModeKey: "full"}
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			_, path := stageVortexFixture(t)
			df, err := table.FileToDataFile(ctx, iceio.LocalFS{}, path, schema, spec, 0, props)
			require.NoError(t, err)
			want := vortexRegistrationMetrics{
				values: map[int]int64{101: 1000, 8: 1000, 33: 1000, 2: 1000},
				nulls:  map[int]int64{101: 0, 8: 143, 33: 200, 2: 0}, nans: map[int]int64{33: 0},
				lower: map[int][]byte{101: vortexRegistrationBound(int64(0)), 8: []byte("name-1"), 33: vortexRegistrationBound(float64(1.5)), 2: {0}},
				upper: map[int][]byte{101: vortexRegistrationBound(int64(999)), 8: []byte("name-999"), 33: vortexRegistrationBound(float64(1498.5)), 2: {1}},
			}
			requireVortexRegistrationMetrics(t, df, 1000, want)
			require.Equal(t, map[int]any{1000: int64(0)}, df.Partition())

			fields := vortexRegistrationScalarSchema().Fields()
			fields[4].Type = iceberg.PrimitiveTypes.Float64
			widened := iceberg.NewSchema(0, fields...)
			df, err = table.FileToDataFile(ctx, iceio.LocalFS{}, "internal/testdata/vortex/rust/default_repeated.vortex", widened, *iceberg.UnpartitionedSpec, 0, props)
			require.NoError(t, err)
			expected := vortexRegistrationDefaultMetrics(0)
			expected.lower[5] = vortexRegistrationBound(float64(1.25))
			expected.upper[5] = vortexRegistrationBound(float64(1.25))
			requireVortexRegistrationMetrics(t, df, 8193, expected)
		})
	}
}

func TestVortexRegistrationPartitionTupleAndFinalRow(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend)+"/tuple", func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			_, path := stageVortexFixture(t)
			spec := iceberg.NewPartitionSpec(
				iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_trunc", Transform: iceberg.TruncateTransform{Width: 1000}},
				iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1001, Name: "id_bucket", Transform: iceberg.BucketTransform{NumBuckets: 1}},
				iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1002, Name: "name_void", Transform: iceberg.VoidTransform{}})
			tbl := newVortexRegistrationTable(t, vortexTestSchema(), &spec, iceberg.Properties{table.DefaultWriteMetricsModeKey: "none"})
			_, df := commitVortexRegistration(t, ctx, tbl, path)
			require.Equal(t, map[int]any{1000: int32(0), 1001: int32(0), 1002: nil}, df.Partition())
			requireVortexRegistrationMetrics(t, df, 1000, vortexRegistrationMetrics{})
		})
		t.Run(string(backend)+"/final_row_changes_partition", func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			path := "internal/testdata/vortex/rust/default_repeated.vortex"
			// row_id 0..8191 maps to 0; only the last source row maps to 8192.
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "row_block", Transform: iceberg.TruncateTransform{Width: 8192}})
			tbl := newVortexRegistrationTable(t, vortexRegistrationScalarSchema(), &spec, iceberg.Properties{table.DefaultWriteMetricsModeKey: "none"})
			tx := tbl.NewTransaction()
			require.Error(t, tx.AddFiles(ctx, []string{path}, nil, false))
			staged, err := tx.StagedTable()
			require.NoError(t, err)
			require.Nil(t, staged.CurrentSnapshot(), "registration must validate the final source row")
		})
	}
}

func TestVortexRegistrationRejectsLargeTruncateOverflow(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			_, path := stageVortexFixture(t)
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "truncated", Transform: iceberg.TruncateTransform{Width: math.MaxInt32}})
			df, err := table.FileToDataFile(ctx, iceio.LocalFS{}, path, vortexTestSchema(), spec, 0, iceberg.Properties{table.DefaultWriteMetricsModeKey: "none"})
			require.ErrorContains(t, err, "exceeds source integer range")
			require.Nil(t, df)
		})
	}
}

func TestVortexRegistrationAliasNameMapping(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			_, path := stageVortexFixture(t)
			fields := vortexTestSchema().Fields()
			fields[0].Name = "renamed_id"
			schema := iceberg.NewSchema(0, fields...)
			mapping := vortexTestSchema().NameMapping()
			mapping[0].Names = append(mapping[0].Names, "renamed_id")
			raw, err := json.Marshal(mapping)
			require.NoError(t, err)
			props := iceberg.Properties{table.DefaultWriteMetricsModeKey: "full", table.DefaultNameMappingKey: string(raw)}
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.TruncateTransform{Width: 1000}})
			_, err = table.FileToDataFile(ctx, iceio.LocalFS{}, path, schema, spec, 0, props)
			require.NoError(t, err, "a valid alias mapping must bind physical id to renamed_id")
		})
	}
}

func TestVortexRegistrationMappedFieldMetrics(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			_, path := stageVortexFixture(t)
			fields := vortexTestSchema().Fields()
			fields[0].Name = "old_id"
			fields[0].Required = false
			fields = append(fields, iceberg.NestedField{ID: 5, Name: "id", Type: iceberg.PrimitiveTypes.Int32, InitialDefault: int32(1700)})
			schema := iceberg.NewSchema(0, fields...)
			raw, err := json.Marshal(vortexTestSchema().NameMapping())
			require.NoError(t, err)
			for _, mode := range []string{"none", "full"} {
				t.Run(mode, func(t *testing.T) {
					props := iceberg.Properties{table.DefaultWriteMetricsModeKey: mode, table.DefaultNameMappingKey: string(raw)}
					tbl, df := commitVortexRegistration(t, ctx, newVortexRegistrationTable(t, schema, iceberg.UnpartitionedSpec, props), path)
					all := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("id")))
					require.EqualValues(t, 1000, all.NumRows())
					require.NotContains(t, df.ValueCounts(), 5)
					require.NotContains(t, df.LowerBoundValues(), 5)
					if mode == "full" {
						require.EqualValues(t, 1000, df.ValueCounts()[1])
					}
					out := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("id"), table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), int32(1700)))))
					require.EqualValues(t, 1000, out.NumRows(), "the physically absent new id column reads its initial default on every row")
				})
			}
		})
	}
}

func TestVortexRegistrationMappedMixedPartition(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			_, path := stageVortexFixture(t)
			fields := vortexTestSchema().Fields()
			fields[0].Name = "old_id"
			fields[0].Required = false
			fields = append(fields, iceberg.NestedField{ID: 5, Name: "id", Type: iceberg.PrimitiveTypes.Int32, InitialDefault: int32(1700)})
			schema := iceberg.NewSchema(0, fields...)
			raw, err := json.Marshal(vortexTestSchema().NameMapping())
			require.NoError(t, err)
			props := iceberg.Properties{table.DefaultWriteMetricsModeKey: "none", table.DefaultNameMappingKey: string(raw)}
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "old_id_part", Transform: iceberg.IdentityTransform{}})
			df, err := table.FileToDataFile(ctx, iceio.LocalFS{}, path, schema, spec, 0, props)
			_ = df
			require.Error(t, err, "the mapped source contains values 0..999 and cannot belong to one identity partition")
		})
	}
}

func requireVortexDefaultValues(t *testing.T, out arrow.Table, want any) {
	t.Helper()
	require.EqualValues(t, 1000, out.NumRows())
	require.EqualValues(t, 1, out.NumCols())
	for _, chunk := range out.Column(0).Data().Chunks() {
		if want == nil {
			require.Equal(t, chunk.Len(), chunk.NullN())

			continue
		}
		expected, err := scalar.MakeScalarParam(want, chunk.DataType())
		require.NoError(t, err)
		if releasable, ok := expected.(scalar.Releasable); ok {
			t.Cleanup(releasable.Release)
		}
		for row := range chunk.Len() {
			actual, err := scalar.GetScalar(chunk, row)
			require.NoError(t, err)
			equal := scalar.Equals(expected, actual)
			if releasable, ok := actual.(scalar.Releasable); ok {
				releasable.Release()
			}
			require.True(t, equal, "default differs at row %d: expected %v", row, want)
		}
	}
}

func TestVortexRegistrationDottedTopLevelMetrics(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		for _, mode := range []string{"full", "truncate(2)"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				ctx := vortex.WithBackend(t.Context(), backend)
				_, path := stageVortexFixture(t)
				fields := vortexTestSchema().Fields()
				fields[0].Name = "renamed.id"
				schema := iceberg.NewSchema(0, fields...)
				mapping := vortexTestSchema().NameMapping()
				mapping[0].Names = append(mapping[0].Names, "renamed.id")
				encoded, err := json.Marshal(mapping)
				require.NoError(t, err)
				props := iceberg.Properties{table.DefaultWriteMetricsModeKey: mode, table.DefaultNameMappingKey: string(encoded)}
				df, err := table.FileToDataFile(ctx, iceio.LocalFS{}, path, schema, *iceberg.UnpartitionedSpec, 0, props)
				require.NoError(t, err)
				require.EqualValues(t, 1000, df.ValueCounts()[1])
				// The shared Iceberg metrics planner treats dotted names as nested.
				// Registration keeps its counts-only behavior until that is fixed.
				require.NotContains(t, df.LowerBoundValues(), 1)
				require.NotContains(t, df.UpperBoundValues(), 1)
				tbl, persisted := commitVortexRegistration(t, ctx, newVortexRegistrationTable(t, schema, iceberg.UnpartitionedSpec, props), path)
				require.NotContains(t, persisted.LowerBoundValues(), 1)
				for reload := range 2 {
					if reload != 0 {
						tbl = reloadVortexMetadata(t, tbl)
					}
					tasks, err := tbl.Scan(table.WithRowFilter(iceberg.GreaterThan(iceberg.Reference("renamed.id"), int32(999)))).PlanFiles(ctx)
					require.NoError(t, err)
					require.Len(t, tasks, 1, "counts-only metrics retain the file for residual filtering")
					matching := tbl.Scan(table.WithSelectedFields("renamed.id"), table.WithRowFilter(iceberg.LessThan(iceberg.Reference("renamed.id"), int32(10))))
					require.EqualValues(t, 10, readVortexScan(t, ctx, matching).NumRows())
				}
			})
		}
	}
}

func TestVortexRegistrationRejectsAmbiguousNames(t *testing.T) {
	path, err := filepath.Abs("internal/testdata/vortex/dotted.vortex")
	require.NoError(t, err)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})
	for _, backend := range vortex.AvailableBackends() {
		for _, mode := range []string{"none", "full"} {
			for _, initialDefault := range []any{nil, "default"} {
				for _, collision := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/default=%v/collision=%t", backend, mode, initialDefault, collision), func(t *testing.T) {
						fields := []iceberg.NestedField{{ID: 1, Name: "a.b", Type: iceberg.PrimitiveTypes.String, InitialDefault: initialDefault}}
						if collision {
							fields = append(fields, iceberg.NestedField{ID: 2, Name: "a", Type: &iceberg.StructType{
								FieldList: []iceberg.NestedField{{ID: 3, Name: "b", Type: iceberg.PrimitiveTypes.String}},
							}})
						}
						df, err := table.FileToDataFile(vortex.WithBackend(t.Context(), backend), iceio.LocalFS{}, path,
							iceberg.NewSchema(0, fields...), spec, 0, iceberg.Properties{table.DefaultWriteMetricsModeKey: mode})
						if collision {
							require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
							require.ErrorContains(t, err, "multiple fields for name a.b")
							require.Nil(t, df, "an ambiguous schema must not produce default/null partition metadata")
						} else {
							require.NoError(t, err)
							require.Equal(t, "actual", df.Partition()[1000], "a literal dotted name remains supported")
						}
					})
				}
			}
		}
	}
}

func reloadVortexMetadata(t *testing.T, tbl *table.Table) *table.Table {
	t.Helper()
	data, err := json.Marshal(tbl.Metadata())
	require.NoError(t, err)
	meta, err := table.ParseMetadataBytes(data)
	require.NoError(t, err)

	return table.New(table.Identifier{"default", "registration"}, meta, tbl.MetadataLocation(),
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &mockedCatalog{meta})
}
