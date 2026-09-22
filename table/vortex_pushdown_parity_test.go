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
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

// Compare public scans over independently generated Parquet rows and the Rust
// Vortex fixture. Its five physical zones include an all-null zone and a
// constant zone. A range retains the tail of zone zero and resumes matching
// in zone two. Deletes
// target original positions before, inside and after that gap.
func TestVortexPositionPushdownParquetParity(t *testing.T) {
	const large = int64(1<<53 + 1)
	tail := iceberg.NewAnd(
		iceberg.GreaterThan(iceberg.Reference("v64"), large),
		iceberg.LessThanEqual(iceberg.Reference("v64"), large+4))
	gaps := iceberg.GreaterThan(iceberg.Reference("v64"), int64(math.MinInt64+1020))
	impossible := iceberg.EqualTo(iceberg.Reference("v64"), int64(0))
	deleted := []int64{0, 1024, 2048, 2050, 3073, 3075, 4098}

	for _, deleteKind := range []string{"none", "position", "dv"} {
		t.Run(deleteKind, func(t *testing.T) {
			parquet := newPushdownParityTable(t, "parquet", "", deleteKind, deleted)
			var readers []pushdownParityTable
			for _, backend := range vortex.AvailableBackends() {
				readers = append(readers, newPushdownParityTable(t, string(backend), backend, deleteKind, deleted))
			}
			for _, lineage := range []bool{false, true} {
				t.Run(fmt.Sprintf("lineage_%t", lineage), func(t *testing.T) {
					for _, tc := range []struct {
						name         string
						filter       iceberg.BooleanExpression
						keep         func(int64) bool
						limit        int64
						metadataOnly bool
					}{
						{"leading_zones", tail, func(v int64) bool { return v > large && v <= large+4 }, 0, false},
						{"limit_after_deletes", tail, func(v int64) bool { return v > large && v <= large+4 }, 2, false},
						{"gap_between_batches", gaps, func(v int64) bool { return v > math.MinInt64+1020 }, 0, false},
						{"limit_across_gap", gaps, func(v int64) bool { return v > math.MinInt64+1020 }, 5, false},
						{"all_pruned", impossible, func(v int64) bool { return v == 0 }, 0, false},
						{"metadata_only_projection", tail, func(v int64) bool { return v > large && v <= large+4 }, 0, true},
						{"metadata_only_all_pruned", impossible, func(v int64) bool { return v == 0 }, 0, true},
					} {
						t.Run(tc.name, func(t *testing.T) {
							var want []int64
							for row := range 4099 {
								_, v64, valid := pushdownParityValues(row)
								if valid && tc.keep(v64) && (deleteKind == "none" || !slices.Contains(deleted, int64(row))) {
									want = append(want, int64(row))
								}
							}
							if tc.limit > 0 && int64(len(want)) > tc.limit {
								want = want[:tc.limit]
							}
							selected := "row_id"
							physical := []string{"row_id", "v64"}
							if tc.metadataOnly {
								selected = iceberg.RowIDColumnName
								physical = []string{"v64"}
							}
							opts := []table.ScanOption{table.WithSelectedFields(selected), table.WithRowFilter(tc.filter)}
							if lineage {
								opts = append(opts, table.WithRowLineage())
							}
							check := func(t *testing.T, reader pushdownParityTable) arrow.Table {
								t.Helper()
								scan := reader.tbl.Scan(opts...)
								if tc.limit > 0 {
									scan = scan.UseRowLimit(tc.limit)
								}
								tasks, err := scan.PlanFiles(reader.ctx)
								require.NoError(t, err)
								require.Len(t, tasks, 1, "the data file must reach the reader, including all-pruned scans")
								require.Len(t, tasks[0].DeleteFiles, boolToInt(deleteKind == "position"))
								require.Len(t, tasks[0].DeletionVectorFiles, boolToInt(deleteKind == "dv"))
								reader.fs.bytes.Store(0)
								out := readVortexScan(t, reader.ctx, scan)
								filteredBytes := reader.fs.bytes.Load()
								require.EqualValues(t, len(want), out.NumRows())
								for _, field := range []string{selected, iceberg.RowIDColumnName} {
									if indices := out.Schema().FieldIndices(field); len(indices) > 0 {
										require.Equal(t, want, chunkedInt64Values(t, out.Column(indices[0]).Data()), "%s", field)
									}
								}
								require.Empty(t, out.Schema().FieldIndices("v64"), "filter-only column must not escape projection")

								// Compare identical physical projections. The unfiltered scan
								// reads every zone; fewer bytes prove backend pruning even
								// when positional deletes or synthesized row IDs are needed.
								reader.fs.bytes.Store(0)
								full := readVortexScan(t, reader.ctx, reader.tbl.Scan(table.WithSelectedFields(physical...)))
								require.Positive(t, full.NumRows())
								fullBytes := reader.fs.bytes.Load()
								require.Positive(t, filteredBytes)
								// Native and Parquet conservatively retain the null zone
								// for this wide range. Other cases prove their pruning.
								if reader.name == string(vortex.FFI) || !tc.filter.Equals(gaps) {
									require.Less(t, filteredBytes, fullBytes, "position-sensitive filter must reduce data-file reads")
								}
								t.Logf("%s: filtered=%d bytes, unfiltered=%d bytes", reader.name, filteredBytes, fullBytes)

								return out
							}
							wantTable := check(t, parquet)
							for _, reader := range readers {
								t.Run(reader.name, func(t *testing.T) {
									got := check(t, reader)
									require.True(t, array.TableEqual(wantTable, got), "schema, values, nulls and lineage must match Parquet")
								})
							}
						})
					}
				})
			}
		})
	}
}

// Canonical input used by the Rust zoned_ints fixture writer, independent of
// either decoder. Parquet gets the same values and 1024-row group boundaries.
func pushdownParityValues(row int) (int32, int64, bool) {
	within := row % 1024
	valid := row/1024 != 1 && row != 2 && row != 2068 && row != 3101 && row != 4097
	switch row / 1024 {
	case 0:
		return math.MinInt32 + int32(within), math.MinInt64 + int64(within), valid
	case 1:
		return 0, 0, false
	case 2:
		return 7, 7, valid
	case 3:
		return 1000 + int32(within), 1<<53 + 1 + int64(within), valid
	default:
		return math.MaxInt32 - 2 + int32(within), math.MaxInt64 - 2 + int64(within), valid
	}
}

type pushdownParityTable struct {
	name string
	ctx  context.Context
	tbl  *table.Table
	fs   *pushdownParityFS
}

func newPushdownParityTable(t *testing.T, name string, backend vortex.Backend, deleteKind string, deleted []int64) pushdownParityTable {
	t.Helper()
	ctx := t.Context()
	location := t.TempDir()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "row_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "v32", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 3, Name: "v64", Type: iceberg.PrimitiveTypes.Int64})
	path := filepath.Join(location, "data", "zoned.parquet")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	if backend == "" {
		var rows strings.Builder
		rows.WriteByte('[')
		for row := range 4099 {
			if row > 0 {
				rows.WriteByte(',')
			}
			v32, v64, valid := pushdownParityValues(row)
			if valid {
				fmt.Fprintf(&rows, `{"row_id":%d,"v32":%d,"v64":%d}`, row, v32, v64)
			} else {
				fmt.Fprintf(&rows, `{"row_id":%d,"v32":null,"v64":null}`, row)
			}
		}
		rows.WriteByte(']')
		arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
		require.NoError(t, err)
		writeParquetFileWithProperties(t, path, arrowSchema, rows.String(), 1024, nil)
	} else {
		ctx = vortex.WithBackend(ctx, backend)
		path = filepath.Join(location, "data", "zoned.vortex")
		data, err := os.ReadFile("internal/testdata/vortex/rust/zoned_ints.vortex")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, data, 0o600))
	}
	fs := &pushdownParityFS{path: path}
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		location, iceberg.Properties{table.PropertyFormatVersion: "3", table.ParquetBatchSizeKey: "127"})
	require.NoError(t, err)
	tbl := table.New(table.Identifier{"default", "pushdown_parity"}, meta,
		filepath.Join(location, "metadata", "v1.metadata.json"),
		func(context.Context) (iceio.IO, error) { return fs, nil }, &mockedCatalog{meta})
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(ctx, []string{path}, nil, false))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	switch deleteKind {
	case "position":
		var rows []string
		for _, pos := range deleted {
			rows = append(rows, fmt.Sprintf(`{"file_path":%q,"pos":%d}`, path, pos))
		}
		deleteSchema, err := table.SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, true, false)
		require.NoError(t, err)
		tbl = addVortexDeleteFile(t, ctx, tbl, iceberg.EntryContentPosDeletes, deleteSchema,
			"["+strings.Join(rows, ",")+"]", int64(len(rows)), nil)
	case "dv":
		writer := dv.NewDVWriter(iceio.LocalFS{}, unpartitionedSpecByID)
		require.NoError(t, writer.Add(path, deleted, 0, nil))
		files, err := writer.Flush(ctx, filepath.Join(location, "data", "delete.puffin"))
		require.NoError(t, err)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.NewRowDelta(nil).AddDeletes(files...).Commit(ctx))
		tbl, err = tx.Commit(ctx)
		require.NoError(t, err)
	}

	return pushdownParityTable{name: name, ctx: ctx, tbl: tbl, fs: fs}
}

type pushdownParityFS struct {
	iceio.LocalFS
	path  string
	bytes atomic.Int64
}

func (fs *pushdownParityFS) Open(path string) (iceio.File, error) {
	f, err := fs.LocalFS.Open(path)
	if err != nil || path != fs.path {
		return f, err
	}

	return &pushdownParityFile{File: f, bytes: &fs.bytes}, nil
}

type pushdownParityFile struct {
	iceio.File
	bytes *atomic.Int64
}

func (f *pushdownParityFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	f.bytes.Add(int64(n))

	return n, err
}

func (f *pushdownParityFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.bytes.Add(int64(n))

	return n, err
}

func boolToInt(value bool) int {
	if value {
		return 1
	}

	return 0
}

// The fixture's nullable int64 field stands in for a physically stored lineage
// column through an Iceberg name mapping. This exercises field IDs independently
// of the physical name, including null filling in a later surviving batch.
func TestVortexPushdownMixedStoredLineage(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			for _, field := range []struct {
				name string
				id   int
			}{
				{iceberg.RowIDColumnName, iceberg.RowIDFieldID},
				{iceberg.LastUpdatedSequenceNumberColumnName, iceberg.LastUpdatedSequenceNumberFieldID},
			} {
				t.Run(field.name, func(t *testing.T) {
					for _, kind := range []string{"position", "dv"} {
						t.Run(kind, func(t *testing.T) {
							deleted := []int64{3073, 4096}
							reader := newPushdownParityTable(t, string(backend), backend, kind, deleted)
							tx := reader.tbl.NewTransaction()
							mapping := fmt.Sprintf(`[{"field-id":1,"names":["row_id"]},{"field-id":2,"names":["v32"]},{"field-id":%d,"names":["v64"]}]`, field.id)
							require.NoError(t, tx.SetProperties(iceberg.Properties{table.DefaultNameMappingKey: mapping}))
							tbl, err := tx.Commit(reader.ctx)
							require.NoError(t, err)
							out := readVortexScan(t, reader.ctx, tbl.Scan(table.WithSelectedFields("row_id"), table.WithRowLineage(),
								table.WithRowFilter(iceberg.GreaterThanEqual(iceberg.Reference("row_id"), int64(3072)))))
							var wantPositions, wantStored []int64
							for row := 3072; row < 4099; row++ {
								if slices.Contains(deleted, int64(row)) {
									continue
								}
								_, value, valid := pushdownParityValues(row)
								if !valid {
									value = int64(row)
									if field.id == iceberg.LastUpdatedSequenceNumberFieldID {
										value = 1
									}
								}
								wantPositions = append(wantPositions, int64(row))
								wantStored = append(wantStored, value)
							}
							require.Equal(t, wantPositions, chunkedInt64Values(t, out.Column(0).Data()))
							indices := out.Schema().FieldIndices(field.name)
							require.Len(t, indices, 1)
							require.Zero(t, out.Column(indices[0]).NullN())
							require.Equal(t, wantStored, chunkedInt64Values(t, out.Column(indices[0]).Data()))
						})
					}
				})
			}
		})
	}
}

// Every emitted row in one zone is deleted before scanning reaches the last
// zone. A row-position cursor must still advance to the next physical batch.
func TestVortexPushdownFullyDeletedBatch(t *testing.T) {
	var deleted []int64
	for row := int64(3072); row < 4096; row++ {
		deleted = append(deleted, row)
	}
	deleted = append(deleted, 4096)
	for _, kind := range []string{"position", "dv"} {
		t.Run(kind, func(t *testing.T) {
			parquet := newPushdownParityTable(t, "parquet", "", kind, deleted)
			opts := []table.ScanOption{
				table.WithSelectedFields("row_id"), table.WithRowLineage(),
				table.WithRowFilter(iceberg.GreaterThanEqual(iceberg.Reference("row_id"), int64(3072))),
			}
			want := readVortexScan(t, parquet.ctx, parquet.tbl.Scan(opts...))
			require.Equal(t, []int64{4097, 4098}, chunkedInt64Values(t, want.Column(0).Data()))
			for _, backend := range vortex.AvailableBackends() {
				t.Run(string(backend), func(t *testing.T) {
					reader := newPushdownParityTable(t, string(backend), backend, kind, deleted)
					out := readVortexScan(t, reader.ctx, reader.tbl.Scan(opts...))
					require.True(t, array.TableEqual(want, out))
				})
			}
		})
	}
}

func TestVortexPushdownAllPrunedTaskThenSurvivors(t *testing.T) {
	for _, backend := range append([]vortex.Backend{""}, vortex.AvailableBackends()...) {
		name := string(backend)
		if backend == "" {
			name = "parquet"
		}
		t.Run(name, func(t *testing.T) {
			reader := newPushdownParityTable(t, name, backend, "dv", []int64{4096})
			scan := reader.tbl.Scan(table.WithSelectedFields("row_id"), table.WithRowLineage(), table.WithMaxConcurrency(1))
			tasks, err := scan.PlanFiles(reader.ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			first, second := tasks[0], tasks[0]
			// All zones of the first task are impossible. ReadTasks bypasses
			// manifest pruning, so both readers are actually opened in sequence.
			first.Residual = iceberg.EqualTo(iceberg.Reference("row_id"), int64(10000))
			second.Residual = iceberg.GreaterThanEqual(iceberg.Reference("row_id"), int64(4096))
			firstRowID := int64(10000)
			second.FirstRowID = &firstRowID
			_, records, err := scan.ReadTasks(reader.ctx, []table.FileScanTask{first, second})
			require.NoError(t, err)
			var positions, rowIDs []int64
			for record, err := range records {
				require.NoError(t, err)
				positions = append(positions, record.Column(0).(*array.Int64).Int64Values()...)
				index := record.Schema().FieldIndices(iceberg.RowIDColumnName)
				require.Len(t, index, 1)
				rowIDs = append(rowIDs, record.Column(index[0]).(*array.Int64).Int64Values()...)
				record.Release()
			}
			require.Equal(t, []int64{4097, 4098}, positions)
			require.Equal(t, []int64{14097, 14098}, rowIDs)
		})
	}
}
