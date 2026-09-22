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

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

// The internal reader oracle checks every source value. These checks additionally
// take ordinary compressed output through registration and Iceberg's residuals.
func TestVortexDefaultWriterScan(t *testing.T) {
	names := []string{"row_id", "flag", "v32", "v64", "f32", "f64", "text", "blob"}
	types := []iceberg.Type{
		iceberg.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64,
		iceberg.PrimitiveTypes.String, iceberg.PrimitiveTypes.Binary,
	}
	fields := make([]iceberg.NestedField, len(names))
	for i, name := range names {
		fields[i] = iceberg.NestedField{ID: i + 1, Name: name, Type: types[i], Required: i == 0}
	}
	schema := iceberg.NewSchema(0, fields...)
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			for kind, fixture := range []string{"repeated", "progression", "broad", "chunked", "precision"} {
				t.Run(fixture, func(t *testing.T) {
					ctx := vortex.WithBackend(t.Context(), backend)
					location := t.TempDir()
					path := filepath.Join(location, "data", "default_"+fixture+".vortex")
					data, err := os.ReadFile(filepath.Join("internal", "testdata", "vortex", "rust", "default_"+fixture+".vortex"))
					require.NoError(t, err)
					require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
					require.NoError(t, os.WriteFile(path, data, 0o600))
					meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
						location, iceberg.Properties{table.PropertyFormatVersion: "3"})
					require.NoError(t, err)
					tbl := table.New(table.Identifier{"default", "writer_corpus"}, meta,
						filepath.Join(location, "metadata", "v1.metadata.json"),
						func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &mockedCatalog{meta})
					tx := tbl.NewTransaction()
					require.NoError(t, tx.AddFiles(ctx, []string{path}, nil, false))
					tbl, err = tx.Commit(ctx)
					require.NoError(t, err)
					tasks, err := tbl.Scan().PlanFiles(ctx)
					require.NoError(t, err)
					require.Len(t, tasks, 1)
					require.Equal(t, iceberg.VortexFile, tasks[0].File.FileFormat())
					require.EqualValues(t, 8193, tasks[0].File.Count())

					// Straddle a source distribution/chunk boundary and project
					// every scalar through the shared Arrow conversion path.
					filter := iceberg.NewAnd(
						iceberg.GreaterThanEqual(iceberg.Reference("row_id"), int64(2040)),
						iceberg.LessThan(iceberg.Reference("row_id"), int64(2057)))
					for range 2 {
						out := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields(names...), table.WithRowFilter(filter)))
						require.EqualValues(t, 17, out.NumRows())
						require.EqualValues(t, 8, out.NumCols())
						records := array.NewTableReader(out, 5)
						row := int64(2040)
						for records.Next() {
							batch := records.RecordBatch()
							for i := range int(batch.NumRows()) {
								require.Equal(t, row, batch.Column(0).(*array.Int64).Value(i))
								for col := 1; col < len(names); col++ {
									valid := (kind != 3 || row < 3072 || row >= 4096) && (row+int64(col*3))%29 != 0
									require.Equal(t, valid, batch.Column(col).IsValid(i), "row=%d column=%s", row, names[col])
								}
								row++
							}
						}
						require.NoError(t, records.Err())
						records.Release()
						require.EqualValues(t, 2057, row)
					}
					empty := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("blob"), table.WithRowFilter(
						iceberg.GreaterThan(iceberg.Reference("row_id"), int64(8192)))))
					require.Zero(t, empty.NumRows())

					if fixture == "progression" {
						// Float comparisons remain in Arrow. String and float
						// predicate columns are absent from the output; the
						// limit applies only after both residuals are satisfied.
						residual := iceberg.NewAnd(
							iceberg.GreaterThanEqual(iceberg.Reference("row_id"), int64(3995)),
							iceberg.NewAnd(
								iceberg.LessThan(iceberg.Reference("f64"), float64(0)),
								iceberg.EqualTo(iceberg.Reference("text"), "vortex dictionary entry")))
						var want []int64
						for row := int64(3995); len(want) < 3; row++ {
							if row%2001 < 1000 && row%8 == 3 && (row+15)%29 != 0 && (row+18)%29 != 0 {
								want = append(want, row)
							}
						}
						out := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("row_id"), table.WithRowFilter(residual)).UseRowLimit(3))
						require.EqualValues(t, 1, out.NumCols())
						require.Equal(t, want, chunkedInt64Values(t, out.Column(0).Data()))
					}
				})
			}
		})
	}
}
