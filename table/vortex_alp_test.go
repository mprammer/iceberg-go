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

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

// The Rust fixture places ALP exceptions across the 1024-row patch boundary.
// Scan through Iceberg to check filtering, projection and physical delete positions.
func TestVortexALPPatchScan(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			location := t.TempDir()
			path := filepath.Join(location, "data", "alp.vortex")
			data, err := os.ReadFile("internal/testdata/vortex/rust/alp_patches.vortex")
			require.NoError(t, err)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
			require.NoError(t, os.WriteFile(path, data, 0o600))
			schema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "row_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "f32_required", Type: iceberg.PrimitiveTypes.Float32, Required: true},
				iceberg.NestedField{ID: 3, Name: "f32_nullable", Type: iceberg.PrimitiveTypes.Float32},
				iceberg.NestedField{ID: 4, Name: "f32_all_null", Type: iceberg.PrimitiveTypes.Float32},
				iceberg.NestedField{ID: 5, Name: "f64_required", Type: iceberg.PrimitiveTypes.Float64, Required: true},
				iceberg.NestedField{ID: 6, Name: "f64_nullable", Type: iceberg.PrimitiveTypes.Float64},
				iceberg.NestedField{ID: 7, Name: "f64_all_null", Type: iceberg.PrimitiveTypes.Float64},
			)
			meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
				location, iceberg.Properties{table.PropertyFormatVersion: "3"})
			require.NoError(t, err)
			tbl := table.New(table.Identifier{"default", "alp"}, meta,
				filepath.Join(location, "metadata", "v1.metadata.json"),
				func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &mockedCatalog{meta})
			tx := tbl.NewTransaction()
			require.NoError(t, tx.AddFiles(ctx, []string{path}, nil, false))
			tbl, err = tx.Commit(ctx)
			require.NoError(t, err)

			filters := []struct {
				name string
				expr iceberg.BooleanExpression
			}{
				{"float32", iceberg.LessThanEqual(iceberg.Reference("f32_nullable"), float32(0))},
				{"float64", iceberg.LessThanEqual(iceberg.Reference("f64_nullable"), float64(0))},
			}
			for _, filter := range filters {
				t.Run(filter.name, func(t *testing.T) {
					expr := iceberg.NewAnd(filter.expr, iceberg.NewAnd(
						iceberg.GreaterThanEqual(iceberg.Reference("row_id"), int64(1020)),
						iceberg.LessThanEqual(iceberg.Reference("row_id"), int64(1030))))
					scan := tbl.Scan(table.WithSelectedFields("row_id"), table.WithRowFilter(expr))
					out := readVortexScan(t, ctx, scan)
					require.EqualValues(t, 1, out.NumCols(), "filter-only ALP column must not leak into projection")
					// 1020 is null; 1023/1024 are signed zeros; 1025 is a positive subnormal.
					require.Equal(t, []int64{1021, 1022, 1023, 1024, 1026, 1027, 1028, 1029, 1030},
						chunkedInt64Values(t, out.Column(0).Data()))
				})
			}

			deleteSchema, err := table.SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, true, false)
			require.NoError(t, err)
			tbl = addVortexDeleteFile(t, ctx, tbl, iceberg.EntryContentPosDeletes, deleteSchema,
				fmt.Sprintf(`[{"file_path":%q,"pos":1022},{"file_path":%q,"pos":1024}]`, path, path), 2, nil)
			for _, filter := range filters {
				t.Run(filter.name+"_deletes_and_limit", func(t *testing.T) {
					expr := iceberg.NewAnd(filter.expr,
						iceberg.GreaterThanEqual(iceberg.Reference("row_id"), int64(1020)))
					scan := tbl.Scan(table.WithSelectedFields("row_id"), table.WithRowFilter(expr), table.WithRowLineage()).UseRowLimit(3)
					tasks, err := scan.PlanFiles(ctx)
					require.NoError(t, err)
					require.Len(t, tasks, 1)
					require.Len(t, tasks[0].DeleteFiles, 1)
					out := readVortexScan(t, ctx, scan)
					require.Equal(t, []int64{1021, 1023, 1026}, chunkedInt64Values(t, out.Column(0).Data()))
					rowID := out.Schema().FieldIndices(iceberg.RowIDColumnName)
					require.Len(t, rowID, 1)
					require.Equal(t, []int64{1021, 1023, 1026}, chunkedInt64Values(t, out.Column(rowID[0]).Data()))
				})
			}
		})
	}
}
