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
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

// The pinned Rust writer gives each integer column five physical zones. Check
// pruning through Iceberg, where residuals must remain exact after projection.
func TestVortexZonedScan(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			location := t.TempDir()
			path := filepath.Join(location, "data", "zoned.vortex")
			data, err := os.ReadFile("internal/testdata/vortex/rust/zoned_ints.vortex")
			require.NoError(t, err)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
			require.NoError(t, os.WriteFile(path, data, 0o600))
			schema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "row_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "v32", Type: iceberg.PrimitiveTypes.Int32},
				iceberg.NestedField{ID: 3, Name: "v64", Type: iceberg.PrimitiveTypes.Int64},
			)
			meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
				location, iceberg.Properties{table.PropertyFormatVersion: "3"})
			require.NoError(t, err)
			tbl := table.New(table.Identifier{"default", "zoned"}, meta,
				filepath.Join(location, "metadata", "v1.metadata.json"),
				func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &mockedCatalog{meta})
			tx := tbl.NewTransaction()
			require.NoError(t, tx.AddFiles(ctx, []string{path}, nil, false))
			tbl, err = tx.Commit(ctx)
			require.NoError(t, err)

			const large = int64(1<<53 + 1)
			preciseRange := iceberg.NewAnd(
				iceberg.GreaterThan(iceberg.Reference("v64"), large),
				iceberg.LessThanEqual(iceberg.Reference("v64"), large+4))
			for _, tc := range []struct {
				name   string
				filter iceberg.BooleanExpression
				want   []int64
			}{
				{"exact_int64_filter_only", preciseRange, []int64{3073, 3074, 3075, 3076}},
				{"int32_min_and_null", iceberg.LessThanEqual(iceberg.Reference("v32"), int32(math.MinInt32+3)), []int64{0, 1, 3}},
				{"int64_min_and_null", iceberg.LessThanEqual(iceberg.Reference("v64"), int64(math.MinInt64+3)), []int64{0, 1, 3}},
				{"int32_max_partial_zone", iceberg.EqualTo(iceberg.Reference("v32"), int32(math.MaxInt32)), []int64{4098}},
				{"int64_max_partial_zone", iceberg.EqualTo(iceberg.Reference("v64"), int64(math.MaxInt64)), []int64{4098}},
				{"impossible", iceberg.EqualTo(iceberg.Reference("v64"), int64(0)), nil},
				{"disjunction", iceberg.NewOr(
					iceberg.EqualTo(iceberg.Reference("row_id"), int64(0)),
					iceberg.EqualTo(iceberg.Reference("row_id"), int64(4098))), []int64{0, 4098}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					// Repeat on the same table to detect mutation of cached statistics/data.
					for range 2 {
						out := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("row_id"), table.WithRowFilter(tc.filter)))
						require.EqualValues(t, 1, out.NumCols())
						require.EqualValues(t, len(tc.want), out.NumRows())
						if len(tc.want) > 0 {
							require.Equal(t, tc.want, chunkedInt64Values(t, out.Column(0).Data()))
						}
					}
				})
			}

			out := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("row_id"), table.WithRowFilter(preciseRange)).UseRowLimit(2))
			require.Equal(t, []int64{3073, 3074}, chunkedInt64Values(t, out.Column(0).Data()))

			deleteSchema, err := table.SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, true, false)
			require.NoError(t, err)
			tbl = addVortexDeleteFile(t, ctx, tbl, iceberg.EntryContentPosDeletes, deleteSchema,
				fmt.Sprintf(`[{"file_path":%q,"pos":3073},{"file_path":%q,"pos":3075}]`, path, path), 2, nil)
			scan := tbl.Scan(table.WithSelectedFields("row_id"), table.WithRowFilter(
				iceberg.GreaterThan(iceberg.Reference("v64"), large)), table.WithRowLineage()).UseRowLimit(3)
			tasks, err := scan.PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			require.Len(t, tasks[0].DeleteFiles, 1)
			out = readVortexScan(t, ctx, scan)
			require.Equal(t, []int64{3074, 3076, 3077}, chunkedInt64Values(t, out.Column(0).Data()))
			rowID := out.Schema().FieldIndices(iceberg.RowIDColumnName)
			require.Len(t, rowID, 1)
			require.Equal(t, []int64{3074, 3076, 3077}, chunkedInt64Values(t, out.Column(rowID[0]).Data()))
		})
	}
}
