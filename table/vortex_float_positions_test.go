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
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

func vortexFloatTableWithDeletes(t *testing.T, ctx context.Context, tbl *table.Table) *table.Table {
	t.Helper()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead}))
	result, err := tx.Commit(ctx)
	require.NoError(t, err)

	return result
}

func requireVortexFloatLineage(t *testing.T, ctx context.Context, tbl *table.Table, filter iceberg.BooleanExpression, ids []int32) {
	t.Helper()

	out := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields("value32", "id"), table.WithRowFilter(filter), table.WithRowLineage()))
	requireVortexIDs(t, out, ids)
	rowID := out.Schema().FieldIndices(iceberg.RowIDColumnName)
	require.Len(t, rowID, 1)
	var wanted []int64
	for _, id := range ids {
		wanted = append(wanted, int64(id))
	}
	require.Equal(t, wanted, chunkedInt64Values(t, out.Column(rowID[0]).Data()))
	seq := out.Schema().FieldIndices(iceberg.LastUpdatedSequenceNumberColumnName)
	require.Len(t, seq, 1)
	for _, v := range chunkedInt64Values(t, out.Column(seq[0]).Data()) {
		require.EqualValues(t, 1, v)
	}
}

func TestVortexFloatPositions(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			filter := iceberg.LessThanEqual(iceberg.Reference("value32"), float32(1))
			t.Run("lineage", func(t *testing.T) {
				tbl := vortexFloatTableWithDeletes(t, ctx, vortexFloatTable(t, ctx))
				requireVortexFloatLineage(t, ctx, tbl, filter, []int32{2, 3, 4, 5, 6})
			})
			t.Run("position_deletes", func(t *testing.T) {
				tbl := vortexFloatTableWithDeletes(t, ctx, vortexFloatTable(t, ctx))
				path, err := filepath.Abs("internal/testdata/vortex/float_edges.vortex")
				require.NoError(t, err)
				schema, err := table.SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, true, false)
				require.NoError(t, err)
				tbl = addVortexDeleteFile(t, ctx, tbl, iceberg.EntryContentPosDeletes, schema, fmt.Sprintf(`[{"file_path":%q,"pos":5},{"file_path":%q,"pos":7}]`, path, path), 2, nil)
				requireVortexFloatLineage(t, ctx, tbl, filter, []int32{2, 3, 4, 6})
			})
			t.Run("ordinary_generated_deletion_vector", func(t *testing.T) {
				tbl := vortexFloatTable(t, ctx)
				tx := tbl.NewTransaction()
				require.NoError(t, tx.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead}))
				var err error
				tbl, err = tx.Commit(ctx)
				require.NoError(t, err)
				tx = tbl.NewTransaction()
				require.NoError(t, tx.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int32(4)), nil))
				tbl, err = tx.Commit(ctx)
				require.NoError(t, err)
				requireVortexFloatLineage(t, ctx, tbl, iceberg.AlwaysTrue{}, []int32{0, 1, 2, 3, 5, 6, 7, 8, 9})
			})
			t.Run("generated_deletion_vector", func(t *testing.T) {
				tbl := vortexFloatTableWithDeletes(t, ctx, vortexFloatTable(t, ctx))
				tx := tbl.NewTransaction()
				require.NoError(t, tx.Delete(ctx, filter, nil))
				var err error
				tbl, err = tx.Commit(ctx)
				require.NoError(t, err)
				requireVortexFloatLineage(t, ctx, tbl, iceberg.AlwaysTrue{}, []int32{0, 1, 7, 8, 9})
			})
		})
	}
}
