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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

func TestVortexNumericPromotionScope(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		for _, floating := range []bool{false, true} {
			name := "integer"
			if floating {
				name = "float"
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				ctx := vortex.WithBackend(t.Context(), backend)
				_, path := stageVortexFixture(t)
				schema := vortexTestSchema()
				field := "id"
				rows := int64(vortexE2ERows)
				target := arrow.INT64
				predicates := []iceberg.BooleanExpression{
					iceberg.EqualTo(iceberg.Reference(field), int64(42)),
					iceberg.NotEqualTo(iceberg.Reference(field), int64(1)<<40),
					iceberg.IsNull(iceberg.Reference(field)),
				}
				fields := schema.Fields()
				fields[0].Type = iceberg.PrimitiveTypes.Int64
				if floating {
					path = "internal/testdata/vortex/float_edges.vortex"
					field, rows, target = "value32", 10, arrow.FLOAT64
					fields = []iceberg.NestedField{
						{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
						{ID: 2, Name: "value64", Type: iceberg.PrimitiveTypes.Float64},
						{ID: 3, Name: field, Type: iceberg.PrimitiveTypes.Float64},
					}
					predicates = []iceberg.BooleanExpression{
						iceberg.EqualTo(iceberg.Reference(field), math.Inf(1)),
						iceberg.LessThan(iceberg.Reference(field), math.Nextafter(1, math.Inf(1))),
						iceberg.IsNull(iceberg.Reference(field)),
					}
				}
				tbl, _ := commitVortexRegistration(t, ctx, newVortexRegistrationTable(t,
					iceberg.NewSchema(0, fields...), iceberg.UnpartitionedSpec,
					iceberg.Properties{table.DefaultWriteMetricsModeKey: "none"}), path)
				projected := readVortexScan(t, ctx, tbl.Scan(table.WithSelectedFields(field)))
				require.Equal(t, rows, projected.NumRows())
				require.Equal(t, target, projected.Schema().Field(0).Type.ID())
				for _, predicate := range predicates {
					for _, lineage := range []bool{false, true} {
						options := []table.ScanOption{table.WithSelectedFields(field), table.WithRowFilter(predicate)}
						if lineage {
							options = append(options, table.WithRowLineage())
						}
						out, err := tbl.Scan(options...).ToArrowTable(ctx)
						if out != nil {
							out.Release()
						}
						require.ErrorIs(t, err, iceberg.ErrNotImplemented)
					}
				}
			})
		}
	}
}

func TestVortexPromotedDeleteRejected(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		for _, mode := range []string{table.WriteModeCopyOnWrite, table.WriteModeMergeOnRead} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				ctx := vortex.WithBackend(t.Context(), backend)
				schema := iceberg.NewSchema(0,
					iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
					iceberg.NestedField{ID: 2, Name: "value64", Type: iceberg.PrimitiveTypes.Float64},
					iceberg.NestedField{ID: 3, Name: "value32", Type: iceberg.PrimitiveTypes.Float64})
				tbl, _ := commitVortexRegistration(t, ctx, newVortexRegistrationTable(t,
					schema, iceberg.UnpartitionedSpec, iceberg.Properties{
						table.DefaultWriteMetricsModeKey: "none", table.WriteDeleteModeKey: mode,
					}), "internal/testdata/vortex/float_edges.vortex")
				filter := iceberg.IsIn(iceberg.Reference("value32"), math.Nextafter(1, math.Inf(1)), float64(-1))
				tx := tbl.NewTransaction()
				require.ErrorIs(t, tx.Delete(ctx, filter, nil), iceberg.ErrNotImplemented)
			})
		}
	}
}
