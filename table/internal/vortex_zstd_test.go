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
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

// The pinned Rust generator omits legacy frame counts while retaining valid
// compressed buffers. Compare each reader with the original input, including nulls.
func TestVortexLegacyZstdValuesAndNulls(t *testing.T) {
	for _, fixture := range []struct {
		name string
		rows int
	}{{"zstd_legacy", 1051}, {"zstd_legacy_empty", 0}} {
		for _, backend := range vortex.AvailableBackends() {
			t.Run(fixture.name+"/"+string(backend), func(t *testing.T) {
				ctx := vortex.WithBackend(t.Context(), backend)
				r, err := vortexFormat{}.Open(ctx, iceio.LocalFS{}, "testdata/vortex/rust/"+fixture.name+".vortex")
				require.NoError(t, err)
				defer r.Close()
				rr, err := r.GetRecords(ctx, nil, nil)
				require.NoError(t, err)
				defer rr.Release()
				rows := 0
				for rr.Next() {
					batch := rr.RecordBatch()
					require.EqualValues(t, 9, batch.NumCols())
					for col, field := range batch.Schema().Fields() {
						a := batch.Column(col)
						for j := range a.Len() {
							row := rows + j
							valid := row%11 >= 3
							if strings.HasSuffix(field.Name, "required") {
								valid = true
							}
							if strings.HasSuffix(field.Name, "all_null") {
								valid = false
							}
							require.Equal(t, valid, a.IsValid(j), "%s row %d validity", field.Name, row)
							if !valid {
								continue
							}
							switch values := a.(type) {
							case *array.Int64:
								require.Equal(t, int64(row)*19-1000, values.Value(j), "%s row %d", field.Name, row)
							case *array.Float64:
								bits := []uint64{0x8000000000000000, 0x7ff8000000001234, 0x7ff0000000000000, 0xfff0000000000000, 1}
								require.Equal(t, bits[row%5], math.Float64bits(values.Value(j)), "%s row %d", field.Name, row)
							case *array.String:
								want := ""
								if row%13 != 0 {
									want = fmt.Sprintf("value-%04d-abcdefghijklmnopqrstuv-λ", row)
								}
								require.Equal(t, want, values.Value(j), "%s row %d", field.Name, row)
							case *array.Binary:
								require.Equal(t, []byte{0, 255, byte(row % 256)}, values.Value(j), "%s row %d", field.Name, row)
							default:
								t.Fatalf("unexpected Zstd output %T", a)
							}
						}
					}
					rows += int(batch.NumRows())
				}
				require.NoError(t, rr.Err())
				require.Equal(t, fixture.rows, rows)
			})
		}
	}
}
