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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

// Compare every value and validity bit to the independent Rust writer's input.
// The tagged run checks native and FFI readers against exactly the same oracle;
// ordinary tests still cover the native adapter without a Rust toolchain.
func TestVortexPCOValuesAndNulls(t *testing.T) {
	const fixture = "testdata/vortex/rust/pco_regressions.vortex"
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			r, err := vortexFormat{}.Open(ctx, iceio.LocalFS{}, fixture)
			require.NoError(t, err)
			defer r.Close()
			rr, err := r.GetRecords(ctx, nil, nil)
			require.NoError(t, err)
			defer rr.Release()
			rows := 0
			for rr.Next() {
				record := rr.RecordBatch()
				require.EqualValues(t, 13, record.NumCols())
				for col, field := range record.Schema().Fields() {
					a := record.Column(col)
					for j := range a.Len() {
						row := rows + j
						wantBits, valid := pcoExpectedValue(t, field.Name, row)
						require.Equal(t, valid, a.IsValid(j), "%s row %d validity", field.Name, row)
						if !valid {
							continue
						}
						var gotBits uint64
						switch values := a.(type) {
						case *array.Int64:
							gotBits = uint64(values.Value(j))
						case *array.Float64:
							gotBits = math.Float64bits(values.Value(j))
						case *array.Float32:
							gotBits = uint64(math.Float32bits(values.Value(j)))
						default:
							t.Fatalf("unexpected Pco output type %T", a)
						}
						require.Equal(t, wantBits, gotBits, "%s row %d bits", field.Name, row)
					}
				}
				rows += int(record.NumRows())
			}
			require.NoError(t, rr.Err())
			require.Equal(t, 8000, rows)
		})
	}
}

func pcoExpectedValue(t *testing.T, name string, row int) (uint64, bool) {
	t.Helper()
	mod := (row * 17) % 77
	// Typed variables preserve Rust's per-operation float32 rounding and -0.
	f32base, f64base := float32(0.1), 0.1
	switch name {
	case "i64_nullable":
		return uint64(row * 7), row%3 != 0
	case "i64_all_null":
		return 0, false
	case "i64_all_valid":
		return uint64(row * 7), true
	case "i64_first":
		return 1 << 63, row == 0
	case "i64_last":
		return math.MaxInt64, row == 7999
	case "i64_null_runs":
		return uint64(-int64(row) * 7), row >= 257 && (row < 510 || row >= 800) && row != 7999
	case "f64_positive":
		return math.Float64bits(float64(mod) * f64base), true
	case "f64_negative":
		return math.Float64bits(float64(mod) * -f64base), true
	case "f64_nullable":
		return math.Float64bits(float64(mod) * f64base), row%3 != 0
	case "f64_large":
		return math.Float64bits(1e13 + float64(mod)*f64base), true
	case "f32_positive":
		return uint64(math.Float32bits(float32(mod) * f32base)), true
	case "f32_negative":
		return uint64(math.Float32bits(float32(mod) * -f32base)), true
	case "f32_nullable":
		return uint64(math.Float32bits(float32(mod) * f32base)), row%3 != 0
	default:
		t.Fatalf("unexpected Pco field %q", name)

		return 0, false
	}
}
