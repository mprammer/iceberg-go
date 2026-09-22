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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

var alpFixtureCases = []struct {
	name             string
	start, end       int
	patches, within  uint64
	indices, offsets []uint64
}{
	{"alp_patches", 0, 4105, 10, 0, []uint64{1, 4, 5, 31, 1023, 1025, 3072, 4095, 4096, 4102}, []uint64{0, 5, 6, 6, 8}},
	{"alp_patches_slice", 5, 4100, 7, 2, []uint64{5, 31, 1023, 1025, 3072, 4095, 4096}, []uint64{0, 5, 6, 6, 8}},
	{"alp_patches_late_slice", 1025, 4100, 4, 0, []uint64{1025, 3072, 4095, 4096}, []uint64{5, 6, 6, 8}},
	{"alp_patches_boundary", 1023, 1026, 2, 4, []uint64{1023, 1025}, []uint64{0, 5}},
	{"alp_patches_empty", 5, 5, 0, 0, nil, nil},
}

func alpOriginalBits(row int, f32 bool) (uint64, bool) {
	positions := [...]int{1, 4, 5, 31, 1023, 1024, 1025, 3072, 4095, 4096, 4102}
	bits32 := [...]uint32{0x7fc01234, 0xffc05678, 0x7f800000, 0xff800000, 0x80000000, 0, 1, 0x80000001, 0x3eaaaaab, 0x7f7fffff, 0x00800000}
	bits64 := [...]uint64{0x7ff8000000001234, 0xfff8000000005678, 0x7ff0000000000000, 0xfff0000000000000, 0x8000000000000000, 0, 1, 0x8000000000000001, 0x3fd5555555555555, 0x7fefffffffffffff, 0x0010000000000000}
	for i, position := range positions {
		if row == position {
			if f32 {
				return uint64(bits32[i]), true
			}

			return bits64[i], true
		}
	}
	if f32 {
		return uint64(math.Float32bits(float32(row - 2000))), row%17 != 0
	}

	return math.Float64bits(float64(row - 2000)), row%17 != 0
}

// Each backend is compared with the original Rust input, including NaN payloads,
// signed zero, and validity. Agreement between readers alone is insufficient.
func TestVortexALPOriginalValues(t *testing.T) {
	for _, fixture := range alpFixtureCases {
		for _, backend := range vortex.AvailableBackends() {
			t.Run(fixture.name+"/"+string(backend), func(t *testing.T) {
				ctx := vortex.WithBackend(t.Context(), backend)
				r, err := vortexFormat{}.Open(ctx, iceio.LocalFS{}, "testdata/vortex/rust/"+fixture.name+".vortex")
				require.NoError(t, err)
				defer r.Close()
				rr, err := r.GetRecords(ctx, nil, nil)
				require.NoError(t, err)
				defer rr.Release()
				require.Equal(t, 7, rr.Schema().NumFields())
				for i, name := range []string{"row_id", "f32_required", "f32_nullable", "f32_all_null", "f64_required", "f64_nullable", "f64_all_null"} {
					field := rr.Schema().Field(i)
					require.Equal(t, name, field.Name)
					require.Equal(t, name != "row_id" && !strings.HasSuffix(name, "required"), field.Nullable, name)
					wantType := arrow.INT64
					if strings.HasPrefix(name, "f32") {
						wantType = arrow.FLOAT32
					} else if strings.HasPrefix(name, "f64") {
						wantType = arrow.FLOAT64
					}
					require.Equal(t, wantType, field.Type.ID(), name)
				}
				rows := 0
				for rr.Next() {
					batch := rr.RecordBatch()
					require.EqualValues(t, 7, batch.NumCols())
					for col, field := range batch.Schema().Fields() {
						a := batch.Column(col)
						for j := range a.Len() {
							row := fixture.start + rows + j
							if field.Name == "row_id" {
								require.True(t, a.IsValid(j))
								require.Equal(t, int64(row), a.(*array.Int64).Value(j))

								continue
							}
							bits, valid := alpOriginalBits(row, strings.HasPrefix(field.Name, "f32"))
							if strings.HasSuffix(field.Name, "required") {
								valid = true
							} else if strings.HasSuffix(field.Name, "all_null") {
								valid = false
							}
							require.Equal(t, valid, a.IsValid(j), "%s row %d validity", field.Name, row)
							if !valid {
								continue
							}
							switch values := a.(type) {
							case *array.Float32:
								require.Equal(t, uint32(bits), math.Float32bits(values.Value(j)), "%s row %d", field.Name, row)
							case *array.Float64:
								require.Equal(t, bits, math.Float64bits(values.Value(j)), "%s row %d", field.Name, row)
							default:
								t.Fatalf("unexpected ALP column %s: %T", field.Name, a)
							}
						}
					}
					rows += int(batch.NumRows())
				}
				require.NoError(t, rr.Err())
				require.Equal(t, fixture.end-fixture.start, rows)
			})
		}
	}
}

func ownedALPOriginalBits(row int, f32 bool) uint64 {
	positions := [...]int{2, 7, 19, 509, 1022, 1024, 1537, 2047, 2048, 3089}
	bits32 := [...]uint32{0x80000000, 0x7f800000, 0xff800000, 0x7fc02468, 0xffc01357, 0x00000001, 0x80000001, 0x7f7fffff, 0x00800000, 0x3e4ccccd}
	bits64 := [...]uint64{0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000002468, 0xfff8000000001357, 0x0000000000000001, 0x8000000000000001, 0x7fefffffffffffff, 0x0010000000000000, 0x3fc999999999999a}
	for i, position := range positions {
		if row == position {
			if f32 {
				return uint64(bits32[i])
			}

			return bits64[i]
		}
	}
	if f32 {
		return uint64(math.Float32bits(float32(row*3 - 4600)))
	}

	return math.Float64bits(float64(row*3 - 4600))
}

func TestVortexOwnedALPFlatOriginalValues(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			r, err := vortexFormat{}.Open(ctx, iceio.LocalFS{}, "testdata/vortex/rust/owned_alp_flat.vortex")
			require.NoError(t, err)
			defer r.Close()
			rr, err := r.GetRecords(ctx, nil, nil)
			require.NoError(t, err)
			defer rr.Release()
			fields := []arrow.Field{
				{Name: "single", Type: arrow.PrimitiveTypes.Float32},
				{Name: "double", Type: arrow.PrimitiveTypes.Float64},
				{Name: "nullable_single", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
				{Name: "nullable_double", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			}
			require.True(t, rr.Schema().Equal(arrow.NewSchema(fields, nil)), "%s", rr.Schema())
			rows := 0
			for rr.Next() {
				batch := rr.RecordBatch()
				require.EqualValues(t, len(fields), batch.NumCols())
				for col, field := range fields {
					a := batch.Column(col)
					for i := range a.Len() {
						row := rows + i
						valid := !field.Nullable || (row%23 != 11 && (row < 1800 || row >= 1831))
						require.Equal(t, valid, a.IsValid(i), "%s row %d validity", field.Name, row)
						if !valid {
							continue
						}
						switch a := a.(type) {
						case *array.Float32:
							require.Equal(t, uint32(ownedALPOriginalBits(row, true)), math.Float32bits(a.Value(i)), "%s row %d", field.Name, row)
						case *array.Float64:
							require.Equal(t, ownedALPOriginalBits(row, false), math.Float64bits(a.Value(i)), "%s row %d", field.Name, row)
						default:
							t.Fatalf("unexpected ALP column %s: %T", field.Name, a)
						}
					}
				}
				rows += int(batch.NumRows())
			}
			require.NoError(t, rr.Err())
			require.Equal(t, 3091, rows)
		})
	}
}
