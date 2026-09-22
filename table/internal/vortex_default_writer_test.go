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
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

const defaultWriterRows = 8193

var (
	defaultWriterFixtures = []string{"repeated", "progression", "broad", "chunked", "precision"}
	defaultWriterFields   = []arrow.Field{
		{Name: "row_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "v32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "v64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "blob", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}
)

func defaultWriterPath(name string) string {
	return "testdata/vortex/rust/default_" + name + ".vortex"
}

func defaultWriterHash(row int) uint64 {
	x := uint64(row) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb

	return x ^ (x >> 31)
}

// Source formulas are independent of both readers and of the compressed file.
// Float expectations are IEEE bits, so NaN payloads and signed zeros are checked.
func defaultWriterOriginal(fixture, col, row int) (any, bool) {
	valid := col == 0 || ((fixture != 3 || row < 3072 || row >= 4096) && (row+col*3)%29 != 0)
	if col == 0 {
		return int64(row), true
	}
	mode := fixture
	if fixture == 3 {
		mode = (row / 2048) % 3
	}
	h := defaultWriterHash(row)
	switch col {
	case 1:
		switch mode {
		case 0:
			return true, valid
		case 1, 4:
			return (row/127)%2 == 0, valid
		default:
			return h&1 != 0, valid
		}
	case 2:
		switch mode {
		case 0:
			return int32(-77), valid
		case 1:
			return int32(row - 4096), valid
		case 4:
			switch row % 1009 {
			case 0:
				return int32(math.MinInt32), valid
			case 1:
				return int32(math.MaxInt32), valid
			default:
				return int32(1000 + row%17), valid
			}
		default:
			switch row % 257 {
			case 0:
				return int32(math.MinInt32), valid
			case 1:
				return int32(math.MaxInt32), valid
			default:
				return int32(h), valid
			}
		}
	case 3:
		switch mode {
		case 0:
			return int64(1<<53 + 1), valid
		case 1:
			return int64(math.MinInt64) + int64(row)*17, valid
		case 4:
			switch row % 1009 {
			case 0:
				return int64(math.MinInt64), valid
			case 1:
				return int64(math.MaxInt64), valid
			default:
				return int64(1<<45) + int64(row%31), valid
			}
		default:
			switch row % 257 {
			case 0:
				return int64(math.MinInt64), valid
			case 1:
				return int64(math.MaxInt64), valid
			default:
				return int64(h), valid
			}
		}
	case 4:
		switch mode {
		case 0:
			return math.Float32bits(1.25), valid
		case 1:
			return math.Float32bits(float32(row%2001-1000) / 10), valid
		default:
			special := [...]uint32{0x80000000, 0, 0x7fc01234, 0xffc05678, 0x7f800000, 0xff800000, 1, 0x80000001, 0x7f7fffff, 0x00800000}
			period := 257
			if mode == 4 {
				period = 1021
			}
			if row%period < len(special) {
				return special[row%period], valid
			}
			if mode == 4 {
				return uint32(0x3f800000) | uint32(h)&0x007fffff, valid
			}

			return uint32(h) & 0xff7fffff, valid
		}
	case 5:
		switch mode {
		case 0:
			return math.Float64bits(-3.5), valid
		case 1:
			return math.Float64bits(float64(row%2001-1000) / 10), valid
		default:
			special := [...]uint64{0x8000000000000000, 0, 0x7ff8000000001234, 0xfff8000000005678, 0x7ff0000000000000, 0xfff0000000000000, 1, 0x8000000000000001, 0x7fefffffffffffff, 0x0010000000000000}
			period := 257
			if mode == 4 {
				period = 1021
			}
			if row%period < len(special) {
				return special[row%period], valid
			}
			if mode == 4 {
				return uint64(0x3ff0000000000000) | h&0x000fffffffffffff, valid
			}

			return h & 0xffefffffffffffff, valid
		}
	case 6:
		switch mode {
		case 0:
			return "constant string longer than twelve bytes", valid
		case 1, 4:
			return [...]string{"", "a", "iceberg", "vortex dictionary entry", "東京", "éclair", "🙂", "a\x00b"}[row%8], valid
		default:
			switch row % 4 {
			case 0:
				return "", valid
			case 1:
				return fmt.Sprintf("s%d", row), valid
			case 2:
				return fmt.Sprintf("row=%05d;hash=%016x;東京;", row, h), valid
			default:
				return fmt.Sprintf("%s:%d:%016x", strings.Repeat("shared long prefix/", 12), row, h), valid
			}
		}
	case 7:
		switch mode {
		case 0:
			return []byte{0, 255, 128, 1, 0, 7}, valid
		case 1, 4:
			return []byte{0, byte(row % 7), 255}, valid
		default:
			b := make([]byte, [...]int{0, 3, 19, 131}[row%4])
			for i := range b {
				b[i] = byte(h>>((i%8)*8)) + byte(i)
			}

			return b, valid
		}
	default:
		panic("unexpected source column")
	}
}

// A tagged run exercises both native and FFI; pure-Go builds retain this oracle.
func TestVortexDefaultWriterOriginalValues(t *testing.T) {
	for fixture, name := range defaultWriterFixtures {
		projections := [][]int{nil}
		for col := range defaultWriterFields {
			projections = append(projections, []int{col})
		}
		for _, cols := range projections {
			projection := "all"
			if cols != nil {
				projection = defaultWriterFields[cols[0]].Name
			}
			for _, backend := range vortex.AvailableBackends() {
				t.Run(name+"/"+string(backend)+"/"+projection, func(t *testing.T) {
					ctx := vortex.WithBackend(t.Context(), backend)
					r, err := vortexFormat{}.Open(ctx, iceio.LocalFS{}, defaultWriterPath(name))
					require.NoError(t, err)
					defer r.Close()
					if cols != nil {
						id := cols[0] + 1
						mapping := iceberg.NameMapping{{Names: []string{projection}, FieldID: &id}}
						_, selected, err := r.PrunedSchema(map[int]struct{}{id: {}}, mapping)
						require.NoError(t, err)
						require.Equal(t, cols, selected)
					}
					rr, err := r.GetRecords(ctx, cols, nil)
					require.NoError(t, err)
					defer rr.Release()
					fields := defaultWriterFields
					indices := []int{0, 1, 2, 3, 4, 5, 6, 7}
					if cols != nil {
						indices = cols
						fields = []arrow.Field{defaultWriterFields[cols[0]]}
						fields[0].Metadata = arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{strconv.Itoa(cols[0] + 1)})
					}
					require.Equal(t, fields, rr.Schema().Fields())
					rows := 0
					for rr.Next() {
						batch := rr.RecordBatch()
						for index, field := range fields {
							col := indices[index]
							values := batch.Column(index)
							for i := range values.Len() {
								row := rows + i
								want, valid := defaultWriterOriginal(fixture, col, row)
								require.Equal(t, valid, values.IsValid(i), "%s row %d validity", field.Name, row)
								if !valid {
									continue
								}
								var got any
								switch a := values.(type) {
								case *array.Boolean:
									got = a.Value(i)
								case *array.Int32:
									got = a.Value(i)
								case *array.Int64:
									got = a.Value(i)
								case *array.Float32:
									got = math.Float32bits(a.Value(i))
								case *array.Float64:
									got = math.Float64bits(a.Value(i))
								case *array.String:
									got = a.Value(i)
								case *array.Binary:
									got = a.Value(i)
								default:
									t.Fatalf("unexpected %s array %T", field.Name, values)
								}
								require.Equal(t, want, got, "%s row %d", field.Name, row)
							}
						}
						rows += int(batch.NumRows())
					}
					require.NoError(t, rr.Err())
					require.Equal(t, defaultWriterRows, rows)
				})
			}
		}
	}
}
