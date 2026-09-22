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
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

type dateDecimalSource struct {
	Rows    int      `json:"rows"`
	Days    []*int32 `json:"date_days"`
	Amounts []*int64 `json:"amount_unscaled"`
}

func TestVortexDateDecimalRustWriter(t *testing.T) {
	var source dateDecimalSource
	// Independent of the file and both readers: these are the generator's inputs.
	require.NoError(t, json.Unmarshal([]byte(`{"rows":8,"date_days":[-100000,-1,0,1,20000,null,2147483647,-2147483648],"amount_unscaled":[-999999999999999,-1,0,1,999999999999999,null,123456789012345,-123456789012345]}`), &source))
	checkDateDecimalSource(t, "testdata/vortex/rust/date_decimal.vortex", source)
}

// The larger generated corpus covers repeated values and multiple input batches.
func TestVortexDateDecimalGeneratedCorpus(t *testing.T) {
	dir := os.Getenv("VORTEX_TYPES_FIXTURE_DIR")
	if dir == "" {
		t.Skip("set VORTEX_TYPES_FIXTURE_DIR to the generate-types.py output directory")
	}
	for _, name := range []string{"boundary", "repeated", "chunked"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(dir, name+".json"))
			require.NoError(t, err)
			var source dateDecimalSource
			require.NoError(t, json.Unmarshal(data, &source))
			checkDateDecimalSource(t, filepath.Join(dir, name+".vortex"), source)
		})
	}
}

func checkDateDecimalSource(t *testing.T, path string, source dateDecimalSource) {
	t.Helper()
	require.Len(t, source.Days, source.Rows)
	require.Len(t, source.Amounts, source.Rows)
	fields := []arrow.Field{
		{Name: "row_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 15, Scale: 2}, Nullable: true},
	}
	for _, backend := range vortex.AvailableBackends() {
		for _, cols := range [][]int{nil, {1}, {2}} {
			name := "all"
			if cols != nil {
				name = fields[cols[0]].Name
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				ctx := vortex.WithBackend(t.Context(), backend)
				reader, err := vortexFormat{}.Open(ctx, iceio.LocalFS{}, path)
				require.NoError(t, err)
				defer reader.Close()
				if cols != nil {
					id := cols[0] + 1
					mapping := iceberg.NameMapping{{Names: []string{name}, FieldID: &id}}
					_, selected, err := reader.PrunedSchema(map[int]struct{}{id: {}}, mapping)
					require.NoError(t, err)
					require.Equal(t, cols, selected)
				}
				records, err := reader.GetRecords(ctx, cols, nil)
				require.NoError(t, err)
				defer records.Release()
				selected := []int{0, 1, 2}
				if cols != nil {
					selected = cols
				}
				for i, index := range selected {
					want := fields[index]
					if cols != nil {
						want.Metadata = arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{strconv.Itoa(index + 1)})
					}
					require.Equal(t, want, records.Schema().Field(i))
				}
				row := 0
				for records.Next() {
					batch := records.RecordBatch()
					require.LessOrEqual(t, row+int(batch.NumRows()), source.Rows)
					for col, index := range selected {
						values := batch.Column(col)
						for i := range values.Len() {
							actualRow := row + i
							switch index {
							case 0:
								require.Equal(t, int64(actualRow), values.(*array.Int64).Value(i))
							case 1:
								want := source.Days[actualRow]
								require.Equal(t, want == nil, values.IsNull(i), "date row "+strconv.Itoa(actualRow))
								if want != nil {
									require.Equal(t, arrow.Date32(*want), values.(*array.Date32).Value(i))
								}
							case 2:
								want := source.Amounts[actualRow]
								require.Equal(t, want == nil, values.IsNull(i), "decimal row "+strconv.Itoa(actualRow))
								if want != nil {
									require.Equal(t, decimal128.FromI64(*want), values.(*array.Decimal128).Value(i))
								}
							}
						}
					}
					row += int(batch.NumRows())
				}
				require.NoError(t, records.Err())
				require.Equal(t, source.Rows, row)
			})
		}
	}
}
