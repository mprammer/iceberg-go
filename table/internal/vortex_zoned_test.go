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
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/vortex"
	native "github.com/mprammer/vortex-go"
	"github.com/stretchr/testify/require"
)

const (
	zonedFixtureDir  = "testdata/vortex/rust/"
	zonedFixturePath = zonedFixtureDir + "zoned_ints.vortex"
	zonedRows        = 4099
	zonedSize        = 1024
)

var zonedFixtures = []struct {
	name      string
	statsRows []uint64
}{
	{"zoned_ints", []uint64{5}},
	{"zoned_ints_chunked_stats", []uint64{2, 2, 1}},
}

// These formulas are the original canonical Rust input, independent of either
// reader and of the serialized aggregate statistics.
func zonedOriginal(row int) (int32, int64, bool) {
	within := row % zonedSize
	valid := row/zonedSize != 1 && row != 2 && row != 2068 && row != 3101 && row != 4097
	switch row / zonedSize {
	case 0:
		return math.MinInt32 + int32(within), math.MinInt64 + int64(within), valid
	case 1:
		return 0, 0, false
	case 2:
		return 7, 7, valid
	case 3:
		return 1000 + int32(within), 1<<53 + 1 + int64(within), valid
	default:
		return math.MaxInt32 - 2 + int32(within), math.MaxInt64 - 2 + int64(within), valid
	}
}

func TestVortexZonedOriginalValues(t *testing.T) {
	for _, fixture := range zonedFixtures {
		t.Run(fixture.name, func(t *testing.T) {
			checkZonedOriginalValues(t, zonedFixtureDir+fixture.name+".vortex")
		})
	}
}

func checkZonedOriginalValues(t *testing.T, path string) {
	t.Helper()
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			r, err := vortexFormat{}.Open(ctx, iceio.LocalFS{}, path)
			require.NoError(t, err)
			defer r.Close()
			rr, err := r.GetRecords(ctx, nil, nil)
			require.NoError(t, err)
			defer rr.Release()
			require.Equal(t, []arrow.Field{
				{Name: "row_id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "v32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: "v64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, rr.Schema().Fields())
			rows := 0
			for rr.Next() {
				batch := rr.RecordBatch()
				ids := batch.Column(0).(*array.Int64)
				v32 := batch.Column(1).(*array.Int32)
				v64 := batch.Column(2).(*array.Int64)
				for i := range ids.Len() {
					row := rows + i
					want32, want64, valid := zonedOriginal(row)
					require.True(t, ids.IsValid(i))
					require.Equal(t, int64(row), ids.Value(i))
					require.Equal(t, valid, v32.IsValid(i), "v32 row %d", row)
					require.Equal(t, valid, v64.IsValid(i), "v64 row %d", row)
					if valid {
						require.Equal(t, want32, v32.Value(i), "row %d", row)
						require.Equal(t, want64, v64.Value(i), "row %d", row)
					}
				}
				rows += int(batch.NumRows())
			}
			require.NoError(t, rr.Err())
			require.Equal(t, zonedRows, rows)
		})
	}
}

type zonedRead struct{ offset, size int64 }

type zonedReader struct {
	io.ReaderAt
	reads []zonedRead
}

func (r *zonedReader) ReadAt(p []byte, offset int64) (int, error) {
	n, err := r.ReaderAt.ReadAt(p, offset)
	r.reads = append(r.reads, zonedRead{offset, int64(n)})

	return n, err
}

func zonedMatches(row int, pred native.Predicate) bool {
	v32, v64, valid := zonedOriginal(row)
	var value int64
	switch pred.Column {
	case "row_id":
		value, valid = int64(row), true
	case "v32":
		value = int64(v32)
	case "v64":
		value = v64
	}
	if !valid {
		return pred.Op == native.OpNEQ // Iceberg's != can match nulls.
	}
	literal := pred.Value
	switch pred.Op {
	case native.OpEQ:
		return value == literal
	case native.OpNEQ:
		return value != literal
	case native.OpGT:
		return value > literal
	case native.OpGTE:
		return value >= literal
	case native.OpLT:
		return value < literal
	case native.OpLTE:
		return value <= literal
	default:
		panic("unexpected comparison")
	}
}

// Source membership is the oracle: advisory pruning may emit false positives,
// but it must retain every row for which all predicates are true.
func scanZonedFixture(t *testing.T, data []byte, preds ...native.Predicate) (rows, pruned, dataReads int, readBytes int64) {
	t.Helper()
	// An unfiltered public scan identifies data ranges without depending on the
	// reader's private footer structures or its zone statistics implementation.
	calibration := &zonedReader{ReaderAt: bytes.NewReader(data)}
	baseline, err := native.Open(t.Context(), calibration, int64(len(data)))
	require.NoError(t, err)
	calibration.reads = nil
	full, err := baseline.NewRecordReader(t.Context(), native.ScanOptions{})
	require.NoError(t, err)
	for full.Next() {
	}
	require.NoError(t, full.Err())
	full.Release()
	require.NoError(t, baseline.Close())
	dataRanges := calibration.reads

	source := &zonedReader{ReaderAt: bytes.NewReader(data)}
	file, err := native.Open(t.Context(), source, int64(len(data)))
	require.NoError(t, err)
	defer file.Close()
	source.reads = nil
	reader, err := file.NewRecordReader(t.Context(), native.ScanOptions{Columns: []string{"row_id", "v32", "v64"}, Predicates: preds})
	require.NoError(t, err)
	defer reader.Release()
	seen := make([]bool, zonedRows)
	for reader.Next() {
		batch := reader.RecordBatch()
		require.EqualValues(t, 3, batch.NumCols())
		ids := batch.Column(0).(*array.Int64)
		values32 := batch.Column(1).(*array.Int32)
		values64 := batch.Column(2).(*array.Int64)
		for i, row := range ids.Int64Values() {
			require.Equal(t, reader.RowOffset()+int64(i), row, "physical row offset")
			require.GreaterOrEqual(t, row, int64(0))
			require.Less(t, row, int64(zonedRows))
			require.False(t, seen[row], "duplicate row %d", row)
			seen[row] = true
			want32, want64, valid := zonedOriginal(int(row))
			require.Equal(t, valid, values32.IsValid(i), "v32 row %d", row)
			require.Equal(t, valid, values64.IsValid(i), "v64 row %d", row)
			if valid {
				require.Equal(t, want32, values32.Value(i), "row %d", row)
				require.Equal(t, want64, values64.Value(i), "row %d", row)
			}
		}
		rows += int(batch.NumRows())
	}
	require.NoError(t, reader.Err())
	for row := range zonedRows {
		matches := true
		for _, pred := range preds {
			matches = matches && zonedMatches(row, pred)
		}
		if matches {
			require.True(t, seen[row], "pruning lost matching source row %d", row)
		}
	}
	for zone := range (zonedRows + zonedSize - 1) / zonedSize {
		retained := false
		for row := zone * zonedSize; row < min((zone+1)*zonedSize, zonedRows); row++ {
			retained = retained || seen[row]
		}
		if !retained {
			pruned++
		}
	}
	for _, read := range source.reads {
		readBytes += read.size
		for _, segment := range dataRanges {
			if read.offset < segment.offset+segment.size && read.offset+read.size > segment.offset {
				dataReads++

				break
			}
		}
	}

	return
}

func TestVortexZonedNativePruningReads(t *testing.T) {
	for _, fixture := range zonedFixtures {
		t.Run(fixture.name, func(t *testing.T) {
			checkZonedNativePruningReads(t, zonedFixtureDir+fixture.name+".vortex")
		})
	}
}

func checkZonedNativePruningReads(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	fullRows, fullPruned, fullReads, fullBytes := scanZonedFixture(t, data)
	require.Equal(t, zonedRows, fullRows)
	require.Zero(t, fullPruned)
	for repeat := range 2 {
		t.Run(fmt.Sprintf("repeat_%d", repeat), func(t *testing.T) {
			rows, pruned, reads, readBytes := scanZonedFixture(t, data,
				native.Predicate{Column: "row_id", Op: native.OpGTE, Value: 3072}, native.Predicate{Column: "v64", Op: native.OpLTE, Value: 9007199254740993})
			require.Equal(t, zonedSize, rows)
			require.Equal(t, 4, pruned)
			require.Less(t, reads, fullReads)
			require.Less(t, readBytes, fullBytes)
			t.Logf("source rows=%d candidates=%d; zones pruned=%d/5; data reads=%d -> %d; scan bytes=%d -> %d",
				fullRows, rows, pruned, fullReads, reads, fullBytes, readBytes)
		})
	}
	rows, pruned, reads, _ := scanZonedFixture(t, data, native.Predicate{Column: "row_id", Op: native.OpGT, Value: 5000})
	require.Zero(t, rows)
	require.Equal(t, 5, pruned)
	require.Zero(t, reads)
}

func TestVortexZonedNativeIntegerCandidates(t *testing.T) {
	data, err := os.ReadFile(zonedFixturePath)
	require.NoError(t, err)
	for _, col := range []struct {
		name   string
		values []int64
	}{
		{"v32", []int64{math.MinInt32, math.MinInt32 + 1, 7, 1000, math.MaxInt32 - 1, math.MaxInt32}},
		{"v64", []int64{int64(math.MinInt64), int64(math.MinInt64 + 1), int64(7), int64(1 << 53), int64(1<<53 + 1), int64(math.MaxInt64 - 1), int64(math.MaxInt64)}},
	} {
		for _, op := range []native.CompareOp{native.OpEQ, native.OpNEQ, native.OpLT, native.OpLTE, native.OpGT, native.OpGTE} {
			for _, value := range col.values {
				t.Run(fmt.Sprintf("%s/%v/%v", col.name, op, value), func(t *testing.T) {
					scanZonedFixture(t, data, native.Predicate{Column: col.name, Op: op, Value: value})
				})
			}
		}
	}
}
