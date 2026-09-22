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
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

type registrationTestBackend struct {
	schema    *arrow.Schema
	batches   []arrow.RecordBatch
	count     uint64
	scanErr   error
	streamErr error
	stopAfter int
	onNext    func(int)
	scans     int
	names     []string
	released  bool
}

func (b *registrationTestBackend) Close() error                   { return nil }
func (b *registrationTestBackend) Schema() (*arrow.Schema, error) { return b.schema, nil }
func (b *registrationTestBackend) RowCount() (uint64, bool)       { return b.count, true }
func (b *registrationTestBackend) Scan(_ context.Context, names []string, filter *VortexScanFilter) (array.RecordReader, error) {
	if filter != nil {
		panic("registration must scan without a filter")
	}
	b.scans++
	b.names = names
	if b.scanErr != nil {
		return nil, b.scanErr
	}
	fields := make([]arrow.Field, len(names))
	indices := make([]int, len(names))
	for i, name := range names {
		indices[i] = b.schema.FieldIndices(name)[0]
		fields[i] = b.schema.Field(indices[i])
	}
	sc := arrow.NewSchema(fields, nil)
	var projected []arrow.RecordBatch
	for _, batch := range b.batches {
		columns := make([]arrow.Array, len(indices))
		for i, index := range indices {
			columns[i] = batch.Column(index)
		}
		projected = append(projected, array.NewRecordBatch(sc, columns, batch.NumRows()))
	}
	rr, err := array.NewRecordReader(sc, projected)
	for _, batch := range projected {
		batch.Release()
	}
	if err != nil {
		return nil, err
	}

	return &registrationTestRecordReader{RecordReader: rr, backend: b}, nil
}

type registrationTestRecordReader struct {
	array.RecordReader
	backend *registrationTestBackend
	next    int
}

func (r *registrationTestRecordReader) Next() bool {
	if r.backend.stopAfter > 0 && r.next >= r.backend.stopAfter {
		return false
	}
	r.next++
	if r.backend.onNext != nil {
		r.backend.onNext(r.next)
	}

	return r.RecordReader.Next()
}

func (r *registrationTestRecordReader) Err() error {
	if r.backend.streamErr != nil {
		return r.backend.streamErr
	}

	return r.RecordReader.Err()
}

func (r *registrationTestRecordReader) Release() {
	r.backend.released = true
	r.RecordReader.Release()
}

func newRegistrationTestReader(t *testing.T, sc *arrow.Schema, rows ...string) (*vortexFileReader, *registrationTestBackend) {
	t.Helper()
	b := &registrationTestBackend{schema: sc}
	for _, data := range rows {
		batch, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(data))
		require.NoError(t, err)
		b.batches = append(b.batches, batch)
		b.count += uint64(batch.NumRows())
		t.Cleanup(batch.Release)
	}

	return &vortexFileReader{schema: sc, backend: b, rowCount: b.count}, b
}

func registrationTestSchema(fields ...iceberg.NestedField) *iceberg.Schema {
	return iceberg.NewSchema(0, fields...)
}

func registrationTestPlan(sc *iceberg.Schema, mode MetricsMode) (map[int]StatisticsCollector, map[string]int) {
	plan := make(map[int]StatisticsCollector)
	mapping := make(map[string]int)
	for _, field := range sc.Fields() {
		primitive, _ := field.Type.(iceberg.PrimitiveType)
		plan[field.ID] = StatisticsCollector{FieldID: field.ID, IcebergTyp: primitive, ColName: field.Name, Mode: mode}
		mapping[field.Name] = field.ID
	}

	return plan, mapping
}

func registrationTestPartition(source int, transform iceberg.Transform) iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{source}, FieldID: 1000, Name: "part", Transform: transform})
}

func TestVortexRegistrationScalarMetrics(t *testing.T) {
	sc := registrationTestSchema(
		iceberg.NestedField{ID: 1, Name: "flag", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "small", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 3, Name: "large", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 4, Name: "single", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 5, Name: "double", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 6, Name: "text", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 7, Name: "bytes", Type: iceberg.PrimitiveTypes.Binary},
	)
	physical := arrow.NewSchema([]arrow.Field{
		{Name: "flag", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "small", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "large", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "single", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "double", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bytes", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
	r, backend := newRegistrationTestReader(t, physical,
		`[{"flag":true,"small":4,"large":9000000000,"single":3.5,"double":-8.25,"text":"zebra","bytes":"eg=="},
		  {"flag":null,"small":null,"large":null,"single":null,"double":null,"text":null,"bytes":null}]`,
		`[{"flag":false,"small":-3,"large":-7000000000,"single":-2.5,"double":5.75,"text":"apple","bytes":"YQ=="}]`)
	plan, mapping := registrationTestPlan(sc, MetricsMode{Typ: MetricModeFull})
	stats, partition, err := CollectVortexRegistrationStatistics(t.Context(), r, sc, *iceberg.UnpartitionedSpec, plan, mapping)
	require.NoError(t, err)
	require.Empty(t, partition)
	require.EqualValues(t, 3, stats.RecordCount)
	require.Equal(t, 1, backend.scans)
	require.True(t, backend.released)
	require.Empty(t, stats.ColSizes)
	require.Empty(t, stats.SplitOffsets)
	expected := []struct{ low, high iceberg.Literal }{
		{iceberg.BoolLiteral(false), iceberg.BoolLiteral(true)},
		{iceberg.Int32Literal(-3), iceberg.Int32Literal(4)},
		{iceberg.Int64Literal(-7000000000), iceberg.Int64Literal(9000000000)},
		{iceberg.Float32Literal(-2.5), iceberg.Float32Literal(3.5)},
		{iceberg.Float64Literal(-8.25), iceberg.Float64Literal(5.75)},
		{iceberg.StringLiteral("apple"), iceberg.StringLiteral("zebra")},
		{iceberg.BinaryLiteral([]byte("a")), iceberg.BinaryLiteral([]byte("z"))},
	}
	for i, bounds := range expected {
		id := i + 1
		require.EqualValues(t, 3, stats.ValueCounts[id])
		require.EqualValues(t, 1, stats.NullValueCounts[id])
		low, err := stats.ColAggs[id].MinAsBytes()
		require.NoError(t, err)
		high, err := stats.ColAggs[id].MaxAsBytes()
		require.NoError(t, err)
		wantLow, err := bounds.low.MarshalBinary()
		require.NoError(t, err)
		wantHigh, err := bounds.high.MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, wantLow, low)
		require.Equal(t, wantHigh, high)
	}
	require.Equal(t, map[int]int64{4: 0, 5: 0}, stats.NanValueCounts)
}

func TestVortexRegistrationMetricsPlanAndPartition(t *testing.T) {
	physical := arrow.NewSchema([]arrow.Field{
		{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "counted", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "ignored", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	sc := registrationTestSchema(
		iceberg.NestedField{ID: 1, Name: "key", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "counted", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 3, Name: "ignored", Type: iceberg.PrimitiveTypes.Int32},
	)
	for _, mode := range []MetricModeType{MetricModeNone, MetricModeCounts, MetricModeFull, MetricModeTruncate} {
		t.Run(string(mode), func(t *testing.T) {
			r, b := newRegistrationTestReader(t, physical, `[{"key":"abc","counted":7,"ignored":3}]`, `[{"key":"abd","counted":null,"ignored":8}]`)
			plan, mapping := registrationTestPlan(sc, MetricsMode{Typ: MetricModeNone})
			plan[1] = StatisticsCollector{Mode: MetricsMode{Typ: mode, Len: 1}}
			plan[2] = StatisticsCollector{Mode: MetricsMode{Typ: MetricModeCounts}}
			stats, part, err := CollectVortexRegistrationStatistics(t.Context(), r, sc,
				registrationTestPartition(1, iceberg.TruncateTransform{Width: 2}), plan, mapping)
			require.NoError(t, err)
			require.Equal(t, map[int]any{1000: "ab"}, part)
			require.Equal(t, []string{"key", "counted"}, b.names)
			require.NotContains(t, stats.ValueCounts, 3)
			require.NotContains(t, stats.ColAggs, 2)
			require.EqualValues(t, 1, stats.NullValueCounts[2])
			if mode == MetricModeNone {
				require.NotContains(t, stats.ValueCounts, 1)
			} else {
				require.EqualValues(t, 2, stats.ValueCounts[1])
			}
			if mode == MetricModeTruncate {
				low, err := stats.ColAggs[1].MinAsBytes()
				require.NoError(t, err)
				high, err := stats.ColAggs[1].MaxAsBytes()
				require.NoError(t, err)
				require.Equal(t, "a", string(low))
				require.Equal(t, "b", string(high))
			}
			_, _, err = CollectVortexRegistrationStatistics(t.Context(), r, sc,
				registrationTestPartition(1, iceberg.IdentityTransform{}), plan, mapping)
			require.ErrorContains(t, err, "more than one partition value")
		})
	}
}

func TestVortexRegistrationFloatBounds(t *testing.T) {
	for _, promote := range []bool{false, true} {
		t.Run(strconv.FormatBool(promote), func(t *testing.T) {
			physical := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Float32, Nullable: true}}, nil)
			target := iceberg.PrimitiveTypes.Float32
			if promote {
				target = iceberg.PrimitiveTypes.Float64
			}
			sc := registrationTestSchema(iceberg.NestedField{ID: 1, Name: "value", Type: target})
			builder := array.NewFloat32Builder(memory.DefaultAllocator)
			builder.AppendValues([]float32{0, float32(math.NaN()), float32(math.Copysign(0, -1)), 0}, []bool{true, true, true, false})
			values := builder.NewArray()
			builder.Release()
			defer values.Release()
			batch := array.NewRecordBatch(physical, []arrow.Array{values}, 4)
			defer batch.Release()
			b := &registrationTestBackend{schema: physical, batches: []arrow.RecordBatch{batch}, count: 4}
			r := &vortexFileReader{schema: physical, backend: b, rowCount: 4}
			plan, mapping := registrationTestPlan(sc, MetricsMode{Typ: MetricModeFull})
			stats, _, err := CollectVortexRegistrationStatistics(t.Context(), r, sc, *iceberg.UnpartitionedSpec, plan, mapping)
			require.NoError(t, err)
			require.EqualValues(t, 1, stats.NanValueCounts[1])
			require.EqualValues(t, 1, stats.NullValueCounts[1])
			low, err := stats.ColAggs[1].MinAsBytes()
			require.NoError(t, err)
			high, err := stats.ColAggs[1].MaxAsBytes()
			require.NoError(t, err)
			if promote {
				require.Equal(t, uint64(1<<63), binary.LittleEndian.Uint64(low))
				require.Equal(t, uint64(0), binary.LittleEndian.Uint64(high))
			} else {
				require.Equal(t, uint32(1<<31), binary.LittleEndian.Uint32(low))
				require.Equal(t, uint32(0), binary.LittleEndian.Uint32(high))
			}
			_, _, err = CollectVortexRegistrationStatistics(t.Context(), r, sc, registrationTestPartition(1, iceberg.IdentityTransform{}), plan, mapping)
			require.ErrorContains(t, err, "more than one partition value")
		})
	}
}

func TestVortexRegistrationNoBoundsForNullOrNaN(t *testing.T) {
	for _, null := range []bool{false, true} {
		physical := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}, nil)
		sc := registrationTestSchema(iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Float64})
		builder := array.NewFloat64Builder(memory.DefaultAllocator)
		builder.AppendValues([]float64{math.NaN(), math.NaN()}, []bool{!null, !null})
		values := builder.NewArray()
		builder.Release()
		batch := array.NewRecordBatch(physical, []arrow.Array{values}, 2)
		values.Release()
		t.Cleanup(batch.Release)
		b := &registrationTestBackend{schema: physical, batches: []arrow.RecordBatch{batch}, count: 2}
		r := &vortexFileReader{schema: physical, backend: b, rowCount: 2}
		plan, mapping := registrationTestPlan(sc, MetricsMode{Typ: MetricModeFull})
		stats, part, err := CollectVortexRegistrationStatistics(t.Context(), r, sc, registrationTestPartition(1, iceberg.IdentityTransform{}), plan, mapping)
		require.NoError(t, err)
		require.Empty(t, stats.ColAggs)
		if null {
			require.Nil(t, part[1000])
			require.EqualValues(t, 2, stats.NullValueCounts[1])
			require.Zero(t, stats.NanValueCounts[1])
		} else {
			require.True(t, math.IsNaN(part[1000].(float64)))
			require.EqualValues(t, 2, stats.NanValueCounts[1])
			require.Zero(t, stats.NullValueCounts[1])
		}
	}
}

func TestVortexRegistrationPartitionProof(t *testing.T) {
	physical := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	sc := registrationTestSchema(iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int32})
	plan, mapping := registrationTestPlan(sc, MetricsMode{Typ: MetricModeNone})
	tests := []struct {
		name      string
		rows      []string
		transform iceberg.Transform
		want      any
		err       string
	}{
		{"same across batches", []string{`[{"value":4}]`, `[{"value":4}]`}, iceberg.IdentityTransform{}, int32(4), ""},
		{"mixed batches", []string{`[{"value":4}]`, `[{"value":5}]`}, iceberg.IdentityTransform{}, nil, "more than one"},
		{"null then value", []string{`[{"value":null}]`, `[{"value":4}]`}, iceberg.IdentityTransform{}, nil, "more than one"},
		{"value then null", []string{`[{"value":4}]`, `[{"value":null}]`}, iceberg.IdentityTransform{}, nil, "more than one"},
		{"all null", []string{`[{"value":null}]`, `[{"value":null}]`}, iceberg.IdentityTransform{}, nil, ""},
		{"truncate same", []string{`[{"value":11}]`, `[{"value":19}]`}, iceberg.TruncateTransform{Width: 10}, int32(10), ""},
		{"large truncate overflow", []string{`[{"value":1}]`, `[{"value":2}]`}, iceberg.TruncateTransform{Width: math.MaxInt32}, nil, "exceeds source integer range"},
		{"truncate below int32 minimum", []string{`[{"value":-2147483648}]`}, iceberg.TruncateTransform{Width: 10}, nil, "exceeds source integer range"},
		{"large truncate negative", []string{`[{"value":-1}]`, `[{"value":-2}]`}, iceberg.TruncateTransform{Width: math.MaxInt32}, int32(-math.MaxInt32), ""},
		{"bucket collision", []string{`[{"value":1}]`, `[{"value":999}]`}, iceberg.BucketTransform{NumBuckets: 1}, int32(0), ""},
		{"invalid bucket", []string{`[{"value":1}]`}, iceberg.BucketTransform{NumBuckets: 0}, nil, "numBuckets"},
		{"invalid truncate", []string{`[{"value":1}]`}, iceberg.TruncateTransform{Width: 0}, nil, "width"},
		{"unsupported transform", []string{`[{"value":1}]`}, iceberg.YearTransform{}, nil, "unsupported"},
		{"nil transform", []string{`[{"value":1}]`}, nil, nil, "has no transform"},
		{"nil transform pointer", []string{`[{"value":1}]`}, (*iceberg.IdentityTransform)(nil), nil, "has no transform"},
		{"empty identity", nil, iceberg.IdentityTransform{}, nil, "empty Vortex"},
		{"empty void", nil, iceberg.VoidTransform{}, nil, ""},
		{"void varied", []string{`[{"value":1},{"value":null},{"value":999}]`}, iceberg.VoidTransform{}, nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := newRegistrationTestReader(t, physical, tt.rows...)
			stats, partition, err := CollectVortexRegistrationStatistics(t.Context(), r, sc, registrationTestPartition(1, tt.transform), plan, mapping)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)

				return
			}
			require.NoError(t, err)
			require.Equal(t, map[int]any{1000: tt.want}, partition)
			require.Empty(t, stats.ValueCounts)
		})
	}
}

func TestVortexRegistrationDefaults(t *testing.T) {
	physical := arrow.NewSchema([]arrow.Field{{Name: "other", Type: arrow.PrimitiveTypes.Int32}}, nil)
	for _, tt := range []struct {
		name     string
		typ      iceberg.Type
		initial  any
		write    any
		required bool
		want     any
		err      string
	}{
		{"optional null", iceberg.PrimitiveTypes.Int32, nil, int32(8), false, nil, ""},
		{"initial before write", iceberg.PrimitiveTypes.Int32, int32(7), int32(8), false, int32(7), ""},
		{"required initial", iceberg.PrimitiveTypes.Int64, int64(11), nil, true, int64(11), ""},
		{"required missing", iceberg.PrimitiveTypes.Int32, nil, int32(8), true, nil, "no initial default"},
		{"json float integer", iceberg.PrimitiveTypes.Int32, float64(7), nil, false, int32(7), ""},
		{"json number", iceberg.PrimitiveTypes.Int64, json.Number("9223372036854775806"), nil, false, int64(9223372036854775806), ""},
		{"binary hex", iceberg.PrimitiveTypes.Binary, "6162", nil, false, []byte("ab"), ""},
		{"invalid default", iceberg.PrimitiveTypes.Int32, struct{}{}, nil, false, nil, "unsupported initial default"},
		{"fractional integer", iceberg.PrimitiveTypes.Int32, 1.5, nil, false, nil, "unsupported initial default"},
		{"overflow integer", iceberg.PrimitiveTypes.Int32, int64(math.MaxInt64), nil, false, nil, "unsupported initial default"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sc := registrationTestSchema(iceberg.NestedField{ID: 1, Name: "value", Type: tt.typ, Required: tt.required, InitialDefault: tt.initial, WriteDefault: tt.write})
			r, b := newRegistrationTestReader(t, physical, `[{"other":1}]`)
			plan, mapping := registrationTestPlan(sc, MetricsMode{Typ: MetricModeFull})
			stats, part, err := CollectVortexRegistrationStatistics(t.Context(), r, sc, registrationTestPartition(1, iceberg.IdentityTransform{}), plan, mapping)
			if tt.initial != nil {
				require.ErrorIs(t, err, iceberg.ErrNotImplemented)

				return
			}
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)

				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, part[1000])
			require.Empty(t, stats.ValueCounts)
			require.Empty(t, stats.NullValueCounts)
			require.Zero(t, b.scans)
			r.rowCount = 0
			_, _, err = CollectVortexRegistrationStatistics(t.Context(), r, sc, registrationTestPartition(1, iceberg.IdentityTransform{}), plan, mapping)
			require.ErrorContains(t, err, "empty Vortex")
		})
	}
}

func TestVortexRegistrationFieldIDsAndPromotion(t *testing.T) {
	physical := arrow.NewSchema([]arrow.Field{
		{Name: "old", Type: arrow.PrimitiveTypes.Int32, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"7"})},
	}, nil)
	sc := registrationTestSchema(iceberg.NestedField{ID: 7, Name: "renamed", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 99, Name: "old", Type: iceberg.PrimitiveTypes.Int32})
	r, _ := newRegistrationTestReader(t, physical, `[{"old":2147483647}]`)
	plan, _ := registrationTestPlan(sc, MetricsMode{Typ: MetricModeFull})
	stats, part, err := CollectVortexRegistrationStatistics(t.Context(), r, sc, registrationTestPartition(7, iceberg.IdentityTransform{}), plan, map[string]int{"old": 99})
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt32), part[1000])
	require.NotContains(t, stats.ValueCounts, 99)
	low, err := stats.ColAggs[7].MinAsBytes()
	require.NoError(t, err)
	require.Len(t, low, 8)
	require.Equal(t, uint64(math.MaxInt32), binary.LittleEndian.Uint64(low))
}

func TestVortexRegistrationErrorsAndCancellation(t *testing.T) {
	physical := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	sc := registrationTestSchema(iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int32})
	plan, mapping := registrationTestPlan(sc, MetricsMode{Typ: MetricModeFull})
	boom := errors.New("test reader error")
	for _, tt := range []struct {
		name string
		edit func(*vortexFileReader, *registrationTestBackend, context.CancelFunc)
		is   error
		err  string
	}{
		{"open scan error", func(_ *vortexFileReader, b *registrationTestBackend, _ context.CancelFunc) { b.scanErr = boom }, boom, ""},
		{"stream error", func(_ *vortexFileReader, b *registrationTestBackend, _ context.CancelFunc) {
			b.streamErr, b.stopAfter = boom, 1
		}, boom, ""},
		{"too few rows", func(r *vortexFileReader, _ *registrationTestBackend, _ context.CancelFunc) { r.rowCount = 3 }, nil, "does not match"},
		{"too many rows", func(r *vortexFileReader, _ *registrationTestBackend, _ context.CancelFunc) { r.rowCount = 1 }, nil, "exceeds footer"},
		{"already canceled", func(_ *vortexFileReader, _ *registrationTestBackend, cancel context.CancelFunc) { cancel() }, context.Canceled, ""},
		{"cancel between batches", func(_ *vortexFileReader, b *registrationTestBackend, cancel context.CancelFunc) {
			b.onNext = func(n int) {
				if n == 2 {
					cancel()
				}
			}
		}, context.Canceled, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, b := newRegistrationTestReader(t, physical, `[{"value":1}]`, `[{"value":2}]`)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			tt.edit(r, b, cancel)
			stats, part, err := CollectVortexRegistrationStatistics(ctx, r, sc, *iceberg.UnpartitionedSpec, plan, mapping)
			if tt.is != nil {
				require.ErrorIs(t, err, tt.is)
			} else {
				require.ErrorContains(t, err, tt.err)
			}
			require.Nil(t, stats)
			require.Nil(t, part)
			if b.scans > 0 && b.scanErr == nil {
				require.True(t, b.released)
			}
		})
	}
}

func TestVortexRegistrationRetainsOwnedBoundsAndPartition(t *testing.T) {
	physical := arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}, {Name: "bytes", Type: arrow.BinaryTypes.Binary}}, nil)
	sc := registrationTestSchema(iceberg.NestedField{ID: 1, Name: "text", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "bytes", Type: iceberg.PrimitiveTypes.Binary})
	r, b := newRegistrationTestReader(t, physical, `[{"text":"owned","bytes":"b3duZWQ="}]`)
	plan, mapping := registrationTestPlan(sc, MetricsMode{Typ: MetricModeFull})
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "text_part", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "binary_part", Transform: iceberg.IdentityTransform{}},
	)
	stats, part, err := CollectVortexRegistrationStatistics(t.Context(), r, sc, spec, plan, mapping)
	require.NoError(t, err)
	// Simulate allocator reuse after the scan releases its Arrow references.
	for _, column := range b.batches[0].Columns() {
		data := column.Data().Buffers()[2].Bytes()
		for i := range data {
			data[i] = 'x'
		}
	}
	require.Equal(t, "owned", part[1000])
	require.Equal(t, []byte("owned"), part[1001])
	for _, id := range []int{1, 2} {
		low, err := stats.ColAggs[id].MinAsBytes()
		require.NoError(t, err)
		high, err := stats.ColAggs[id].MaxAsBytes()
		require.NoError(t, err)
		require.Equal(t, "owned", string(low))
		require.Equal(t, "owned", string(high))
	}
}

func TestVortexRegistrationUnsupportedAndAmbiguousFields(t *testing.T) {
	for _, tt := range []struct {
		name     string
		physical arrow.Field
		field    iceberg.NestedField
		mapping  map[string]int
		err      string
	}{
		{"date omitted", arrow.Field{Name: "value", Type: arrow.FixedWidthTypes.Date32}, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Date}, map[string]int{"value": 1}, ""},
		{"nested omitted", arrow.Field{Name: "value", Type: arrow.StructOf(arrow.Field{Name: "child", Type: arrow.PrimitiveTypes.Int32})}, iceberg.NestedField{ID: 1, Name: "value", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 2, Name: "child", Type: iceberg.PrimitiveTypes.Int32}}}}, map[string]int{"value": 1}, ""},
		{"bad physical ID", arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"bad"})}, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int32}, map[string]int{"value": 1}, "invalid Vortex field ID"},
		{"ambiguous dotted name", arrow.Field{Name: "a.b", Type: arrow.PrimitiveTypes.Int32}, iceberg.NestedField{ID: 1, Name: "a.b", Type: iceberg.PrimitiveTypes.Int32}, map[string]int{"a.b": 2}, "ambiguous Vortex name mapping"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			physical := arrow.NewSchema([]arrow.Field{tt.physical}, nil)
			sc := registrationTestSchema(tt.field)
			r, b := newRegistrationTestReader(t, physical)
			r.rowCount = 1
			plan, _ := registrationTestPlan(sc, MetricsMode{Typ: MetricModeFull})
			stats, _, err := CollectVortexRegistrationStatistics(t.Context(), r, sc, *iceberg.UnpartitionedSpec, plan, tt.mapping)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)

				return
			}
			require.NoError(t, err)
			require.Empty(t, stats.ValueCounts)
			require.Zero(t, b.scans)
			_, _, err = CollectVortexRegistrationStatistics(t.Context(), r, sc, registrationTestPartition(1, iceberg.IdentityTransform{}), plan, tt.mapping)
			require.ErrorContains(t, err, "unsupported Vortex partition source")
		})
	}
}

func TestVortexRegistrationAndProjectionPreferPhysicalID(t *testing.T) {
	physical := arrow.NewSchema([]arrow.Field{{Name: "reused", Type: arrow.PrimitiveTypes.Int32, Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"7"})}}, nil)
	r, _ := newRegistrationTestReader(t, physical, `[{"reused":42}]`)
	sc := registrationTestSchema(iceberg.NestedField{ID: 7, Name: "renamed", Type: iceberg.PrimitiveTypes.Int32}, iceberg.NestedField{ID: 99, Name: "reused", Type: iceberg.PrimitiveTypes.Int32})
	mapping := sc.NameMapping()
	projected, indices, err := r.PrunedSchema(map[int]struct{}{7: {}}, mapping)
	require.NoError(t, err)
	require.Equal(t, []int{0}, indices)
	require.Equal(t, "reused", projected.Field(0).Name)
	id, ok := projected.Field(0).Metadata.GetValue("PARQUET:field_id")
	require.True(t, ok)
	require.Equal(t, "7", id)
	projected, indices, err = r.PrunedSchema(map[int]struct{}{99: {}}, mapping)
	require.NoError(t, err)
	require.Empty(t, indices)
	require.Zero(t, projected.NumFields())
	plan, colMapping := registrationTestPlan(sc, MetricsMode{Typ: MetricModeFull})
	stats, _, err := CollectVortexRegistrationStatistics(t.Context(), r, sc, *iceberg.UnpartitionedSpec, plan, colMapping)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.ValueCounts[7])
	require.NotContains(t, stats.ValueCounts, 99)
}

func TestVortexRegistrationStringBinaryPromotion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		physical arrow.DataType
		target   iceberg.PrimitiveType
		row      string
		want     any
		reject   bool
	}{
		{"binary_to_string", arrow.BinaryTypes.Binary, iceberg.PrimitiveTypes.String, `[{"value":"Y2Fmw6k="}]`, "café", false},
		{"string_to_binary", arrow.BinaryTypes.String, iceberg.PrimitiveTypes.Binary, `[{"value":"café"}]`, []byte("café"), false},
		{"invalid_utf8", arrow.BinaryTypes.Binary, iceberg.PrimitiveTypes.String, `[{"value":"/w=="}]`, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			physical := arrow.NewSchema([]arrow.Field{{Name: "value", Type: tc.physical}}, nil)
			sc := registrationTestSchema(iceberg.NestedField{ID: 1, Name: "value", Type: tc.target})
			for _, mode := range []MetricModeType{MetricModeNone, MetricModeCounts, MetricModeFull} {
				r, _ := newRegistrationTestReader(t, physical, tc.row, tc.row)
				plan, mapping := registrationTestPlan(sc, MetricsMode{Typ: mode})
				stats, part, err := CollectVortexRegistrationStatistics(t.Context(), r, sc, registrationTestPartition(1, iceberg.IdentityTransform{}), plan, mapping)
				if tc.reject {
					require.Error(t, err)

					continue
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, part[1000])
				if mode != MetricModeNone {
					require.EqualValues(t, 2, stats.ValueCounts[1])
				}
				if mode == MetricModeFull {
					lower, err := stats.ColAggs[1].MinAsBytes()
					require.NoError(t, err)
					upper, err := stats.ColAggs[1].MaxAsBytes()
					require.NoError(t, err)
					require.Equal(t, []byte("café"), lower)
					require.Equal(t, lower, upper)
				}
			}
		})
	}
}

func TestVortexRegistrationDottedAlias(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(strconv.FormatBool(reverse), func(t *testing.T) {
			physicalFields := []arrow.Field{
				{Name: "a.b", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: "a", Type: arrow.StructOf(arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int32, Nullable: true}), Nullable: true},
			}
			nested := iceberg.NestedField{ID: 2, Name: "a", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 3, Name: "b", Type: iceberg.PrimitiveTypes.Int32}}}}
			resolvedFields := []iceberg.NestedField{{ID: 1, Name: "a.b", Type: iceberg.PrimitiveTypes.Int32}, nested}
			if reverse {
				physicalFields[0], physicalFields[1] = physicalFields[1], physicalFields[0]
				resolvedFields[0], resolvedFields[1] = resolvedFields[1], resolvedFields[0]
			}
			physical := arrow.NewSchema(physicalFields, nil)
			sc := registrationTestSchema(iceberg.NestedField{ID: 1, Name: "renamed", Type: iceberg.PrimitiveTypes.Int32}, nested)
			mapping := sc.NameMapping()
			mapping[0].Names = append(mapping[0].Names, "a.b")
			r, _ := newRegistrationTestReader(t, physical, `[{"a.b":11,"a":{"b":99}},{"a.b":12,"a":{"b":99}}]`)
			projected, _, err := r.PrunedSchema(map[int]struct{}{1: {}, 2: {}, 3: {}}, mapping)
			require.NoError(t, err)
			dotted := projected.FieldIndices("a.b")
			require.Len(t, dotted, 1)
			id, ok := projected.Field(dotted[0]).Metadata.GetValue("PARQUET:field_id")
			require.True(t, ok)
			require.Equal(t, "1", id)
			paths := VortexRegistrationColumnMapping(registrationTestSchema(resolvedFields...))
			require.Equal(t, map[string]int{"a.b": 1, "a": 2}, paths)
			plan, _ := registrationTestPlan(sc, MetricsMode{Typ: MetricModeFull})
			stats, _, err := CollectVortexRegistrationStatistics(t.Context(), r, sc, *iceberg.UnpartitionedSpec, plan, paths)
			require.NoError(t, err)
			require.EqualValues(t, 2, stats.ValueCounts[1])
			require.NotContains(t, stats.ValueCounts, 3)
			lower, err := stats.ColAggs[1].MinAsBytes()
			require.NoError(t, err)
			require.Equal(t, uint32(11), binary.LittleEndian.Uint32(lower))
			_, _, err = CollectVortexRegistrationStatistics(t.Context(), r, sc, registrationTestPartition(1, iceberg.IdentityTransform{}), plan, paths)
			require.ErrorContains(t, err, "more than one partition")
		})
	}
}
