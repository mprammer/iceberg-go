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
	"cmp"
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

type vortexRegistrationColumn struct {
	field     iceberg.NestedField
	physical  arrow.Field
	metrics   bool
	agg       StatsAgg
	update    func(iceberg.Literal)
	partition []*vortexRegistrationPartition
}

type vortexRegistrationPartition struct {
	field iceberg.PartitionField
	void  bool
	seen  bool
	value iceberg.Optional[iceberg.Literal]
}

// VortexRegistrationColumnMapping maps resolved top-level field names to IDs.
// Registration consumes top-level scalars, so a literal column name "a.b" must
// not collide with the flattened path of a nested field.
func VortexRegistrationColumnMapping(sc *iceberg.Schema) map[string]int {
	mapping := make(map[string]int, sc.NumFields())
	for _, field := range sc.Fields() {
		mapping[field.Name] = field.ID
	}

	return mapping
}

// CollectVortexRegistrationStatistics scans the scalar columns needed by the
// metrics plan and partition spec once. Partition membership uses full values
// independently of metrics modes and bounds truncation. Missing physical fields
// have no metrics; initial defaults are used only to prove partition membership.
// When no physical columns are needed, the exact footer count suffices.
func CollectVortexRegistrationStatistics(ctx context.Context, rdr FileReader, sc *iceberg.Schema,
	spec iceberg.PartitionSpec, statsCols map[int]StatisticsCollector, colMapping map[string]int,
) (*DataFileStatistics, map[int]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	r, ok := rdr.(*vortexFileReader)
	if !ok || r == nil || r.backend == nil || sc == nil || r.schema == nil {
		return nil, nil, fmt.Errorf("%w: Vortex registration requires an open Vortex reader and schema", iceberg.ErrInvalidArgument)
	}
	if r.rowCount > math.MaxInt64 {
		return nil, nil, fmt.Errorf("%w: Vortex row count exceeds int64", iceberg.ErrInvalidArgument)
	}
	stats := &DataFileStatistics{
		RecordCount: int64(r.rowCount), ValueCounts: make(map[int]int64),
		NullValueCounts: make(map[int]int64), NanValueCounts: make(map[int]int64),
		ColAggs: make(map[int]StatsAgg),
	}
	columns, partitions, err := planVortexRegistration(r.schema, sc, spec, statsCols, colMapping)
	if err != nil {
		return nil, nil, err
	}
	for _, p := range partitions {
		if !p.void && r.rowCount == 0 {
			return nil, nil, fmt.Errorf("cannot infer partition field %q from an empty Vortex file", p.field.Name)
		}
	}
	if len(columns) > 0 {
		if err := scanVortexRegistration(ctx, r, columns, stats); err != nil {
			return nil, nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	values := make(map[int]any, len(partitions))
	for _, p := range partitions {
		values[p.field.FieldID] = nil
		if p.value.Valid {
			values[p.field.FieldID] = p.value.Val.Any()
		}
	}

	return stats, values, nil
}

func planVortexRegistration(physical *arrow.Schema, sc *iceberg.Schema, spec iceberg.PartitionSpec,
	statsCols map[int]StatisticsCollector, colMapping map[string]int,
) ([]*vortexRegistrationColumn, []*vortexRegistrationPartition, error) {
	topLevel := make(map[int]iceberg.NestedField, sc.NumFields())
	for _, field := range sc.Fields() {
		topLevel[field.ID] = field
	}
	physicalByID := make(map[int]arrow.Field)
	physicalIDs := make([]int, 0, physical.NumFields())
	physicalNames := make(map[string]struct{}, physical.NumFields())
	for _, field := range physical.Fields() {
		if _, duplicate := physicalNames[field.Name]; duplicate {
			return nil, nil, fmt.Errorf("duplicate Vortex column name %q", field.Name)
		}
		physicalNames[field.Name] = struct{}{}
		id, ok := colMapping[field.Name]
		// In-file IDs are authoritative, including when a mapping still contains
		// an old name. A malformed physical ID must never fall back to names.
		if value, exists := field.Metadata.GetValue("PARQUET:field_id"); exists {
			var err error
			id, err = strconv.Atoi(value)
			if err != nil || id <= 0 {
				return nil, nil, fmt.Errorf("invalid Vortex field ID %q for column %q", value, field.Name)
			}
			ok = true
		}
		if !ok {
			continue
		}
		if _, hasID := field.Metadata.GetValue("PARQUET:field_id"); !hasID {
			if _, top := topLevel[id]; !top {
				for _, source := range topLevel {
					if source.Name == field.Name {
						return nil, nil, fmt.Errorf("ambiguous Vortex name mapping for top-level column %q", field.Name)
					}
				}
			}
		}
		if _, duplicate := physicalByID[id]; duplicate {
			return nil, nil, fmt.Errorf("duplicate Vortex field ID %d", id)
		}
		physicalByID[id] = field
		physicalIDs = append(physicalIDs, id)
	}

	byID := make(map[int]*vortexRegistrationColumn)
	for id, plan := range statsCols {
		if plan.Mode.Typ == MetricModeNone {
			continue
		}
		field, exists := topLevel[id]
		phys, present := physicalByID[id]
		if !exists || !present || !vortexRegistrationTypeSupported(field.Type) {
			continue
		}
		if _, ok := vortexRegistrationArrowType(phys.Type); !ok {
			continue
		}
		if err := validateVortexRegistrationType(phys.Type, field.Type); err != nil {
			return nil, nil, fmt.Errorf("vortex metrics field %q: %w", field.Name, err)
		}
		switch plan.Mode.Typ {
		case MetricModeCounts, MetricModeFull:
		case MetricModeTruncate:
			if plan.Mode.Len <= 0 {
				return nil, nil, fmt.Errorf("invalid Vortex metrics truncation length for %q", field.Name)
			}
		default:
			return nil, nil, fmt.Errorf("unsupported Vortex metrics mode %q for %q", plan.Mode.Typ, field.Name)
		}
		column := &vortexRegistrationColumn{field: field, physical: phys, metrics: true}
		if plan.Mode.Typ != MetricModeCounts {
			trunc := 0
			if plan.Mode.Typ == MetricModeTruncate {
				switch field.Type.(type) {
				case iceberg.StringType, iceberg.BinaryType:
					trunc = plan.Mode.Len
				}
			}
			column.agg, column.update = newVortexRegistrationAgg(field.Type.(iceberg.PrimitiveType), trunc)
		}
		byID[id] = column
	}

	partitions := make([]*vortexRegistrationPartition, 0, spec.NumFields())
	partitionIDs := make(map[int]struct{}, spec.NumFields())
	for _, field := range spec.Fields() {
		if _, duplicate := partitionIDs[field.FieldID]; duplicate {
			return nil, nil, fmt.Errorf("duplicate Vortex partition field ID %d", field.FieldID)
		}
		partitionIDs[field.FieldID] = struct{}{}
		p := &vortexRegistrationPartition{field: field}
		partitions = append(partitions, p)
		if field.Transform == nil || (reflect.ValueOf(field.Transform).Kind() == reflect.Pointer && reflect.ValueOf(field.Transform).IsNil()) {
			return nil, nil, fmt.Errorf("vortex partition field %q has no transform", field.Name)
		}
		if _, ok := field.Transform.(iceberg.VoidTransform); ok {
			p.void, p.seen = true, true

			continue
		}
		if _, ok := field.Transform.(*iceberg.VoidTransform); ok {
			p.void, p.seen = true, true

			continue
		}
		if len(field.SourceIDs) != 1 {
			return nil, nil, fmt.Errorf("vortex partition field %q requires one source column", field.Name)
		}
		source, exists := topLevel[field.SourceID()]
		if !exists || !vortexRegistrationTypeSupported(source.Type) {
			return nil, nil, fmt.Errorf("unsupported Vortex partition source %d for field %q: requires a supported top-level scalar", field.SourceID(), field.Name)
		}
		if err := validateVortexRegistrationTransform(field.Transform, source.Type); err != nil {
			return nil, nil, fmt.Errorf("vortex partition field %q: %w", field.Name, err)
		}
		phys, present := physicalByID[source.ID]
		if !present {
			value, err := vortexRegistrationDefault(source)
			if err != nil {
				return nil, nil, fmt.Errorf("vortex partition field %q: %w", field.Name, err)
			}
			// The default proves membership for nonempty files. The caller
			// rejects empty files, which have no tuple to infer.
			if err := p.add(value); err != nil {
				return nil, nil, err
			}

			continue
		}
		if err := validateVortexRegistrationType(phys.Type, source.Type); err != nil {
			return nil, nil, fmt.Errorf("vortex partition field %q: %w", field.Name, err)
		}
		column, exists := byID[source.ID]
		if !exists {
			column = &vortexRegistrationColumn{field: source, physical: phys}
			byID[source.ID] = column
		}
		column.partition = append(column.partition, p)
	}

	// File order makes the projection stable. Scans may return fields in a
	// different order, so the batch loop resolves each column by name.
	columns := make([]*vortexRegistrationColumn, 0, len(byID))
	for _, id := range physicalIDs {
		if column, ok := byID[id]; ok {
			columns = append(columns, column)
		}
	}

	return columns, partitions, nil
}

func scanVortexRegistration(ctx context.Context, r *vortexFileReader, columns []*vortexRegistrationColumn, stats *DataFileStatistics) error {
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.physical.Name
	}
	rr, err := r.scan(ctx, names, nil, nil)
	if err != nil {
		return err
	}
	defer rr.Release()
	var rows int64
	for rr.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch := rr.RecordBatch()
		if batch == nil || batch.NumRows() < 0 || batch.NumRows() > stats.RecordCount-rows {
			return fmt.Errorf("vortex registration row count exceeds footer count %d", stats.RecordCount)
		}
		rows += batch.NumRows()
		for _, column := range columns {
			indices := batch.Schema().FieldIndices(column.physical.Name)
			if len(indices) != 1 {
				return fmt.Errorf("vortex scan did not return exactly one column %q", column.physical.Name)
			}
			values := batch.Column(indices[0])
			if int64(values.Len()) != batch.NumRows() || !arrow.TypeEqual(values.DataType(), column.physical.Type) {
				return fmt.Errorf("vortex scan returned an incompatible array for column %q", column.physical.Name)
			}
			id := column.field.ID
			if column.metrics {
				stats.ValueCounts[id] += batch.NumRows()
				stats.NullValueCounts[id] += int64(values.NullN())
				if vortexRegistrationFloat(column.field.Type) {
					stats.NanValueCounts[id] += 0
				}
			}
			for row := range values.Len() {
				if row%1024 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				var value iceberg.Optional[iceberg.Literal]
				if !values.IsNull(row) {
					value.Valid = true
					value.Val = vortexRegistrationLiteral(values, row)
					if !value.Val.Type().Equals(column.field.Type) {
						value.Val, err = promoteVortexRegistrationLiteral(value.Val, column.field.Type)
						if err != nil {
							return err
						}
					}
					if vortexRegistrationNaN(value.Val) {
						if column.metrics {
							stats.NanValueCounts[id]++
						}
					} else if column.update != nil {
						column.update(value.Val)
					}
				}
				for _, p := range column.partition {
					if err := p.add(value); err != nil {
						return err
					}
				}
			}
		}
	}
	if err := rr.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if rows != stats.RecordCount {
		return fmt.Errorf("vortex registration row count %d does not match footer count %d", rows, stats.RecordCount)
	}
	for _, column := range columns {
		if !column.metrics {
			continue
		}
		// Empty files still have exact zero counts for their selected columns.
		stats.ValueCounts[column.field.ID] += 0
		stats.NullValueCounts[column.field.ID] += 0
		if vortexRegistrationFloat(column.field.Type) {
			stats.NanValueCounts[column.field.ID] += 0
		}
		if column.agg != nil && column.agg.Min() != nil {
			stats.ColAggs[column.field.ID] = column.agg
		}
	}

	return nil
}

func (p *vortexRegistrationPartition) add(value iceberg.Optional[iceberg.Literal]) error {
	transformed := p.field.Transform.Apply(value)
	if value.Valid && (!transformed.Valid || transformed.Val == nil) {
		return fmt.Errorf("cannot transform Vortex partition field %q", p.field.Name)
	}
	// Fixed-width integer truncation can wrap when its mathematical bucket is
	// below the source type's minimum. Such a partition cannot survive schema
	// promotion safely, so do not persist it as verified registration metadata.
	switch p.field.Transform.(type) {
	case iceberg.TruncateTransform, *iceberg.TruncateTransform:
		var overflow bool
		switch source := value.Val.(type) {
		case iceberg.Int32Literal:
			overflow = transformed.Valid && transformed.Val.(iceberg.Int32Literal) > source
		case iceberg.Int64Literal:
			overflow = transformed.Valid && transformed.Val.(iceberg.Int64Literal) > source
		}
		if overflow {
			return fmt.Errorf("vortex partition field %q truncation exceeds source integer range", p.field.Name)
		}
	}
	if !p.seen {
		p.seen = true
		p.value = transformed
		if transformed.Valid {
			p.value.Val = ownVortexRegistrationLiteral(transformed.Val)
		}

		return nil
	}
	if p.value.Valid != transformed.Valid || (transformed.Valid && !equalVortexRegistrationLiteral(p.value.Val, transformed.Val)) {
		return fmt.Errorf("vortex file contains more than one partition value for field %q", p.field.Name)
	}

	return nil
}

func validateVortexRegistrationTransform(transform iceberg.Transform, typ iceberg.Type) error {
	switch transform.(type) {
	case iceberg.IdentityTransform, *iceberg.IdentityTransform,
		iceberg.BucketTransform, *iceberg.BucketTransform,
		iceberg.TruncateTransform, *iceberg.TruncateTransform:
	default:
		return fmt.Errorf("unsupported Vortex partition transform %T", transform)
	}
	if !transform.CanTransform(typ) {
		return fmt.Errorf("partition transform %s cannot transform %s", transform, typ)
	}
	_, err := transform.MarshalText()

	return err
}

func vortexRegistrationTypeSupported(typ iceberg.Type) bool {
	switch typ.(type) {
	case iceberg.BooleanType, iceberg.Int32Type, iceberg.Int64Type, iceberg.Float32Type,
		iceberg.Float64Type, iceberg.StringType, iceberg.BinaryType:
		return true
	}

	return false
}

func vortexRegistrationArrowType(typ arrow.DataType) (iceberg.PrimitiveType, bool) {
	switch typ.ID() {
	case arrow.BOOL:
		return iceberg.PrimitiveTypes.Bool, true
	case arrow.INT32:
		return iceberg.PrimitiveTypes.Int32, true
	case arrow.INT64:
		return iceberg.PrimitiveTypes.Int64, true
	case arrow.FLOAT32:
		return iceberg.PrimitiveTypes.Float32, true
	case arrow.FLOAT64:
		return iceberg.PrimitiveTypes.Float64, true
	case arrow.STRING, arrow.LARGE_STRING:
		return iceberg.PrimitiveTypes.String, true
	case arrow.BINARY, arrow.LARGE_BINARY:
		return iceberg.PrimitiveTypes.Binary, true
	}

	return nil, false
}

func validateVortexRegistrationType(physical arrow.DataType, target iceberg.Type) error {
	source, ok := vortexRegistrationArrowType(physical)
	if !ok {
		return fmt.Errorf("unsupported Vortex physical partition source type %s", physical)
	}
	if source.Equals(target) {
		return nil
	}
	_, err := iceberg.PromoteType(source, target)

	return err
}

func vortexRegistrationLiteral(values arrow.Array, row int) iceberg.Literal {
	switch values := values.(type) {
	case *array.Boolean:
		return iceberg.BoolLiteral(values.Value(row))
	case *array.Int32:
		return iceberg.Int32Literal(values.Value(row))
	case *array.Int64:
		return iceberg.Int64Literal(values.Value(row))
	case *array.Float32:
		return iceberg.Float32Literal(values.Value(row))
	case *array.Float64:
		return iceberg.Float64Literal(values.Value(row))
	case *array.String:
		return iceberg.StringLiteral(values.Value(row))
	case *array.LargeString:
		return iceberg.StringLiteral(values.Value(row))
	case *array.Binary:
		return iceberg.BinaryLiteral(values.Value(row))
	case *array.LargeBinary:
		return iceberg.BinaryLiteral(values.Value(row))
	default:
		panic("Vortex registration array type was not validated")
	}
}

func promoteVortexRegistrationLiteral(value iceberg.Literal, target iceberg.Type) (iceberg.Literal, error) {
	if binary, ok := value.(iceberg.BinaryLiteral); ok {
		if _, stringType := target.(iceberg.StringType); stringType {
			if !utf8.Valid(binary) {
				return nil, fmt.Errorf("%w: cannot promote binary to string: invalid UTF-8", iceberg.ErrBadCast)
			}

			return iceberg.StringLiteral(string(binary)), nil
		}
	}

	return value.To(target)
}

func ownVortexRegistrationLiteral(value iceberg.Literal) iceberg.Literal {
	switch value := value.(type) {
	case iceberg.StringLiteral:
		return iceberg.StringLiteral(strings.Clone(string(value)))
	case iceberg.BinaryLiteral:
		// A valid empty value must stay non-nil: manifests encode nil bytes
		// as null, which is represented separately by Optional.Valid.
		owned := make(iceberg.BinaryLiteral, len(value))
		copy(owned, value)

		return owned
	default:
		return value
	}
}

func vortexRegistrationNaN(value iceberg.Literal) bool {
	switch value := value.(type) {
	case iceberg.Float32Literal:
		return math.IsNaN(float64(value))
	case iceberg.Float64Literal:
		return math.IsNaN(float64(value))
	}

	return false
}

func vortexRegistrationFloat(typ iceberg.Type) bool {
	switch typ.(type) {
	case iceberg.Float32Type, iceberg.Float64Type:
		return true
	}

	return false
}

// Iceberg orders negative zero before positive zero. cmp.Compare (also used by
// the existing literal comparator) treats the two zeros as equal.
func compareVortexRegistrationFloat[T float32 | float64](left, right T) int {
	if left == 0 && right == 0 {
		return cmp.Compare(math.Float64bits(float64(right))>>63, math.Float64bits(float64(left))>>63)
	}

	return cmp.Compare(left, right)
}

func equalVortexRegistrationLiteral(left, right iceberg.Literal) bool {
	switch left := left.(type) {
	case iceberg.Float32Literal:
		return compareVortexRegistrationFloat(float32(left), float32(right.(iceberg.Float32Literal))) == 0
	case iceberg.Float64Literal:
		return compareVortexRegistrationFloat(float64(left), float64(right.(iceberg.Float64Literal))) == 0
	}

	return left.Equals(right)
}

func newVortexRegistrationAgg(typ iceberg.PrimitiveType, trunc int) (StatsAgg, func(iceberg.Literal)) {
	switch typ.(type) {
	case iceberg.BooleanType:
		return typedVortexRegistrationAgg[bool](typ, trunc, nil)
	case iceberg.Int32Type:
		return typedVortexRegistrationAgg[int32](typ, trunc, nil)
	case iceberg.Int64Type:
		return typedVortexRegistrationAgg[int64](typ, trunc, nil)
	case iceberg.Float32Type:
		return typedVortexRegistrationAgg[float32](typ, trunc, compareVortexRegistrationFloat[float32])
	case iceberg.Float64Type:
		return typedVortexRegistrationAgg[float64](typ, trunc, compareVortexRegistrationFloat[float64])
	case iceberg.StringType:
		return typedVortexRegistrationAgg[string](typ, trunc, nil)
	case iceberg.BinaryType:
		return typedVortexRegistrationAgg[[]byte](typ, trunc, nil)
	default:
		panic("Vortex registration metrics type was not validated")
	}
}

func typedVortexRegistrationAgg[T iceberg.LiteralType](typ iceberg.PrimitiveType, trunc int, compare iceberg.Comparator[T]) (StatsAgg, func(iceberg.Literal)) {
	agg := newStatAgg[T](typ, trunc).(*statsAggregator[T])
	if compare != nil {
		agg.cmp = compare
	}

	return agg, func(value iceberg.Literal) {
		v := value.(iceberg.TypedLiteral[T]).Value()
		lower := agg.curMin == nil || agg.cmp(v, agg.curMin.Value()) < 0
		upper := agg.curMax == nil || agg.cmp(v, agg.curMax.Value()) > 0
		if lower || upper {
			owned := ownVortexRegistrationLiteral(value).(iceberg.TypedLiteral[T])
			if lower {
				agg.curMin = owned
			}
			if upper {
				agg.curMax = owned
			}
		}
	}
}

func vortexRegistrationDefault(field iceberg.NestedField) (iceberg.Optional[iceberg.Literal], error) {
	var out iceberg.Optional[iceberg.Literal]
	if field.InitialDefault == nil {
		if field.Required {
			return out, fmt.Errorf("missing required Vortex partition source %q has no initial default", field.Name)
		}

		return out, nil
	}
	return out, fmt.Errorf("%w: missing Vortex partition source %q has a non-null initial default", iceberg.ErrNotImplemented, field.Name)
}
