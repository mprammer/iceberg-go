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
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	native "github.com/mprammer/vortex-go"
	"github.com/stretchr/testify/require"
)

func TestVortexListElementMappingPreservesPhysicalSchema(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(arrow.Field) arrow.DataType
	}{
		{"list", func(f arrow.Field) arrow.DataType { return arrow.ListOfField(f) }},
		{"large_list", func(f arrow.Field) arrow.DataType { return arrow.LargeListOfField(f) }},
		{"fixed_list", func(f arrow.Field) arrow.DataType { return arrow.FixedSizeListOfField(3, f) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parentID, elementID := 1, 2
			element := arrow.Field{
				Name: "physical_item", Type: arrow.BinaryTypes.StringView,
				Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{"source": "element"}),
			}
			parent := arrow.Field{
				Name: "labels", Type: tc.make(element), Nullable: true,
				Metadata: arrow.MetadataFrom(map[string]string{"source": "parent"}),
			}
			metadata := arrow.MetadataFrom(map[string]string{"source": "schema"})
			reader := &vortexFileReader{schema: normalizeViewSchema(arrow.NewSchema([]arrow.Field{parent}, &metadata))}
			mapping := iceberg.NameMapping{{
				Names: []string{"labels"}, FieldID: &parentID,
				Fields: []iceberg.MappedField{{Names: []string{"element"}, FieldID: &elementID}},
			}}

			out, cols, err := reader.PrunedSchema(map[int]struct{}{elementID: {}}, mapping)
			require.NoError(t, err)
			require.Equal(t, []int{0}, cols, "selecting an element must retain its top-level list")
			require.Equal(t, metadata, out.Metadata())
			got := out.Field(0)
			require.True(t, got.Nullable)
			requireVortexFieldMetadata(t, got.Metadata, parentID, "parent")
			require.Equal(t, parent.Type.ID(), got.Type.ID())
			child := got.Type.(arrow.NestedType).Fields()[0]
			require.Equal(t, "physical_item", child.Name)
			require.False(t, child.Nullable)
			require.Equal(t, arrow.STRING, child.Type.ID())
			requireVortexFieldMetadata(t, child.Metadata, elementID, "element")
			if fixed, ok := got.Type.(*arrow.FixedSizeListType); ok {
				require.EqualValues(t, 3, fixed.Len())
			}
		})
	}
}

func TestVortexMapProjectionPreservesChildIDsAndMetadata(t *testing.T) {
	parentID, keyID, valueID, elementID := 1, 2, 3, 4
	key := arrow.Field{
		Name: "key", Type: arrow.BinaryTypes.StringView,
		Metadata: arrow.MetadataFrom(map[string]string{"source": "key"}),
	}
	element := arrow.Field{
		Name: "item", Type: arrow.BinaryTypes.BinaryView,
		Metadata: arrow.MetadataFrom(map[string]string{"source": "element"}),
	}
	value := arrow.Field{
		Name: "value", Type: arrow.ListOfField(element), Nullable: false,
		Metadata: arrow.MetadataFrom(map[string]string{"source": "value"}),
	}
	dtype := arrow.MapOfFields(key, value)
	dtype.KeysSorted = true
	parent := arrow.Field{
		Name: "attributes", Type: dtype, Nullable: true,
		Metadata: arrow.MetadataFrom(map[string]string{"source": "parent"}),
	}
	reader := &vortexFileReader{schema: normalizeViewSchema(arrow.NewSchema([]arrow.Field{parent}, nil))}
	mapping := iceberg.NameMapping{{
		Names: []string{"attributes"}, FieldID: &parentID,
		Fields: []iceberg.MappedField{
			{Names: []string{"key"}, FieldID: &keyID},
			{
				Names: []string{"value"}, FieldID: &valueID,
				Fields: []iceberg.MappedField{{Names: []string{"element"}, FieldID: &elementID}},
			},
		},
	}}

	out, cols, err := reader.PrunedSchema(map[int]struct{}{elementID: {}}, mapping)
	require.NoError(t, err)
	require.Equal(t, []int{0}, cols)
	require.True(t, out.Field(0).Nullable)
	requireVortexFieldMetadata(t, out.Field(0).Metadata, parentID, "parent")
	got := out.Field(0).Type.(*arrow.MapType)
	require.True(t, got.KeysSorted)
	requireVortexFieldMetadata(t, got.KeyField().Metadata, keyID, "key")
	require.Equal(t, arrow.STRING, got.KeyType().ID())
	require.False(t, got.KeyField().Nullable)
	requireVortexFieldMetadata(t, got.ItemField().Metadata, valueID, "value")
	require.False(t, got.ItemField().Nullable)
	gotElement := got.ItemType().(*arrow.ListType).ElemField()
	requireVortexFieldMetadata(t, gotElement.Metadata, elementID, "element")
	require.Equal(t, arrow.BINARY, gotElement.Type.ID())
	require.False(t, gotElement.Nullable)
}

func TestVortexNormalizesMapViewArrays(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)
	dtype := arrow.MapOfFields(
		arrow.Field{Name: "key", Type: arrow.BinaryTypes.StringView},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.StringView, Nullable: true})
	builder := array.NewMapBuilderWithType(mem, dtype)
	defer builder.Release()
	builder.Append(true)
	builder.KeyBuilder().(*array.StringViewBuilder).Append("discarded")
	builder.ItemBuilder().(*array.StringViewBuilder).Append("discarded")
	builder.Append(true)
	builder.KeyBuilder().(*array.StringViewBuilder).Append("key")
	builder.ItemBuilder().(*array.StringViewBuilder).Append("value longer than twelve bytes")
	builder.AppendNull()
	whole := builder.NewMapArray()
	defer whole.Release()
	// A nonzero parent offset must still point at the same entries after
	// rebuilding nested arrays with normalized string leaves.
	values := array.NewSlice(whole, 1, 3)
	defer values.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "attributes", Type: dtype, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 2)
	defer record.Release()
	inner, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
	require.NoError(t, err)
	reader := newViewNormalizingReader(ctx, inner)
	defer reader.Release()

	require.True(t, reader.Next(), "%v", reader.Err())
	out := reader.RecordBatch().Column(0).(*array.Map)
	require.Equal(t, 2, out.Len())
	require.False(t, out.IsNull(0))
	require.True(t, out.IsNull(1))
	start, end := out.ValueOffsets(0)
	require.EqualValues(t, 1, end-start)
	require.Equal(t, "key", out.Keys().(*array.String).Value(int(start)))
	require.Equal(t, "value longer than twelve bytes", out.Items().(*array.String).Value(int(start)))
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
}

type vortexPredicateRow []any

func (r vortexPredicateRow) Size() int            { return len(r) }
func (r vortexPredicateRow) Get(i int) any        { return r[i] }
func (r vortexPredicateRow) Set(i int, value any) { r[i] = value }

func TestVortexNativePruningKeepsEveryMatchingRow(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "label", Type: iceberg.PrimitiveTypes.String})
	integer := iceberg.GreaterThan(iceberg.Reference("id"), int64(10))
	unsupported := iceberg.StartsWith(iceberg.Reference("label"), "yes")
	negative := iceberg.LessThan(iceberg.Reference("id"), int64(0))
	for _, tc := range []struct {
		name string
		expr iceberg.BooleanExpression
	}{
		{"integer", integer},
		{"partial_and", iceberg.NewAnd(integer, unsupported)},
		{"partial_or", iceberg.NewOr(integer, unsupported)},
		{"negated_and", iceberg.NewNot(iceberg.NewAnd(integer, unsupported))},
		{"negated_or", iceberg.NewNot(iceberg.NewOr(integer, unsupported))},
		{"and_with_or", iceberg.NewAnd(integer, iceberg.NewOr(unsupported, negative))},
		{"or_with_and", iceberg.NewOr(negative, iceberg.NewAnd(integer, unsupported))},
		{"large_integer", iceberg.GreaterThan(iceberg.Reference("id"), int64(9007199254740992))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := iceberg.BindExpr(schema, tc.expr, true)
			require.NoError(t, err)
			predicates, err := iceberg.VisitExpr(bound, nativeVortexPredicates{})
			require.NoError(t, err)
			evaluate, err := iceberg.ExpressionEvaluator(schema, tc.expr, true)
			require.NoError(t, err)
			var pruned int
			for _, id := range []int64{-1, 0, 9, 10, 11, 9007199254740992, 9007199254740993} {
				for _, label := range []any{nil, "yes", "no"} {
					matches, err := evaluate(vortexPredicateRow{id, label})
					require.NoError(t, err)
					dropped := !nativePredicateCandidates(id, predicates)
					if matches {
						require.False(t, dropped, "matching row id=%d label=%v must survive pruning", id, label)
					}
					if dropped {
						pruned++
					}
				}
			}
			if tc.name == "integer" {
				require.Positive(t, pruned, "supported integer bounds must be used for pruning")
			}
		})
	}
}

func requireVortexFieldMetadata(t *testing.T, metadata arrow.Metadata, id int, source string) {
	t.Helper()
	require.Equal(t, map[string]string{"PARQUET:field_id": strconv.Itoa(id), "source": source}, metadata.ToMap())
}

// Evaluate the translated conjunction independently of the reader's min/max code.
// On a single-value zone this establishes whether the visitor can lose a match.
func nativePredicateCandidates(value int64, predicates []native.Predicate) bool {
	for _, predicate := range predicates {
		switch predicate.Op {
		case native.OpEQ:
			if value != predicate.Value {
				return false
			}
		case native.OpNEQ:
			if value == predicate.Value {
				return false
			}
		case native.OpLT:
			if value >= predicate.Value {
				return false
			}
		case native.OpLTE:
			if value > predicate.Value {
				return false
			}
		case native.OpGT:
			if value <= predicate.Value {
				return false
			}
		case native.OpGTE:
			if value < predicate.Value {
				return false
			}
		default:
			panic("unexpected native pruning operator")
		}
	}

	return true
}
