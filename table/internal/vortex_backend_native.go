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
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	native "github.com/mprammer/vortex-go"
)

type nativeVortexReader struct {
	file   *native.File
	schema *arrow.Schema
}

func openNativeVortex(ctx context.Context, input io.ReaderAt, size int64) (vortexBackendReader, error) {
	f, err := native.Open(ctx, input, size)
	if err != nil {
		return nil, err
	}
	schema := f.Schema()
	for _, field := range schema.Fields() {
		if !nativeVortexSupportedType(field.Type) {
			f.Close()

			return nil, fmt.Errorf("%w: native Vortex field %s has unsupported type %s", iceberg.ErrNotImplemented, field.Name, field.Type)
		}
	}

	return &nativeVortexReader{file: f, schema: schema}, nil
}

// Keep Iceberg's supported scalar storage types explicit at the adapter boundary.
func nativeVortexSupportedType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.BOOL, arrow.INT32, arrow.INT64, arrow.FLOAT32, arrow.FLOAT64, arrow.STRING, arrow.BINARY, arrow.DATE32:
		return true
	case arrow.DECIMAL128:
		decimal := dt.(*arrow.Decimal128Type)

		return decimal.Precision > 0 && decimal.Precision <= 38 && decimal.Scale >= 0 && decimal.Scale <= decimal.Precision
	default:
		return false
	}
}

func (r *nativeVortexReader) Close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil

	return err
}
func (r *nativeVortexReader) Schema() (*arrow.Schema, error) { return r.schema, nil }
func (r *nativeVortexReader) RowCount() (uint64, bool) {
	if r.file == nil {
		return 0, false
	}

	return uint64(r.file.NumRows()), true
}

func (r *nativeVortexReader) Scan(ctx context.Context, names []string, filter *VortexScanFilter) (array.RecordReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.file == nil {
		return nil, fmt.Errorf("%w: native Vortex reader is closed", iceberg.ErrInvalidArgument)
	}
	opts := native.ScanOptions{Columns: names}
	if filter != nil && filter.Expr != nil {
		preds, err := iceberg.VisitExpr(filter.Expr, nativeVortexPredicates{})
		if err == nil {
			opts.Predicates = preds
		}
	}
	reader, err := r.file.NewRecordReader(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &nativeVortexRecordReader{RecordReader: reader}, nil
}

// Native min/max pruning initially handles integer comparisons joined by AND.
// OR, NOT, transforms, missing statistics and other types keep the data. In
// particular float pruning needs NaN counts before it can safely handle !=.
type nativeVortexPredicates struct{}

func (nativeVortexPredicates) VisitTrue() []native.Predicate                  { return nil }
func (nativeVortexPredicates) VisitFalse() []native.Predicate                 { return nil }
func (nativeVortexPredicates) VisitNot([]native.Predicate) []native.Predicate { return nil }
func (nativeVortexPredicates) VisitOr([]native.Predicate, []native.Predicate) []native.Predicate {
	return nil
}

func (nativeVortexPredicates) VisitAnd(a, b []native.Predicate) []native.Predicate {
	return append(a, b...)
}

func (nativeVortexPredicates) VisitUnbound(iceberg.UnboundPredicate) []native.Predicate {
	return nil
}

func (nativeVortexPredicates) VisitBound(p iceberg.BoundPredicate) []native.Predicate {
	ref, ok := p.Term().(iceberg.BoundReference)
	if !ok || len(ref.PosPath()) != 1 {
		return nil
	}
	lit, ok := p.(iceberg.BoundLiteralPredicate)
	if !ok {
		return nil
	}
	var value int64
	switch v := lit.Literal().(type) {
	case iceberg.Int32Literal:
		value = int64(v)
	case iceberg.Int64Literal:
		value = int64(v)
	default:
		return nil
	}
	var op native.CompareOp
	switch p.Op() {
	case iceberg.OpEQ:
		op = native.OpEQ
	case iceberg.OpNEQ:
		op = native.OpNEQ
	case iceberg.OpGT:
		op = native.OpGT
	case iceberg.OpGTEQ:
		op = native.OpGTE
	case iceberg.OpLT:
		op = native.OpLT
	case iceberg.OpLTEQ:
		op = native.OpLTE
	default:
		return nil
	}

	return []native.Predicate{{Column: ref.Field().Name, Op: op, Value: value}}
}

type nativeVortexRecordReader struct {
	*native.RecordReader
	positions [1]RowGroupSpan
}

func (r *nativeVortexRecordReader) RowPositions() []RowGroupSpan {
	if r.RecordBatch() == nil || r.RecordBatch().NumRows() == 0 {
		return nil
	}
	r.positions[0] = RowGroupSpan{FirstRowPos: r.RowOffset(), NumRows: r.RecordBatch().NumRows()}

	return r.positions[:]
}
