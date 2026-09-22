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

//go:build vortex && cgo

package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal/vortexffi"
)

type ffiVortexReader struct {
	session *vortexffi.Session
	source  *vortexffi.DataSource
}

func openFFIVortex(ctx context.Context, input io.ReaderAt, size int64, name string) (vortexBackendReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	session, err := vortexffi.NewSession()
	if err != nil {
		return nil, err
	}
	source, err := session.OpenReaderAt(name, size, input)
	if err != nil {
		return nil, errors.Join(err, session.Close())
	}

	return &ffiVortexReader{session: session, source: source}, nil
}
func (r *ffiVortexReader) Close() error                   { return errors.Join(r.source.Close(), r.session.Close()) }
func (r *ffiVortexReader) Schema() (*arrow.Schema, error) { return r.source.Schema() }
func (r *ffiVortexReader) RowCount() (uint64, bool)       { return r.source.RowCount() }
func (r *ffiVortexReader) Scan(ctx context.Context, cols []string, filter *VortexScanFilter) (array.RecordReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	req := vortexffi.ScanRequest{Projection: cols}
	arena := vortexffi.NewExprArena()
	defer arena.Close()
	if filter != nil && filter.Expr != nil {
		expr, err := BuildVortexFilter(arena, filter.FileSchema, filter.Expr)
		if err != nil {
			return nil, err
		}
		req.Filter = expr
	}
	needPositions := filter != nil && filter.NeedPositions
	if needPositions {
		schema, err := r.source.Schema()
		if err != nil {
			return nil, err
		}
		if cols == nil {
			cols = make([]string, schema.NumFields())
			for i, field := range schema.Fields() {
				cols[i] = field.Name
			}
		}
		names := append([]string(nil), cols...)
		expressions := make([]*vortexffi.Expr, 0, len(names)+1)
		for _, name := range names {
			column, err := arena.Column([]string{name})
			if err != nil {
				return nil, err
			}
			expressions = append(expressions, column)
		}
		// The helper is removed by ordinal, and its name cannot shadow a
		// physical field even if that field was not selected by this scan.
		positionName := "__iceberg_vortex_row_position"
		for len(schema.FieldIndices(positionName)) > 0 {
			positionName += "_"
		}
		position, err := arena.RowIdx()
		if err != nil {
			return nil, err
		}
		names = append(names, positionName)
		expressions = append(expressions, position)
		req.ProjectionExpr, err = arena.Pack(names, expressions, false)
		if err != nil {
			return nil, err
		}
	}

	reader, err := r.source.Scan(req)
	if err != nil || !needPositions {
		return reader, err
	}

	return newFFIVortexPositionReader(reader), nil
}

// ffiVortexPositionReader strips the private row-index projection and coalesces
// its indices into spans. Shared validation checks ordering, range, and count
// before the Iceberg pipeline sees the batch.
type ffiVortexPositionReader struct {
	inner     array.RecordReader
	schema    *arrow.Schema
	current   arrow.RecordBatch
	positions []RowGroupSpan
	err       error
	refs      atomic.Int64
}

func newFFIVortexPositionReader(inner array.RecordReader) array.RecordReader {
	sc := inner.Schema()
	meta := sc.Metadata()
	r := &ffiVortexPositionReader{
		inner:  inner,
		schema: arrow.NewSchema(sc.Fields()[:sc.NumFields()-1], &meta),
	}
	r.refs.Store(1)

	return r
}

func (r *ffiVortexPositionReader) Schema() *arrow.Schema          { return r.schema }
func (r *ffiVortexPositionReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *ffiVortexPositionReader) Record() arrow.RecordBatch      { return r.current }
func (r *ffiVortexPositionReader) RowPositions() []RowGroupSpan   { return r.positions }
func (r *ffiVortexPositionReader) Err() error {
	if r.err != nil {
		return r.err
	}

	return r.inner.Err()
}
func (r *ffiVortexPositionReader) Retain() { r.refs.Add(1) }
func (r *ffiVortexPositionReader) Release() {
	if r.refs.Add(-1) == 0 {
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
		r.inner.Release()
	}
}

func (r *ffiVortexPositionReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	r.positions = r.positions[:0]
	if r.err != nil || !r.inner.Next() {
		return false
	}
	batch := r.inner.RecordBatch()
	indices, ok := batch.Column(r.schema.NumFields()).(*array.Uint64)
	if !ok || indices.NullN() != 0 || int64(indices.Len()) != batch.NumRows() {
		r.err = fmt.Errorf("%w: invalid Vortex row-index column", iceberg.ErrInvalidArgument)

		return false
	}
	for i := range indices.Len() {
		index := indices.Value(i)
		if index > math.MaxInt64 {
			r.err = fmt.Errorf("%w: Vortex row index exceeds int64", iceberg.ErrInvalidArgument)

			return false
		}
		position := int64(index)
		if n := len(r.positions); n > 0 &&
			r.positions[n-1].FirstRowPos <= math.MaxInt64-r.positions[n-1].NumRows &&
			r.positions[n-1].FirstRowPos+r.positions[n-1].NumRows == position {
			r.positions[n-1].NumRows++
		} else {
			r.positions = append(r.positions, RowGroupSpan{FirstRowPos: position, NumRows: 1})
		}
	}
	r.current = array.NewRecordBatch(r.schema, batch.Columns()[:r.schema.NumFields()], batch.NumRows())

	return true
}
