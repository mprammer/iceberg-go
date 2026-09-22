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
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

// RowPositionedRecordReader reports the physical rows in the current batch.
// Spans are ordered, contain no gaps within a span, and are borrowed until Next
// or Release. Their row counts sum to the batch's row count.
type RowPositionedRecordReader interface {
	array.RecordReader
	RowPositions() []RowGroupSpan
}

// validateVortexRowPositions checks both a batch's span inventory and its order
// relative to the preceding batch. Subtraction avoids overflow on corrupt spans.
func validateVortexRowPositions(spans []RowGroupSpan, rows, fileRows, previousEnd int64) (int64, error) {
	if rows < 0 || fileRows < 0 || previousEnd < 0 || previousEnd > fileRows {
		return previousEnd, fmt.Errorf("%w: invalid Vortex row counts", iceberg.ErrInvalidArgument)
	}
	var counted int64
	end := previousEnd
	for _, span := range spans {
		if span.NumRows <= 0 || span.FirstRowPos < end || span.FirstRowPos > fileRows ||
			span.NumRows > fileRows-span.FirstRowPos || span.NumRows > rows-counted {
			return previousEnd, fmt.Errorf("%w: invalid Vortex physical row span %+v", iceberg.ErrInvalidArgument, span)
		}
		counted += span.NumRows
		end = span.FirstRowPos + span.NumRows
	}
	if counted != rows {
		return previousEnd, fmt.Errorf("%w: Vortex row spans cover %d rows, batch has %d", iceberg.ErrInvalidArgument, counted, rows)
	}

	return end, nil
}

// vortexPositionReader validates backend metadata before exposing any batch to
// a position-dependent consumer. The embedded reader owns all Arrow resources.
type vortexPositionReader struct {
	RowPositionedRecordReader
	fileRows    int64
	previousEnd int64
	err         error
}

func newVortexPositionReader(inner array.RecordReader, fileRows int64) (array.RecordReader, error) {
	positioned, ok := inner.(RowPositionedRecordReader)
	if !ok {
		return nil, fmt.Errorf("%w: Vortex backend did not return physical row positions", iceberg.ErrInvalidArgument)
	}

	return &vortexPositionReader{RowPositionedRecordReader: positioned, fileRows: fileRows}, nil
}

func (r *vortexPositionReader) Next() bool {
	if r.err != nil || !r.RowPositionedRecordReader.Next() {
		return false
	}
	r.previousEnd, r.err = validateVortexRowPositions(r.RowPositions(), r.RecordBatch().NumRows(), r.fileRows, r.previousEnd)

	return r.err == nil
}

func (r *vortexPositionReader) Err() error {
	if r.err != nil {
		return r.err
	}

	return r.RowPositionedRecordReader.Err()
}

// vortexEmptyProjectionReader removes physical filter columns only after the
// backend has used them. Positions remain attached to the candidate rows.
type vortexEmptyProjectionReader struct {
	inner   array.RecordReader
	schema  *arrow.Schema
	current arrow.RecordBatch
	refs    atomic.Int64
}

func newVortexEmptyProjectionReader(inner array.RecordReader) array.RecordReader {
	r := &vortexEmptyProjectionReader{inner: inner, schema: arrow.NewSchema(nil, nil)}
	r.refs.Store(1)

	return r
}

func (r *vortexEmptyProjectionReader) RowPositions() []RowGroupSpan {
	if positioned, ok := r.inner.(RowPositionedRecordReader); ok {
		return positioned.RowPositions()
	}

	return nil
}
func (r *vortexEmptyProjectionReader) Schema() *arrow.Schema          { return r.schema }
func (r *vortexEmptyProjectionReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *vortexEmptyProjectionReader) Record() arrow.RecordBatch      { return r.current }
func (r *vortexEmptyProjectionReader) Err() error                     { return r.inner.Err() }
func (r *vortexEmptyProjectionReader) Retain()                        { r.refs.Add(1) }
func (r *vortexEmptyProjectionReader) Release() {
	if r.refs.Add(-1) == 0 {
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
		r.inner.Release()
	}
}

func (r *vortexEmptyProjectionReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if !r.inner.Next() {
		return false
	}
	r.current = array.NewRecordBatch(r.schema, nil, r.inner.RecordBatch().NumRows())

	return true
}
