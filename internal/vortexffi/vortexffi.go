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

// Package vortexffi is a thin cgo binding over the Vortex C FFI (vortex-ffi).
//
// It exposes only what the Iceberg read path needs: open a Vortex file, read
// its Arrow schema, and scan it (with an optional column projection) as a
// stream of Arrow record batches. Everything crossing the boundary does so via
// the Arrow C Data Interface, so no data is copied or re-encoded in Go.
//
// Building requires the `vortex` build tag and a linkable libvortex_ffi. The
// vendored include/vortex.h pins the ABI; refresh it from the same vortex
// checkout that produced the library. See README.md.
//
//nolint:nlreturn // cgo generates return statements inside its call wrappers.
package vortexffi

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo LDFLAGS: -lvortex_ffi

#include <stdlib.h>
#include "vortex.h"
*/
import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// takeErr converts a vx_error out-param into a Go error and frees it. It
// returns nil when err is nil, so call sites can pass it through directly.
func takeErr(cerr *C.vx_error, op string) error {
	if cerr == nil {
		return nil
	}
	defer C.vx_error_free(cerr)

	msg := C.vx_error_message(cerr)
	detail := "unknown error"
	if msg.ptr != nil {
		detail = C.GoStringN(msg.ptr, C.int(msg.len))
	}

	return fmt.Errorf("vortex: %s: %s (code %d)", op, detail, C.vx_error_get_code(cerr))
}

// Session owns a Vortex FFI session. It is safe for concurrent use; the
// underlying session is reference counted in Rust.
type Session struct {
	mu  sync.RWMutex
	ptr *C.vx_session
}

// NewSession creates a Vortex session. Call Close when done.
func NewSession() (*Session, error) {
	ptr := C.vx_session_new()
	if ptr == nil {
		return nil, errors.New("vortex: vx_session_new returned NULL")
	}

	return &Session{ptr: ptr}, nil
}

// Close releases the session.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr != nil {
		C.vx_session_free(s.ptr)
		s.ptr = nil
	}

	return nil
}

// DataSource owns an opened Vortex file and an independent session handle.
// Scans retain both until their readers are released. For OpenReaderAt, the
// caller must keep the underlying file open until all scans have been released.
type DataSource struct {
	session *Session
	ptr     *C.vx_data_source
	reader  *registeredReader

	closeMu sync.Mutex
	closed  bool
	scans   int
}

// OpenPath opens a Vortex file by path or URI, letting Vortex do its own I/O.
// Use this only when Vortex can reach the location itself (a local path, or a
// URI whose credentials it can resolve); otherwise use OpenReaderAt so that the
// bytes come through the caller's own I/O layer.
func (s *Session) OpenPath(path string) (*DataSource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ptr == nil {
		return nil, errors.New("vortex: session is closed")
	}
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	view := C.vx_view{ptr: cpath, len: C.size_t(len(path))}
	opts := C.vx_data_source_options{paths: &view, paths_len: 1}

	var cerr *C.vx_error
	ptr := C.vx_data_source_new(s.ptr, &opts, &cerr)
	if err := takeErr(cerr, "open "+path); err != nil {
		return nil, err
	}
	if ptr == nil {
		return nil, fmt.Errorf("vortex: opening %s returned NULL without an error", path)
	}

	return &DataSource{session: &Session{ptr: C.vx_session_clone(s.ptr)}, ptr: ptr}, nil
}

// OpenBuffer opens a Vortex file from an independent copy of data. Reads copy
// the requested ranges into Rust-owned memory, so retained Arrow batches remain
// valid after the data source is closed.
func (s *Session) OpenBuffer(data []byte) (*DataSource, error) {
	if len(data) == 0 {
		return nil, errors.New("vortex: cannot open an empty buffer")
	}

	return s.OpenReaderAt("", int64(len(data)), bytes.NewReader(bytes.Clone(data)))
}

// Close prevents new operations and releases the source after its last scan is
// released. It does not close the caller's ReaderAt. Read errors retain their
// original Go error identity, including errors reported through the C stream.
func (d *DataSource) Close() error {
	d.closeMu.Lock()
	defer d.closeMu.Unlock()

	d.closed = true
	d.freeClosed()

	return d.readError(nil)
}

// freeClosed is called with closeMu held. Scans can still borrow the session and
// source after Close, and must finish before their owning handles are freed.
func (d *DataSource) freeClosed() {
	if !d.closed || d.scans != 0 || d.ptr == nil {
		return
	}
	C.vx_data_source_free(d.ptr)
	d.ptr = nil
	d.session.Close()
	if d.reader != nil {
		d.reader.stop()
	}
}

func (d *DataSource) releaseScan() {
	d.closeMu.Lock()
	defer d.closeMu.Unlock()
	d.scans--
	d.freeClosed()
}

func (d *DataSource) readError(err error) error {
	if d.reader != nil {
		return errors.Join(err, d.reader.firstErr())
	}

	return err
}

// Schema returns the file's top-level schema as an Arrow schema.
func (d *DataSource) Schema() (*arrow.Schema, error) {
	d.closeMu.Lock()
	defer d.closeMu.Unlock()
	if d.closed {
		return nil, errors.New("vortex: data source is closed")
	}
	dtype := C.vx_data_source_dtype(d.ptr)
	if dtype == nil {
		return nil, errors.New("vortex: data source has no dtype")
	}
	defer C.vx_dtype_free(dtype)

	return dtypeToArrowSchema(d.session, dtype)
}

// RowCount returns the file's row count and whether that count is exact.
func (d *DataSource) RowCount() (count uint64, exact bool) {
	d.closeMu.Lock()
	defer d.closeMu.Unlock()
	if d.closed {
		return 0, false
	}
	var est C.vx_estimate
	C.vx_data_source_get_row_count(d.ptr, &est)

	return uint64(est.estimate), est._type == C.VX_ESTIMATE_EXACT
}

// dtypeToArrowSchema converts a Vortex struct dtype to an Arrow schema by way
// of the C Data Interface. The session is needed to resolve extension types.
func dtypeToArrowSchema(s *Session, dtype *C.vx_dtype) (*arrow.Schema, error) {
	// Allocated in C memory: cdata imports from this address and cgo forbids
	// handing C a pointer into Go memory that it may retain.
	cschema := (*C.struct_ArrowSchema)(C.calloc(1, C.sizeof_struct_ArrowSchema))
	if cschema == nil {
		return nil, errors.New("vortex: failed to allocate an ArrowSchema")
	}
	defer C.free(unsafe.Pointer(cschema))

	var cerr *C.vx_error
	if C.vx_dtype_to_arrow_schema(s.ptr, dtype, cschema, &cerr) != 0 {
		if err := takeErr(cerr, "convert dtype to arrow schema"); err != nil {
			return nil, err
		}

		return nil, errors.New("vortex: dtype to arrow schema conversion failed")
	}

	return cdata.ImportCArrowSchema((*cdata.CArrowSchema)(unsafe.Pointer(cschema)))
}

// ScanRequest describes what a scan should read.
type ScanRequest struct {
	// Projection names the top-level columns to read. Empty reads every column.
	// Ignored when ProjectionExpr is set.
	Projection []string
	// ProjectionExpr is a projection built as an expression, for shapes a name
	// list cannot describe: pruning inside a nested struct, or projecting the
	// row index alongside the data. Like Filter, it must outlive the call.
	ProjectionExpr *Expr
	// Filter, when non-nil, is pushed into the scan so Vortex can skip chunks
	// its statistics rule out and drop rows that do not match. It must outlive
	// the call, i.e. its arena must not be closed until Scan returns.
	//
	// Because Vortex applies the predicate for real rather than only pruning,
	// the rows it emits are no longer contiguous in the file. Callers that need
	// each row's original file position must also project RowIdx().
	Filter *Expr
}

// Scan reads the data source as Arrow record batches. The returned reader's
// schema holds the projected columns in the order requested.
//
// The caller must release the reader. Each batch is borrowed until the next
// Next or Release call; retain a batch explicitly to use it beyond that point.
func (d *DataSource) Scan(req ScanRequest) (array.RecordReader, error) {
	d.closeMu.Lock()
	defer d.closeMu.Unlock()
	if d.closed {
		return nil, errors.New("vortex: data source is closed")
	}
	var (
		opts     C.vx_scan_options
		projExpr *C.vx_expression
	)

	switch {
	case req.ProjectionExpr != nil:
		opts.projection = req.ProjectionExpr.ptr
	case len(req.Projection) > 0:
		var err error
		projExpr, err = selectExpr(req.Projection)
		if err != nil {
			return nil, err
		}
		defer C.vx_expression_free(projExpr)

		opts.projection = projExpr
	}

	if req.Filter != nil {
		opts.filter = req.Filter.ptr
	}

	// Storage order: Iceberg's read path pairs batches with file row positions
	// for deletes and row lineage, so batches must not be reordered.
	opts.ordered = C.bool(true)

	var cerr *C.vx_error
	scan := C.vx_data_source_scan(d.ptr, &opts, nil, &cerr)
	if err := takeErr(cerr, "start scan"); err != nil {
		return nil, d.readError(err)
	}
	if scan == nil {
		return nil, errors.New("vortex: scan returned NULL without an error")
	}

	sc, err := scanSchema(d.session, scan)
	if err != nil {
		C.vx_scan_free(scan)

		return nil, err
	}

	d.scans++
	return newPartitionReader(d, scan, sc), nil
}

// scanSchema reads the scan's output schema. It must be called before the first
// partition is pulled.
func scanSchema(s *Session, scan *C.vx_scan) (*arrow.Schema, error) {
	var cerr *C.vx_error
	dtype := C.vx_scan_dtype(scan, &cerr)
	if err := takeErr(cerr, "read scan dtype"); err != nil {
		return nil, err
	}
	if dtype == nil {
		return nil, errors.New("vortex: scan has no dtype")
	}
	defer C.vx_dtype_free(dtype)

	return dtypeToArrowSchema(s, dtype)
}

// selectExpr builds a Vortex `select` expression over the root scope, naming the
// top-level columns to project.
func selectExpr(names []string) (*C.vx_expression, error) {
	root := C.vx_expression_root()
	if root == nil {
		return nil, errors.New("vortex: vx_expression_root returned NULL")
	}
	defer C.vx_expression_free(root)

	// The name bytes are copied by vx_expression_select, but they must stay
	// valid for the duration of the call, hence the explicit C allocations.
	views := make([]C.vx_view, len(names))
	cstrs := make([]*C.char, len(names))
	defer func() {
		for _, s := range cstrs {
			if s != nil {
				C.free(unsafe.Pointer(s))
			}
		}
	}()

	for i, name := range names {
		cstrs[i] = C.CString(name)
		views[i] = C.vx_view{ptr: cstrs[i], len: C.size_t(len(name))}
	}

	expr := C.vx_expression_select(&views[0], C.size_t(len(views)), root)
	if expr == nil {
		return nil, errors.New("vortex: vx_expression_select returned NULL")
	}

	return expr, nil
}

// partitionReader presents a Vortex scan's partitions as one flat
// array.RecordReader. A scan yields partitions; each partition converts to its
// own ArrowArrayStream, so the reader walks partitions lazily and only holds one
// stream open at a time.
type partitionReader struct {
	src    *DataSource
	scan   *C.vx_scan
	schema *arrow.Schema

	cstream unsafe.Pointer
	current arrow.RecordBatch
	inner   array.RecordReader

	refCount int64
	err      error
	done     bool
}

func newPartitionReader(src *DataSource, scan *C.vx_scan, schema *arrow.Schema) *partitionReader {
	r := &partitionReader{src: src, scan: scan, schema: schema, refCount: 1}
	runtime.SetFinalizer(r, (*partitionReader).Release)

	return r
}

func (r *partitionReader) Schema() *arrow.Schema { return r.schema }

func (r *partitionReader) Err() error { return r.err }

func (r *partitionReader) Retain() { atomic.AddInt64(&r.refCount, 1) }

func (r *partitionReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) != 0 {
		return
	}

	r.releaseCurrent()
	r.closeInner()

	if r.scan != nil {
		C.vx_scan_free(r.scan)
		r.scan = nil
	}

	r.done = true
	r.src.releaseScan()
	runtime.SetFinalizer(r, nil)
}

func (r *partitionReader) releaseCurrent() {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
}

func (r *partitionReader) closeInner() {
	if r.inner != nil {
		r.inner.Release()
		r.inner = nil
	}
	if r.cstream != nil {
		C.free(r.cstream)
		r.cstream = nil
	}
}

func (r *partitionReader) RecordBatch() arrow.RecordBatch { return r.current }

// Record is the pre-RecordBatch spelling of RecordBatch, kept because
// array.RecordReader still requires it.
func (r *partitionReader) Record() arrow.RecordBatch { return r.current }

func (r *partitionReader) Next() bool {
	if r.err != nil || r.done {
		return false
	}

	r.releaseCurrent()

	for {
		if r.inner == nil {
			if !r.nextPartition() {
				return false
			}
		}

		if r.inner.Next() {
			r.current = r.inner.RecordBatch()
			r.current.Retain()

			return true
		}

		if err := r.inner.Err(); err != nil {
			r.err = r.src.readError(err)

			return false
		}

		// Partition exhausted; fall through to the next one.
		r.closeInner()
	}
}

// nextPartition pulls the next partition off the scan and opens its Arrow
// stream. It reports false once the scan is exhausted or on error.
func (r *partitionReader) nextPartition() bool {
	var cerr *C.vx_error
	part := C.vx_scan_next_partition(r.scan, &cerr)
	if err := takeErr(cerr, "read next partition"); err != nil {
		r.err = r.src.readError(err)

		return false
	}
	if part == nil {
		r.done = true

		return false
	}

	cstream := C.calloc(1, C.sizeof_struct_ArrowArrayStream)
	if cstream == nil {
		C.vx_partition_free(part)
		r.err = errors.New("vortex: failed to allocate an ArrowArrayStream")

		return false
	}

	// vx_partition_scan_arrow consumes the partition, and frees it itself on
	// error, so it must not be freed here on either path.
	if C.vx_partition_scan_arrow(r.src.session.ptr, part,
		(*C.struct_ArrowArrayStream)(cstream), &cerr) != 0 {
		C.free(cstream)
		if err := takeErr(cerr, "scan partition to arrow"); err != nil {
			r.err = r.src.readError(err)
		} else {
			r.err = errors.New("vortex: partition scan to arrow failed")
		}

		return false
	}

	stream := (*cdata.CArrowArrayStream)(cstream)
	inner, err := cdata.ImportCRecordReader(stream, r.schema)
	if err != nil {
		C.free(cstream)
		r.err = r.src.readError(err)

		return false
	}

	rr, ok := inner.(array.RecordReader)
	if !ok {
		if releaser, ok := inner.(interface{ Release() }); ok {
			releaser.Release()
		}
		C.free(cstream)
		r.err = errors.New("vortex: imported arrow stream is not a RecordReader")

		return false
	}

	r.cstream = cstream
	r.inner = rr

	return true
}
