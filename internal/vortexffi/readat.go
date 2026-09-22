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

//nolint:nlreturn // cgo generates return statements inside its call wrappers.
package vortexffi

/*
#include <stdlib.h>
#include "vortex.h"

// Declared in readat_export.go and implemented by cgo as a real C symbol, so
// its address can be stored in the vx_readat descriptor.
extern int64_t vortexGoReadAt(void *ctx, uint64_t offset, uint8_t *dst, size_t length);
extern void vortexGoReleaseReader(void *ctx);

static void vortex_set_read_at(vx_readat *r) {
    r->read_at = vortexGoReadAt;
    r->release = vortexGoReleaseReader;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"unicode/utf8"
	"unsafe"
)

// Vortex reads through a caller-supplied io.ReaderAt instead of opening the
// file itself. That keeps one I/O path in the process - the host's, with its
// credentials and retry policy - while still letting Vortex fetch only the byte
// ranges a scan needs, which is what reading the whole file into a buffer gives
// up.
//
// Vortex calls back from its own worker threads, concurrently, so the ReaderAt
// must be safe for concurrent positional reads. io.ReaderAt already requires
// exactly that.

// readerRegistry maps the integer handles handed to C back to Go readers.
//
// The handle is an integer stored in C memory rather than a Go pointer, because
// cgo forbids C retaining a Go pointer across calls, which is precisely what a
// long-lived data source does.
var readerRegistry = struct {
	sync.RWMutex
	next    uint64
	readers map[uint64]*registeredReader
}{readers: make(map[uint64]*registeredReader)}

type registeredReader struct {
	ioMu    sync.RWMutex
	stopped bool
	r       io.ReaderAt
	size    int64
	// err holds the first read failure, so it can be reported as a Go error
	// rather than only as an opaque code through the FFI boundary.
	mu  sync.Mutex
	err error
}

func (rr *registeredReader) recordErr(err error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if rr.err == nil {
		rr.err = err
	}
}

func (rr *registeredReader) firstErr() error {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	return rr.err
}

func registerReader(r io.ReaderAt, size int64) (uint64, *registeredReader) {
	entry := &registeredReader{r: r, size: size}

	readerRegistry.Lock()
	defer readerRegistry.Unlock()

	readerRegistry.next++
	id := readerRegistry.next
	readerRegistry.readers[id] = entry

	return id, entry
}

func lookupReader(id uint64) *registeredReader {
	readerRegistry.RLock()
	defer readerRegistry.RUnlock()

	return readerRegistry.readers[id]
}

func unregisterReader(id uint64) {
	readerRegistry.Lock()
	defer readerRegistry.Unlock()

	delete(readerRegistry.readers, id)
}

// OpenReaderAt opens a Vortex file whose bytes are supplied by r.
//
// name should be the file's location; Vortex uses it for cache keys and error
// messages, so it must be stable and unique. An empty name creates an anonymous
// source. size must be the file's exact length: Vortex locates the footer
// relative to the end, so a wrong size reads as a corrupt file.
//
// The returned DataSource and its scans keep r alive. Neither closes r.
func (s *Session) OpenReaderAt(name string, size int64, r io.ReaderAt) (*DataSource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ptr == nil {
		return nil, errors.New("vortex: session is closed")
	}
	if !utf8.ValidString(name) {
		return nil, errors.New("vortex: reader name must be valid UTF-8")
	}
	if r == nil {
		return nil, errors.New("vortex: nil reader")
	}
	if size <= 0 {
		return nil, fmt.Errorf("vortex: invalid file size %d", size)
	}

	id, entry := registerReader(r, size)

	// The handle lives in C memory: it is what Vortex hands back to the
	// callback, and it must outlive this call.
	idPtr := C.malloc(C.size_t(unsafe.Sizeof(C.uint64_t(0))))
	if idPtr == nil {
		unregisterReader(id)

		return nil, errors.New("vortex: failed to allocate a reader handle")
	}
	*(*C.uint64_t)(idPtr) = C.uint64_t(id)

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	desc := C.vx_readat{
		ctx:  idPtr,
		len:  C.uint64_t(size),
		name: C.vx_view{ptr: cname, len: C.size_t(len(name))},
	}
	C.vortex_set_read_at(&desc)
	// These validated arguments guarantee that Rust constructs the callback
	// owner. Its release callback then runs on both successful open and footer
	// read failure, after the last outstanding read drops its reference.

	var cerr *C.vx_error
	ptr := C.vx_data_source_new_readat(s.ptr, &desc, &cerr)
	if err := takeErr(cerr, "open "+name); err != nil {
		entry.stop()

		// A read failure during open is more specific than the generic FFI
		// error wrapping it, so report it in addition.
		if readErr := entry.firstErr(); readErr != nil {
			return nil, fmt.Errorf("%w (reader: %w)", err, readErr)
		}

		return nil, err
	}
	if ptr == nil {
		entry.stop()

		return nil, fmt.Errorf("vortex: opening %s returned NULL without an error", name)
	}

	return &DataSource{session: &Session{ptr: C.vx_session_clone(s.ptr)}, ptr: ptr, reader: entry}, nil
}

// readAtFromC services one callback from Vortex. It is separated from the
// exported cgo shim so it can be written in ordinary Go.
//
// Vortex expects the number of bytes written; it treats a short count or a
// negative value as a failed read. Each failure returns a distinct negative
// code, and entry.readAt already rejects short reads, so success is always the
// full requested length.
func readAtFromC(ctx unsafe.Pointer, offset uint64, dst *C.uint8_t, length C.size_t) int64 {
	if ctx == nil || dst == nil {
		return -1
	}

	id := uint64(*(*C.uint64_t)(ctx))
	entry := lookupReader(id)
	if entry == nil {
		return -2
	}

	if uint64(length) > uint64(^uint(0)>>1) || offset > uint64(entry.size) ||
		uint64(length) > uint64(entry.size)-offset {
		entry.recordErr(fmt.Errorf("vortex: read %d+%d exceeds source size %d", offset, length, entry.size))
		return -3
	}

	// A slice over C memory, borrowed only for this callback.
	buf := unsafe.Slice((*byte)(unsafe.Pointer(dst)), int(length))
	if err := entry.readAt(buf, int64(offset)); err != nil {
		return -4
	}

	return int64(length)
}

func (rr *registeredReader) readAt(buf []byte, offset int64) (err error) {
	rr.ioMu.RLock()
	defer rr.ioMu.RUnlock()
	if rr.stopped {
		return io.ErrClosedPipe
	}
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("vortex: reader panic at %d: %v", offset, p)
		}
		if err != nil {
			rr.recordErr(err)
		}
	}()
	n, err := rr.r.ReadAt(buf, offset)
	if n != len(buf) {
		return fmt.Errorf("reading %d bytes at %d: got %d: %w", len(buf), offset, n,
			errors.Join(io.ErrUnexpectedEOF, err))
	}
	// ReaderAt permits io.EOF alongside a full read.
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("reading %d bytes at %d: %w", len(buf), offset, err)
	}

	return nil
}

// stop waits for Go reads already in progress and prevents any later callback
// from touching the caller's file. Rust may retain the callback descriptor in
// its I/O cache until a later runtime turn, so waiting for release would deadlock.
func (rr *registeredReader) stop() {
	rr.ioMu.Lock()
	defer rr.ioMu.Unlock()
	rr.stopped = true
	rr.r = nil
}

func releaseReaderFromC(ctx unsafe.Pointer) {
	id := uint64(*(*C.uint64_t)(ctx))
	C.free(ctx)
	unregisterReader(id)
}
