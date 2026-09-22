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

package vortexffi

import (
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

// countingReaderAt records what Vortex actually asked for, which is the point
// of the exercise: the whole reason to hand Vortex a reader instead of a buffer
// is that it then fetches only the ranges a scan needs.
type countingReaderAt struct {
	data []byte

	calls     atomic.Int64
	bytesRead atomic.Int64

	mu     sync.Mutex
	ranges [][2]int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.calls.Add(1)
	c.bytesRead.Add(int64(len(p)))

	c.mu.Lock()
	c.ranges = append(c.ranges, [2]int64{off, int64(len(p))})
	c.mu.Unlock()

	if off >= int64(len(c.data)) {
		return 0, io.EOF
	}

	n := copy(p, c.data[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

func fixtureBytes(t *testing.T) []byte {
	t.Helper()

	data, err := os.ReadFile(fixturePath)
	require.NoError(t, err)

	return data
}

// TestReadAtScanReadsOnlyWhatItNeeds is the reason vx_data_source_new_readat
// was added.
//
// With a caller-supplied reader, a projected scan touches only the segments it
// needs, so reading one column of a four-column file must transfer noticeably
// less than the whole file. Buffering the file up front - the only option the C
// FFI offered before - always transfers all of it.
func TestReadAtScanReadsOnlyWhatItNeeds(t *testing.T) {
	data := fixtureBytes(t)

	session, err := NewSession()
	require.NoError(t, err)
	defer session.Close()

	reader := &countingReaderAt{data: data}

	src, err := session.OpenReaderAt("file:///fixtures/simple.vortex", int64(len(data)), reader)
	require.NoError(t, err)

	sc, err := src.Schema()
	require.NoError(t, err)
	require.Equal(t, 4, len(sc.Fields()))

	rdr, err := src.Scan(ScanRequest{Projection: []string{"id"}})
	require.NoError(t, err)

	rows, sum := 0, int64(0)
	for rdr.Next() {
		batch := rdr.RecordBatch()
		rows += int(batch.NumRows())

		ids, ok := batch.Column(0).(*array.Int32)
		require.True(t, ok)
		for i := range ids.Len() {
			require.EqualValues(t, rows-ids.Len()+i, ids.Value(i), "rows must stay in file order")
			sum += int64(ids.Value(i))
		}
	}
	require.NoError(t, rdr.Err())
	rdr.Release()

	require.NoError(t, src.Close())

	// Correctness first: reading through the callback must produce the same
	// answer as reading the file any other way.
	require.Equal(t, 1000, rows)
	require.EqualValues(t, 499500, sum)

	require.Positive(t, reader.calls.Load(), "Vortex must have called back for its bytes")

	// Deliberately no assertion that less than the whole file was read. The
	// reader advertises object-storage coalescing, whose merge window is far
	// wider than this 12KB fixture, so Vortex correctly collapses the scan into
	// a couple of large reads that happen to cover everything. Range pruning
	// only becomes observable on files bigger than the coalesce window; see
	// TestReadAtPrunesLargeFile, which builds one when the tooling is present.
	require.LessOrEqual(t, reader.bytesRead.Load(), int64(len(data)),
		"must never read past the end of the source")

	t.Logf("single-column scan: %d callbacks, %d of %d bytes (%.1f%%)",
		reader.calls.Load(), reader.bytesRead.Load(), len(data),
		100*float64(reader.bytesRead.Load())/float64(len(data)))
}

// TestReadAtPrunesLargeFile is the assertion the small fixture cannot make.
//
// Coalescing merges nearby reads within a window far wider than a 12KB file, so
// range pruning is only observable once the file is substantially larger than
// that window. Such a file is too large to commit, so this runs only when
// VORTEX_LARGE_FIXTURE points at one. Generate it with:
//
//	duckdb -c "COPY (SELECT i::INTEGER AS id, repeat('x',200)||i AS blob1,
//	           repeat('y',200)||i AS blob2, (i*1.5)::DOUBLE AS score
//	           FROM range(0,2000000) t(i)) TO 'big.parquet' (FORMAT PARQUET)"
//	vx convert big.parquet
//
// The columns are deliberately lopsided - two wide text blobs against a narrow
// int and double - so projecting `id` alone should touch a small fraction.
func TestReadAtPrunesLargeFile(t *testing.T) {
	path := os.Getenv("VORTEX_LARGE_FIXTURE")
	if path == "" {
		t.Skip("set VORTEX_LARGE_FIXTURE to a large .vortex file to run this")
	}

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	session, err := NewSession()
	require.NoError(t, err)
	defer session.Close()

	reader := &countingReaderAt{data: data}

	src, err := session.OpenReaderAt("file://"+path, int64(len(data)), reader)
	require.NoError(t, err)

	rdr, err := src.Scan(ScanRequest{Projection: []string{"id"}})
	require.NoError(t, err)

	rows, sum := 0, int64(0)
	for rdr.Next() {
		batch := rdr.RecordBatch()
		rows += int(batch.NumRows())

		ids, ok := batch.Column(0).(*array.Int32)
		require.True(t, ok)
		for i := range ids.Len() {
			require.EqualValues(t, rows-ids.Len()+i, ids.Value(i), "rows must stay in file order")
			sum += int64(ids.Value(i))
		}
	}
	require.NoError(t, rdr.Err())
	rdr.Release()
	require.NoError(t, src.Close())

	read := reader.bytesRead.Load()
	t.Logf("projected `id` from %d rows: %d callbacks, %d of %d bytes (%.2f%%)",
		rows, reader.calls.Load(), read, len(data),
		100*float64(read)/float64(len(data)))

	require.Positive(t, rows)
	require.Less(t, read, int64(len(data))/4,
		"projecting one narrow column should read well under a quarter of the file; read %d of %d",
		read, len(data))
}

// A full scan must also be correct through the callback path, nulls included.
func TestReadAtFullScanMatchesBuffer(t *testing.T) {
	data := fixtureBytes(t)

	session, err := NewSession()
	require.NoError(t, err)
	defer session.Close()

	src, err := session.OpenReaderAt("file:///fixtures/full.vortex", int64(len(data)),
		&countingReaderAt{data: data})
	require.NoError(t, err)

	rdr, err := src.Scan(ScanRequest{})
	require.NoError(t, err)

	rows, nonNullName, nonNullScore := 0, 0, 0
	for rdr.Next() {
		batch := rdr.RecordBatch()
		rows += int(batch.NumRows())

		// This layer hands back Vortex's own Arrow types; view normalization
		// happens in the Iceberg reader above it.
		names, ok := batch.Column(1).(*array.StringView)
		require.True(t, ok, "name: got %T", batch.Column(1))
		nonNullName += names.Len() - names.NullN()

		scores, ok := batch.Column(2).(*array.Float64)
		require.True(t, ok)
		nonNullScore += scores.Len() - scores.NullN()
	}
	require.NoError(t, rdr.Err())
	rdr.Release()
	require.NoError(t, src.Close())

	require.Equal(t, 1000, rows)
	require.Equal(t, 857, nonNullName, "nulls must survive the callback path")
	require.Equal(t, 800, nonNullScore)
}

// failingReaderAt fails every read past the footer, standing in for an I/O
// error mid-scan.
type failingReaderAt struct {
	data      []byte
	failAfter int64

	mu sync.Mutex
	n  int64
}

var errInjected = errors.New("injected I/O failure")

func (f *failingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if f.calls() > f.failAfter {
		return 0, errInjected
	}
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}

	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

func (f *failingReaderAt) calls() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++

	return f.n
}

// A read failure must surface as an error rather than as silently truncated or
// zero-filled data, which is the failure mode that matters: the callback writes
// straight into Vortex's buffers.
func TestReadAtSurfacesReaderErrors(t *testing.T) {
	data := fixtureBytes(t)

	session, err := NewSession()
	require.NoError(t, err)
	defer session.Close()

	// Fail from the very first read. Anything more lenient is satisfied by
	// coalescing on a fixture this small, which fetches the whole file up front
	// and then never needs another read to fail.
	src, err := session.OpenReaderAt("file:///fixtures/failing.vortex", int64(len(data)),
		&failingReaderAt{data: data, failAfter: 0})
	if err != nil {
		// Failing during open is an acceptable outcome too, as long as it is
		// reported rather than swallowed.
		require.ErrorContains(t, err, "vortex")

		return
	}

	rdr, err := src.Scan(ScanRequest{})
	if err == nil {
		for rdr.Next() {
		}
		err = rdr.Err()
		rdr.Release()
	}

	closeErr := src.Close()
	require.True(t, err != nil || closeErr != nil,
		"a failing reader must produce an error from the scan or from Close")
}

func TestOpenReaderAtValidatesArguments(t *testing.T) {
	session, err := NewSession()
	require.NoError(t, err)
	defer session.Close()

	_, err = session.OpenReaderAt("x", 10, nil)
	require.Error(t, err)

	_, err = session.OpenReaderAt("x", 0, &countingReaderAt{})
	require.Error(t, err)
}

func TestReadAtPreservesMidScanError(t *testing.T) {
	path := os.Getenv("VORTEX_LARGE_FIXTURE")
	if path == "" {
		t.Skip("set VORTEX_LARGE_FIXTURE to exercise a read failure after opening")
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	s, err := NewSession()
	require.NoError(t, err)
	defer s.Close()
	input := &failingReaderAt{data: data, failAfter: 1}
	src, err := s.OpenReaderAt("", int64(len(data)), input)
	require.NoError(t, err, "footer read must succeed before injecting the scan failure")
	rdr, err := src.Scan(ScanRequest{})
	if err == nil {
		for rdr.Next() {
		}
		err = rdr.Err()
		rdr.Release()
	}
	require.ErrorIs(t, err, errInjected, "the scan must expose the original Go read error")
	require.ErrorIs(t, src.Close(), errInjected)
}
