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
	"context"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

func TestScanRetainsClosedSourceAndSession(t *testing.T) {
	data := fixtureBytes(t)
	s, err := NewSession()
	require.NoError(t, err)
	src, err := s.OpenReaderAt("", int64(len(data)), &countingReaderAt{data: data})
	require.NoError(t, err)

	rdr, err := src.Scan(ScanRequest{Projection: []string{"id"}})
	require.NoError(t, err)
	defer rdr.Release()
	require.NoError(t, src.Close())
	require.NoError(t, s.Close())
	_, err = src.Schema()
	require.ErrorContains(t, err, "closed")
	_, err = src.Scan(ScanRequest{})
	require.ErrorContains(t, err, "closed")
	count, exact := src.RowCount()
	require.Zero(t, count)
	require.False(t, exact)

	var row int32
	for rdr.Next() {
		ids := rdr.RecordBatch().Column(0).(*array.Int32)
		for i := range ids.Len() {
			require.Equal(t, row, ids.Value(i), "storage order must survive owner close")
			row++
		}
	}
	require.NoError(t, rdr.Err())
	require.EqualValues(t, 1000, row)
}

func TestRetainedBatchSurvivesReaderAndSource(t *testing.T) {
	data := fixtureBytes(t)
	s, err := NewSession()
	require.NoError(t, err)
	src, err := s.OpenBuffer(data)
	require.NoError(t, err)
	// OpenBuffer owns its own copy, including when its first read is lazy.
	clear(data)
	rdr, err := src.Scan(ScanRequest{})
	require.NoError(t, err)
	require.True(t, rdr.Next())
	batch := rdr.RecordBatch()
	batch.Retain()
	defer batch.Release()
	rdr.Release()
	require.NoError(t, src.Close())
	require.NoError(t, s.Close())
	runtime.GC()

	ids := batch.Column(0).(*array.Int32)
	names := batch.Column(1).(*array.StringView)
	for i := range ids.Len() {
		require.EqualValues(t, i, ids.Value(i))
		require.Equal(t, i%7 == 0, names.IsNull(i))
		if !names.IsNull(i) {
			require.NotEmpty(t, names.Value(i))
		}
	}
}

func TestClosedSessionRejectsOpen(t *testing.T) {
	s, err := NewSession()
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
	_, err = s.OpenReaderAt("x", 1, &countingReaderAt{})
	require.ErrorContains(t, err, "closed")
	_, err = s.OpenBuffer([]byte{0})
	require.ErrorContains(t, err, "closed")
	_, err = s.OpenPath(fixturePath)
	require.ErrorContains(t, err, "closed")
}

type readerAtFunc func([]byte, int64) (int, error)

func (f readerAtFunc) ReadAt(p []byte, off int64) (int, error) { return f(p, off) }

func TestReadAtCallbackFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		read readerAtFunc
		want error
	}{
		{"short", func(p []byte, _ int64) (int, error) { return len(p) - 1, nil }, io.ErrUnexpectedEOF},
		{"cancelled", func([]byte, int64) (int, error) { return 0, context.Canceled }, context.Canceled},
		{"full EOF", func(p []byte, _ int64) (int, error) { return len(p), io.EOF }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := &registeredReader{r: tc.read}
			err := rr.readAt(make([]byte, 10), 0)
			require.ErrorIs(t, err, tc.want)
			require.ErrorIs(t, rr.firstErr(), tc.want)
		})
	}

	rr := &registeredReader{r: readerAtFunc(func([]byte, int64) (int, error) { panic("reader bug") })}
	require.ErrorContains(t, rr.readAt(make([]byte, 1), 0), "reader bug")
}

func TestOpenReaderAtPreservesContextError(t *testing.T) {
	s, err := NewSession()
	require.NoError(t, err)
	defer s.Close()
	_, err = s.OpenReaderAt("", 1000, readerAtFunc(func([]byte, int64) (int, error) {
		return 0, context.Canceled
	}))
	require.ErrorIs(t, err, context.Canceled)
}

func TestReaderStopDrainsActiveCallback(t *testing.T) {
	started := make(chan struct{})
	finish := make(chan struct{})
	rr := &registeredReader{r: readerAtFunc(func(p []byte, _ int64) (int, error) {
		close(started)
		<-finish

		return len(p), nil
	})}
	var wg sync.WaitGroup
	wg.Go(func() { require.NoError(t, rr.readAt(make([]byte, 1), 0)) })
	<-started
	stopped := make(chan struct{})
	wg.Go(func() { rr.stop(); close(stopped) })
	select {
	case <-stopped:
		t.Fatal("stop must wait for the active callback")
	case <-time.After(10 * time.Millisecond):
	}
	close(finish)
	wg.Wait()
	require.ErrorIs(t, rr.readAt(make([]byte, 1), 0), io.ErrClosedPipe)
	require.NoError(t, rr.firstErr(), "teardown must not turn a successful scan into an I/O error")
}
