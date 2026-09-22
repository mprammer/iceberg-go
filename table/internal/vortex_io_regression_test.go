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
	"io"
	"os"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

type (
	vortexEOFFS   struct{ iceio.LocalFS }
	vortexEOFFile struct {
		iceio.File
		size int64
	}
)

func (fs vortexEOFFS) Open(path string) (iceio.File, error) {
	f, err := fs.LocalFS.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()

		return nil, err
	}

	return vortexEOFFile{File: f, size: st.Size()}, nil
}

func (f vortexEOFFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	if err == nil && n == len(p) && off+int64(n) == f.size {
		return n, io.EOF
	}

	return n, err
}

func TestVortexCompleteFinalReadWithEOF(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			r, err := vortexFormat{}.Open(ctx, vortexEOFFS{}, vortexFixture)
			require.NoError(t, err)
			defer r.Close()
			out, err := r.ReadTable(ctx)
			require.NoError(t, err)
			defer out.Release()
			require.EqualValues(t, fixtureRows, out.NumRows())
		})
	}
}

func TestVortexEmptyScalarFile(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			ctx := vortex.WithBackend(t.Context(), backend)
			r, err := vortexFormat{}.Open(ctx, iceio.LocalFS{}, "testdata/vortex/empty.vortex")
			require.NoError(t, err)
			defer r.Close()
			out, err := r.ReadTable(ctx)
			require.NoError(t, err)
			defer out.Release()
			require.Zero(t, out.NumRows())
			require.Equal(t, []string{"id", "value"}, fieldNames(out.Schema()))
			id, value := 1, 2
			mapping := iceberg.NameMapping{
				{Names: []string{"id"}, FieldID: &id},
				{Names: []string{"value"}, FieldID: &value},
			}
			_, selected, err := r.PrunedSchema(map[int]struct{}{id: {}}, mapping)
			require.NoError(t, err)
			for _, cols := range [][]int{nil, selected, {}} {
				rr, err := r.GetRecords(ctx, cols, nil)
				require.NoError(t, err)
				require.False(t, rr.Next())
				require.NoError(t, rr.Err())
				if cols != nil {
					require.Equal(t, len(cols), rr.Schema().NumFields())
				}
				rr.Release()
			}
		})
	}
}

func TestVortexNativeIteratorCleanup(t *testing.T) {
	for _, stop := range []string{"release", "cancel", "exhaustion", "read error"} {
		t.Run(stop, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx, cancel := context.WithCancel(compute.WithAllocator(t.Context(), mem))
			defer cancel()
			fs := &vortexTrackingFS{}
			src, err := fs.Open(vortexFixture)
			require.NoError(t, err)
			defer src.Close()
			stat, err := src.Stat()
			require.NoError(t, err)
			backend, err := openNativeVortex(ctx, src, stat.Size())
			require.NoError(t, err)
			defer backend.Close()
			rr, err := backend.Scan(ctx, nil, nil)
			require.NoError(t, err)
			if stop == "read error" {
				fs.file.failReads.Store(true)
				require.False(t, rr.Next())
				require.ErrorIs(t, rr.Err(), errVortexTestRead)
			} else {
				require.True(t, rr.Next(), "%v", rr.Err())
				switch stop {
				case "cancel":
					cancel()
					require.False(t, rr.Next())
					require.ErrorIs(t, rr.Err(), context.Canceled)
				case "exhaustion":
					for rr.Next() {
					}
					require.NoError(t, rr.Err())
				}
			}
			rr.Release()
			require.Nil(t, rr.RecordBatch())
			require.False(t, rr.Next())
			_, err = src.Stat()
			require.NoError(t, err, "record reader must not close caller-owned input")
		})
	}
}

type vortexByteFS struct {
	iceio.LocalFS
	file *vortexByteFile
}
type vortexByteFile struct {
	iceio.File
	bytes atomic.Int64
}

func (fs *vortexByteFS) Open(path string) (iceio.File, error) {
	f, err := fs.LocalFS.Open(path)
	if err != nil {
		return nil, err
	}
	fs.file = &vortexByteFile{File: f}

	return fs.file, nil
}

func (f *vortexByteFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.bytes.Add(int64(n))

	return n, err
}

func TestVortexNativeLargeFileEarlyStopIO(t *testing.T) {
	path := os.Getenv("VORTEX_LARGE_FIXTURE")
	if path == "" {
		t.Skip("set VORTEX_LARGE_FIXTURE to the Rust multi-chunk fixture")
	}
	ctx := vortex.WithBackend(t.Context(), vortex.Native)
	fs := &vortexByteFS{}
	r, err := vortexFormat{}.Open(ctx, fs, path)
	require.NoError(t, err)
	defer r.Close()
	opened := fs.file.bytes.Load()
	rr, err := r.GetRecords(ctx, nil, nil)
	require.NoError(t, err)
	defer rr.Release()
	require.Equal(t, opened, fs.file.bytes.Load(), "creating the iterator must not read data")
	require.True(t, rr.Next(), "%v", rr.Err())
	read := fs.file.bytes.Load() - opened
	t.Logf("file bytes=%d; metadata bytes=%d; first batch data bytes=%d; rows=%d", r.SourceFileSize(), opened, read, rr.RecordBatch().NumRows())
	require.Less(t, read, r.SourceFileSize(), "early stop must avoid future physical chunks")
}
