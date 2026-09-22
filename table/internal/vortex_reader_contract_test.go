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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

var errVortexTestRead = errors.New("injected Vortex FileIO read failure")

type vortexTrackingFS struct {
	iceio.LocalFS
	file      *vortexTrackingFile
	failReads bool
}

func (fs *vortexTrackingFS) Open(path string) (iceio.File, error) {
	f, err := fs.LocalFS.Open(path)
	if err != nil {
		return nil, err
	}
	fs.file = &vortexTrackingFile{File: f}
	fs.file.failReads.Store(fs.failReads)

	return fs.file, nil
}

type vortexTrackingFile struct {
	iceio.File
	readAtCalls atomic.Int64
	readCalls   atomic.Int64
	closeCalls  atomic.Int64
	failReads   atomic.Bool
}

func (f *vortexTrackingFile) Read(p []byte) (int, error) {
	f.readCalls.Add(1)

	return 0, errors.New("Vortex must use the FileIO ReaderAt")
}

func (f *vortexTrackingFile) ReadAt(p []byte, off int64) (int, error) {
	f.readAtCalls.Add(1)
	if f.failReads.Load() {
		return 0, errVortexTestRead
	}

	return f.File.ReadAt(p, off)
}

func (f *vortexTrackingFile) Close() error {
	f.closeCalls.Add(1)

	return f.File.Close()
}

func testVortexCountOnlyProjection(t *testing.T, ctx context.Context) {
	fs := &vortexTrackingFS{}
	rdr, err := vortexFormat{}.Open(ctx, fs, vortexFixture)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rdr.Close()) })

	schema, cols, err := rdr.PrunedSchema(map[int]struct{}{}, fixtureNameMapping())
	require.NoError(t, err)
	require.Zero(t, schema.NumFields())
	require.NotNil(t, cols, "empty projection must remain distinct from nil/all columns")
	require.Empty(t, cols)

	// An empty projection can use the exact physical row count. No data
	// decoding or further source reads are needed after the footer is open.
	reads := fs.file.readAtCalls.Load()
	fs.file.failReads.Store(true)
	records, err := rdr.GetRecords(ctx, cols, nil)
	require.NoError(t, err)
	defer records.Release()
	require.Zero(t, records.Schema().NumFields())
	var rows int64
	for records.Next() {
		record := records.RecordBatch()
		require.Zero(t, record.NumCols())
		rows += record.NumRows()
	}
	require.NoError(t, records.Err())
	require.EqualValues(t, fixtureRows, rows)
	require.Equal(t, reads, fs.file.readAtCalls.Load())
}

func testVortexReaderLifecycle(t *testing.T, ctx context.Context) {
	fs := &vortexTrackingFS{}
	rdr, err := vortexFormat{}.Open(ctx, fs, vortexFixture)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rdr.Close()) })
	require.NotNil(t, fs.file)
	require.Zero(t, fs.file.closeCalls.Load(), "opening must leave the source alive for the scan")
	require.Positive(t, fs.file.readAtCalls.Load())

	records, err := rdr.GetRecords(ctx, nil, nil)
	require.NoError(t, err)
	require.True(t, records.Next())
	require.NoError(t, records.Err())
	records.Release()
	require.Zero(t, fs.file.closeCalls.Load())
	require.Zero(t, fs.file.readCalls.Load())

	require.NoError(t, rdr.Close())
	require.EqualValues(t, 1, fs.file.closeCalls.Load())
	require.NoError(t, rdr.Close())
	require.EqualValues(t, 1, fs.file.closeCalls.Load(), "reader Close must be idempotent")
	_, err = rdr.GetRecords(ctx, nil, nil)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func testVortexCancellation(t *testing.T, ctx context.Context) {
	t.Run("before_open", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		fs := &vortexTrackingFS{}
		_, err := vortexFormat{}.Open(canceled, fs, vortexFixture)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, fs.file)
	})

	t.Run("before_scan", func(t *testing.T) {
		rdr := openFixture(t, ctx)
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := rdr.GetRecords(canceled, nil, nil)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("before_next", func(t *testing.T) {
		rdr := openFixture(t, ctx)
		canceled, cancel := context.WithCancel(ctx)
		defer cancel()
		records, err := rdr.GetRecords(canceled, nil, nil)
		require.NoError(t, err)
		defer records.Release()
		cancel()
		require.False(t, records.Next())
		require.ErrorIs(t, records.Err(), context.Canceled)
	})
}

func testVortexReadFailure(t *testing.T, ctx context.Context) {
	fs := &vortexTrackingFS{failReads: true}
	rdr, err := vortexFormat{}.Open(ctx, fs, vortexFixture)
	require.ErrorContains(t, err, errVortexTestRead.Error())
	require.Nil(t, rdr)
	require.NotNil(t, fs.file)
	require.EqualValues(t, 1, fs.file.closeCalls.Load(), "failed Open must release the source")
}

func TestVortexUnknownBackend(t *testing.T) {
	ctx := vortex.WithBackend(t.Context(), vortex.Backend("unknown"))
	fs := &vortexTrackingFS{}
	_, err := vortexFormat{}.Open(ctx, fs, vortexFixture)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	if fs.file != nil {
		require.EqualValues(t, 1, fs.file.closeCalls.Load())
	}
}
