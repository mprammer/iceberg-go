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
	"math/big"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

// The fixture is the same file the Iceberg reader tests use: 1000 rows, id
// 0..999, name NULL where id%7==0, score NULL where id%5==0, flag = id%3==0.
const fixturePath = "../../table/internal/testdata/vortex/simple.vortex"

func openFixture(t *testing.T) (*Session, *DataSource) {
	t.Helper()

	data, err := os.ReadFile(fixturePath)
	require.NoError(t, err)

	session, err := NewSession()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })

	src, err := session.OpenBuffer(data)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	return session, src
}

// TestRowIdxSurvivesAFilteredScan is the reason vx_expression_row_idx was added
// to the C FFI.
//
// Vortex applies a pushed-down predicate for real, so the surviving rows are not
// contiguous and their original file positions are otherwise unrecoverable. That
// is what forces Iceberg to choose between predicate pushdown and any feature
// defined in terms of physical row positions. Projecting the row index alongside
// the data removes the choice, and this test is the proof: with `id >= 990`
// pushed into the scan, the positions must come back as 990..999, not 0..9.
func TestRowIdxSurvivesAFilteredScan(t *testing.T) {
	_, src := openFixture(t)

	arena := NewExprArena()
	defer arena.Close()

	root, err := arena.Root()
	require.NoError(t, err)

	idCol, err := arena.GetItem(root, "id")
	require.NoError(t, err)

	pos, err := arena.RowIdx()
	require.NoError(t, err)

	projection, err := arena.Pack([]string{"id", "_pos"}, []*Expr{idCol, pos}, false)
	require.NoError(t, err)

	threshold, err := arena.LiteralInt32(990)
	require.NoError(t, err)

	filter, err := arena.Binary(OpGreaterEqual, idCol, threshold)
	require.NoError(t, err)

	rdr, err := src.Scan(ScanRequest{ProjectionExpr: projection, Filter: filter})
	require.NoError(t, err)
	defer rdr.Release()

	require.Equal(t, []string{"id", "_pos"},
		[]string{rdr.Schema().Field(0).Name, rdr.Schema().Field(1).Name})

	var ids []int32
	var positions []int64
	for rdr.Next() {
		batch := rdr.RecordBatch()

		idArr, ok := batch.Column(0).(*array.Int32)
		require.True(t, ok, "id: got %T", batch.Column(0))
		ids = append(ids, idArr.Int32Values()...)

		posArr, ok := batch.Column(1).(*array.Uint64)
		require.True(t, ok, "_pos: got %T", batch.Column(1))
		for i := range posArr.Len() {
			positions = append(positions, int64(posArr.Value(i)))
		}
	}
	require.NoError(t, rdr.Err())

	require.Len(t, ids, 10)
	require.Len(t, positions, 10)

	// id equals file position in this fixture, so the two must agree - and
	// neither may be a dense 0..9 sequence over the survivors.
	for i := range ids {
		require.EqualValues(t, ids[i], positions[i])
	}
	require.EqualValues(t, 990, positions[0])
	require.EqualValues(t, 999, positions[9])
}

// TestPackAssemblesAProjection covers the other addition: pack builds a struct
// from arbitrary expressions, so a projection can be reshaped rather than only
// trimmed. Renaming and reordering here stands in for the nested-struct pruning
// it exists to enable, which select alone cannot express.
func TestPackAssemblesAProjection(t *testing.T) {
	_, src := openFixture(t)

	arena := NewExprArena()
	defer arena.Close()

	root, err := arena.Root()
	require.NoError(t, err)

	score, err := arena.GetItem(root, "score")
	require.NoError(t, err)

	id, err := arena.GetItem(root, "id")
	require.NoError(t, err)

	// Reversed relative to the file, and renamed.
	projection, err := arena.Pack([]string{"the_score", "the_id"}, []*Expr{score, id}, false)
	require.NoError(t, err)

	rdr, err := src.Scan(ScanRequest{ProjectionExpr: projection})
	require.NoError(t, err)
	defer rdr.Release()

	require.Equal(t, "the_score", rdr.Schema().Field(0).Name)
	require.Equal(t, "the_id", rdr.Schema().Field(1).Name)

	rows, nonNullScore := 0, 0
	for rdr.Next() {
		batch := rdr.RecordBatch()
		rows += int(batch.NumRows())

		scores, ok := batch.Column(0).(*array.Float64)
		require.True(t, ok, "the_score: got %T", batch.Column(0))
		nonNullScore += scores.Len() - scores.NullN()
	}
	require.NoError(t, rdr.Err())

	require.Equal(t, 1000, rows)
	require.Equal(t, 800, nonNullScore, "nulls must survive a packed projection")
}

func TestPackRejectsMismatchedArguments(t *testing.T) {
	arena := NewExprArena()
	defer arena.Close()

	root, err := arena.Root()
	require.NoError(t, err)

	_, err = arena.Pack([]string{"a", "b"}, []*Expr{root}, false)
	require.Error(t, err)

	_, err = arena.Pack(nil, nil, false)
	require.Error(t, err)
}

func TestDecimalLiteralRejectsOverflow(t *testing.T) {
	arena := NewExprArena()
	defer arena.Close()
	tooLarge := new(big.Int).Lsh(big.NewInt(1), 128)
	_, err := arena.LiteralDecimal(tooLarge, 38, 0)
	require.Error(t, err, "a value outside signed 128-bit range must not wrap to zero")
	_, err = arena.LiteralDecimal(big.NewInt(1), 257, 0)
	require.Error(t, err, "precision must not truncate to an 8-bit value")
	_, err = arena.LiteralDecimal(big.NewInt(1000), 2, 0)
	require.Error(t, err, "the C scalar's precision error must propagate")
	_, err = arena.LiteralDecimal(nil, 1, 0)
	require.Error(t, err)
}

func TestSignedDecimalEncodingBounds(t *testing.T) {
	for _, tc := range []struct {
		value   int64
		encoded byte
	}{
		{-128, 0x80}, {-1, 0xff}, {0, 0}, {127, 0x7f},
	} {
		encoded, err := twosComplementLE(big.NewInt(tc.value), 1)
		require.NoError(t, err)
		require.Equal(t, []byte{tc.encoded}, encoded)
	}
	for _, overflow := range []int64{-129, 128, 256} {
		_, err := twosComplementLE(big.NewInt(overflow), 1)
		require.Error(t, err)
	}
}
