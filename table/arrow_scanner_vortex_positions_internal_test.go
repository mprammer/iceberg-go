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

package table

import (
	"testing"

	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/require"
)

func TestVortexBatchPositionsResetIndependentCursors(t *testing.T) {
	source := &rowPositionSource{}
	lineage, deletes := source.cursor(), source.cursor()
	source.setBatch([]tblutils.RowGroupSpan{{FirstRowPos: 5, NumRows: 2}, {FirstRowPos: 10, NumRows: 1}})
	require.True(t, source.pruned())
	require.Equal(t, []int64{5, 6, 10}, []int64{lineage.next(), lineage.next(), lineage.next()})
	require.Equal(t, []int64{5, 6, 10}, []int64{deletes.next(), deletes.next(), deletes.next()})
	source.setBatch([]tblutils.RowGroupSpan{{FirstRowPos: 20, NumRows: 2}})
	require.EqualValues(t, 20, lineage.next())
	require.EqualValues(t, 20, deletes.next())
	require.EqualValues(t, 21, deletes.next())
	require.EqualValues(t, 21, lineage.next())
	source.setBatch(nil)
	require.True(t, source.pruned(), "even empty positioned batches must use the cursor path")
	source.setBatch([]tblutils.RowGroupSpan{{FirstRowPos: 30, NumRows: 1}})
	require.EqualValues(t, 30, lineage.next())
	require.EqualValues(t, 30, deletes.next())
}
