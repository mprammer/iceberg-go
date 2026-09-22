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

package table_test

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/apache/iceberg-go/vortex"
	"github.com/stretchr/testify/require"
)

func TestVortexInvalidPostscriptReturnsScanError(t *testing.T) {
	for _, backend := range vortex.AvailableBackends() {
		t.Run(string(backend), func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				postscript []byte
			}{
				{"missing", nil},
				{"root_out_of_range", []byte{255, 255, 255, 255}},
				{"invalid_vtable", []byte{4, 0, 0, 0, 255, 255, 255, 127}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx := vortex.WithBackend(t.Context(), backend)
					location, filePath := stageVortexFixture(t)
					tx := newVortexTable(t, location).NewTransaction()
					require.NoError(t, tx.AddFiles(ctx, []string{filePath}, nil, false))
					data := append([]byte("VTXF"), tc.postscript...)
					trailer := make([]byte, 8)
					binary.LittleEndian.PutUint16(trailer, 1)
					binary.LittleEndian.PutUint16(trailer[2:], uint16(len(tc.postscript)))
					copy(trailer[4:], "VTXF")
					require.NoError(t, os.WriteFile(filePath, append(data, trailer...), 0o600))
					scan, err := tx.Scan()
					require.NoError(t, err)
					result, err := scan.ToArrowTable(ctx)
					if result != nil {
						result.Release()
					}
					require.Error(t, err)
				})
			}
		})
	}
}
