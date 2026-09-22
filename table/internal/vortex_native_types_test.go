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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestVortexNativeScalarTypeBoundary(t *testing.T) {
	for _, dtype := range []arrow.DataType{
		arrow.FixedWidthTypes.Boolean, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64,
		arrow.BinaryTypes.String, arrow.BinaryTypes.Binary, arrow.FixedWidthTypes.Date32,
		&arrow.Decimal128Type{Precision: 15, Scale: 2}, &arrow.Decimal128Type{Precision: 38, Scale: 0},
	} {
		require.True(t, nativeVortexSupportedType(dtype), "%s", dtype)
	}
	for _, dtype := range []arrow.DataType{
		arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Int16, arrow.FixedWidthTypes.Date64,
		&arrow.TimestampType{Unit: arrow.Microsecond}, arrow.ListOf(arrow.PrimitiveTypes.Int32),
		&arrow.Decimal128Type{Precision: 0, Scale: 0}, &arrow.Decimal128Type{Precision: 39, Scale: 2},
		&arrow.Decimal128Type{Precision: 15, Scale: 16}, &arrow.Decimal128Type{Precision: 15, Scale: -1},
	} {
		require.False(t, nativeVortexSupportedType(dtype), "%s", dtype)
	}
}
