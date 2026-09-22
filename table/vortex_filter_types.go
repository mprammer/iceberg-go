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
	"fmt"

	"github.com/apache/iceberg-go"
)

// validateVortexFilterTypes rejects numeric promotions that the shared residual would narrow.
func validateVortexFilterTypes(filter iceberg.BooleanExpression, logical, physical *iceberg.Schema) error {
	if filter == nil || logical == nil || physical == nil {
		return nil
	}
	ids, err := iceberg.ExtractFieldIDs(filter)
	if err != nil {
		return err
	}
	for _, id := range ids {
		source, found := physical.FindTypeByID(id)
		if !found {
			continue
		}
		target, found := logical.FindTypeByID(id)
		if !found {
			continue
		}
		if (source.Equals(iceberg.PrimitiveTypes.Int32) && target.Equals(iceberg.PrimitiveTypes.Int64)) ||
			(source.Equals(iceberg.PrimitiveTypes.Float32) && target.Equals(iceberg.PrimitiveTypes.Float64)) {
			return fmt.Errorf("%w: Vortex filtering on field %d promoted from %s to %s", iceberg.ErrNotImplemented, id, source, target)
		}
	}
	return nil
}
