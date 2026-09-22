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

// This file holds nothing but the exported callback. cgo forbids a file
// containing //export directives from also defining anything in its preamble,
// so the C-side glue that takes this function's address lives in readat.go.

/*
#include <stdint.h>
#include <stdlib.h>
*/
import "C"

import "unsafe"

// vortexGoReadAt is called by Vortex, on its own worker threads, to fetch a
// byte range. It returns the number of bytes written; Vortex fails the read on
// a short count or a negative value.
//
//export vortexGoReadAt
func vortexGoReadAt(ctx unsafe.Pointer, offset C.uint64_t, dst *C.uint8_t, length C.size_t) C.int64_t {
	return C.int64_t(readAtFromC(ctx, uint64(offset), dst, length))
}

// vortexGoReleaseReader runs after Rust drops the last reference to the source,
// including references held by outstanding reads.
//
//export vortexGoReleaseReader
func vortexGoReleaseReader(ctx unsafe.Pointer) {
	releaseReaderFromC(ctx)
}
