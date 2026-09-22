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

// Package vortex configures the Vortex data-file reader used by Iceberg scans
// and file registration. Backend selection is local to a context, not table metadata.
package vortex

import "context"

// Backend selects the implementation used to read Vortex files.
type Backend string

const (
	// Native uses the pure-Go reader and requires no Rust library.
	Native Backend = "native"
	// FFI uses the Rust reader through cgo. Build with -tags vortex and link
	// the compatible libvortex_ffi documented in VORTEX.md.
	FFI Backend = "ffi"
)

type backendKey struct{}

// WithBackend selects a backend for operations using ctx, including AddFiles
// and scans. Unknown or unavailable backends return an error when a file is opened.
func WithBackend(ctx context.Context, backend Backend) context.Context {
	return context.WithValue(ctx, backendKey{}, backend)
}

// BackendFromContext returns the selected backend, defaulting to Native.
func BackendFromContext(ctx context.Context) Backend {
	if backend, ok := ctx.Value(backendKey{}).(Backend); ok {
		return backend
	}

	return Native
}

// AvailableBackends returns the backends compiled into this binary. Availability
// does not validate the contents of a file or the runtime library installation.
func AvailableBackends() []Backend {
	if ffiEnabled {
		return []Backend{Native, FFI}
	}

	return []Backend{Native}
}
