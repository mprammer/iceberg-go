<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# vortexffi

A read-only cgo binding to the Vortex C API. It opens files through the caller's
`io.ReaderAt`, exposes schema and exact row count, and returns ordered Arrow
batches. The shared Iceberg adapter selects this backend with
`vortex.WithBackend(ctx, vortex.FFI)` in a build with `-tags vortex` and cgo enabled.
The native backend needs neither this package nor a Rust linker.

## Reproducible library build

The required ABI is Vortex revision
`01f23e7f7b0a496a44929afbe8c6f10eca427625`, the branch `mp/iceberg-go` on the
fork the build script fetches from. It carries two changes that are in upstream
review and not yet in a released revision:

- the ReaderAt callback constructor, row-index expression, and packed
  projections, without which this binding cannot supply its own I/O.
- preservation of the serialized patch offset within a chunk when reading
  sliced ALP and BitPacked arrays. A revision without it resets that offset to
  zero and misreads valid files sliced across chunk boundaries.

Both land upstream eventually; when they do, this pin moves to an upstream
revision and the fork drops out. The superseded patch files under `patches/`
are the earlier form of those changes against `46a8d39c03` and are no longer
applied.

```sh
internal/vortexffi/build-lib.sh /tmp/vortex-ffi-build
CGO_LDFLAGS=-L/tmp/vortex-ffi-build/target/release \
LD_LIBRARY_PATH=/tmp/vortex-ffi-build/target/release \
go test -tags vortex ./internal/vortexffi
```

The script requires Git, a C linker, and Cargo. The pinned checkout specifies
Rust 1.97.1; this integration was also built and tested with Rust 1.98.0. The
script fetches that source revision, applies both patches, builds with `--locked`,
and compares its generated header with `include/vortex.h`. Choose an output
directory without an existing `source` subdirectory.

All source and default Cargo cache/build output live under the chosen directory.
Existing `CARGO_HOME`, `CARGO_TARGET_DIR`, and toolchain overrides remain effective.
`VORTEX_BUILD_PROFILE=dev` selects an unoptimized build and puts libraries in
`target/debug`; `VORTEX_GIT_URL` can point to a local Git checkout containing the
pinned revision. No global Git or Cargo configuration is modified. On macOS, use
`DYLD_LIBRARY_PATH` instead of `LD_LIBRARY_PATH`.

The source header must match the library. The C API is not ABI stable, so an
unrelated release of `libvortex_ffi` is not a compatible replacement.

## Ownership and errors

- Each source owns a cloned session handle; active scans retain the source.
  Closing a session or source prevents new operations while existing scans finish.
- Release each record reader. A reader's current batch is borrowed until the next
  `Next` or `Release`; call `Retain` to keep a batch longer. Retained batches remain
  valid after the reader and source close.
- `OpenReaderAt` borrows the caller's file and never closes it. Release scans and
  close the source before closing that file. Source cleanup drains active Go
  reads and prevents later queued callbacks from touching the file. Rust's actual
  release callback frees its registry handle once its final reference is dropped.
- `OpenBuffer` copies the supplied bytes and uses the same callback path, so an
  Arrow batch never borrows memory freed by the Go source wrapper.
- Reader errors, including context cancellation, preserve their Go identity in
  open/scan errors and `Close`. Short reads fail; panics in a caller's ReaderAt
  become errors at the callback boundary. The shared Iceberg adapter supplies
  context checks around I/O and batch reads.
- Filter expressions are copied into the Rust scan. Their arena can close after
  `Scan` returns. Scans preserve storage order; filtered rows need an explicit
  row-index projection when their original positions are required.

## Larger I/O fixture

The checked-in `simple.vortex` is about 13 KB and fits in the initial footer
read. Generate the larger fixture to verify projection I/O savings and a failure
that occurs after opening the footer:

```sh
cd /tmp/vortex-ffi-build/source
CARGO_HOME=/tmp/vortex-ffi-build/cargo-home \
CARGO_TARGET_DIR=/tmp/vortex-ffi-build/target \
cargo run --locked --release -p vortex-ffi --example iceberg_go_fixture -- \
  /tmp/vortex-large.vortex
```

Then, from the Iceberg-Go directory:

```sh
VORTEX_LARGE_FIXTURE=/tmp/vortex-large.vortex \
CGO_LDFLAGS=-L/tmp/vortex-ffi-build/target/release \
LD_LIBRARY_PATH=/tmp/vortex-ffi-build/target/release \
go test -tags vortex -race -v ./internal/vortexffi
```

The deterministic generator writes 100,000 rows in eight input batches with an
integer ID and two wide strings. In the validated development build the file was
25,064,480 bytes; projecting `id` read 65,655 bytes in two callbacks (0.26%) and
returned every ID in file order. The generated file remains outside the repository.
