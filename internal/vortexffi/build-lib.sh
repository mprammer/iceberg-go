#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
set -euo pipefail

binding_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
build_dir=${1:?Usage: build-lib.sh OUTPUT_DIRECTORY}
mkdir -p -- "$build_dir"
build_dir=$(cd -- "$build_dir" && pwd)
source_dir="$build_dir/source"
if [[ -e "$source_dir" ]]; then
    echo "Source directory already exists: $source_dir; choose a new output directory." >&2
    exit 1
fi
# Temporary: the reader-at/expressions and sliced-patch topics are in upstream
# review. Until they land, build from the flattened fork branch mp/iceberg-go,
# which carries both on top of upstream develop a542cbd419. The patches/ files
# are the superseded 46a8d39 form and are no longer applied.
revision=01f23e7f7b0a496a44929afbe8c6f10eca427625
git init --quiet "$source_dir"
git -C "$source_dir" remote add origin "${VORTEX_GIT_URL:-https://github.com/mprammer/vortex.git}"
git -C "$source_dir" fetch --quiet --depth 1 origin "$revision"
git -C "$source_dir" checkout --quiet --detach FETCH_HEAD
mkdir -p "$source_dir/vortex-ffi/examples"
cp "$binding_dir/testdata/generate_large.rs" "$source_dir/vortex-ffi/examples/iceberg_go_fixture.rs"

export CARGO_HOME=${CARGO_HOME:-"$build_dir/cargo-home"}
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-"$build_dir/target"}
profile=${VORTEX_BUILD_PROFILE:-release}
(cd -- "$source_dir" && cargo build --locked --profile "$profile" -p vortex-ffi)
cmp "$binding_dir/include/vortex.h" "$source_dir/vortex-ffi/cinclude/vortex.h"
if [[ "$profile" == dev ]]; then profile=debug; fi
printf 'Vortex revision: %s (fork branch mp/iceberg-go)\n' "$revision"
printf 'Library directory: %s/%s\n' "$CARGO_TARGET_DIR" "$profile"
