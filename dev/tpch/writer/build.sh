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
# Build the pinned reference writer; the final stdout line is the executable path.
# Usage: build.sh [BUILD_DIRECTORY]
# VORTEX_RUST_SOURCE optionally supplies a local Git checkout of the pinned revision.
# Its contents are never changed. VORTEX_GIT_URL can select a Git mirror.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "$script_dir/../../.." && pwd)
build_dir=${1:-"$repo_dir/.cache/tpch-writer"}
mkdir -p -- "$build_dir"
build_dir=$(cd -- "$build_dir" && pwd)
source_dir="$build_dir/vortex"
# Matches the revision internal/vortexffi/build-lib.sh builds the reader from:
# the fork branch mp/iceberg-go, current develop plus the two in-review topics.
revision=01f23e7f7b0a496a44929afbe8c6f10eca427625

if [[ ! -d "$source_dir/.git" ]]; then
    if [[ -e "$source_dir" ]]; then
        echo "Expected a Git checkout at $source_dir" >&2
        exit 1
    fi
    git init --quiet "$source_dir"
    origin=${VORTEX_RUST_SOURCE:-${VORTEX_GIT_URL:-https://github.com/mprammer/vortex.git}}
    git -C "$source_dir" fetch --quiet --depth 1 "$origin" "$revision"
    git -C "$source_dir" checkout --quiet --detach FETCH_HEAD
fi
if [[ "$(git -C "$source_dir" rev-parse HEAD)" != "$revision" ]]; then
    echo "Vortex source must be at $revision" >&2
    exit 1
fi

# HEAD alone cannot establish provenance for a reused checkout. Compare its
# contents with a separate index containing exactly the pin;
# never reset or stage changes in the checkout's own index.
verification_dir=$(mktemp -d)
trap 'rm -rf -- "$verification_dir"' EXIT
expected_git() {
    GIT_INDEX_FILE="$verification_dir/index" git -C "$source_dir" "$@"
}
expected_git read-tree "$revision"
if ! expected_git diff --no-ext-diff --quiet; then
    echo "Vortex source differs from $revision:" >&2
    expected_git diff --no-ext-diff --name-only >&2
    exit 1
fi
# Honor the source tree's ignore rules for generated build products, but not
# user-level excludes that could hide an extra Rust source or Cargo configuration.
unexpected_files=$(expected_git ls-files --others --exclude-per-directory=.gitignore)
if [[ -n "$unexpected_files" ]]; then
    printf 'Unexpected untracked files in Vortex source:\n%s\n' "$unexpected_files" >&2
    exit 1
fi
rm -rf -- "$verification_dir"
trap - EXIT

# The binary reports its own revision into every benchmark report, so a stale
# constant would misattribute the files it wrote. Fail the build instead.
if ! grep -q "const REVISION: &str = \"$revision\"" "$script_dir/src/main.rs"; then
    echo "writer REVISION constant does not match $revision" >&2
    exit 1
fi
mkdir -p "$build_dir/writer/src"
cp "$script_dir/Cargo.toml" "$script_dir/Cargo.lock" "$build_dir/writer/"
cp "$script_dir/src/"*.rs "$build_dir/writer/src/"
# Keep build products on the build directory's disk, independent of any global
# target setting. The user's configured rustc wrapper (e.g. kache) still applies.
profile=${VORTEX_BUILD_PROFILE:-release}
(cd -- "$build_dir/writer" && cargo build --locked --profile "$profile" \
    --target-dir "$build_dir/target")
if [[ "$profile" == dev ]]; then profile=debug; fi
printf '%s\n' "$build_dir/target/$profile/iceberg-tpch-vortex-writer"
