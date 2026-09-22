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
# Regenerate the current Rust reference corpus for Iceberg reader tests.
# Usage: generate.sh [OUTPUT_DIRECTORY]
# Requires Cargo and Git; current-source pin matches internal/vortexffi/build-lib.sh.
# VORTEX_RUST_SOURCE reuses a pinned checkout.
# VORTEX_GIT_URL selects a mirror. Each Cargo package has its own lockfile.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
output_dir=${1:-"$script_dir"}
mkdir -p -- "$output_dir"
output_dir=$(cd -- "$output_dir" && pwd)
work_dir=$(mktemp -d)
trap 'rm -rf -- "$work_dir"' EXIT
current_revision=46a8d39c032b1e9f8ae28a13efe8abc9ebad556f
checkout_source() {
    local revision=$1 reuse=$2 destination=$3
    if [[ -n "$reuse" ]]; then
        [[ "$(git -C "$reuse" rev-parse HEAD)" == "$revision" ]]
        ln -s "$(cd -- "$reuse" && pwd)" "$destination"
    else
        git init --quiet "$destination"
        git -C "$destination" remote add origin "${VORTEX_GIT_URL:-https://github.com/vortex-data/vortex.git}"
        git -C "$destination" fetch --quiet --depth 1 origin "$revision"
        git -C "$destination" checkout --quiet --detach FETCH_HEAD
    fi
}
mkdir -p "$work_dir/current"
checkout_source "$current_revision" "${VORTEX_RUST_SOURCE:-}" "$work_dir/current/vortex"
cp -R "$script_dir/generators/regressions" "$work_dir/current/testgen"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-"$work_dir/target"}
profile=${VORTEX_BUILD_PROFILE:-release}
for generator in pco_regressions pco_modes zstd_legacy alp_patches zoned_stats default_writer independent_corpus; do
    cargo run --locked --profile "$profile" --manifest-path "$work_dir/current/testgen/Cargo.toml" \
        --bin "$generator" -- "$output_dir/rust"
done
# Check every expected output after its generator has completed successfully.
for file in pco_regressions zstd_legacy zstd_legacy_empty \
    alp_patches alp_patches_slice alp_patches_late_slice alp_patches_boundary alp_patches_empty \
    zoned_ints zoned_ints_chunked_stats \
    default_repeated default_progression default_broad default_chunked default_precision \
    owned_nullable owned_alp_flat; do
    test -s "$output_dir/rust/$file.vortex"
done
printf 'Generated current Rust fixtures in %s (revision %s)\n' "$output_dir" "$current_revision"
