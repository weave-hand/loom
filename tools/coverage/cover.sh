#!/usr/bin/env bash
# Coverage worker — runs INSIDE a buck2 genrule action (see tools/coverage/BUCK).
# Args: <llvm_dist_dir> <out_dir> <ignore_regex> <test_bin>...
# Runs each instrumented rust_test binary under LLVM_PROFILE_FILE, merges the
# .profraw files, and writes lcov.info + report.txt into <out_dir>, dropping
# files matching <ignore_regex> (absolute paths like the rust stdlib, third-party
# crates, and the test files themselves, leaving the library's own src/). Exits
# non-zero with a clear
# message if no coverage data was produced (target built without
# --config loom.coverage=true).
#
# NOTE: LLVM 22's llvm-cov ignores a positional source-path filter; the only
# working restriction is -ignore-filename-regex (confirmed by the spike).
set -euo pipefail

llvm_dir="$1"; out_dir="$2"; ignore_regex="$3"; shift 3
bins=("$@")

prof_dir="$(mktemp -d)"
i=0
for bin in "${bins[@]}"; do
    LLVM_PROFILE_FILE="$prof_dir/cov-$i.profraw" "$bin" >/dev/null 2>&1 || true
    i=$((i + 1))
done

shopt -s nullglob
raws=("$prof_dir"/*.profraw)
if [ ${#raws[@]} -eq 0 ]; then
    echo "coverage: no .profraw produced — instrumented binaries are required." >&2
    echo "coverage: build with --config loom.coverage=true" >&2
    exit 1
fi

"$llvm_dir/bin/llvm-profdata" merge -sparse "${raws[@]}" -o "$prof_dir/merged.profdata"

# First binary is positional; the rest are passed via -object.
head_bin="${bins[0]}"
object_args=()
for bin in "${bins[@]:1}"; do
    object_args+=("-object" "$bin")
done

"$llvm_dir/bin/llvm-cov" export -format=lcov \
    -instr-profile="$prof_dir/merged.profdata" \
    -ignore-filename-regex="$ignore_regex" \
    "$head_bin" "${object_args[@]}" > "$out_dir/lcov.info"

"$llvm_dir/bin/llvm-cov" report \
    -instr-profile="$prof_dir/merged.profdata" \
    -ignore-filename-regex="$ignore_regex" \
    "$head_bin" "${object_args[@]}" > "$out_dir/report.txt"
