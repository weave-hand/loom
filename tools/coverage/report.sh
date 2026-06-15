#!/usr/bin/env bash
# Merge a crate's .profraw files and render lcov + a text table.
# Args: <llvm_dist_dir> <out_dir> <profdata_out> <ignore_regex> \
#       <profraw>... -- <test_bin>...
# Writes <out_dir>/lcov.info and <out_dir>/report.txt, and the merged profile to
# <profdata_out>. LLVM 22's llvm-cov ignores a positional source filter, so the
# only restriction is -ignore-filename-regex.
set -euo pipefail
llvm="$1"; out_dir="$2"; profdata="$3"; ignore="$4"; shift 4

mkdir -p "$out_dir"
profraws=(); bins=(); mode="p"
for a in "$@"; do
    if [ "$a" = "--" ]; then mode="b"; continue; fi
    if [ "$mode" = "p" ]; then profraws+=("$a"); else bins+=("$a"); fi
done

"$llvm/bin/llvm-profdata" merge -sparse "${profraws[@]}" -o "$profdata"

head_bin="${bins[0]}"; object_args=()
for b in "${bins[@]:1}"; do object_args+=("-object" "$b"); done

"$llvm/bin/llvm-cov" export -format=lcov -instr-profile="$profdata" \
    -ignore-filename-regex="$ignore" "$head_bin" "${object_args[@]}" > "$out_dir/lcov.info"
"$llvm/bin/llvm-cov" report -instr-profile="$profdata" \
    -ignore-filename-regex="$ignore" "$head_bin" "${object_args[@]}" > "$out_dir/report.txt"
