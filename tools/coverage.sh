#!/usr/bin/env bash
# Dev driver for Rust test coverage (prototype: control-plane/core).
#
# Builds the //tools/coverage:<crate> genrule under the coverage config, prints
# the llvm-cov table to STDOUT (for agentic/terminal use), and writes lcov.info,
# report.txt, and a browsable HTML report into the gitignored .loom/coverage/.
#
# Usage: tools/coverage.sh [crate]          (default crate: core)
#
# COST NOTE: the FIRST run under --config loom.coverage=true recompiles the
# dependency graph with -Cinstrument-coverage (a separate build configuration),
# which can saturate all cores. On a constrained machine cap it with e.g.
#   LOOM_COVERAGE_JOBS=6 tools/coverage.sh
# Subsequent runs are cache hits and cheap.
#
# Do NOT run `buck2 test`/`buck2 run` under --config loom.coverage=true: those
# execute instrumented binaries without LLVM_PROFILE_FILE set and litter
# default_*.profraw into the repo root. This driver (via cover.sh) always
# captures profiles into a temp dir, so it never leaks.
set -euo pipefail

crate="${1:-core}"
target="//tools/coverage:${crate}"
jobs="${LOOM_COVERAGE_JOBS:-}"
qcfg=(--config loom.coverage=true)            # queries: config only (reject -j)
cfg=("${qcfg[@]}")                            # builds: config + optional job cap
[ -n "$jobs" ] && cfg+=(-j "$jobs")

# Keep this in sync with the -ignore-filename-regex in tools/coverage/BUCK:
# drop absolute paths (stdlib/cargo), third-party crates, and test files.
ignore_regex='^/|^third-party/|/tests/'

out=.loom/coverage
mkdir -p "$out"

# 1. Build the cacheable artifacts (lcov + report + merged profdata).
buck2 build "${cfg[@]}" "$target" >/dev/null
lcov="$(buck2 build "${cfg[@]}" "${target}[lcov]" --show-simple-output 2>/dev/null)"
report="$(buck2 build "${cfg[@]}" "${target}[report]" --show-simple-output 2>/dev/null)"
profdata="$(buck2 build "${cfg[@]}" "${target}[profdata]" --show-simple-output 2>/dev/null)"
cp "$lcov" "$out/lcov.info"
cp "$report" "$out/report.txt"
cp "$profdata" "$out/coverage.profdata"

# 2. Render HTML from the cached profdata. llvm-cov show needs the source tree
#    (present here, unlike the genrule sandbox) and the instrumented binaries —
#    discovered from the genrule's own rust_test deps, so there's no duplicated
#    target list. show reads the binaries' coverage maps; it does not run them.
llvm="$(buck2 build "${cfg[@]}" toolchains//:llvm-x86_64-linux --show-simple-output 2>/dev/null)"
bins=()
while read -r t; do
    t="${t%% (*}"   # strip cquery's " (cfg#hash)" suffix -> plain label
    [ -n "$t" ] && bins+=("$(buck2 build "${cfg[@]}" "$t" --show-simple-output 2>/dev/null)")
done < <(buck2 cquery "${qcfg[@]}" "kind('rust_test', deps(${target}))" 2>/dev/null)

if [ "${#bins[@]}" -eq 0 ]; then
    echo "coverage: found no rust_test deps under ${target}; cannot render HTML." >&2
    exit 1
fi

head_bin="${bins[0]}"
object_args=()
for b in "${bins[@]:1}"; do object_args+=("-object" "$b"); done
rm -rf "$out/html"
"$llvm/bin/llvm-cov" show -format=html -output-dir="$out/html" \
    -instr-profile="$out/coverage.profdata" \
    -ignore-filename-regex="$ignore_regex" \
    "$head_bin" "${object_args[@]}" 2>/dev/null

# 3. Emit the table to stdout (agentic/terminal signal), then point at the rest.
cat "$out/report.txt"
echo
echo "lcov : $out/lcov.info   (editor gutters)"
echo "html : $out/html/index.html   (browser)"
