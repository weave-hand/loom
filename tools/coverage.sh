#!/usr/bin/env bash
# tools/coverage.sh — dev wrapper: run the BXL coverage pipeline, copy outputs
# out of buck-out, and (best-effort) render an HTML report with source context.
#
# Usage:
#   tools/coverage.sh                        # whole codebase, all 22 crates
#   tools/coverage.sh control-plane/core     # single crate (package under src/)
#
# Output lands in .loom/coverage/ (gitignored):
#   combined/      — combined.profdata, lcov.info, report.txt (all crates in scope)
#   <crate>/       — per-crate report.txt + lcov.info
#   html/          — HTML source-annotated report (best-effort; needs source tree)
#
# COST NOTE:
#   The first instrumented build recompiles every crate in scope with
#   -Cinstrument-coverage (~same wall time as a clean build). Fixture crates
#   (postgres, worker, ingest, query-api) boot initdb + postgres locally and are
#   pinned local-only in the bxl; they're the slow part. Cap parallelism with:
#     LOOM_COVERAGE_JOBS=4 tools/coverage.sh
#   Subsequent runs are cache hits and fast.
#
# FOOTGUN:
#   Never run an instrumented test binary directly outside this flow. The bxl's
#   run_one.sh captures profraw to a declared buck-out path. Running binaries
#   built under the coverage_enabled modifier outside that flow emits
#   default_<pid>.profraw files into the working directory, which can pollute
#   subsequent coverage merges. This wrapper never executes binaries directly.

set -euo pipefail

crate="${1:-}"
out=".loom/coverage"
log=$(mktemp /tmp/coverage-XXXXXX.log)

# Build the -j flag array (honor LOOM_COVERAGE_JOBS if set, else let buck2 choose)
jobs="${LOOM_COVERAGE_JOBS:-}"
jflag=()
[ -n "$jobs" ] && jflag=(-j "$jobs")

echo "==> Running BXL coverage pipeline (log: $log)" >&2

# Build the bxl arg array
bxl_args=(//tools/coverage:cov.bxl:cov)
[ -n "$crate" ] && bxl_args+=(-- --crate "$crate")

# Run the bxl and capture all stdout. buck2 stdout is fully consumed by the
# $(...) substitution, so this does NOT stall (the stall happens only with an
# unconsumed live pipe like `buck2 ... | tail` at the shell level). We extract
# the last line (the combined-dir path) from the captured string with awk —
# never by piping the live buck2 process to tail.
bxl_stdout=$(buck2 bxl "${jflag[@]}" "${bxl_args[@]}" 2>"$log")
combined_dir=$(printf '%s\n' "$bxl_stdout" | awk 'END{print}')

if [ -z "$combined_dir" ]; then
    echo "ERROR: bxl produced no output; see $log" >&2
    exit 1
fi

echo "==> BXL combined dir: $combined_dir" >&2

# Prepare local output directory
rm -rf "$out"
mkdir -p "$out"

# Copy combined dir (contains report.txt, lcov.info, combined.profdata after
# the report.sh tweak that mirrors profdata into the output dir)
cp -r "$combined_dir" "$out/combined"

# Copy per-crate dirs: they are siblings of combined_dir under the same parent.
bxl_root=$(dirname "$combined_dir")
for d in "$bxl_root"/*/; do
    dname=$(basename "$d")
    [ "$dname" = "combined" ] && continue
    [ -f "${d}report.txt" ] && cp -r "$d" "$out/$dname"
done

echo "==> Copied coverage outputs to $out/" >&2

# ── Combined HTML report (best-effort; source tree is present here) ───────────
# Reuse the EXACT combined-ignore regex the bxl used — report.sh writes it into
# the combined dir (ignore.regex), so there is no second copy to drift. Fall
# back to the known pattern only if the file is somehow absent.
if [ -f "$out/combined/ignore.regex" ]; then
    COMBINED_IGNORE=$(cat "$out/combined/ignore.regex")
else
    COMBINED_IGNORE='^/|^third-party/|/tests/|^src/control-plane/testkit/'
fi

echo "==> Resolving LLVM dist path (cache hit from bxl run)" >&2
llvm=$(buck2 build "${jflag[@]}" toolchains//:llvm-x86_64-linux --show-simple-output 2>>"$log")
llvm="${llvm%$'\n'}"  # strip trailing newline

# Discover all rust_test targets in scope
if [ -n "$crate" ]; then
    test_universe="//src/${crate}/..."
else
    test_universe="//src/..."
fi

echo "==> Discovering instrumented test binaries (cache hits)" >&2
test_targets=$(buck2 cquery "kind('rust_test', ${test_universe})" 2>>"$log") || {
    echo "WARNING: cquery failed; skipping HTML report (see $log)" >&2
    test_targets=""
}

bins=()
if [ -n "$test_targets" ]; then
    while IFS= read -r t; do
        [ -z "$t" ] && continue
        # Strip configured-target suffix (e.g. " (cfg:...)" or " (<hash>)")
        label=$(printf '%s' "$t" | awk '{print $1}')
        bin=$(buck2 build "${jflag[@]}" -m //tools/coverage:coverage_enabled \
            "$label" --show-simple-output 2>>"$log") || continue
        bin="${bin%$'\n'}"
        [ -n "$bin" ] && bins+=("$bin")
    done <<< "$test_targets"
fi

profdata="$out/combined/combined.profdata"

if [ ${#bins[@]} -gt 0 ] && [ -f "$profdata" ]; then
    echo "==> Generating HTML report (${#bins[@]} binaries)" >&2
    mkdir -p "$out/html"
    object_args=()
    for b in "${bins[@]:1}"; do
        object_args+=(-object "$b")
    done
    "$llvm/bin/llvm-cov" show \
        -format=html \
        -output-dir="$out/html" \
        -instr-profile="$profdata" \
        -ignore-filename-regex="$COMBINED_IGNORE" \
        "${bins[0]}" "${object_args[@]}" 2>>"$log" \
        || echo "WARNING: HTML generation failed (see $log)" >&2
else
    echo "WARNING: HTML step skipped (bins=${#bins[@]}, profdata=$(test -f "$profdata" && echo present || echo missing))" >&2
fi

# ── Results ───────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cat "$out/combined/report.txt"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  lcov data : $out/combined/lcov.info"
[ -f "$out/html/index.html" ] && echo "  HTML      : $out/html/index.html"
echo "  full log  : $log"
