#!/usr/bin/env bash
# Wrapper around `reindeer buckify` that regenerates third-party/BUCK from the
# workspace Cargo.lock, then fixes a known ordering issue in reindeer's output.
#
# reindeer emits each crate's buildscript_run() before its http_archive(). The
# buildscript_run prelude macro resolves the crate archive via rule_exists(),
# which only sees rules defined earlier in the file, so a build script that
# needs source files ends up with an empty manifest_dir. Fix: move each
# http_archive ahead of its matching buildscript_run.
#
# Usage: ./tools/buckify.sh   (run from anywhere; resolves the repo root itself)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUCK_FILE="$REPO_ROOT/third-party/BUCK"

cd "$REPO_ROOT"

# reindeer shells out to `cargo metadata`, so put loom's hermetic Rust toolchain
# (cargo + rustc + sysroot, assembled by //tools:rust-host-toolchain) ahead of
# anything on PATH. This keeps buckify reproducible and means no host rustup is
# needed — locally or in CI.
TOOLCHAIN="$REPO_ROOT/$(buck2 build root//tools:rust-host-toolchain --show-output 2>/dev/null | awk '{print $2}')"
export PATH="$TOOLCHAIN/bin:$PATH"
export LD_LIBRARY_PATH="$TOOLCHAIN/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# The PYFIX step below runs `python3`. Put loom's hermetic interpreter
# (python-build-standalone, via toolchains//:cpython_archive) ahead of PATH so
# buckify needs no system python — locally or in CI's reindeer-check hook. The
# install_only dist is relocatable, so bin/python3 finds its sibling lib/.
PY_ARCHIVE="$REPO_ROOT/$(buck2 build toolchains//:cpython_archive --show-output 2>/dev/null | awk 'NR==1{print $2}')"
export PATH="$PY_ARCHIVE/bin:$PATH"

buck2 run root//tools:reindeer -- buckify "$@"

# Two post-processing fixes to reindeer's output:
#
# (1) Ordering. reindeer emits each crate's buildscript_run() before its
#     http_archive(). The buildscript_run prelude macro resolves the crate
#     archive via rule_exists(), which only sees rules defined earlier in the
#     file, so a build script that needs source files ends up with an empty
#     manifest_dir. Fix: move each http_archive ahead of its buildscript_run.
#
# (2) Native-lib link directives. A `links = "..."` crate (e.g. libduckdb-sys
#     with the `bundled` feature) compiles a native lib in its build script and
#     emits cargo:rustc-link-lib / cargo:rustc-link-search so the final binary
#     links it. The prelude's buildscript_run only forwards those directives
#     when rustc_link_lib = True / rustc_link_search = True are set, but the
#     pinned reindeer never emits them — so the directives are silently dropped
#     and the final link fails with undefined symbols. reindeer surfaces a
#     `links` crate by setting CARGO_MANIFEST_LINKS in the run's env, so we key
#     off that: any buildscript_run whose env has CARGO_MANIFEST_LINKS gets both
#     attrs set to True. (Crates without native libs never set that env var.)
python3 - "$BUCK_FILE" <<'PYFIX'
import re, sys

path = sys.argv[1]
with open(path) as f:
    content = f.read()

# (1) Reorder: each archive is "<pkg>-<ver>.crate", its run is "<pkg>-<ver>-build-script-run".
order = re.compile(
    r'(buildscript_run\(\s*\n\s*name = "([^"]+)-build-script-run".*?\n\))'
    r'(.*?)'
    r'(http_archive\(\s*\n\s*name = "\2\.[^"]*\.crate".*?\n\))',
    re.DOTALL,
)
content = order.sub(lambda m: m.group(4) + m.group(3) + m.group(1), content)

# (2) Forward native-lib link directives for `links` crates (env has
#     CARGO_MANIFEST_LINKS). Insert the two bool attrs just before the closing
#     paren of each such buildscript_run(...) call.
run_call = re.compile(r'buildscript_run\(.*?\n\)', re.DOTALL)

def add_link_attrs(m):
    block = m.group(0)
    if "CARGO_MANIFEST_LINKS" not in block:
        return block
    if "rustc_link_lib" in block:
        return block
    # block ends in "...\n)"; insert the attrs as their own lines before it.
    assert block.endswith("\n)"), block[-20:]
    return block[:-2] + "\n    rustc_link_lib = True,\n    rustc_link_search = True,\n)"

content = run_call.sub(add_link_attrs, content)

with open(path, "w") as f:
    f.write(content)
PYFIX

echo "buckify complete (ordering + native-link fixes applied)"
