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

# (3) Reconcile two public majors of one crate. The iceberg writer (slice 2) needs
#     arrow/parquet 57; ingest/datafusion-io need 58. Both majors are direct deps of
#     some workspace member, so reindeer marks both "public" and emits one bare
#     alias per major — BOTH named e.g. `parquet` — which buck2 rejects as a double
#     registration. The native fix (a Cargo dep rename) is honoured only from the
#     ROOT package's deps (index.rs: "Only the root package's renames matter"), and
#     loom is a VIRTUAL workspace with no root package, so renames are ignored for
#     alias naming. reindeer also hardcodes the versioned library to Visibility
#     ::Private, so a `visibility` fixup (which only tunes the alias) can't expose
#     it. Hence two deterministic corrections here:
#       (a) widen the specific :*-57 targets first-party code depends on to PUBLIC;
#       (b) de-duplicate each bare alias to its highest public version, so existing
#           bare-alias users (datafusion-io → //third-party:parquet) keep 58 while
#           postgres depends on the explicit //third-party:parquet-57 target.
#     Upstream-clean fixes (build dep_renamed from all members, or add a
#     per-(package,version) alias-name fixup) are tracked for a reindeer PR.
PUBLIC_VERSIONED_TARGETS = ["parquet-57", "arrow-array-57", "arrow-schema-57"]
for name in PUBLIC_VERSIONED_TARGETS:
    lib = re.compile(
        r'(cargo\.rust_library\(\s*\n\s*name = "' + re.escape(name) + r'",.*?\n)(\s*)visibility = \[\]',
        re.DOTALL)
    content, n = lib.subn(lambda m: m.group(1) + m.group(2) + 'visibility = ["PUBLIC"]', content)
    assert n == 1, f"visibility pass: expected exactly one {name} library, found {n}"

# (b) Dedupe bare aliases. For each alias name emitted more than once, keep the one
#     pointing at the highest version (parsed from the `:name-<ver>` actual) and drop
#     the rest. First-party code that needs a non-highest version uses the explicit
#     versioned target (made PUBLIC above). Intentionally permissive: when reindeer
#     emits no duplicate (the common case for every other crate), this no-ops. A
#     genuinely-missed duplicate would still be caught loudly — by buck2's own
#     double-registration error — so there is no silent-failure path here.
alias_block = re.compile(
    r'\nalias\(\s*\n\s*name = "([^"]+)",\s*\n\s*actual = ":([^"]+)",\s*\n(?:\s*visibility = \[[^\]]*\],\s*\n)?\)\n')
def ver_key(actual):
    # actual is "<crate>-<ver>"; take the trailing version, compare numerically.
    ver = actual.rsplit("-", 1)[-1]
    return tuple(int(p) if p.isdigit() else 0 for p in ver.split("."))
by_name = {}
for m in alias_block.finditer(content):
    by_name.setdefault(m.group(1), []).append(m)
drop_spans = []
for name, ms in by_name.items():
    if len(ms) > 1:
        keep = max(ms, key=lambda m: ver_key(m.group(2)))
        drop_spans += [(m.start(), m.end()) for m in ms if m is not keep]
for start, end in sorted(drop_spans, reverse=True):
    # The match consumes the blank-line separator before the alias and the newline
    # after its `)`; splice with "" (not "\n") so the kept neighbour keeps exactly
    # one blank-line separator rather than gaining a second.
    content = content[:start] + content[end:]

with open(path, "w") as f:
    f.write(content)
PYFIX

echo "buckify complete (ordering + native-link fixes applied)"
