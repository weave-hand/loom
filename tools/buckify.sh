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
buck2 run root//tools:reindeer -- buckify "$@"

# Reorder: each archive is "<pkg>-<ver>.crate", its run is "<pkg>-<ver>-build-script-run".
python3 - "$BUCK_FILE" <<'PYFIX'
import re, sys

path = sys.argv[1]
with open(path) as f:
    content = f.read()

pattern = re.compile(
    r'(buildscript_run\(\s*\n\s*name = "([^"]+)-build-script-run".*?\n\))'
    r'(.*?)'
    r'(http_archive\(\s*\n\s*name = "\2\.[^"]*\.crate".*?\n\))',
    re.DOTALL,
)

content = pattern.sub(lambda m: m.group(4) + m.group(3) + m.group(1), content)

with open(path, "w") as f:
    f.write(content)
PYFIX

echo "buckify complete (with ordering fix applied)"
