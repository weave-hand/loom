#!/usr/bin/env bash
# Run clippy over every first-party Rust target in the buck2 graph and fail if
# any of them emitted diagnostics.
#
# buck2 has no `//...[clippy.txt]` syntax (sub-targets don't combine with a
# recursive pattern), so we query the Rust targets first, then request each
# one's `[clippy.txt]` sub-target. That file is empty when clippy is clean and
# holds the warning/error text otherwise. New crates are picked up automatically
# — nothing here is hard-coded to a specific target.
#
# Usage: tools/clippy-all.sh  (run from the repo root, e.g. via prek)
set -euo pipefail

# Collect every FIRST-PARTY Rust target, then project each onto its clippy
# diagnostics file. Scope is root//src/... — we lint our own crates, not the
# vendored third-party deps under //third-party (whose clippy is not our concern
# and which may not even lint cleanly under buck2's clippy driver).
mapfile -t subtargets < <(
    buck2 uquery "kind('rust_(binary|library|test)', set(root//src/...))" 2>/dev/null \
        | sed 's/$/[clippy.txt]/'
)

if [ "${#subtargets[@]}" -eq 0 ]; then
    echo "clippy-all: no Rust targets found"
    exit 0
fi

# Building the sub-targets runs clippy; --show-output prints "<target> <path>".
mapfile -t outputs < <(
    buck2 build "${subtargets[@]}" --show-output 2>/dev/null | awk '{print $2}'
)

status=0
for out in "${outputs[@]}"; do
    if [ -s "$out" ]; then
        cat "$out"
        status=1
    fi
done

exit "$status"
