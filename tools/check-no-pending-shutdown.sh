#!/usr/bin/env bash
# Fails if a service binary's `main.rs` uses `pending()` as its shutdown source.
# That is a future which never resolves, so the graceful-shutdown plumbing behind
# it can never fire and the process is SIGKILLed at the end of its container
# termination grace period — the defect fixed in #587. Binaries must use
# `Shutdown::install(...)` + `.signalled()` from `loom_lifecycle` instead.
#
# `service_runtime::serve()` legitimately uses it (it is the explicit "serve until
# the process is killed" helper, used only by test support), so the check is scoped
# to `src/services/*/src/main.rs`. It matches the bare call form as well as the
# `future::pending` path form, and the `^[^/]*` prefix keeps a `//` comment that
# merely MENTIONS the old shutdown source from tripping it — those comments are
# exactly what the fixed mains carry. Invoked by prek with the changed Rust files
# as arguments (`pass_filenames = true`).
set -euo pipefail

# Call syntax only, and only when no `/` precedes it on the line (so `// ... pending()`
# in a comment does not match).
pat='^[^/]*\bpending[[:space:]]*(::<[^>]*>)?[[:space:]]*\('

status=0
for f in "$@"; do
    case "$f" in
        src/services/*/src/main.rs) : ;;
        *) continue ;;
    esac
    if grep -nE "$pat" "$f" >/dev/null; then
        echo "error: $f calls pending() — a shutdown source that never resolves."
        echo "       use service_runtime::Shutdown::install(...) + .signalled() (see #587)."
        grep -nE "$pat" "$f" | sed 's/^/         /'
        status=1
    fi
done
exit "$status"
