#!/usr/bin/env bash
# Fails if a first-party Rust *source* file contains an inline `#[test]` /
# `#[tokio::test]`. buck2 only executes `rust_test` integration targets — an inline
# `#[cfg(test)]` unit test inside a `rust_library`/`rust_binary` src compiles but
# is SILENTLY NEVER RUN. Put unit tests in `tests/<name>.rs` wired as a `rust_test`
# target instead. See CLAUDE.md ("Testing").
#
# Integration tests (anything under a `tests/` directory) legitimately use
# `#[test]`/`#[tokio::test]` and are skipped. Invoked by prek with the changed Rust
# files as arguments (`pass_filenames = true`).
set -euo pipefail

status=0
for f in "$@"; do
    case "$f" in
        */tests/*) continue ;;        # integration tests: #[test] is correct there
    esac
    case "$f" in
        src/*.rs | */src/*.rs) : ;;   # only first-party crate sources
        *) continue ;;
    esac
    if grep -nE '^[[:space:]]*#\[(test|tokio::test)' "$f" >/dev/null; then
        echo "error: inline test in $f — buck2 does not run inline #[cfg(test)] tests."
        echo "       move it to tests/<name>.rs + a rust_test target (see CLAUDE.md)."
        grep -nE '^[[:space:]]*#\[(test|tokio::test)' "$f" | sed 's/^/         /'
        status=1
    fi
done
exit "$status"
