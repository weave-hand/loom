"""Shared wrapper for first-party `rust_test` targets.

Test code legitimately panics on failed setup (`unwrap`/`expect`/indexing in a
test is the idiomatic way to assert), so the panic-safety lints in the enforced
clippy set are allowed for every test target. The *non-panic* enforced lints
(stray `dbg!`/`todo!`, swallowed must-use, `#[allow]` without a reason, …) stay
on, so test code is still gated for those.

Why a wrapper and not the toolchain's `rustc_test_flags`: that prelude field is
unusable — `rust_binary.bzl:rust_test_impl` does `extra_flags += ["--test"]` on
the frozen provider list when it is non-empty (`rust_binary.bzl:561`), a Starlark
mutation error that breaks every `rust_test` build. Per-target `rustc_flags`
(what this wrapper sets) flows through a different, non-mutating path.

Usage: add `load("//src:loom_test.bzl", "rust_test")` to a BUCK file and existing
`rust_test(...)` calls pick up the exemption with no call-site changes. The
`loom_fixture_test` macro injects the same `LOOM_TEST_LINT_ALLOWS`.
"""

# Panic-safety / test-assertion lints allowed for test + harness code only.
LOOM_TEST_LINT_ALLOWS = [
    "-Aclippy::unwrap_used",
    "-Aclippy::expect_used",
    "-Aclippy::indexing_slicing",
    "-Aclippy::panic",
    "-Aclippy::get_unwrap",
    "-Aclippy::unwrap_in_result",
    "-Aclippy::panic_in_result_fn",
    "-Aclippy::unreachable",
]

def rust_test(rustc_flags = [], **kwargs):
    native.rust_test(
        rustc_flags = rustc_flags + LOOM_TEST_LINT_ALLOWS,
        **kwargs
    )
