# Python toolchain over a pinned python-build-standalone CPython.
#
# This is a loom-local copy of the prelude's `remote_python_toolchain`
# (`prelude//toolchains:python.bzl`) with ONE behavioural change: the `cpython`
# `command_alias` sets `PYTHONDONTWRITEBYTECODE=1`.
#
# WHY (the bug this exists to prevent):
#
# The interpreter is executed in place, directly out of its materialized buck2
# output at `buck-out/v2/art/toolchains/<hash>/__cpython_archive__/cpython_archive/`.
# CPython byte-compiles each stdlib module it imports into a `__pycache__/`
# directory NEXT TO THE SOURCE — i.e. *inside* an artifact buck2 believes it owns
# exclusively. buck2 must be able to clean an output path to re-materialize it,
# and any file it did not put there can defeat that:
#
#   Error materializing artifact at path `.../__cpython_archive__/cpython_archive`
#   Error cleaning output path
#   remove_dir_all(...): Permission denied (os error 13)     <- .pyc owned by another uid
#   remove_dir_all(...): Directory not empty (os error 39)   <- a writer racing the clean
#
# Normally this is latent: the stray `.pyc` files are yours and `remove_dir_all`
# simply deletes them. It becomes a HARD WEDGE when either (a) something ran the
# interpreter as a different user (loom hit this with root-owned `logging`/
# `pathlib` `__pycache__`, left by an early claude-box that bind-mounted the host
# repo read-write and warmed the toolchain during its root phase), or (b) a writer
# races buck2's clean. Case (b) bit the BuildBuddy `affected` runner, whose
# `buck-out` is deliberately preserved across runs
# (`git clean -x -d --force -e buck-out`) — so it never self-heals and every retry
# fails identically. It presents as "flaky CI"; it is deterministic.
#
# Verified: `python -c 'import logging, pathlib'` writes 6 `__pycache__` entries
# into the artifact; with `PYTHONDONTWRITEBYTECODE=1` it writes none. The cost is
# re-compiling stdlib modules on each interpreter start, which is noise next to an
# action's own runtime.
#
# WHY A COPY RATHER THAN A PRELUDE PATCH: the prelude is a pinned git submodule
# and is bumped by checking out an upstream commit, so a local patch would be
# clobbered by every bump. Upstream `main` (94db932b6, 2026-07-31 — 285 commits
# past our pin) still has no `env` on that alias, so bumping does not fix it.
# `toolchains/` already carries loom copies of prelude toolchains for exactly this
# reason (`cxx_dist.bzl`, `rust_dist.bzl`), and the prelude's own docs expect its
# demo toolchains to be copy-pasted and configured.
#
# DELETE THIS FILE when upstream sets the env itself (or otherwise stops the
# interpreter mutating its own artifact) and the prelude pin has moved past it:
# drop `python_dist.bzl`, and restore the `remote_python_toolchain` load + call in
# `toolchains/BUCK`.

load("@prelude//:prelude.bzl", "native")
load("@prelude//toolchains:python.bzl", "python_bootstrap_toolchain", "python_toolchain")

def remote_python_toolchain_no_bytecode(
        name: str,
        visibility: list[str],
        cpython_urls: dict[str, dict[str, dict[str, str]]],
        bootstrap: bool = True,
        **kwargs) -> None:
    """`remote_python_toolchain`, but the interpreter never writes `.pyc` files.

    Mirrors the prelude macro's target names (`cpython_archive`, `cpython`,
    `libpython_symbols`, `<name>_bootstrap`) so call sites and any
    `$(location :cpython_archive[...])` references are unchanged.
    """
    native.http_archive(
        name = "cpython_archive",
        urls = [
            select({
                "prelude//os:{}".format(os): select({
                    "prelude//cpu:{}".format(cpu): archive["url"]
                    for cpu, archive in value.items()
                })
                for os, value in cpython_urls.items()
            }),
        ],
        sha256 = select({
            "prelude//os:{}".format(os): select({
                "prelude//cpu:{}".format(cpu): archive["sha256"]
                for cpu, archive in value.items()
            })
            for os, value in cpython_urls.items()
        }),
        strip_prefix = "python",
        sub_targets = {
            "include": [
                select({"DEFAULT": "include/python3.13", "prelude//os:windows": "include"}),
            ],
            "lib": [select({"DEFAULT": "lib", "prelude//os:windows": "libs"})],
            "python": [
                select({"DEFAULT": "bin/python", "prelude//os:windows": "python.exe"}),
            ],
        },
    )

    # The one line this whole file exists for. This alias is the interpreter for
    # BOTH the full and the bootstrap toolchain, so setting it here covers every
    # path that runs the hermetic python.
    native.command_alias(
        name = "cpython",
        exe = ":cpython_archive[python]",
        visibility = visibility,
        resources = [":cpython_archive"],
        env = {"PYTHONDONTWRITEBYTECODE": "1"},
    )

    if bootstrap:
        python_bootstrap_toolchain(
            name = "{}_bootstrap".format(name),
            visibility = visibility,
            interpreter = ":cpython",
        )

    native.genrule(
        name = "libpython_symbols",
        out = "linker_args",
        cmd = '$(exe_target prelude//python/tools:gather_libpython_symbols) "$OUT"',
    )

    python_toolchain(
        name = name,
        visibility = visibility,
        interpreter = ":cpython",
        extension_linker_flags = select({
            "DEFAULT": ["-L$(location :cpython_archive[lib])", "@$(location :libpython_symbols)"],
            "prelude//os:windows": ["/LIBPATH:$(location :cpython_archive[lib])"],
        }),
        **kwargs
    )
