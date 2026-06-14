# Hermetic python toolchain

## Problem

loom vendors every build toolchain hermetically — the Rust nightly
(`hermetic_rust_toolchain` over rustc/std/cargo dists), the C/C++ toolchain
(`hermetic_cxx_tools` over an LLVM dist), reindeer/prek/btd dev tools — *except
python*. `toolchains/BUCK` still wires:

```python
system_python_bootstrap_toolchain(name = "python_bootstrap", visibility = ["PUBLIC"])
```

which resolves to whatever `python3` happens to be on `PATH`. There are two
consumers of that system python, both non-hermetic:

1. **buck2's `python_bootstrap` toolchain** — the prelude requires it to exist,
   and prelude actions shell out to the resolved interpreter for every *local*
   Rust build/link step (`rustc_action.py`, `deferred_link_action.py`, etc.). On
   a host with no `python3`, every local Rust action dies with "Spawning
   executable python3 failed", which forces all work onto RE.
2. **`tools/buckify.sh`** — its reindeer post-processing runs an inline
   `python3 - <<PYFIX` (stdlib `re`/`sys` only) to reorder `http_archive` blocks
   and add native-link attrs. It needs `python3` on `PATH`, locally and in CI's
   `reindeer-check` hook.

This bit hard in the `ldev/claude-box` dev container: with no `python3`, local
Rust links were forced to RE, and RE-built binaries (glibc 2.39) couldn't run on
the container's older glibc — so the local-pinned fixture tests couldn't run. The
container has since been re-based on the RE worker image (so it now has a system
`python3` and a matching glibc), but the *repo* is still non-hermetic: a fresh
host or any environment without a suitable system `python3` hits the same wall.

## Goal

Make python hermetic like rust and llvm: pinned in `toolchains/BUCK`, materialized
by buck2 on host **and** RE, with no reliance on a system interpreter. Cover both
the `python_bootstrap` toolchain and a full `python_toolchain`, for linux x86_64
and aarch64.

## Approach

Do **not** hand-roll an `http_archive` + interpreter wrapper (the rust/llvm
path). The pinned prelude already ships `remote_python_toolchain`
(`prelude/toolchains/python.bzl`), which downloads a
[python-build-standalone](https://github.com/astral-sh/python-build-standalone)
CPython distribution (relocatable `install_only` tarball), exposes the
interpreter, and wires both the bootstrap and full toolchains via `select()` on
`prelude//os:*` / `prelude//cpu:*`. loom's default platform
(`root//platforms:default`, built from `host_configuration.cpu/os`) sets those
constraints, so the `select()` resolves.

A single call emits every target we need:

- `:cpython_archive` — the `http_archive` (per-arch via `select`), with
  `include` / `lib` / `python` sub-targets.
- `:cpython` — a `command_alias` over the interpreter (a `RunInfo`).
- `:python_bootstrap` — the hermetic `python_bootstrap_toolchain` (the name the
  prelude's rules look for), replacing `system_python_bootstrap_toolchain`.
- `:python` — the full `python_toolchain` (interpreter + extension linker flags),
  available for any future `python_binary` / `python_library` targets.

### Change 1 — `toolchains/BUCK`: adopt `remote_python_toolchain`, version pinned in-repo

`remote_python_toolchain` defaults `cpython_urls` to the prelude's
`CPYTHON_ARCHIVE` constant (currently CPython `3.13.6+20250807`). Inheriting that
default *is* a pin, but the pin lives in the prelude submodule — a prelude bump
could silently move the python version. To match loom's convention of pinning
every toolchain version explicitly next to `RUST_NIGHTLY` / `LLVM_VERSION`, pass
an in-repo `cpython_urls` (linux only — the only OS loom targets):

```python
# was:
load("@prelude//toolchains:python.bzl", "system_python_bootstrap_toolchain")
system_python_bootstrap_toolchain(name = "python_bootstrap", visibility = ["PUBLIC"])

# now:
load("@prelude//toolchains:python.bzl", "remote_python_toolchain")

# -- CPython (python-build-standalone, install_only_stripped) -----------------
# Version + release are the single source of truth, interpolated into the URLs
# (like RUST_NIGHTLY / LLVM_VERSION). To bump: update CPYTHON_VERSION /
# CPYTHON_RELEASE and the two sha256s from a python-build-standalone release.
# Covers 3.13.x patch bumps; a major bump also needs the prelude's python
# toolchain, which hardcodes `include/python3.13`.
CPYTHON_VERSION = "3.13.6"
CPYTHON_RELEASE = "20250807"
_CPYTHON_BASE = "https://github.com/astral-sh/python-build-standalone/releases/download"
CPYTHON_URLS = {
    "linux": {
        "x86_64": {
            "sha256": "e3e280d4b1ead63de6ebc9816de71792fc8c71b7a6a999ea82f937047beba037",
            "url": "{}/{}/cpython-{}+{}-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz".format(
                _CPYTHON_BASE, CPYTHON_RELEASE, CPYTHON_VERSION, CPYTHON_RELEASE),
        },
        "arm64": {
            "sha256": "829d615905b5ae8c50353f2ceb3d6665793442d4cbc64503bc9b27b5b9f6fb8a",
            "url": "{}/{}/cpython-{}+{}-aarch64-unknown-linux-gnu-install_only_stripped.tar.gz".format(
                _CPYTHON_BASE, CPYTHON_RELEASE, CPYTHON_VERSION, CPYTHON_RELEASE),
        },
    },
}

remote_python_toolchain(
    name = "python",
    bootstrap = True,
    cpython_urls = CPYTHON_URLS,
    visibility = ["PUBLIC"],
)
```

This is the crux of the change. `name = "python"` yields the full toolchain at
`:python` and the bootstrap toolchain at `:python_bootstrap` (the prelude appends
the `_bootstrap` suffix), matching the names buck2 resolves.

### Change 2 — expose a hermetic `python3` to dev tooling and CI

`tools/buckify.sh` runs `python3` as a plain shell command, so it needs the
hermetic interpreter on `PATH` — the same problem buckify already solves for
cargo/rustc by resolving `//tools:rust-host-toolchain` and prepending its `bin`.
Apply the identical pattern for python:

- **`tools/buckify.sh`**: resolve `toolchains//:cpython_archive` via
  `buck2 build --show-output` and prepend `$ARCHIVE/bin` to `PATH` before the
  `PYFIX` step. The `install_only` dist is relocatable — `bin/python3` finds its
  sibling `lib/`. This makes CI's `reindeer-check` hook hermetic too (no system
  python on the runner).
- **`tools/env.sh`**: resolve the same archive and symlink `.loom/bin/python3`
  (and `python`) at the hermetic interpreter, so the activated dev shell has it.

Reference the concrete `bin/python3` inside the materialized archive, **not** the
`:cpython` `command_alias` — env.sh already notes that `command_alias`
trampolines resolve a sibling via `$0` and break when symlinked into
`.loom/bin`.

### Change 3 — `ldev/claude-box` container: no change required

The dev container is now based on the RE worker image
(`gcr.io/flame-public/rbe-ubuntu24-04:latest`), which ships a system `python3`
(3.12) and a glibc matching RE. Once this toolchain lands, the buck2 build uses
the hermetic interpreter regardless, and `env.sh`/`buckify.sh` put the hermetic
`python3` ahead of the system one on `PATH`. No Dockerfile edit is needed; the
system python simply becomes an unused fallback. (Recorded here so the container
work and this change stay consistent — see the claude-box README.)

## Verification

- `buck2 build //src/...` and `buck2 test //src/...` stay green.
- `git grep -n system_python` returns nothing under `toolchains/`.
- `./tools/buckify.sh` regenerates a byte-identical `third-party/BUCK`
  (`git diff --exit-code third-party/BUCK` clean) using the hermetic interpreter;
  the `reindeer-check` hook passes.
- In an activated dev shell, `command -v python3` points into `.loom/bin` and
  `python3 --version` reports `3.13.6`.
- Build the toolchain on RE (not just `--local-only`) before merge — per the
  "verify native deps on RE" lesson, a hermetic dist that resolves locally can
  still fail to materialize on an RE worker.

## Risks and out of scope

- **Constraint alias check** — the prelude's `select()` keys on `prelude//cpu:x86_64`
  / `prelude//os:linux` (aliases), while `platforms/BUCK` uses
  `prelude//cpu/constraints:x86_64` / `prelude//os/constraints:linux`. These are
  the prelude's own conventions used by its own toolchain function, so they are
  expected to resolve; confirm during implementation with a real build.
- **Full toolchain is speculative** — loom has no `python_binary`/`python_library`
  targets today, so `:python` is wired but unexercised until python source is
  added. Included deliberately per the chosen scope.
- **OS coverage** — only linux dists are pinned (the only OS loom builds on/for).
  macOS/Windows are intentionally omitted; a non-linux target platform would fail
  to resolve the `select()`, which is acceptable.
- **No new python source** — this change adds a toolchain, not python code.
