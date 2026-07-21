# Python build infrastructure via muntjac Design

> **Status:** design (direction). This spec makes `road-python-build-infra` build-ready
> (promoted, with [[road-python-sdk-v1]], from `fut-python-bindings`). A separate work agent
> writes the implementation plan from it and builds it. Sequenced **before**
> `road-python-sdk-v1`, which consumes the machinery this spec adds.

## Problem

loom has no Python dependency machinery. That blocks the Python SDK
([[road-python-sdk-v1]]) outright, and has already cost coverage elsewhere — the
differential-oracle idea in FUTURE (`fut-iceberg-differential-oracle`) was deferred
explicitly "for lack of a clean Python-dependency path" (pyiceberg as an independent read
oracle). The tree's rule everywhere else is hermetic, pinned, buck2-native tooling
(reindeer for Rust, python-build-standalone CPython for the toolchain, prebuilt vendored
binaries under `//tools`), so ad-hoc `pip install` / host virtualenvs are not an option.

**muntjac** ([weave-hand/muntjac](https://github.com/weave-hand/muntjac), fork of
rsJames-ttrpg/muntjac, v0.2.0) is the reindeer-pattern answer for Python: it reads
`uv.lock` + `muntjac.toml` and emits `third-party/python/BUCK` (+ `muntjac.bzl`,
`wiring.bzl`, `config/BUCK`) — per-platform wheel `remote_file`-style downloads behind a
`pypi_package` macro, PEP 503/517 and marker evaluation handled. This is its **first
production adoption**; dogfood findings are part of the deliverable.

## Decision (operator, 2026-07-21): adopt muntjac + hermetic uv, reindeer-style

Operator decisions taken in the design session:

- **Gaps are fixed upstream.** When muntjac can't handle something (native-extension
  wheels, loom's `remote_python_toolchain`, RE), patch muntjac itself (fork or upstream)
  and re-consume — workarounds in loom's tree are the fallback, not the default. Findings
  are recorded as dogfood feedback either way.
- **uv is vendored too** — the lockflow (`uv lock`) must not depend on a host uv.

### Components

1. **`//tools:muntjac`** — pinned prebuilt binary in `tools/BUCK`, following the
   btd/rust-code-analysis pattern exactly: muntjac has **no prebuilt release binaries
   today** ("planned post-launch"), so the first work item is adding a release workflow to
   muntjac (upstream `rsJames-ttrpg/muntjac` or the `weave-hand` fork — wherever the tag
   lands, loom consumes that repo's tarball) producing per-arch `.tar.gz` assets with
   sha256 sidecars. Then `MUNTJAC_VERSION` + per-arch `sha256`s in `tools/BUCK`.
2. **`//tools:uv`** — astral-sh/uv ships prebuilt per-arch tarballs
   (`uv-<triple>.tar.gz` + `.sha256` sidecars); same genrule pattern, `UV_VERSION`
   pinned in `tools/BUCK`.
3. **`muntjac.toml`** at the repo root: `manifest_path` pointing at the SDK's
   `pyproject.toml` (`src/sdk/python/pyproject.toml` — **this item** creates it with the
   SDK's real dependency set (`httpx`, `pyarrow`, `pydantic`) but no package code, so the
   pipeline is proven standalone; [[road-python-sdk-v1]] fills the package in),
   `third_party_dir = "third-party/python"`,
   `python_versions = ["3.13"]` (matching `CPYTHON_VERSION = "3.13.6"` in
   `toolchains/BUCK` — single-version policy, like the single Rust nightly), platforms
   `linux-x86_64-gnu` + `linux-aarch64-gnu` with `manylinux = "2_28"` (pyarrow's cp313
   linux-gnu wheels are `manylinux_2_28`-only, and a 2_28 platform still accepts the
   older-tag `2_17` wheels other deps, e.g. pydantic-core, publish; no macOS, same as
   the Rust toolchain). Fixups registry: `"none"`
   initially; in-tree `third-party/python/fixups/` if a wheel needs one.
4. **`tools/pybuckify.sh`** — the `buckify.sh` analog: activates the hermetic env, runs
   `uv lock` → `muntjac vendor` → `muntjac buckify` via the vendored binaries. The only
   supported way to (re)generate `third-party/python/`.
5. **`muntjac-check` prek hook** — mirrors `reindeer-check`: when
   `pyproject.toml`/`uv.lock` under the muntjac tree change, run `pybuckify.sh` and
   `git diff --exit-code third-party/python/`, so generated rules can't drift.
6. **`tools/env.sh`** exposes `uv` and `muntjac` alongside reindeer/prek/btd.

### Toolchain seam

loom keeps its existing `toolchains//:python` (`remote_python_toolchain` over hermetic
python-build-standalone CPython). muntjac's fixture skeleton assumes
`system_python_toolchain`, but its generated rules are macro-over-prelude-rules and should
be toolchain-agnostic; any place they aren't is the first dogfood finding, fixed upstream.
Wheel fetches are pinned-URL downloads → RE-compatible and cache-friendly; nothing
`local_only` lands on the common build path (per `docs/build-execution.md`).

### Acceptance

- `buck2 build //third-party/python/...` green (RE), from a lockfile containing the SDK's
  real deps (`httpx`, `pyarrow`, `pydantic`) — pyarrow's large native wheel is the
  canary.
- A minimal `python_test` under `//src/...` that imports all three from the hermetic
  toolchain and passes in the normal `buck2 test //src/...` sweep (proves interpreter ↔
  cp313 wheel agreement end-to-end; the SDK's real tests replace it as the load-bearing
  proof once [[road-python-sdk-v1]] lands).
- prek `muntjac-check` red when `third-party/python/` is stale, green after
  `pybuckify.sh`.

### Out of scope

- macOS / Windows platforms (loom is linux-only end-to-end today).
- The community fixups registry (stay `"none"` until a wheel forces the question).
- Multi-tree config (single dependency universe until proven otherwise).
- The SDK itself — [[road-python-sdk-v1]].
