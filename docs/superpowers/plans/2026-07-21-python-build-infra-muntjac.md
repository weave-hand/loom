# Python build infrastructure via muntjac Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give loom hermetic, buck2-native Python dependency machinery — vendored muntjac + uv, a generated `third-party/python/` tree, and an RE-proven import test — implementing `road-python-build-infra` (spec: `docs/superpowers/specs/2026-07-21-python-build-infra-muntjac-design.md`).

**Architecture:** muntjac (weave-hand/muntjac, reindeer-pattern) reads `uv.lock` + root `muntjac.toml` and emits `third-party/python/{BUCK,muntjac.bzl,wiring.bzl,config/BUCK}` — per-platform wheel `http_file`s behind `prebuilt_python_library` variants selected via muntjac-generated `config_setting`s. Host-platform selection rides buck2 cfg modifiers (`set_cfg_modifiers` in scoped `PACKAGE` files; loom's root `PACKAGE` already registers the cfg constructor). muntjac and uv are consumed as pinned prebuilt release binaries in `tools/BUCK`, exactly like prek/btd.

**Tech Stack:** buck2 + prelude python rules, muntjac v0.2.1 (fork release cut by Task 1), uv 0.11.30, hermetic CPython 3.13.6 (`toolchains//:python`, already wired).

## Global Constraints

- Python version is **3.13 only** (`python_versions = ["3.13"]`), matching `CPYTHON_VERSION = "3.13.6"` in `toolchains/BUCK`.
- Platforms: `linux-x86_64-gnu` + `linux-aarch64-gnu`, `manylinux = "2_28"` (pyarrow cp313 ships manylinux_2_28-only; 2_28 also accepts pydantic-core's 2_17 wheels). No macOS/Windows/musl.
- All tooling hermetic: no host python/uv/muntjac anywhere — vendored binaries only, per CLAUDE.md.
- No `local_only` actions on the common build path (`docs/build-execution.md`); wheel fetches are `http_file` → RE-fine.
- Conventional Commits messages; run `buck2 run //tools:prek -- run --all-files` before every commit and `git add` new files first (prek skips untracked files).
- muntjac gaps are fixed in the weave-hand/muntjac fork (operator decision), not worked around in loom.
- The work branch is `work/road-python-build-infra` (claim already held). Lease-check before every push: `git ls-remote origin work/road-python-build-infra` must be an ancestor of local HEAD.

---

### Task 1: muntjac release with sha256 sidecars (weave-hand/muntjac repo)

muntjac already has `.github/workflows/release.yml` (tag-triggered, builds `muntjac-<target>.tar.gz` for linux x86_64/aarch64 + macOS arm64, binary at archive root) but: no `.sha256` sidecars, no `workflow_dispatch`, and the `github-release` job `needs` the `publish-crate` job — which can never succeed in the fork (no `CARGO_REGISTRY_TOKEN`), so a fork tag would publish nothing.

**Files (in a scratchpad clone of `weave-hand/muntjac`, NOT in loom):**
- Modify: `.github/workflows/release.yml`
- Modify: `Cargo.toml` (version `0.2.0` → `0.2.1`), `Cargo.lock` (same bump), `CHANGELOG.md`

**Interfaces:**
- Produces: GitHub release `v0.2.1` on `weave-hand/muntjac` with assets `muntjac-x86_64-unknown-linux-gnu.tar.gz`, `muntjac-aarch64-unknown-linux-gnu.tar.gz`, `muntjac-aarch64-apple-darwin.tar.gz`, each with a `.sha256` sidecar (`sha256sum` format: `<hex>  <filename>`). Task 2 consumes the two linux assets + sidecar hashes.

- [ ] **Step 1: Clone the fork into the scratchpad**

```bash
git clone https://github.com/weave-hand/muntjac /tmp/claude-1000/-workspace/6427494e-0996-4de6-9dbe-63caabf99057/scratchpad/muntjac
cd /tmp/claude-1000/-workspace/6427494e-0996-4de6-9dbe-63caabf99057/scratchpad/muntjac
```

- [ ] **Step 2: Patch `release.yml`**

Three edits, mirroring `rsJames-ttrpg/buck2-change-detector`'s release workflow (the pattern loom's other vendored tools follow):

1. Trigger — add `workflow_dispatch` alongside the tag trigger:

```yaml
on:
  push:
    tags: ["v*"]
  workflow_dispatch:
    inputs:
      tag:
        description: "Tag to create the release under (e.g. v0.2.1)"
        required: true
```

2. In the `build-binaries` matrix job, after the existing tar step (`tar -czf dist/muntjac-${{ matrix.target }}.tar.gz -C target/${{ matrix.target }}/release muntjac`), add the sidecar:

```bash
( cd dist && sha256sum "muntjac-${{ matrix.target }}.tar.gz" > "muntjac-${{ matrix.target }}.tar.gz.sha256" )
```

and extend the `actions/upload-artifact@v4` step's `path` to include `dist/*.sha256` (if it uploads `dist/*` already, no change needed — check).

3. Decouple the GitHub release from crates publishing: change `github-release`'s `needs: [publish-crate, build-binaries]` to `needs: [build-binaries]`, and gate `publish-crate` so it skips cleanly where the secret is absent (fork-friendly AND upstreamable):

```yaml
  publish-crate:
    if: github.repository == 'rsJames-ttrpg/muntjac'
```

Also make the release tag resolution honor the dispatch input: wherever the workflow uses `$GITHUB_REF_NAME` / `github.ref_name` for the release name, use `${{ github.event.inputs.tag || github.ref_name }}`.

- [ ] **Step 3: Bump version + changelog**

In `Cargo.toml`: `version = "0.2.1"`. Run `cargo update --workspace --offline 2>/dev/null || cargo update -p muntjac` to sync `Cargo.lock` (or edit the lock's `muntjac` version field directly — verify with `git diff`). Add a `CHANGELOG.md` entry under `## v0.2.1`:

```markdown
## v0.2.1

- release.yml: sha256 sidecar per release asset, `workflow_dispatch` trigger,
  GitHub release decoupled from crates.io publishing (fork-friendly).
```

- [ ] **Step 4: Commit and push to the fork's main**

```bash
git add -A
git commit -m "ci(release): sha256 sidecars, workflow_dispatch, decouple GH release from crates publish"
git push origin main
```

Expected: push accepted (weave-hand org access; if `main` is protected, push a branch + PR + merge instead).

- [ ] **Step 5: Tag and let the release build**

```bash
git tag v0.2.1
git push origin v0.2.1
gh run watch --repo weave-hand/muntjac $(gh run list --repo weave-hand/muntjac --workflow=release.yml --limit 1 --json databaseId --jq '.[0].databaseId')
```

Expected: workflow green; `gh release view v0.2.1 --repo weave-hand/muntjac --json assets --jq '.assets[].name'` lists the 3 `.tar.gz` + 3 `.tar.gz.sha256` assets.

- [ ] **Step 6: Record the two linux sha256s for Task 2**

```bash
for a in x86_64 aarch64; do
  curl -sL "https://github.com/weave-hand/muntjac/releases/download/v0.2.1/muntjac-$a-unknown-linux-gnu.tar.gz.sha256"
done
```

Copy the two hex digests — Task 2 pastes them into `tools/BUCK`. (No loom commit in this task; it's all in the muntjac repo.)

---

### Task 2: vendor `//tools:muntjac` and `//tools:uv` (loom repo, `work/road-python-build-infra`)

**Files:**
- Modify: `/workspace/tools/BUCK` (append two tool blocks at the end, after the `lucidshark-duplo` section)

**Interfaces:**
- Produces: buck2 targets `root//tools:muntjac` and `root//tools:uv` (`command_alias`es), plus concrete per-arch genrules `:muntjac-x86_64-linux` / `:uv-x86_64-linux` (and `-aarch64-`) whose single output is the executable. Tasks 3–6 invoke them via `buck2 run root//tools:muntjac -- <args>` / `buck2 run root//tools:uv -- <args>`, and `env.sh`/`pybuckify.sh` resolve the concrete genrules.

- [ ] **Step 1: Append the muntjac block to `tools/BUCK`**

muntjac's release assets put the binary at the **archive root** (no wrapper dir — `tar -C target/<t>/release … muntjac`), so no `--strip-components` (unlike prek). To bump: replace `MUNTJAC_VERSION`, refresh each sha256 from the `.tar.gz.sha256` sidecars.

```python
# muntjac (https://github.com/weave-hand/muntjac) — uv.lock → buck2 rules importer
# for Python third-party deps (the reindeer analog; see CLAUDE.md "Third-party
# Python deps"). Consumed from the weave-hand fork's release (upstream
# rsJames-ttrpg/muntjac ships no prebuilts). The release assets are `.tar.gz`
# with the binary at the archive ROOT (no wrapper dir, unlike prek), so the
# genrules untar without --strip-components. To bump: tag a new vX.Y.Z on
# weave-hand/muntjac, update MUNTJAC_VERSION, refresh each sha256 from the
# release's `.tar.gz.sha256` sidecars.

MUNTJAC_VERSION = "v0.2.1"

_MUNTJAC_URL = "https://github.com/weave-hand/muntjac/releases/download/{version}/muntjac-{triple}.tar.gz"

http_file(
    name = "muntjac-x86_64-linux.tar.gz",
    urls = [_MUNTJAC_URL.format(version = MUNTJAC_VERSION, triple = "x86_64-unknown-linux-gnu")],
    sha256 = "<x86_64 sha256 from Task 1 step 6>",
)

http_file(
    name = "muntjac-aarch64-linux.tar.gz",
    urls = [_MUNTJAC_URL.format(version = MUNTJAC_VERSION, triple = "aarch64-unknown-linux-gnu")],
    sha256 = "<aarch64 sha256 from Task 1 step 6>",
)

genrule(
    name = "muntjac-x86_64-linux",
    out = "muntjac",
    cmd = "mkdir -p $TMP/x && tar -xzf $(location :muntjac-x86_64-linux.tar.gz) -C $TMP/x && cp $TMP/x/muntjac $OUT && chmod +x $OUT",
    executable = True,
)

genrule(
    name = "muntjac-aarch64-linux",
    out = "muntjac",
    cmd = "mkdir -p $TMP/x && tar -xzf $(location :muntjac-aarch64-linux.tar.gz) -C $TMP/x && cp $TMP/x/muntjac $OUT && chmod +x $OUT",
    executable = True,
)

command_alias(
    name = "muntjac",
    exe = select({
        "prelude//cpu/constraints:arm64": ":muntjac-aarch64-linux",
        "prelude//cpu/constraints:x86_64": ":muntjac-x86_64-linux",
    }),
    visibility = ["PUBLIC"],
)
```

- [ ] **Step 2: Append the uv block to `tools/BUCK`**

uv's archives DO have a `uv-<triple>/` wrapper dir (like prek), so `--strip-components=1`. The archive also contains `uvx`; only `uv` is extracted.

```python
# uv (https://github.com/astral-sh/uv) — the Python package/dependency resolver
# muntjac drives (`uv lock`). Vendored so tools/pybuckify.sh needs no host uv.
# Assets are `.tar.gz` with a `uv-<triple>/` wrapper dir (strip-components=1,
# like prek); each has a `.sha256` sidecar. To bump: replace UV_VERSION and
# refresh each sha256 from the sidecars.

UV_VERSION = "0.11.30"

_UV_URL = "https://github.com/astral-sh/uv/releases/download/{version}/uv-{triple}.tar.gz"

http_file(
    name = "uv-x86_64-linux.tar.gz",
    urls = [_UV_URL.format(version = UV_VERSION, triple = "x86_64-unknown-linux-gnu")],
    sha256 = "04bc7d180d6138bf6dc08387acf507a823f397a98fea55da36b0ccc7fbce3b68",
)

http_file(
    name = "uv-aarch64-linux.tar.gz",
    urls = [_UV_URL.format(version = UV_VERSION, triple = "aarch64-unknown-linux-gnu")],
    sha256 = "8c11d90f5f66d232930cf8ae3a085c39877690d409e10878234802b028b20e2a",
)

genrule(
    name = "uv-x86_64-linux",
    out = "uv",
    cmd = "mkdir -p $TMP/x && tar -xzf $(location :uv-x86_64-linux.tar.gz) -C $TMP/x --strip-components=1 && cp $TMP/x/uv $OUT && chmod +x $OUT",
    executable = True,
)

genrule(
    name = "uv-aarch64-linux",
    out = "uv",
    cmd = "mkdir -p $TMP/x && tar -xzf $(location :uv-aarch64-linux.tar.gz) -C $TMP/x --strip-components=1 && cp $TMP/x/uv $OUT && chmod +x $OUT",
    executable = True,
)

command_alias(
    name = "uv",
    exe = select({
        "prelude//cpu/constraints:arm64": ":uv-aarch64-linux",
        "prelude//cpu/constraints:x86_64": ":uv-x86_64-linux",
    }),
    visibility = ["PUBLIC"],
)
```

- [ ] **Step 3: Verify both run**

```bash
buck2 run --console none root//tools:muntjac -- --help
buck2 run --console none root//tools:uv -- --version
```

Expected: muntjac prints its clap help (subcommands `init`, `vendor`, `buckify`, `fixups`, …); uv prints `uv 0.11.30`.

- [ ] **Step 4: Commit**

```bash
git add tools/BUCK
buck2 run //tools:prek -- run --all-files
git commit -m "feat(tools): vendor muntjac v0.2.1 + uv 0.11.30 prebuilt binaries"
```

---

### Task 3: SDK manifest, `muntjac.toml`, and the lockfile

**Files:**
- Create: `/workspace/src/sdk/python/pyproject.toml`
- Create: `/workspace/muntjac.toml` (repo root — muntjac resolves config at `<workdir>/muntjac.toml`, paths relative to it)
- Create (generated): `/workspace/src/sdk/python/uv.lock`

**Interfaces:**
- Produces: the manifest muntjac reads (`manifest_path = "src/sdk/python/pyproject.toml"`) and its committed `uv.lock`. Task 4's buckify consumes both. `road-python-sdk-v1` later fills in real package code behind this same manifest.

- [ ] **Step 1: Write `src/sdk/python/pyproject.toml`**

Per the spec, this item ships the real dependency set but **no package code**, so the project is a uv "virtual" project (`package = false` — nothing is built/installed from it; `road-python-sdk-v1` flips this when the package lands):

```toml
[project]
name = "loom-sdk"
version = "0.1.0"
description = "Python SDK for loom — data ingestion and pydantic ontology creation (road-python-sdk-v1 fills in the package)."
requires-python = ">=3.13"
dependencies = [
    "httpx>=0.28",
    "pyarrow>=25",
    "pydantic>=2.11",
]

[tool.uv]
package = false
```

(pydantic is a core `dependencies` entry HERE deliberately: the infra item must prove the native pydantic-core wheel path. The SDK item later moves it to `[project.optional-dependencies] pydantic` per its spec — a lock/buckify re-run, nothing structural.)

- [ ] **Step 2: Write `/workspace/muntjac.toml`**

```toml
# muntjac (uv.lock → buck2 rules) config — the Python analog of reindeer.toml.
# Paths are relative to this file (the repo root). Regenerate third-party/python
# with ./tools/pybuckify.sh; see CLAUDE.md "Third-party Python deps".

manifest_path = "src/sdk/python/pyproject.toml"
third_party_dir = "third-party/python"
# Single version, matching CPYTHON_VERSION in toolchains/BUCK (3.13.6).
python_versions = ["3.13"]

# pyarrow cp313 ships manylinux_2_28-only wheels; 2_28 also accepts the
# older-tag (2_17) wheels of pydantic-core. Linux-only, like the Rust toolchain.
[platforms.linux-x86_64-gnu]
target = "x86_64-unknown-linux-gnu"
manylinux = "2_28"

[platforms.linux-aarch64-gnu]
target = "aarch64-unknown-linux-gnu"
manylinux = "2_28"

[fixups]
registry = "none"
allow_local_overrides = true

[buck]
file_name = "BUCK"
vendor = false
```

- [ ] **Step 3: Validate the config**

```bash
buck2 run --console none root//tools:muntjac -- config check
```

Expected: exits 0 (validates muntjac.toml; any schema complaint here is a Task 3 bug — fix the toml).

- [ ] **Step 4: Generate the lockfile with the vendored uv**

```bash
buck2 run --console none root//tools:uv -- lock --project src/sdk/python
```

Expected: writes `src/sdk/python/uv.lock` resolving httpx (+ anyio/httpcore/h11/certifi/idna/sniffio), pyarrow, pydantic (+ pydantic-core/typing-extensions/annotated-types). Inspect: `grep -c 'name = ' src/sdk/python/uv.lock` ≥ 10.

- [ ] **Step 5: Commit**

```bash
git add src/sdk/python/pyproject.toml muntjac.toml src/sdk/python/uv.lock
buck2 run //tools:prek -- run --all-files
git commit -m "feat(python): loom-sdk manifest, muntjac.toml, and uv.lock"
```

---

### Task 4: `tools/pybuckify.sh` + generate and wire `third-party/python/`

**Files:**
- Create: `/workspace/tools/pybuckify.sh` (mode 755)
- Create (generated by muntjac): `/workspace/third-party/python/BUCK`, `/workspace/third-party/python/muntjac.bzl`, `/workspace/third-party/python/wiring.bzl`, `/workspace/third-party/python/config/BUCK`, `/workspace/third-party/python/prebake/` (gitignored contents)
- Create: `/workspace/third-party/python/PACKAGE`
- Create: `/workspace/src/sdk/PACKAGE`

**Interfaces:**
- Consumes: `root//tools:muntjac`, `root//tools:uv` (Task 2); `muntjac.toml` + `uv.lock` (Task 3).
- Produces: buildable targets `root//third-party/python:httpx`, `:pyarrow`, `:pydantic` (public aliases emitted by the generated `pypi_package` calls) — Task 5's test deps — and `tools/pybuckify.sh` as the only regeneration entry point (Task 6's prek hook runs it).

- [ ] **Step 1: Write `tools/pybuckify.sh`**

muntjac shells out to `uv` (lock-freshness check), so the vendored uv must be on PATH. Mirror `buckify.sh`'s structure:

```bash
#!/usr/bin/env bash
# Regenerate third-party/python/ from muntjac.toml + src/sdk/python/uv.lock:
# `uv lock` → `muntjac vendor` → `muntjac buckify`. The Python analog of
# tools/buckify.sh.
#
# --frozen skips the `uv lock` step and passes --frozen to muntjac (which
# forbids its own when-stale re-lock) — the deterministic, network-free form
# the muntjac-check prek hook and CI use. muntjac's staleness gate is a raw
# mtime comparison, and fresh CI checkouts have arbitrary mtimes; --frozen
# keeps the lint job from ever resolving against PyPI.
#
# vendor runs in prebake-only mode ([buck] vendor = false): with an all-wheels
# dependency set it only (re)writes third-party/python/prebake/'s manifest —
# it becomes load-bearing the day a dependency ships sdist-only.
#
# Usage: ./tools/pybuckify.sh [--frozen]   (run from anywhere)
set -euo pipefail

FROZEN=""
if [ "${1:-}" = "--frozen" ]; then FROZEN="--frozen"; fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# muntjac invokes `uv lock` when pyproject.toml is newer than uv.lock, so the
# vendored uv goes on PATH ahead of anything else — no host uv anywhere. Resolve
# the concrete per-arch genrule, NOT the :uv command_alias: alias trampolines
# resolve siblings via $0 and break when symlinked (same reason env.sh points at
# genrules directly).
case "$(uname -m)" in
  x86_64) UV_TARGET="root//tools:uv-x86_64-linux" ;;
  aarch64) UV_TARGET="root//tools:uv-aarch64-linux" ;;
  *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac
UV_BIN="$REPO_ROOT/$(buck2 build "$UV_TARGET" --show-output 2>/dev/null | awk '{print $2}')"
UV_DIR="$(mktemp -d)"
trap 'rm -rf "$UV_DIR"' EXIT
ln -s "$UV_BIN" "$UV_DIR/uv"
export PATH="$UV_DIR:$PATH"

if [ -z "$FROZEN" ]; then
    uv lock --project src/sdk/python
fi
buck2 run --console none root//tools:muntjac -- $FROZEN vendor
buck2 run --console none root//tools:muntjac -- $FROZEN buckify

echo "pybuckify complete"
```

(Symlinking the genrule output is safe — it is a real standalone binary, unlike the command_alias trampoline. The default mode's unconditional `uv lock` is what the spec's component 4 describes; `--frozen` is the check-mode variant.)

```bash
chmod +x tools/pybuckify.sh
```

- [ ] **Step 2: Run it — the first real muntjac buckify**

```bash
./tools/pybuckify.sh
git status --short third-party/python/
```

Expected: `third-party/python/` now holds `BUCK` (one `pypi_package` per locked dep with `py313-linux-x86_64-gnu` / `py313-linux-aarch64-gnu` wheel cells), `muntjac.bzl`, `wiring.bzl` (exporting `MUNTJAC_HOST_MODIFIERS`), `config/BUCK` (`python_version`/`platform` constraint settings, `py313-*` config_settings), and `prebake/` (self-gitignored).

**If muntjac fails or emits wrong output here** (cp313 selection, cfg naming, cell resolution): this is the dogfood wall — fix it in the scratchpad muntjac clone, re-run its `cargo test`, push to the fork, cut `v0.2.2` (Task 1 steps 5–6), bump `MUNTJAC_VERSION` + sha256s in `tools/BUCK`, and retry. Do NOT hand-edit generated files.

- [ ] **Step 3: Wire the cfg modifiers via scoped `PACKAGE` files**

muntjac's contract: `set_cfg_modifiers(cfg_modifiers = MUNTJAC_HOST_MODIFIERS)` from a `PACKAGE` file (never a `.bzl` helper), with the cfg constructor registered — loom's root `PACKAGE` already registers it (`set_cfg_constructor(...)`, `PACKAGE:23`). Scope the modifiers to the two Python subtrees rather than the root so Rust configurations are untouched (a root-level modifier would change every target's cfg hash and invalidate the whole RE cache). Since `python_versions` has exactly one entry, pin `py313` in the same place so no per-target `modifiers` are ever needed.

`third-party/python/PACKAGE` (this file is hand-written, NOT muntjac-generated — it survives regeneration):

```python
load("@prelude//cfg/modifier/set_cfg_modifiers.bzl", "set_cfg_modifiers")
load("//third-party/python:wiring.bzl", "MUNTJAC_HOST_MODIFIERS")

# Muntjac platform selection (host os/cpu → //third-party/python/config:* platform
# constraint) + the single pinned python version. Scoped here and in src/sdk/PACKAGE
# — deliberately NOT the root PACKAGE, so Rust target configurations are untouched.
# The cfg constructor these modifiers need is registered in the root PACKAGE.
set_cfg_modifiers(cfg_modifiers = MUNTJAC_HOST_MODIFIERS + [
    "root//third-party/python/config:py313",
])
```

`src/sdk/PACKAGE`: identical content (same load lines, same call).

- [ ] **Step 4: Build the whole generated tree — on RE**

```bash
buck2 build -v0 --console none root//third-party/python/...
```

Expected: exit 0, silent — every wheel `http_file` fetches, every `prebuilt_python_library` variant + alias resolves for the host cfg. This is the spec's first acceptance criterion. If `select()` fails to resolve (missing platform/py constraint), the `PACKAGE` wiring in step 3 is wrong — fix there.

- [ ] **Step 5: Commit**

```bash
git add tools/pybuckify.sh third-party/python/ src/sdk/PACKAGE
buck2 run //tools:prek -- run --all-files
git commit -m "feat(python): pybuckify.sh + muntjac-generated third-party/python tree"
```

---

### Task 5: acceptance `python_test` — hermetic imports on the real toolchain

**Files:**
- Create: `/workspace/src/sdk/python/BUCK`
- Create: `/workspace/src/sdk/python/tests/imports_test.py`

**Interfaces:**
- Consumes: `root//third-party/python:httpx`, `:pyarrow`, `:pydantic` (Task 4).
- Produces: test target `root//src/sdk/python:imports` in the `buck2 test //src/...` sweep — the spec's interpreter↔cp313-wheel end-to-end proof. `road-python-sdk-v1` later grows this BUCK file with the real library/test targets.

- [ ] **Step 1: Write the failing test**

`src/sdk/python/tests/imports_test.py` — stdlib `unittest` (the prelude test runner speaks it; no pytest dep in v1). Import each dep and exercise one native-code touchpoint so a broken wheel can't pass as an empty namespace package:

```python
"""Acceptance test for road-python-build-infra.

The three SDK deps import and exercise their native code on the hermetic
CPython 3.13 toolchain, proving the muntjac-generated //third-party/python
wheels agree with the interpreter.
"""

import sys
import unittest


class ImportsTest(unittest.TestCase):
    def test_interpreter_is_pinned_cpython_313(self) -> None:
        self.assertEqual((sys.version_info.major, sys.version_info.minor), (3, 13))

    def test_httpx(self) -> None:
        import httpx

        request = httpx.Request("GET", "http://loom.invalid/objects/Customer")
        self.assertEqual(request.url.host, "loom.invalid")

    def test_pyarrow_native(self) -> None:
        import pyarrow as pa

        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.column("id").type, pa.int64())

    def test_pydantic_core_native(self) -> None:
        import pydantic

        class Row(pydantic.BaseModel):
            id: int
            name: str

        row = Row.model_validate({"id": 7, "name": "ada"})
        self.assertEqual(row.id, 7)
        with self.assertRaises(pydantic.ValidationError):
            Row.model_validate({"id": "not-an-int-7x", "name": "ada"})


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Write `src/sdk/python/BUCK`**

```python
python_test(
    name = "imports",
    srcs = ["tests/imports_test.py"],
    deps = [
        "//third-party/python:httpx",
        "//third-party/python:pyarrow",
        "//third-party/python:pydantic",
    ],
)
```

- [ ] **Step 3: Verify the test actually gates (watch it fail)**

Run once with a deliberately broken assertion to prove the runner executes the file (buck2 silently-never-runs inline Rust tests; prove Python tests DO run): temporarily change `(3, 13)` to `(3, 12)` in `test_interpreter_is_pinned_cpython_313`, then:

```bash
buck2 test --console none root//src/sdk/python:imports
```

Expected: FAIL, naming `test_interpreter_is_pinned_cpython_313`. Revert to `(3, 13)`.

- [ ] **Step 4: Run for real**

```bash
buck2 test --console none root//src/sdk/python:imports
buck2 test --console none root//src/...
```

Expected: first command `Tests finished: Pass 1. Fail 0` (4 test cases); second stays fully green (the new target rides the sweep; use `-j 8` locally if the pg fixture slots starve).

- [ ] **Step 5: Commit**

```bash
git add src/sdk/python/BUCK src/sdk/python/tests/imports_test.py
buck2 run //tools:prek -- run --all-files
git commit -m "test(python): hermetic import acceptance test for the muntjac dep tree"
```

---

### Task 6: drift guard + dev-shell + CI scope + docs

**Files:**
- Modify: `/workspace/prek.toml` (add `muntjac-check` after the `reindeer-check` hook block)
- Modify: `/workspace/tools/env.sh` (two entries in the `TOOLS` map)
- Modify: `/workspace/buildbuddy.yaml` (extend `build-test`'s build scope)
- Modify: `/workspace/CLAUDE.md` (dev-tools bullets + a "Third-party Python deps" section)

**Interfaces:**
- Consumes: `tools/pybuckify.sh` (Task 4), `root//tools:{muntjac,uv}` genrules (Task 2).
- Produces: the `muntjac-check` prek hook (runs in CI's `lint` job automatically, since that job just runs all prek hooks).

- [ ] **Step 1: Add the prek hook**

In `prek.toml`, directly after the `reindeer-check` hook:

```toml
# When the Python manifest/lock or muntjac config changes, regenerate
# third-party/python/ (frozen: no re-lock, no network — muntjac's staleness
# gate is mtime-based and CI checkout mtimes are arbitrary) and fail if the
# generated tree or the lockfile drifted — the Python reindeer-check.
[[repos.hooks]]
id = "muntjac-check"
name = "muntjac in sync"
entry = "bash -c './tools/pybuckify.sh --frozen && git diff --exit-code third-party/python src/sdk/python/uv.lock'"
language = "system"
files = "(^|/)(pyproject\\.toml|uv\\.lock)$|^muntjac\\.toml$"
pass_filenames = false
```

(`src/sdk/python/uv.lock` rides the diff so a future non-frozen regeneration that only moves the lock still trips the gate; under `--frozen` the lock is never rewritten, so it is inert but harmless there.)

- [ ] **Step 2: Verify the hook both passes and fails**

The red-test must change generated **output**, not just the manifest (a constraint loosening like `>=0.28` → `>=0.28.1` re-locks to the identical resolution and stays green). Hand-introduce drift in the generated tree:

```bash
buck2 run //tools:prek -- run muntjac-check --all-files          # expect: Passed
sed -i 's/^pypi_package($/pypi_package(  # drift/' third-party/python/BUCK   # or append a comment line
buck2 run //tools:prek -- run muntjac-check --all-files          # expect: Failed (regeneration reverts the edit → diff)
git checkout third-party/python/
```

Also probe the frozen path's stale-lock behavior once, and record what happens (this is dogfood signal for muntjac): `touch src/sdk/python/pyproject.toml` then run the hook — if muntjac `--frozen` errors on a stale-by-mtime manifest, the hook correctly goes red on un-relocked edits; if it silently proceeds, note in CLAUDE.md's new section that lock freshness is enforced by review + the SDK's own CI builds, not this hook. `git checkout` anything touched afterwards.

- [ ] **Step 3: keep the generated tree CI-built**

`build-test` builds only `//src/...` and `affected` filters btd output to `root//src/` — after this PR merges, nothing would rebuild the generated tree except the three aliases the imports test pulls in, so wheel-URL/sha rot in the unused targets (and all aarch64 variants) would be silent. Extend `build-test`'s build line in `buildbuddy.yaml` (the `buck2 build` step of the `build-test` action) from `//src/...` to `//src/... //third-party/python/...` — building the package in the host cfg still fetches/verifies every per-arch wheel `http_file` (they're data, not executed), which is exactly the rot check needed. The `affected` job's src-only filter stays (the `muntjac-check` lint hook already regenerates the tree on PRs that touch the manifests).

- [ ] **Step 4: env.sh entries**

In the `declare -A TOOLS=(` map in `tools/env.sh`, after the `[lucidshark-duplo]` line:

```bash
    [muntjac]="root//tools:muntjac-x86_64-linux"
    [uv]="root//tools:uv-x86_64-linux"
```

Verify: `eval "$(./tools/env.sh)" && uv --version && muntjac --help >/dev/null && echo OK` → `OK`.

- [ ] **Step 5: CLAUDE.md**

Two edits:

1. In **Dev tools**, after the `//tools:lucidshark-duplo` bullet, add bullets for `//tools:muntjac` (uv.lock→BUCK importer, weave-hand fork releases, binary at archive root, bump via `MUNTJAC_VERSION` + sidecar sha256s, normally driven via `./tools/pybuckify.sh`) and `//tools:uv` (vendored resolver muntjac shells out to; wrapper-dir archive like prek; bump via `UV_VERSION` + sidecars).
2. New section **"Third-party Python deps"** after "Third-party Rust deps", covering: muntjac non-vendored mode (wheels downloaded at build time, sources not committed); config `muntjac.toml` at repo root → `src/sdk/python/pyproject.toml` + `uv.lock`; workflow (edit pyproject → `buck2 run //tools:uv -- lock --project src/sdk/python` → `./tools/pybuckify.sh` → depend on `//third-party/python:<pkg>`); the `muntjac-check` prek hook; the scoped-`PACKAGE` cfg-modifier wiring (root PACKAGE registers the constructor; `third-party/python/PACKAGE` + `src/sdk/PACKAGE` set `MUNTJAC_HOST_MODIFIERS` + `py313`; new Python-target directories outside `src/sdk` need the same two-line PACKAGE); python 3.13-only matching the toolchain pin; manylinux 2_28 rationale (pyarrow); fixups `"none"` until needed.

- [ ] **Step 6: Full-gate commit**

```bash
git add prek.toml tools/env.sh buildbuddy.yaml CLAUDE.md
buck2 run //tools:prek -- run --all-files
git commit -m "feat(python): muntjac-check prek hook, CI scope, env.sh tools, CLAUDE.md docs"
```

---

### Task 7: register close, metric gate, PR

**Files:**
- Modify: `/workspace/docs/ROADMAP.md` (remove the `road-python-build-infra` entry; `road-python-sdk-v1` stays)
- Modify: `/workspace/docs/system-capabilities/build-and-test.md` (record the landed capability; drop the now-stale "promoted from fut-python-bindings" phrasing for the infra half)
- Modify: `/workspace/docs/superpowers/specs/2026-07-21-python-build-infra-muntjac-design.md` (correct `manylinux = "2_17"` → `"2_28"` in component 3 — the spec predates the PyPI check; pyarrow cp313 linux-gnu wheels are manylinux_2_28-only, and a 2_28 platform still accepts pydantic-core's 2_17 wheels. Note the correction in the PR body.)

**Interfaces:**
- Consumes: everything above, complete and green.
- Produces: the `work/road-python-build-infra` PR.

- [ ] **Step 1: Close the register item (loom-docs-update)**

Remove the `road-python-build-infra` list entry from `docs/ROADMAP.md` (the `## build` section header stays — `road-python-sdk-v1` still references the machinery; an empty section is also fine per the other headers). In `docs/system-capabilities/build-and-test.md`, add the capability where the build tooling is described: muntjac+uv vendored tools, `muntjac.toml`/`pybuckify.sh`/`muntjac-check`, the scoped-PACKAGE cfg wiring, and the `//src/sdk/python:imports` acceptance test; update its open-items list line to reference only `#road-python-sdk-v1`. Run `bash tools/docs.sh validate` → OK. Record any NEW deferral discovered during implementation (e.g. muntjac issues worked around rather than fixed — there should be none per the operator decision) as FUTURE items with `from:2026-07-21-python-build-infra-muntjac-design`.

- [ ] **Step 2: Metric gate (mandatory, fix-not-file)**

```bash
git fetch origin main
```

Run `loom-complexity diff` and `loom-duplication diff` per their skills, comparing touched files against `git merge-base HEAD origin/main`. This branch is shell/BUCK/docs + one small Python test — no Rust — so expect no findings; if the duplication gate flags the four near-identical tool blocks in `tools/BUCK` against the merge-base, that is the established per-tool idiom (each block is the bump-procedure documentation for its tool), only extractable by a load()-helper that would obscure the sha256 pins — check whether a shared macro already exists in `tools/` (it does not today) before accepting that reasoning. Fix anything real; put before/after numbers in the PR body.

- [ ] **Step 3: Final verification sweep**

```bash
buck2 build -v0 -M none --console none root//src/... root//third-party/python/...
buck2 test --console none root//src/...
buck2 run //tools:prek -- run --all-files
bash tools/docs.sh validate
```

(`-M none` — verification needs exit status, not materialized artifacts; a whole-tree build without it is ~29 GiB and ENOSPCs cloud sessions, per CLAUDE.md's disk-cap warning. Scope the test run to touched targets if in a cloud session.)

All green. Lease-check, then push:

```bash
git ls-remote origin work/road-python-build-infra   # tip must be an ancestor of local HEAD
git push origin work/road-python-build-infra
```

- [ ] **Step 4: Open the PR**

```bash
gh pr create --head work/road-python-build-infra --title "feat(python): muntjac-based Python build infrastructure (road-python-build-infra)" --body "<summary: closes road-python-build-infra; muntjac v0.2.1 fork release + //tools:muntjac + //tools:uv; muntjac.toml (py3.13, manylinux 2_28, linux x86_64+aarch64); generated third-party/python tree; scoped-PACKAGE cfg modifiers; pybuckify.sh + muntjac-check hook; //src/sdk/python:imports acceptance test; metric-gate numbers; dogfood findings fed back to weave-hand/muntjac>"
```

Poll CI via the commit-status endpoint + BuildBuddy MCP (NOT `gh pr checks`). On green, merge per the finishing flow.
