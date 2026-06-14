# Hermetic python toolchain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace loom's `system_python_bootstrap_toolchain` with a hermetic, in-repo-pinned python toolchain so the buck2 build needs no system `python3`.

**Architecture:** Adopt the prelude's `remote_python_toolchain` (downloads a pinned python-build-standalone CPython, wires both the bootstrap and full toolchains). Then put that same hermetic interpreter on `PATH` for the two dev-tooling consumers (`tools/buckify.sh`, `tools/env.sh`) that shell out to `python3` directly.

**Tech Stack:** buck2 + the vendored prelude (`prelude/toolchains/python.bzl`), Starlark, bash, python-build-standalone (CPython 3.13.6).

**Spec:** `docs/superpowers/specs/2026-06-14-hermetic-python-toolchain-design.md`

**Where to run:** Any environment with `buck2` + the loom env active (host with `.loom/bin` on PATH, or the `ldev/claude-box` container). Fixture/RE checks need `BUILDBUDDY_API_KEY`.

---

## File structure

- `toolchains/BUCK` (modify) — swap the system python bootstrap toolchain for `remote_python_toolchain`, with CPython pinned in-repo. This is the crux.
- `tools/buckify.sh` (modify) — prepend the hermetic interpreter's `bin` to `PATH` before the inline `python3` PYFIX step.
- `tools/env.sh` (modify) — symlink `.loom/bin/python3` (+ `python`) at the hermetic interpreter for the dev shell.
- `CLAUDE.md` (modify) — document that python is now hermetic and how to bump `CPYTHON_VERSION`.

No container change: `ldev/claude-box` is already on the RBE base (system python is an unused fallback once this lands).

---

### Task 1: Wire the hermetic python toolchain

**Files:**
- Modify: `toolchains/BUCK` (the `load(... python.bzl ...)` line near the top, and the `system_python_bootstrap_toolchain(...)` call in the "Toolchains" section)

- [ ] **Step 1: Capture the baseline (system toolchain resolves today)**

Run: `buck2 build toolchains//:python_bootstrap 2>&1 | tail -3`
Expected: builds successfully (the current `system_python_bootstrap_toolchain`). This confirms the target name we are replacing.

- [ ] **Step 2: Swap the load symbol**

In `toolchains/BUCK`, change the import:

```python
# from:
load("@prelude//toolchains:python.bzl", "system_python_bootstrap_toolchain")
# to:
load("@prelude//toolchains:python.bzl", "remote_python_toolchain")
```

- [ ] **Step 3: Replace the toolchain call with the in-repo-pinned remote toolchain**

In `toolchains/BUCK`, replace the whole call:

```python
system_python_bootstrap_toolchain(
    name = "python_bootstrap",
    visibility = ["PUBLIC"],
)
```

with:

```python
# -- CPython 3.13.6 (python-build-standalone, install_only_stripped) ----------
# Pinned in-repo (not inherited from the prelude default) so a prelude bump can't
# silently move the python version. To bump: pick a release from
# github.com/astral-sh/python-build-standalone, update CPYTHON_VERSION and each
# sha256 from that release's `*-install_only_stripped.tar.gz` assets.
CPYTHON_VERSION = "3.13.6+20250807"
_CPYTHON_BASE = "https://github.com/astral-sh/python-build-standalone/releases/download"
CPYTHON_URLS = {
    "linux": {
        "x86_64": {
            "sha256": "e3e280d4b1ead63de6ebc9816de71792fc8c71b7a6a999ea82f937047beba037",
            "url": "{}/20250807/cpython-3.13.6+20250807-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz".format(_CPYTHON_BASE),
        },
        "arm64": {
            "sha256": "829d615905b5ae8c50353f2ceb3d6665793442d4cbc64503bc9b27b5b9f6fb8a",
            "url": "{}/20250807/cpython-3.13.6+20250807-aarch64-unknown-linux-gnu-install_only_stripped.tar.gz".format(_CPYTHON_BASE),
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

- [ ] **Step 4: Resolve + run the hermetic interpreter**

Run: `buck2 run toolchains//:cpython -- --version`
Expected: `Python 3.13.6` (downloads + extracts the dist on first run).

- [ ] **Step 5: Verify both toolchain targets build and the system one is gone**

Run: `buck2 build toolchains//:python toolchains//:python_bootstrap 2>&1 | tail -3 && git grep -n system_python toolchains/ ; echo "grep-exit=$?"`
Expected: both targets build; the `git grep` prints nothing and `grep-exit=1` (no matches = the system toolchain is fully removed).

- [ ] **Step 6: Confirm a real Rust build still works through the new bootstrap toolchain (on RE)**

Run: `buck2 build //src/control-plane/core/... 2>&1 | tail -5`
Expected: builds successfully. (The prelude routes local Rust actions through the bootstrap interpreter; a clean build proves the swap is wired. Per the "verify native deps on RE" lesson, ensure this runs with RE enabled, i.e. `BUILDBUDDY_API_KEY` set, not `--local-only`.)

- [ ] **Step 7: Commit**

```bash
git add toolchains/BUCK
git commit -m "build(toolchains): hermetic python via remote_python_toolchain

Replace system_python_bootstrap_toolchain with the prelude's
remote_python_toolchain over a python-build-standalone CPython 3.13.6 dist,
pinned in-repo (CPYTHON_VERSION + sha256) next to RUST_NIGHTLY/LLVM_VERSION.
Wires both :python_bootstrap and the full :python toolchain, x86_64 + aarch64.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Put the hermetic python3 on PATH for buckify.sh

**Files:**
- Modify: `tools/buckify.sh` (the toolchain-PATH block, just before `buck2 run root//tools:reindeer -- buckify`)

- [ ] **Step 1: Add the hermetic-python PATH export**

In `tools/buckify.sh`, immediately after these existing lines:

```bash
TOOLCHAIN="$REPO_ROOT/$(buck2 build root//tools:rust-host-toolchain --show-output 2>/dev/null | awk '{print $2}')"
export PATH="$TOOLCHAIN/bin:$PATH"
export LD_LIBRARY_PATH="$TOOLCHAIN/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

insert:

```bash
# The PYFIX step below runs `python3`. Put loom's hermetic interpreter
# (python-build-standalone, via toolchains//:cpython_archive) ahead of PATH so
# buckify needs no system python — locally or in CI's reindeer-check hook. The
# install_only dist is relocatable, so bin/python3 finds its sibling lib/.
PY_ARCHIVE="$REPO_ROOT/$(buck2 build toolchains//:cpython_archive --show-output 2>/dev/null | awk 'NR==1{print $2}')"
export PATH="$PY_ARCHIVE/bin:$PATH"
```

- [ ] **Step 2: Verify buckify uses the hermetic interpreter**

Run: `cd "$(git rev-parse --show-toplevel)" && PY_ARCHIVE="$(buck2 build toolchains//:cpython_archive --show-output 2>/dev/null | awk 'NR==1{print $2}')"; "./$PY_ARCHIVE/bin/python3" --version`
Expected: `Python 3.13.6` (the archive ships a runnable interpreter at `bin/python3`).

- [ ] **Step 3: Run buckify end-to-end and confirm no drift**

Run: `./tools/buckify.sh && git diff --exit-code third-party/BUCK; echo "diff-exit=$?"`
Expected: `buckify complete (...)` then `diff-exit=0` (the hermetic python produces byte-identical output; nothing to regenerate).

- [ ] **Step 4: Commit**

```bash
git add tools/buckify.sh
git commit -m "build(tools): buckify uses loom's hermetic python3

Prepend toolchains//:cpython_archive/bin to PATH before the reindeer PYFIX
step, so the inline python3 needs no system interpreter (locally or in CI's
reindeer-check hook).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Expose the hermetic python3 in the dev shell (env.sh)

**Files:**
- Modify: `tools/env.sh` (after the `TOOLS` symlink loop)

- [ ] **Step 1: Add python symlinks to the dev-shell bin**

In `tools/env.sh`, immediately after this existing loop:

```bash
for name in "${!TOOLS[@]}"; do
    if p="$(resolve "${TOOLS[$name]}")"; then ln -sfn "$p" "$BIN/$name"; fi
done
```

insert:

```bash
# Hermetic python (python-build-standalone) for buckify + ad-hoc dev use. The
# http_archive output is the extracted dist dir; its relocatable bin/python3
# finds its sibling lib/. Symlink both python3 and python so either name works.
if py="$(resolve toolchains//:cpython_archive)"; then
    ln -sfn "$py/bin/python3" "$BIN/python3"
    ln -sfn "$py/bin/python3" "$BIN/python"
fi
```

- [ ] **Step 2: Activate and verify python3 resolves into .loom/bin**

Run: `eval "$(./tools/env.sh)" && command -v python3 && python3 --version`
Expected: a path ending in `/.loom/bin/python3`, then `Python 3.13.6`.

- [ ] **Step 3: Commit**

```bash
git add tools/env.sh
git commit -m "build(tools): env.sh puts hermetic python3 on PATH

Symlink .loom/bin/python3 (+ python) at the python-build-standalone interpreter
so the activated dev shell has a hermetic python, matching cargo/rustc.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Document the hermetic python toolchain

**Files:**
- Modify: `CLAUDE.md` (the build/toolchain discussion — the `//tools:rustfmt`/`clippy` dev-tools area documents the pinned toolchains; add python alongside)

- [ ] **Step 1: Add a short note on the hermetic python pin**

In `CLAUDE.md`, add a bullet near the other toolchain-bump notes (e.g. after the clippy/`RUST_NIGHTLY` bump guidance):

```markdown
- **python** is hermetic too: `toolchains/BUCK` wires the prelude's
  `remote_python_toolchain` over a pinned python-build-standalone CPython
  (`CPYTHON_VERSION`), giving `toolchains//:python_bootstrap` (the buck2 bootstrap
  toolchain) and `toolchains//:python` (full). `tools/buckify.sh` and
  `tools/env.sh` put that same interpreter on `PATH`, so no system `python3` is
  needed anywhere — locally, in CI, or on RE. To bump: update `CPYTHON_VERSION`
  and the sha256s in `toolchains/BUCK` from a python-build-standalone release.
```

- [ ] **Step 2: Verify the doc lint passes**

Run: `buck2 run //tools:prek -- run --files CLAUDE.md 2>&1 | tail -5`
Expected: `trim trailing whitespace`, `fix end of files` etc. report `Passed` (no EOF/whitespace damage).

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: hermetic python toolchain + CPYTHON bump note

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification (after all tasks)

- [ ] `git grep -n system_python toolchains/` → no output.
- [ ] `buck2 build //src/... 2>&1 | tail -3` → green (with `BUILDBUDDY_API_KEY` set, so RE is exercised).
- [ ] `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` → fixture tests run and pass (do not pipe `buck2 test` to `tail`).
- [ ] `./tools/buckify.sh && git diff --exit-code third-party/BUCK` → clean.
- [ ] Fresh dev shell: `eval "$(./tools/env.sh)"; python3 --version` → `Python 3.13.6` from `.loom/bin`.
- [ ] Open a PR with `--base main` and confirm CI (`build-test`/`affected` + `lint`) is green before merge.
