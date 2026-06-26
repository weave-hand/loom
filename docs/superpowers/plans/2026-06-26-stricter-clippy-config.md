# Stricter Clippy Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adopt broad `clippy::pedantic` + `clippy::restriction` as loom's enforced first-party lint policy, wired once through the buck2 toolchain, and bring the tree to a green `clippy-all.sh`.

**Architecture:** The buck2 prelude's `RustToolchainInfo` already carries `warn_lints` / `deny_lints` / `allow_lints` / `clippy_toml` fields that `_lintify` (`prelude/rust/build.bzl:954-968`) turns into clippy flags. loom's `hermetic_rust_toolchain` (`toolchains/rust_dist.bzl`) doesn't forward them, so step one is to add and thread those attributes (mirroring the prelude's own `system_rust_toolchain`, `prelude/toolchains/rust.bzl:42-77`). Then enable the groups once on the `:rust` toolchain in `toolchains/BUCK`, measure the resulting diagnostics, and reach green via a census-driven split of code-fixes vs an `allow_lints` allowlist plus a root `clippy.toml` for test exemptions.

**Tech Stack:** buck2 + vendored prelude, hermetic Rust toolchain (pinned nightly), clippy via the `[clippy.txt]` sub-target, `tools/clippy-all.sh` (the enforced gate, also the `clippy` prek hook + CI `lint` job).

## Global Constraints

- **Enforced gate fails on ANY diagnostic.** `tools/clippy-all.sh` builds each first-party target's `[clippy.txt]` sub-target and exits non-zero if any diagnostics file is non-empty. "Green" means `tools/clippy-all.sh` exits 0 over `//src/...`.
- **Scope is first-party only:** `//src/...`. Third-party (`//third-party`) and `//tools` are out of scope (the gate already scopes to `root//src/...`).
- **buck2 does not read Cargo `[lints]`.** All lint config flows through `RustToolchainInfo`; do NOT add `[workspace.lints]` (the gate would ignore it).
- **Tests are separate `rust_test` crates, not `#[cfg(test)]` modules.** Whether clippy's `allow-*-in-tests` exemptions fire for them is verified in Task 1, with a documented fallback.
- **Lint fields take plain strings**, mirroring `prelude/toolchains/rust.bzl` (`attrs.list(attrs.string())`). Lint values are written like `"clippy::pedantic"` (no embedded quotes).
- **Use `warn`, not `deny`.** The gate already converts any diagnostic to a failure; `warn` keeps a bare local `buck2 build` non-fatal.
- **Don't pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to a file and grep it (`> /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`). `buck2 build | tail` is fine.
- **Commits follow Conventional Commits** (the `conventional-commit` commit-msg hook enforces it). End commit messages with the `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.
- Work happens on the existing `clippy-strict-lints` branch.

---

### Task 1: Wire toolchain lint fields + enable-pedantic spike (de-risk)

Proves the configure-once mechanism end to end on one group (`clippy::pedantic`) before any bulk work: the gate sees clippy diagnostics, plain `rustc` builds stay clean, and the `clippy.toml` test-exemption behavior is established.

**Files:**
- Modify: `toolchains/rust_dist.bzl` (the `hermetic_rust_toolchain` rule + its impl)
- Modify: `toolchains/BUCK:169-182` (the `:rust` toolchain instance)
- Create: `clippy.toml` (repo root)

**Interfaces:**
- Produces: a `:rust` toolchain whose `RustToolchainInfo` has populated `warn_lints` / `allow_lints` / `clippy_toml`. Later tasks only change the *values* of `warn_lints` / `allow_lints` in `toolchains/BUCK` and the body of `clippy.toml` — the rule plumbing is fixed here.

- [ ] **Step 1: Add the four lint attributes to the `hermetic_rust_toolchain` rule**

In `toolchains/rust_dist.bzl`, extend the `attrs` dict (currently ends after `use_bundled_linker`, lines 84-93) to add:

```python
hermetic_rust_toolchain = rule(
    impl = _hermetic_rust_toolchain_impl,
    is_toolchain_rule = True,
    attrs = {
        "rustc_dist": attrs.dep(),
        "std_dist": attrs.dep(),
        "clippy_dist": attrs.dep(),
        "rustc_target_triple": attrs.string(default = "x86_64-unknown-linux-gnu"),
        "host_triple": attrs.string(default = "x86_64-unknown-linux-gnu"),
        "default_edition": attrs.string(default = "2024"),
        "rustc_flags": attrs.list(attrs.string(), default = []),
        "use_bundled_linker": attrs.bool(default = False),
        # Lint policy: forwarded into RustToolchainInfo. clippy::* lints in
        # warn_lints/allow_lints are applied to the clippy action; see
        # prelude/rust/build.bzl:_lintify. clippy_toml configures lint
        # parameters (e.g. allow-*-in-tests) for the clippy action only.
        "allow_lints": attrs.list(attrs.string(), default = []),
        "deny_lints": attrs.list(attrs.string(), default = []),
        "warn_lints": attrs.list(attrs.string(), default = []),
        "clippy_toml": attrs.option(attrs.source(), default = None),
    },
)
```

- [ ] **Step 2: Thread the attributes into `RustToolchainInfo`**

In `_hermetic_rust_toolchain_impl`, extend the `RustToolchainInfo(...)` constructor (currently lines 69-78) to forward the new fields:

```python
        RustToolchainInfo(
            compiler = RunInfo(args = [rustc]),
            rustdoc = RunInfo(args = [rustdoc]),
            clippy_driver = RunInfo(args = [clippy_driver]),
            panic_runtime = PanicRuntime("unwind"),
            default_edition = ctx.attrs.default_edition,
            rustc_target_triple = target,
            sysroot_path = sysroot,
            rustc_flags = rustc_flags,
            allow_lints = ctx.attrs.allow_lints,
            deny_lints = ctx.attrs.deny_lints,
            warn_lints = ctx.attrs.warn_lints,
            clippy_toml = ctx.attrs.clippy_toml,
        ),
```

- [ ] **Step 3: Create the root `clippy.toml` with test exemptions**

Create `clippy.toml` at the repo root:

```toml
# Clippy configuration for loom's first-party crates. Applied to the clippy
# action via the :rust toolchain's clippy_toml field (toolchains/BUCK).
# These configure lint *parameters* only — lint groups/levels are set with
# warn_lints/allow_lints in toolchains/BUCK. See
# docs/superpowers/specs/2026-06-26-stricter-clippy-config-design.md.

# Exempt test code from the panic-policy restriction lints. NOTE: loom's tests
# are separate rust_test crates, not #[cfg(test)] modules; Task 1 Step 7
# verifies whether these fire for that layout.
allow-unwrap-in-tests = true
allow-panic-in-tests = true
allow-expect-in-tests = true
allow-indexing-slicing-in-tests = true
allow-dbg-in-tests = true
```

- [ ] **Step 4: Enable `clippy::pedantic` only, on the `:rust` toolchain**

In `toolchains/BUCK`, edit the `hermetic_rust_toolchain(name = "rust", ...)` instance (lines 169-182) to add the lint fields just before `visibility`:

```python
    rustc_flags = ["-Copt-level=2"] + select({
        "root//tools/coverage:coverage_enabled": ["-Cinstrument-coverage"],
        "DEFAULT": [],
    }),
    # Lint policy — spike: pedantic only (Task 1). Task 2 adds restriction.
    warn_lints = ["clippy::pedantic"],
    allow_lints = [],
    clippy_toml = "clippy.toml",
    visibility = ["PUBLIC"],
```

- [ ] **Step 5: Verify the gate now sees pedantic diagnostics on one crate**

Run:
```bash
buck2 build '//src/control-plane/core:core[clippy.txt]' --show-output 2>/dev/null \
  | awk '{print $2}' | xargs -r cat | tee /tmp/clippy_pedantic.txt
```
Expected: non-empty output containing `clippy::` pedantic warnings (e.g. `must_use_candidate`, `missing_errors_doc`, `module_name_repetitions` — exact lints vary). If empty, the mechanism is not wired — recheck Steps 1-4. Confirms: toolchain `warn_lints` reaches the clippy action.

- [ ] **Step 6: Verify a plain build stays clean (no `-Wclippy::*` noise reaching rustc)**

Run:
```bash
buck2 build //src/control-plane/core:core 2>&1 | tee /tmp/build_plain.txt
grep -i 'unknown lint\|clippy::' /tmp/build_plain.txt || echo "CLEAN: no clippy-lint noise on rustc build"
```
Expected: `CLEAN: ...` — the normal rustc compile must not emit `unknown lint` warnings for the `clippy::*` flags. **If it is NOT clean** (rustc warns on the tool-lint flags): the fallback is to additionally pass `-A unknown_lints` only on the normal build, or to use the `_lintify` quote-strip convention; record the resolution inline in `toolchains/rust_dist.bzl` and proceed. (rustc registers `clippy` as a tool, so this is expected to be clean.)

- [ ] **Step 7: Probe whether `allow-*-in-tests` fires for a `rust_test` crate**

Pick a test crate that calls `.unwrap()` (e.g. `//src/control-plane/core:page`) and lint it:
```bash
buck2 build '//src/control-plane/core:page[clippy.txt]' --show-output 2>/dev/null \
  | awk '{print $2}' | xargs -r cat | grep -c 'unwrap_used' || true
```
Note the result for Task 3. Pedantic doesn't include `unwrap_used`, so add a throwaway `warn_lints = ["clippy::pedantic", "clippy::unwrap_used"]` for this probe only, then revert to `["clippy::pedantic"]`. Expected: if the count is 0, the `allow-unwrap-in-tests` exemption works for separate test crates; if >0, it does NOT — record that Task 3 must instead `allow` the panic-policy lints for `rust_test` targets via a `loom_rust_test` wrapper or test-root `#![allow(...)]` (fallback per the spec §2).

- [ ] **Step 8: Run the full gate to capture the pedantic-only baseline**

Run:
```bash
./tools/clippy-all.sh > /tmp/clippy_pedantic_all.txt 2>&1; echo "exit=$?"
wc -l /tmp/clippy_pedantic_all.txt
```
Expected: non-zero exit, with pedantic warnings across crates. This is the expected red state — do NOT fix yet. It confirms the policy is live tree-wide.

- [ ] **Step 9: Commit**

```bash
git add toolchains/rust_dist.bzl toolchains/BUCK clippy.toml
git commit -m "build(toolchain): forward clippy lint fields; enable pedantic spike

Thread warn_lints/deny_lints/allow_lints/clippy_toml through
hermetic_rust_toolchain into RustToolchainInfo (mirroring the prelude's
system_rust_toolchain) and enable clippy::pedantic on :rust as a spike.
Adds root clippy.toml for test exemptions.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Enable `restriction` and capture the full census

Turns on the full target policy and records exactly what fires, so Task 3 can triage from data rather than guesses.

**Files:**
- Modify: `toolchains/BUCK` (the `:rust` `warn_lints` value)
- Create: `/tmp/clippy_census.txt` (working artifact, not committed)

**Interfaces:**
- Consumes: the wired toolchain from Task 1.
- Produces: a census (lint name → count → representative files) used by Task 3.

- [ ] **Step 1: Add `clippy::restriction` to `warn_lints`**

In `toolchains/BUCK`, update the `:rust` `warn_lints` and pre-allow only the one lint that the group-enable itself requires:

```python
    warn_lints = ["clippy::pedantic", "clippy::restriction"],
    allow_lints = [
        # Required: warns merely for enabling the restriction group broadly.
        "clippy::blanket_clippy_restriction_lints",
    ],
    clippy_toml = "clippy.toml",
```

- [ ] **Step 2: Run the gate and capture raw diagnostics**

Run:
```bash
./tools/clippy-all.sh > /tmp/clippy_raw.txt 2>&1; echo "exit=$?"
```
Expected: non-zero exit, large output. This is the working census source.

- [ ] **Step 3: Summarize the census by lint, sorted by frequency**

Run:
```bash
grep -oE 'clippy::[a-z_]+' /tmp/clippy_raw.txt | sort | uniq -c | sort -rn \
  | tee /tmp/clippy_census.txt
```
Expected: a frequency table, e.g.
```
   412 clippy::implicit_return
   233 clippy::missing_docs_in_private_items
    ...
     3 clippy::dbg_macro
```
Keep `/tmp/clippy_census.txt` and `/tmp/clippy_raw.txt` for Task 3. (No commit — Task 2's only tracked change is the one-line `warn_lints`/`allow_lints` edit, which is committed as part of Task 3 once the allowlist is finalized.)

---

### Task 3: Triage the census → finalize `allow_lints` and `clippy.toml`

Splits every firing lint into keep-and-fix vs allow-with-reason, per the spec §2 policy, and writes the final allowlist. This task produces NO code fixes (Task 4 does); it produces the decision and the config.

**Files:**
- Modify: `toolchains/BUCK` (the `:rust` `allow_lints` list)
- Modify: `clippy.toml` (only if Task 1 Step 7 found test exemptions don't fire)

**Interfaces:**
- Consumes: `/tmp/clippy_census.txt`, `/tmp/clippy_raw.txt` (Task 2); the Task 1 Step 7 test-exemption result.
- Produces: the final `allow_lints` block; the set of "kept" lints whose violations Task 4 must fix.

- [ ] **Step 1: Classify each lint in the census using the policy**

For every lint in `/tmp/clippy_census.txt`, assign **KEEP** (high-signal, fix the code) or **ALLOW** (stylistic / contradictory / whole-API-surface, add to `allow_lints` with a reason). Apply the spec §2 governing rule:

- **KEEP (fix the code):** panic safety — `unwrap_used`, `indexing_slicing`, `string_slice`, `panic`, `unwrap_in_result`, `panic_in_result_fn`, `get_unwrap`; error handling — `let_underscore_must_use`, `let_underscore_future`, `map_err_ignore`; unsafe — `undocumented_unsafe_blocks`, `multiple_unsafe_ops_per_block`, `mem_forget`; async — `await_holding_lock`, `await_holding_refcell_ref`, `large_futures`; leftovers — `dbg_macro`, `todo`, `unimplemented`, `print_stdout`, `print_stderr`; discipline — `allow_attributes`, `allow_attributes_without_reason`.
- **ALLOW (with reason):** pervasive style — `implicit_return`, `missing_docs_in_private_items`, `question_mark_used`, `min_ident_chars`, `single_char_lifetime_names`, `single_call_fn`, `ref_patterns`, `else_if_without_else`, `pattern_type_mismatch`; numeric noise — `arithmetic_side_effects`, `as_conversions`, `integer_division`, `modulo_arithmetic`, `default_numeric_fallback`; whole-type-surface — `exhaustive_enums`, `exhaustive_structs`, `field_scoped_visibility_modifiers`, `partial_pub_fields`; contradictory pairs (allow one side) — `mod_module_files`, `semicolon_inside_block` (or outside), `pub_with_shorthand` (or without), `shadow_reuse`/`shadow_same`/`shadow_unrelated`, `separated_literal_suffix` (or unseparated); not-applicable — `std_instead_of_core`, `std_instead_of_alloc`.
- **Demotion rule:** any KEEP lint whose count is large with low payoff may be moved to ALLOW; record the reason in the comment. The census decides borderline cases, not this list.
- For any lint in the census **not** named above, default to ALLOW if it is purely stylistic, else KEEP; note the call.

Write the classification as a scratch list (e.g. `/tmp/clippy_triage.txt`) for reference.

- [ ] **Step 2: Write the final `allow_lints` block**

In `toolchains/BUCK`, replace the `:rust` `allow_lints` with the finalized list, grouped with one-line reason comments, e.g.:

```python
    allow_lints = [
        # Required when enabling the restriction group broadly.
        "clippy::blanket_clippy_restriction_lints",
        # Pervasive style — fire on nearly every expression; no signal here.
        "clippy::implicit_return",
        "clippy::question_mark_used",
        "clippy::min_ident_chars",
        "clippy::single_char_lifetime_names",
        # Docs not required on private items in an application codebase.
        "clippy::missing_docs_in_private_items",
        # ~85% false positives on ordinary indexing/arithmetic (per emschwartz).
        "clippy::arithmetic_side_effects",
        "clippy::as_conversions",
        # Whole-API-surface; not a goal for a non-library service tree.
        "clippy::exhaustive_enums",
        "clippy::exhaustive_structs",
        # Contradictory-pair lints — keep one side only.
        "clippy::mod_module_files",
        "clippy::semicolon_outside_block",
        "clippy::shadow_reuse",
        "clippy::shadow_same",
        "clippy::shadow_unrelated",
        # ... remainder from the Step 1 classification, each with a reason ...
    ],
```

- [ ] **Step 3: Apply the test-exemption fallback if needed**

If Task 1 Step 7 found `allow-*-in-tests` does NOT fire for `rust_test` crates, the panic-policy KEEP lints would redden the whole test suite. Resolve by adding the panic-policy lints to `allow_lints` scoped to tests is not possible at toolchain granularity, so instead: keep them KEEP for lib/bin and accept fixing test-side violations, OR (preferred) add a `loom_rust_test` wrapper macro that injects `rustc_flags = ["-Aclippy::unwrap_used", "-Aclippy::indexing_slicing", "-Aclippy::panic", "-Aclippy::expect_used"]` and migrate `rust_test` targets to it. Record which path was taken in the design doc's §2. If the exemptions DO fire, skip this step.

- [ ] **Step 4: Re-run the gate to confirm only KEEP-lint violations remain**

Run:
```bash
./tools/clippy-all.sh > /tmp/clippy_after_allow.txt 2>&1; echo "exit=$?"
grep -oE 'clippy::[a-z_]+' /tmp/clippy_after_allow.txt | sort | uniq -c | sort -rn
```
Expected: non-zero exit, but the remaining lints are ONLY the KEEP set from Step 1. If an ALLOWed lint still appears, it was misspelled in `allow_lints` — fix it. This is the precise worklist for Task 4.

- [ ] **Step 5: Commit the policy (config only, tree still red)**

```bash
git add toolchains/BUCK clippy.toml
git commit -m "build(lints): enable clippy::restriction; finalize allowlist

Enable clippy::restriction alongside pedantic and add the census-driven
allow_lints exceptions (each with a reason). Tree is intentionally still
red on the kept high-signal lints; fixed in the following commits.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Fix the KEEP-lint violations to green

Resolves every remaining diagnostic so `clippy-all.sh` exits 0. Worked as a fix-and-recheck loop, committing in reviewable batches by lint family (or by crate), because the exact edits are determined by the Task 3 worklist.

**Files:**
- Modify: first-party `src/**/*.rs` (exact files from `/tmp/clippy_after_allow.txt`)

**Interfaces:**
- Consumes: the KEEP worklist from Task 3 Step 4.
- Produces: a green `clippy-all.sh`.

- [ ] **Step 1: Pick the next lint family from the worklist**

From the Task 3 Step 4 frequency list, take one lint (start with the lowest-count, most-mechanical, e.g. `dbg_macro`, `todo`). List its sites:
```bash
grep -B2 'clippy::<lint_name>' /tmp/clippy_after_allow.txt | grep -oE 'src/[^ :]+\.rs'
```

- [ ] **Step 2: Fix each site by hand using clippy's suggestion**

Apply the idiomatic fix the diagnostic recommends. Examples by family:
- `dbg_macro` / leftover `print_stdout` → delete, or convert to `tracing::debug!`/`info!` (this tree uses `tracing`; see CLAUDE.md).
- `unwrap_used` / `expect_used` / `get_unwrap` in lib/bin → propagate with `?` over a `Result`, or `.ok_or(...)?` / `.context(...)?`; for genuine invariants use `.expect("reason")` only where the KEEP policy allows, else restructure.
- `indexing_slicing` → `.get(i).ok_or(...)?` / `.first()` / `.get(range)`.
- `undocumented_unsafe_blocks` → add a `// SAFETY: <why>` comment above the `unsafe` block.
- `let_underscore_must_use` / `let_underscore_future` → handle the value (`?`, `.await`, or an explicit `let _: () =`/`drop()` with justification).
- `allow_attributes_without_reason` → convert `#[allow(x)]` to `#[expect(x, reason = "…")]`.

Prefer fixing over allowing. If a specific KEEP lint proves to have a large, low-value tail after starting, demote it: move it to `allow_lints` in `toolchains/BUCK` with a reason, and note it. Do NOT use inline `#[allow(...)]` to silence — use `#[expect(..., reason = "…")]` (the `allow_attributes` lint requires it).

- [ ] **Step 3: Re-lint just the affected crate(s)**

For each touched crate, e.g. core:
```bash
buck2 build '//src/control-plane/core:core[clippy.txt]' --show-output 2>/dev/null \
  | awk '{print $2}' | xargs -r cat
```
Expected: the targeted lint no longer appears for that crate.

- [ ] **Step 4: Keep tests green as you go**

After fixing a crate, run its tests (fixtures route local automatically):
```bash
buck2 test //src/control-plane/core/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: `Tests finished: ... 0 failed`. Refactors for `?`-propagation can change signatures/behavior — this catches regressions early.

- [ ] **Step 5: Commit the batch**

```bash
git add -A
git commit -m "fix(clippy): resolve <lint_name> across <area>

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: Repeat Steps 1-5 until the worklist is empty**

Loop over the remaining lint families. After each, re-run `./tools/clippy-all.sh > /tmp/c.txt 2>&1; echo exit=$?` periodically to watch the count fall. Exit the loop when:
```bash
./tools/clippy-all.sh; echo "exit=$?"
```
Expected: `exit=0`.

---

### Task 5: Full verification + document the policy

Confirms the whole suite is green under the new policy and records the mechanism for future contributors.

**Files:**
- Modify: `CLAUDE.md` (add a lint-policy section)
- Modify: `docs/superpowers/specs/2026-06-26-stricter-clippy-config-design.md` (resolve the test-exemption fork note with what actually happened)

- [ ] **Step 1: Run the full clippy gate**

```bash
./tools/clippy-all.sh; echo "exit=$?"
```
Expected: `exit=0`.

- [ ] **Step 2: Run the full test sweep**

```bash
buck2 test //src/... > /tmp/full_test.log 2>&1; grep -E "Tests finished|FAIL|error:" /tmp/full_test.log
```
Expected: `Tests finished: ... 0 failed`, no `FAIL`/`error:`. (Per CLAUDE.md the fixture tests can flake on resource contention; re-run a clean sweep to confirm any failure is non-deterministic before treating it as real.)

- [ ] **Step 3: Add the lint-policy section to CLAUDE.md**

Under the existing clippy bullet in the "Dev tools" section, add a paragraph:

```markdown
- **Clippy lint policy.** loom enables `clippy::pedantic` + `clippy::restriction`
  tree-wide as `warn`, configured **once** on the `:rust` toolchain
  (`toolchains/BUCK`) via the prelude's `warn_lints`/`allow_lints`/`clippy_toml`
  fields (forwarded through `hermetic_rust_toolchain` in `toolchains/rust_dist.bzl`).
  Every first-party crate and `rust_test` inherits it automatically; the enforced
  gate (`tools/clippy-all.sh`, the `clippy` prek hook) fails on any diagnostic.
  To silence a lint **globally**, add it to `allow_lints` in `toolchains/BUCK` with
  a one-line reason; to silence **locally**, use `#[expect(lint, reason = "…")]`
  (bare `#[allow]` is itself denied by `allow_attributes`). The root `clippy.toml`
  configures lint parameters (e.g. `allow-*-in-tests`), not levels. A nightly
  toolchain bump can add new group members and redden the gate — fix or `allow`
  them like any other. See docs/superpowers/specs/2026-06-26-stricter-clippy-config-design.md.
```

- [ ] **Step 4: Resolve the spec's test-exemption fork**

In the design doc §2, replace the "If constraint #4 proves…" hedge with one sentence stating what Task 1 Step 7 found (exemptions fire / required the `loom_rust_test` fallback).

- [ ] **Step 5: Commit the docs**

```bash
git add CLAUDE.md docs/superpowers/specs/2026-06-26-stricter-clippy-config-design.md
git commit -m "docs(clippy): document the toolchain lint policy

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: Push and open the PR**

```bash
git push -u origin clippy-strict-lints
gh pr create --title "Stricter clippy: enable pedantic + restriction tree-wide" \
  --body "$(cat <<'EOF'
Adopts broad clippy::pedantic + clippy::restriction as loom's enforced
first-party lint policy, wired once through the :rust toolchain
(warn_lints/allow_lints/clippy_toml). Census-driven allowlist for the
stylistic/contradictory lints; high-signal lints fixed to green.

Spec: docs/superpowers/specs/2026-06-26-stricter-clippy-config-design.md
Plan: docs/superpowers/plans/2026-06-26-stricter-clippy-config.md

- clippy-all.sh exits 0 over //src/...
- buck2 test //src/... green

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
Expected: PR opens; the `affected` + `lint` BuildBuddy actions run. Merge on green.

---

## Self-Review

**Spec coverage:**
- Spec §1 (mechanism) → Task 1 Steps 1-2. ✓
- Spec §2 (lint policy: keep/allow + clippy.toml exemptions) → Task 1 Step 3, Task 3 Steps 1-3. ✓
- Spec §3 Step 0 (spike) → Task 1 Steps 4-8. ✓
- Spec §3 Step 1 (census) → Task 2. ✓
- Spec §3 Step 2 (triage) → Task 3. ✓
- Spec §3 Step 3 (fix) → Task 4. ✓
- Spec §3 Step 4 (green + docs) → Task 5. ✓
- Spec §4 (verification & scope) → Task 5 Steps 1-2; scope stated in Global Constraints. ✓
- Spec risks (test exemptions, rustc-vs-clippy routing) → Task 1 Steps 6-7 with fallbacks. ✓

**Placeholder scan:** The census/triage tasks are inherently data-driven; they carry the full decision procedure (Task 3 Step 1) and concrete commands/examples rather than fixed code, which is the correct shape for measurement-driven work — not a placeholder. Code-bearing steps (Task 1) contain exact edits.

**Type consistency:** Field names `allow_lints`/`deny_lints`/`warn_lints`/`clippy_toml` match `RustToolchainInfo` (`prelude/rust/rust_toolchain.bzl:81-100`) and the attr names added in Task 1 Step 1. The `clippy_toml = "clippy.toml"` value is a source path resolved by `attrs.option(attrs.source())`. Consistent throughout.
