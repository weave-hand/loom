# Vector-Key Codec Homogeneity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Make mixed `VectorKey` kinds a loud `Validation` error at build and stray trailing bytes a loud decode error, so the LVIX identity block can never silently corrupt.

**Architecture:** Two changes in `src/control-plane/core/src/vector_index/codec.rs` — a homogeneity check in `pack_rows` (the single rows→columns seam all three builds use) and an `expect_eof` full-consumption check appended to all three `deserialize`s — plus red-first unit and property tests.

**Tech Stack:** Rust, buck2 `rust_test` targets (pure-logic, RE-eligible), proptest.

**Spec:** `docs/superpowers/specs/2026-07-03-vector-key-codec-homogeneity-design.md`

## Global Constraints

- Blob format stays LVIX version 1 — byte-identical output for homogeneous builds (all `GOLDEN_*` tests must stay green).
- Error types: `ControlPlaneError::Validation` at build; `bad(...)` (→ `Backend`) at decode.
- Tests live in `tests/*.rs` files wired as `rust_test` targets — never inline `#[cfg(test)]`.
- prek `--all-files` clean before every commit; run tests via file redirect, never piped to `tail`.

---

### Task 1: Encode-side homogeneity rejection in `pack_rows`

**Files:**
- Modify: `src/control-plane/core/src/vector_index/codec.rs:147-210` (`write_keys` doc, `pack_rows`)
- Test: `src/control-plane/core/tests/vector_index.rs`

**Interfaces:**
- Consumes: `pack_rows(dim, rows) -> Result<(Vec<VectorKey>, Vec<f32>)>` (existing, `pub(super)`).
- Produces: same signature; new error `Validation("mixed identity key kinds: all index keys must be Int or all Str")`.

- [x] **Step 1: Write the failing tests** — append to `tests/vector_index.rs`:

```rust
#[test]
fn mixed_key_kinds_rejected_at_build() {
    let mixed = |a, b| vec![(a, vec![0.0]), (b, vec![0.0])];
    let cases = [
        mixed(VectorKey::Int(0), VectorKey::Str("\0".into())),
        mixed(VectorKey::Str("\0".into()), VectorKey::Int(0)),
    ];
    for rows in cases {
        for err in [
            FlatIndex::build(1, Metric::Cosine, rows.clone()).err(),
            IvfFlatIndex::build(1, Metric::Cosine, rows.clone(), None).err(),
            HnswIndex::build(1, Metric::Cosine, rows.clone(), None, None).err(),
        ] {
            let e = err.expect("mixed key kinds must fail build");
            assert!(
                matches!(e, ControlPlaneError::Validation(ref m) if m.contains("mixed identity key kinds")),
                "wrong error: {e}"
            );
        }
    }
}
```

(Signatures verified: `FlatIndex::build(dim, metric, rows)`, `IvfFlatIndex::build(dim, metric, rows, nlist: Option<u32>)`, `HnswIndex::build(dim, metric, rows, m: Option<u32>, ef_construction: Option<u32>)`; `decode(bytes) -> Result<Box<dyn VectorIndex>>` and trait `serialize()` are re-exported via `control_plane_core::{decode, ...}` — the file convention is per-test LOCAL `use` blocks — e.g. `use control_plane_core::{IndexKind, decode};` at :259 and `use control_plane_core::{ControlPlaneError, ...};` at :622 — add `ControlPlaneError` (Task 1) and `decode` (Task 2) that way; both are re-exported at the crate root.)

- [x] **Step 2: Run to verify it fails**

Run: `buck2 test //src/control-plane/core:vector-index > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: FAIL — all three builds return `Ok` today.

- [x] **Step 3: Implement** — in `pack_rows`, after the `dim` binding:

```rust
    let expected_kind = rows.first().map(|(k, _)| std::mem::discriminant(k));
    for (key, v) in rows {
        if Some(std::mem::discriminant(&key)) != expected_kind {
            return Err(ControlPlaneError::Validation(
                "mixed identity key kinds: all index keys must be Int or all Str".to_string(),
            ));
        }
        // ...existing dim check + pushes...
    }
```

(Restructure the existing loop accordingly; `expected_kind` computed once before it.) Update `pack_rows`'s doc comment to name both validations, and rewrite `write_keys`'s comment ("All vectors share one key_kind…") to cite the enforced `pack_rows` invariant rather than describing a hope.

- [x] **Step 4: Run to verify pass + goldens green**

Run: `buck2 test //src/control-plane/core:vector-index //src/control-plane/core:vector-index-codec > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS, 0 failures (goldens byte-identical).

- [x] **Step 5: prek + commit**

```bash
buck2 run -v0 //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -c Failed /tmp/p.log  # expect 0
git add src/control-plane/core/src/vector_index/codec.rs src/control-plane/core/tests/vector_index.rs
git commit -m "fix(vector-index): reject mixed identity key kinds at build"
```

### Task 2: Decode-side full-consumption check (`expect_eof`)

**Files:**
- Modify: `src/control-plane/core/src/vector_index/codec.rs` (new helper), `flat.rs:73-86`, `ivf.rs:119-149`, `hnsw.rs:364-437`
- Test: `src/control-plane/core/tests/vector_index.rs`

**Interfaces:**
- Produces: `pub(super) fn expect_eof(r: &ByteReader<'_>) -> Result<()>` — `Err(bad("trailing bytes after identity block"))` when `r.remaining() > 0`.

- [x] **Step 1: Write the failing tests** — append to `tests/vector_index.rs`:

```rust
#[test]
fn trailing_bytes_rejected_at_decode() {
    let rows = vec![(VectorKey::Int(1), vec![0.5]), (VectorKey::Int(2), vec![0.25])];
    let blobs = [
        FlatIndex::build(1, Metric::Cosine, rows.clone()).expect("build").serialize(),
        IvfFlatIndex::build(1, Metric::Cosine, rows.clone(), None).expect("build").serialize(),
        HnswIndex::build(1, Metric::Cosine, rows, None, None).expect("build").serialize(),
    ];
    for mut blob in blobs {
        blob.push(0xAB);
        let e = decode(&blob).err().expect("trailing byte must fail decode");
        assert!(e.to_string().contains("trailing bytes after identity block"), "wrong error: {e}");
    }
}
```

(`decode` is the public entry at `vector_index/mod.rs:271`; the trait `serialize()` is on `VectorIndex`.)

- [x] **Step 2: Run to verify it fails**

Run: `buck2 test //src/control-plane/core:vector-index > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: FAIL — all three decode `Ok` today, silently ignoring the byte.

- [x] **Step 3: Implement** — in `codec.rs`, next to `bad`:

```rust
/// Final-step decode guard: the identity block ends every LVIX format, so any
/// unread byte means a corrupt/padded blob (or a mixed-kind blob written by a
/// pre-fix loom whose two encodings happened to sum compatibly).
pub(super) fn expect_eof(r: &ByteReader<'_>) -> Result<()> {
    if r.remaining() > 0 {
        return Err(bad("trailing bytes after identity block"));
    }
    Ok(())
}
```

Then in each of the three `deserialize`s, after the `read_keys` line and before the `Ok(...)` struct expression: `expect_eof(&r)?;` (import via the existing `super::codec::{...}` use list).

- [x] **Step 4: Run to verify pass + goldens + round-trips green**

Run: `buck2 test //src/control-plane/core:vector-index //src/control-plane/core:vector-index-codec //src/control-plane/core:vector-index-decode-props > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS, 0 failures.

- [ ] **Step 5: prek + commit**

```bash
buck2 run -v0 //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -c Failed /tmp/p.log  # expect 0
git add src/control-plane/core/src/vector_index/ src/control-plane/core/tests/vector_index.rs
git commit -m "fix(vector-index): reject trailing bytes after the identity block at decode"
```

### Task 3: Property tests + stale doc note

**Files:**
- Modify: `src/control-plane/core/tests/vector_index_decode_props.rs`

**Interfaces:**
- Consumes: Task 1's build error and Task 2's decode error.

- [ ] **Step 1: Write the two properties** (red only if Tasks 1-2 regressed — they pin the invariant):

```rust
/// Generator: rows with at least one Int AND one Str key (dim fixed small).
fn mixed_rows() -> impl Strategy<Value = Vec<(VectorKey, Vec<f32>)>> {
    // Build on dim_and_rows()'s row generator: prepend one key of each kind,
    // then shuffle — copy its vector-generation shape for the payloads.
    ...
}

proptest! {
    #[test]
    fn mixed_keys_rejected_at_build(rows in mixed_rows()) {
        // Assert the MESSAGE, not just is_err(): a mis-built generator (e.g.
        // payload len != 1) would otherwise pass vacuously via dim-mismatch.
        for e in [
            FlatIndex::build(1, Metric::Cosine, rows.clone()).unwrap_err(),
            IvfFlatIndex::build(1, Metric::Cosine, rows.clone(), None).unwrap_err(),
            HnswIndex::build(1, Metric::Cosine, rows, None, None).unwrap_err(),
        ] {
            prop_assert!(e.to_string().contains("mixed identity key kinds"), "wrong error: {e}");
        }
    }

    #[test]
    fn trailing_suffix_never_decodes((dim, rows) in dim_and_rows(), m in metric(), suffix in proptest::collection::vec(any::<u8>(), 1..16)) {
        let mut blob = FlatIndex::build(dim, m, rows).expect("build").serialize();
        blob.extend_from_slice(&suffix);
        prop_assert!(decode(&blob).is_err());
    }
}
```

(The `...` in `mixed_rows` is filled by reading `dim_and_rows()`'s generator and reusing its row shape — interleave `Int`/`Str` keys so at least one of each is present, and generate every payload at length 1 to match the pinned `dim=1` (a longer payload would trip the dim check first and the message assertion would catch the vacuity). This is the one place the implementer composes from the file's existing generators rather than pasting: the exact combinators must match that file's proptest style.)

Also rewrite the `dim_and_rows()` doc-comment note (line ~29, "…`iss-vector-index-mixed-key-kind-corrupts-decode`, not exercised here.") to state the invariant is now enforced at build and exercised by `mixed_keys_rejected_at_build`.

- [ ] **Step 2: Run**

Run: `buck2 test //src/control-plane/core:vector-index-decode-props > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log`
Expected: PASS.

- [ ] **Step 3: prek + commit**

```bash
buck2 run -v0 //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -c Failed /tmp/p.log  # expect 0
git add src/control-plane/core/tests/vector_index_decode_props.rs
git commit -m "test(vector-index): property-pin key-kind homogeneity and full-consumption decode"
```

### Task 4: Register close + capability doc + final gates

**Files:**
- Modify: `docs/ISSUES.md` (remove the `iss-vector-index-mixed-key-kind-corrupts-decode` entry), `docs/system-capabilities/vector-search.md` (codec section gains the enforced invariant + decode guard, PR ref added at PR time)

- [ ] **Step 1: Close the register item** per `loom-docs-update`: delete the entry block; check no surviving `[[iss-vector-index-mixed-key-kind-corrupts-decode]]` links remain (rewrite any as `` `#id` `` spans — `dim_and_rows` doc and the spec name it in prose only, fine). `bash tools/docs.sh validate` → OK.
- [ ] **Step 2: Update `docs/system-capabilities/vector-search.md`** — in the codec theme: one sentence that key-kind homogeneity is validated at build and decode requires full buffer consumption (cite `(#N)` once the PR number exists). ALSO delete the `#iss-vector-index-mixed-key-kind-corrupts-decode` bullet from its `## Known gaps` (line ~41) — `docs.sh validate` does not scan capability docs, so this dangles silently if forgotten.
- [ ] **Step 3: Fixture backstop** — run the postgres vector suites the spec names:

`buck2 test //src/control-plane/postgres:puffin-roundtrip //src/control-plane/postgres:vector-index-build //src/control-plane/postgres:vector-index-multi --unstable-allow-all-tests-on-re > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log` (adjust target names to the BUCK file; if running locally use `-j 8`).
Expected: PASS.

- [ ] **Step 4: prek + commit**

```bash
git add docs/ISSUES.md docs/system-capabilities/vector-search.md
git commit -m "docs: close iss-vector-index-mixed-key-kind-corrupts-decode"
```
