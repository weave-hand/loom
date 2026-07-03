# Vector-index codec — key-kind homogeneity enforcement

**Status:** approved
**Date:** 2026-07-03
**Fixes:** `iss-vector-index-mixed-key-kind-corrupts-decode`
**Code:** `src/control-plane/core/src/vector_index/{codec,flat,ivf,hnsw}.rs`

## Problem

The LVIX identity block is written by `codec.rs::write_keys` with a single
`key_kind` header byte derived from `keys.first()` alone (`Str` → 1, `Int` or
empty → 0), but the per-key loop then encodes each key by its **own** variant
(`Int` → 8-byte i64 LE, `Str` → u32 len + UTF-8). `read_keys` decodes all
`row_count` keys uniformly by the one header byte. Nothing between them
enforces the invariant the format assumes — `flat.rs`'s wire-format comment
("All vectors share one key_kind") is documentation only.

`pack_rows` — the shared rows→`(keys, data)` seam all three `build`s use —
validates only vector length against `dim`. So `FlatIndex::build(1, Cosine,
vec![(Int(0), vec![0.0]), (Str("\0"), vec![0.0])])` returns `Ok`, and
`serialize()` emits an identity block whose byte length disagrees with its own
header. `decode(&idx.serialize())` then either errors
`Backend("vector index decode: truncated")` (the shrunk case above, observed
identically for flat/IVF/HNSW by the `*_round_trips` properties in
`tests/vector_index_decode_props.rs`) or — when the two encodings happen to sum
to a compatible length (e.g. one `Int` + one 4-byte `Str` under kind 0 reads as
two garbage `Int`s) — **silently misparses**, returning wrong identities with
no error. None of the three `deserialize`s checks that the buffer is fully
consumed, so under-consumption (stray trailing bytes) is also silent.

## Design

**Mixed kinds are unreachable with valid data, so the blob format stays at
version 1 and homogeneity becomes a fail-loud validated precondition.** The
code evidence: a `VectorKey`'s variant is a pure function of the identity
column's declared logical type (`Integer`/`Long` → `Int`, `String` → `Str` —
`mod.rs::VectorKey` doc), and every production build resolves exactly one
declared identity column per object type (`postgres/src/vector_index.rs::
identity_column_for`, which errors on a NULL identity). Both source tiers
decode that one column by its declared type — `extract_rows` for cold Parquet,
and the hot inline delta deliberately keys off the DECLARED `BaseType` (the
`iss-inline-delta-string-identity` fix) — so one build can never legitimately
produce mixed kinds. Per-key kind tags plus a format-version bump would spend
a migration/rebuild story on a state no valid caller can reach.

Two changes, both in `core/src/vector_index/`:

1. **Encode-side rejection in `pack_rows`.** Alongside the existing
   dim-mismatch check: take the first key's discriminant as the expected kind;
   any later row with the other variant returns
   `ControlPlaneError::Validation("mixed identity key kinds: all index keys
   must be Int or all Str")`. `pack_rows` is the single constructor seam —
   `FlatIndex`/`IvfFlatIndex`/`HnswIndex` fields are private, `build` and
   `deserialize` are the only constructors, and `deserialize` is homogeneous
   by construction (`read_keys` decodes one kind) — so after this check
   `write_keys` can never observe mixed keys. Update `write_keys`'s doc
   comment from describing a hope to citing the enforced invariant.

2. **Decode-side integrity check: full-consumption.** The identity block is
   the final section of all three formats, so add
   `codec.rs::expect_eof(r: &ByteReader) -> Result<()>` — `remaining() > 0` →
   `bad("trailing bytes after identity block")` — called as the last step of
   `FlatIndex::deserialize`, `IvfFlatIndex::deserialize`, and
   `HnswIndex::deserialize`. This upgrades the silent under-consumption arm
   (e.g. `[Str("ab"), Int(_)]` under header kind 1 misreads the i64's LE bytes
   as a length-prefixed string and leaves a stray byte) to a loud decode
   error, and rejects padded/corrupt blobs generally. It is deliberately
   **defence-in-depth, not a complete mixed-blob detector** — a
   coincidental-length mixed blob still parses as garbage keys — which is why
   change 1 is the primary fix: no loom version can write such a blob again.
   The Puffin read path (`puffin.rs::read_index_blob`) hands `decode` the
   exact blob payload, so well-formed blobs carry no trailing bytes; the
   golden-bytes tests and round-trip properties pin that no existing blob is
   rejected. No migration: production blobs are homogeneous (see above), so
   nothing on disk needs rebuilding.

Error-type choice: `Validation` at build (caller-shaped input defect, matching
the dim-mismatch precedent), `Backend` via `bad(...)` at decode (corrupt-blob
class, matching every other decode error).

## Acceptance criteria

Red-first, all pure-logic `rust_test` targets (RE-eligible):

- `tests/vector_index.rs` (`//src/control-plane/core:vector-index`): for each
  of `FlatIndex::build` / `IvfFlatIndex::build` / `HnswIndex::build`, the
  shrunk mixed-key case (`dim=1`, `[(Int(0), [0.0]), (Str("\0"), [0.0])]`)
  returns `Err(Validation(...))` mentioning "mixed identity key kinds" — red
  today (all three return `Ok`). Both orderings (`Int` first, `Str` first).
- `tests/vector_index.rs`: a serialized homogeneous index with one extra byte
  appended fails `decode` with "trailing bytes after identity block" — red
  today (decodes `Ok`, silently ignoring the byte). One case per index kind.
- `tests/vector_index_decode_props.rs`
  (`:vector-index-decode-props`): a new `mixed_keys_rejected_at_build`
  property — generator sampling per-row `Int`/`Str` with at least one of each
  — asserts every `build` errs; the note pointing at this issue in
  `dim_and_rows`'s doc comment is rewritten to cite the enforced invariant.
  A second property: `decode(encode(x) ++ arbitrary non-empty suffix)` never
  returns `Ok`.
- Unchanged and green: all `GOLDEN_*` constants in
  `tests/vector_index_codec.rs` (byte-identical output for homogeneous
  builds), the three `*_round_trips` properties, and the postgres
  `puffin_roundtrip`/`vector_index_build`/`vector_index_multi` fixture suites.

## Out of scope

- Per-key kind tags / LVIX version 2 — rejected above; revisit only if a
  composite-identity or mixed-type identity feature ever lands.
- The production build path's identity handling (`extract_rows`, hot-delta
  string identities) — owned by `road-vector-build-decomposition`.
- Search-path key handling (`engine-serving`'s `build_result_batch` collapses
  mixed merged keys to a sentinel; hot/cold merge semantics) — unchanged, and
  unreachable-mixed for the same declared-identity reason.
- Vector-index staleness and rebuild scheduling
  (`iss-overwrite-vector-index-staleness`).
