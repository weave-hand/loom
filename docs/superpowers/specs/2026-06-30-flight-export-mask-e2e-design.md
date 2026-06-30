# Masked-column governed Flight export — end-to-end coverage

- **Date:** 2026-06-30
- **Area:** test
- **Register items:** resolves [[iss-flight-export-mask-e2e]] (stays in ISSUES)
- **Status:** spec (ready for a work agent to plan + build)

## North star

The governed Arrow Flight export ([[road-governed-flight-export]]) carries a
**column-masked** column correctly all the way through the **live engine** — the
advertised `get_flight_info` schema and the streamed `do_get` data agree, and the
masked column arrives as a `Utf8` array of the literal `'***'`. Today this is proven by
construction and a unit test, but never driven end-to-end; this slice adds the e2e case
that pins it.

## Problem

[[road-governed-flight-export]] (PR #204) compiles a masked export column as `'***' AS
col`, so the column streams as a `Utf8` literal and `get_flight_info`'s advertised
schema stays in lockstep with the `do_get` data schema. This is covered by the
`export_arrow_schema` **unit** test and is correct by construction (the mask is a SQL
constant in the compiled `SELECT`), but the masked path is **not** exercised
end-to-end through the live engine in `governed_flight_export_e2e.rs`, which today drives
only:

- `export_streams_vectors_value_exact` — all rows, vector carried as `List<Float32>`,
  value-exact;
- `export_denied_without_grant` — coarse deny → permission error;
- `export_requires_bearer_token` — auth;
- the row-cap case (`setup_with_cap`).

A masked column is the **one place** an advertised-schema-vs-data-schema divergence would
surface (the engine streams a Utf8 constant where the type's native column is something
else). The risk is low — masking is shared with the HTTP read path, which has masking
e2e coverage — but the export wire's own schema lockstep is unverified.

## Design

Add **one** `#[tokio::test]` case to `src/services/query-api/tests/governed_flight_export_e2e.rs`,
reusing the file's existing harness (`rust_test` integration target; the e2e support
library `e2e_support` is already a dep).

### Setup variant

Add a `setup_with_mask` helper mirroring the existing `setup_with_cap`: identical to
`setup` (land the `vector(4)` `Chunk` dataset through the real Iceberg landing path,
register the `Chunk` ontology type, boot the engine `FlightDataService` over a UDS, build
the `FlightExportService` over a `FlightSqlClient`), but grant through
`e2e_support::grant_read_columns(&cp, &role, "Chunk", deny_columns: vec![],
mask_columns: vec!["<scalar col>".into()])` instead of the plain `grant_read`.

The masked column is a **scalar** property of the `Chunk` type (not the `vector(4)`
embedding): the `'***'` constant renders as `Utf8`, which is the masking design's
behaviour for a scalar. The vector column stays `List<Float32>`.

### Assertions

Authed `get_flight_info` + `do_get` exactly as the existing cases drive them, then assert
**three** things:

1. **Advertised schema:** the masked column's field in the `get_flight_info`
   `FlightInfo` schema is `DataType::Utf8`.
2. **Data schema lockstep:** the same column in the **streamed** `do_get` batch schema is
   also `DataType::Utf8` — the divergence guard (advertised must equal data).
3. **Masked values:** every value in that column's array across all batches is the literal
   `"***"`.

A spot check on an **unmasked** scalar (value-exact, as `export_streams_vectors_value_exact`
already does for a known row) guards against over-masking — only the masked column is
replaced.

## Scope

In scope:

- `setup_with_mask` helper + one masked-export e2e case in `governed_flight_export_e2e.rs`.

Out of scope:

- Any production code change (masking is shared with the governed HTTP read path and is
  unchanged; this is a pure coverage addition).
- Masking the **vector** column (the design masks scalars to the `'***'` Utf8 constant; a
  vector-column mask is not a current capability and is not in scope).
- Backends other than the one the export e2e already uses (the export path is
  Iceberg-only; the new case runs there, like its siblings).

## Testing

The deliverable **is** the test. Acceptance: the new case passes in the normal
`buck2 test //src/...` sweep (a `loom_fixture_test`, local-routed like the other
fixture-backed export cases), and the existing four export cases continue to pass
(the `setup_with_mask` addition does not touch the shared harness behaviour).

## Risk

- Minimal: one fixture case + one setup helper variant, no production change. The masking
  ACL machinery (`grant_read_columns`, `mask_columns`) and the export harness both already
  exist and are tested.
- The only subtlety is choosing a scalar column whose masked rendering is `Utf8`; pinned by
  asserting the advertised and data schemas both report `Utf8` and the values are `'***'`.
