# loom wire reference for the Python SDK (companion to the v1 plan)

Verified against the tree at `3a5f915b` (post-`road-python-build-infra`). Every task
of `2026-07-21-python-sdk-v1.md` reads this first; JSON field names here are
authoritative (traced to serde structs, file:line refs in the research notes).

## Service → endpoint routing (two base URLs)

- **ingest_url**: `POST /datasets/{schema}/{table}`, `POST /models/{type}` (+ login/session routes). Ingest mounts NO `/admin/*`.
- **query_url**: all reads (`/ontology/*`, `/datasets*` GETs), `POST /admin/models`, `POST /admin/links` (+ login/session routes).
- `POST /auth/login` exists on both; tokens are opaque random strings whose SHA-256 lives in the shared postgres control plane — **one token works on both services** in the co-deployed/single-postgres model. Bearer header: `Authorization: Bearer <token>`.

## Ingest writes

Content type (both): `application/vnd.apache.arrow.stream` — Arrow IPC stream bytes.

`POST /datasets/{s}/{t}`:
- Query: `mode=stream` (first-write log-table declaration), `buckets` (int ≥1, stream only).
- Headers: `X-Loom-Model`: JSON `{"columns":[{"name":str,"ty":str,"required":bool},...]}` (optional model gate — REQUIRED for zero-row bootstrap of date/timestamp columns, see below); `X-Loom-Run-Id`: optional UUID.
- 200: `{"snapshot_id": <i64>, "dataset": "<schema>.<table>"}`.

`POST /models/{type}`:
- ACL: `Write` on the type; deny → **403 empty body** (even for nonexistent types).
- Query: `identity` (only used when the type is absent → infer-and-create), `mode=cdc`, `buckets`, `merge_engine` ∈ `last_row|first_row|versioned`.
- 200: `{"snapshot_id": <i64>, "type": "<name>"}` (key is `type`).

Errors (both): 400 plain-text message; 403 empty; 422 JSON `{"violations":[...]}`; 500 plain-text `"internal error"`.

**422 violation object**: `{column: str, reason: str, expected?: str, found?: str, rule?: str}` — `reason` ∈ `missing_required|type_mismatch|unsupported|constraint`; `expected`/`found` only on `type_mismatch`; `rule` (∈ `range|length|pattern|one_of`) only on `constraint`. Absent fields are omitted, never null.

**Zero-row landing works**: an empty Arrow IPC stream to `POST /datasets/...` commits a snapshot, creates the mirror table, and records the column schema (inline-append path has no row-count guard). Column types come from `X-Loom-Model` when present, else Arrow-schema inference — and **inference only maps** Int32→integer, Int64→long, Float64→double, Boolean→boolean, Utf8→string; Date32/Timestamp are REJECTED (422 `unsupported`) by inference, so any model with date/timestamp columns must send the `X-Loom-Model` header on the bootstrap land.

## Admin ontology writes (query_url, admin role required)

Admin gate: bearer auth + `ADMIN_ROLE` membership; non-admin → 403 `"forbidden"`.

`POST /admin/models` body:
```json
{"name": str, "table": {"schema": str, "name": str}, "identity": str|null,
 "properties": [{"name": str, "ty": str, "required": bool(default false),
                 "constraints": {"range":{"min":f64?,"max":f64?},"length":{"min":u32?,"max":u32?},
                                 "pattern": str?, "one_of":[str]?}?, "description": str?}],
 "derived": [...]?, "description": str?}
```
201: `{"name": "<name>"}`. 400: JSON `{"error": "<msg>"}` (bad agg/constraint) or mapped Validation; 404 NotFound; 409 Conflict.
**`define_model` does NOT check the backing table exists** — physical conformance bites at land time only.

`POST /admin/links` body (serde shapes of `LinkDef`):
```json
{"name": str, "from": str, "to": str, "cardinality": "One"|"Many",
 "backing": {"ForeignKey": {"from_column": str, "to_column": str}}
          | {"JoinTable": {"table":{"schema":str,"name":str},"from_key":str,"from_column":str,"to_column":str,"to_key":str}},
 "description": str?}
```
201 plain-text `"defined"`. 400 `"invalid LinkDef: ..."`; 404 unknown from/to type.
**Cardinality asymmetry**: writes take `"One"/"Many"`; the read surface renders `"one"/"many"` — `apply`'s idempotency compare must case-normalize.

## Auth

`POST /auth/login` `{"username": str, "password": str}` → 200 `{"token": str}` (no expiry in body; session TTL server-side, default 24h). Failure → uniform 401 plain-text.
Service tokens (admin-minted) are used identically; the SDK only needs a token or login credentials.

## Reads (query_url)

- `GET /ontology/types` → `{"types": [str,...]}`.
- `GET /ontology/types/{name}` → `{"name", "table":{"schema","name"}, "identity": str|null, "properties":[{"name","ty","required","description"?}], "links":[LinkView], "links_to":[LinkView], "description"?}`; LinkView = `{"name","from","to","cardinality"("one"|"many"),"description"?}`. 404 with type name in body.
- `GET /datasets` → `{"datasets":[{"schema","name","project","updated","kind"("table"|"view"),"base"?}]}`. No pagination.
- `GET /datasets/{s}/{t}` (`as_of` RFC3339 / `as_of_snapshot` i64, mutually exclusive) → `{"table":{"schema","name"},"snapshot_id":i64,"snapshot_time":str,"columns":[{"name","ty","nullable"}],"kind","base"?}`. 404 body `"dataset not found"` (no existence oracle); 410 below retention.
- `GET /datasets/{s}/{t}/preview?limit=` (default 20, clamp [1,200]) → `{"columns":[str],"rows":[[str,...]],"sampled":true}` — **every cell is a display string** (nulls → `""`).
- **Long renders as a JSON string** on object/identity reads elsewhere in loom (int64 precision); preview stringifies everything anyway.

## Logical types (the pydantic mapping's target vocabulary)

Ontology property `ty` names (case-insensitive in, canonical lowercase out; closed vocab):
`integer`(Int32) `long`(Int64) `double`(Float64) `boolean`(Bool) `string`(Utf8) `date`(Date32) `timestamp`(Timestamp µs, no tz) `vector(N)`(List<f32 "item" non-null>); aliases `emailaddress|url|phonenumber` → string. No implicit widening (Integer ≠ Long).

Python → loom → Arrow mapping (locked for the SDK):
`int → long → pa.int64()`, `str → string → pa.string()`, `float → double → pa.float64()`, `bool → boolean → pa.bool_()`, `datetime.datetime → timestamp → pa.timestamp("us")`, `datetime.date → date → pa.date32()`; `X | None` → `required=False` / nullable column.

## E2E harness facts

- Closest precedent: `src/services/standalone/tests/composite_e2e.rs` — boots ingest+engine+query-api+worker via `standalone::run` with **embedded postgres** (`LOOM_PG_MODE=embedded`, `POSTGRES_BIN_DIR`, `POSTGRES_LD_LIBRARY_PATH` from the `loom_fixture_test` env; data dir `<LOOM_DATA_PATH>/pgdata`, socket `<LOOM_DATA_PATH>/pgrun`), `file://` warehouse in a TempDir, **no MinIO**, real `reqwest` HTTP against ephemeral ports.
- `loom_fixture_test` (src/control-plane/postgres/defs.bzl) injects `POSTGRES_BIN_DIR`, `POSTGRES_LD_LIBRARY_PATH`, `LOOM_PG_FIXTURE_SLOT_DIR`; `minio = False` default.
- Auth seeding in existing e2e is out-of-band (direct control-plane pool), not via HTTP.
