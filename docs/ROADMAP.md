# Roadmap register

_As of 37d7a4f2._

Committed and sequenced work — open (`planned`) items only. Shipped work is
documented per subsystem in [`system-capabilities/`](system-capabilities/README.md)
(the slice-by-slice history lives in git). Deferred ideas live in
[`FUTURE.md`](FUTURE.md); known defects in [`ISSUES.md`](ISSUES.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## catalog

## ontology

- [ ] **Surface derived-property and vector-index descriptions on the read path** `{#road-ontology-derived-index-read-surface area:ontology status:planned from:2026-07-16-ontology-semantic-descriptions-design pr:- spec:2026-07-21-ontology-derived-index-read-surface-design}`
  Promoted from `#fut-ontology-derived-index-read-surface`. `road-ontology-semantic-descriptions` (shipped, #451) left `DerivedPropertyDef.description` and `VectorIndexDef.description` write-only — settable via `POST /admin/models`, invisible on every read surface. The spec commits the full surface: `GET /ontology/types/{name}` gains `derived[]` and `vector_indexes[]` arrays (with descriptions, documented in `TypeDetailResponse`), and the per-type OpenAPI **read** component schemas additionally declare derived properties as `readOnly` — fixing the standing docs inaccuracy where object-read rows carry derived columns the generated document never mentions. No control-plane or wire changes: every read op used (`get_type`'s `derived`, `vector_indexes_for`) already exists on both the direct and wire control planes.

## acl

- [ ] **Comprehensive auth capability — build-vs-adopt a fuller identity layer** `{#road-auth-comprehensive area:acl status:planned from:2026-06-23-auth-password-session-design pr:- spec:2026-07-01-auth-comprehensive-adopt-design}`
  The committed decision to grow loom's hand-rolled `#road-auth-password-session` foundation into a **fuller identity layer**, taken as one unit rather than a scatter of independent slices. The spec's **first job is the build-vs-adopt fork**: continue extending loom's own `auth` concern, or integrate an established auth library/service — evaluated against loom's constraints (hermetic buck2 build, no external mail infra today, `SubjectId`-centric ACL model, self-hosted deploy). Whichever path wins, its **acceptance surface** is the deferred identity follow-ons this subsumes: TOTP/OTP second factor [[fut-auth-totp-mfa]], passkeys/WebAuthn [[fut-auth-passkeys]], SAML federation [[fut-auth-saml]], per-IP login rate-limiting [[fut-auth-login-rate-limit]], password policy (forced rotation + strength) [[fut-auth-password-policy]], and session refresh / sliding expiry [[fut-auth-session-refresh]] — those `fut-auth-*` ideas stay in FUTURE as the detailed surface this item delivers against. **Out (stay deferred, not part of this item):** email reset ([[fut-auth-email-reset]], hard-blocked on mail infra), service-token scoping ([[fut-auth-token-scoping]]) and the first-class auth-admin capability ([[fut-auth-admin-capability]]) (both gate on a real least-privilege / multi-operator need), and the `Debug`-redaction hygiene fix ([[fut-auth-credential-debug-redact]]). Depends on `#road-auth-password-lifecycle` landing first. Spec to follow — expect it to decompose into slices once the build-vs-adopt call is made.

## query

## transform

## iceberg

- [ ] **Carry `GcSummary.held_by_mv_floor` on the `GcTable` RPC and into worker logs** `{#road-gc-hold-count-on-wire area:iceberg status:planned from:2026-07-12-mv-watermark-aware-gc-design pr:- spec:2026-07-21-gc-hold-count-on-wire-design}`
  Promoted from `#fut-gc-hold-count-on-wire`. `gc_table` computes the candidates the MV read-position floor withheld, but the engine drops the count when building the three-field `GcTableResponse`, the wire client returns a bare 3-tuple, and the worker discards even that (`handle_gc`'s `.map(|_| ())`) — so "GC reclaimed nothing" is indistinguishable from "GC was blocked by a wedged MV / ghost watermark / pre-declaration hold ([[iss-mv-floor-holds-pre-declaration-files]])" anywhere outside the engine's own `tracing::warn!`. The spec commits: a fourth proto field (`held_by_mv_floor`), a named `GcCounts` struct on the engine-wire client, and worker logging of all four counts (warn when held > 0). Laggard-MV identity strings stay engine-log-only; query-api's fire-and-forget 202 is out of scope.

## deploy

## build

## devx

- [ ] **Programmatic ACL admin surface in `loom-sdk` (`client.admin.roles`)** `{#road-python-sdk-acl-admin area:devx status:planned from:2026-07-21-python-sdk-v1-design pr:- spec:2026-07-22-python-sdk-acl-admin-design}`
  Promoted from `fut-python-sdk-acl-admin`. Wrap the full roles + grants + user-role lifecycle (the nine existing `/admin/roles*` and `/admin/users/{u}/roles*` endpoints) as a typed `client.admin.roles` namespace on both shells: sans-IO builders/parsers in `_core.py`, a `GrantEntry` model, line-identical sync/async shells. Acceptance: the e2e smoke test's `_grant_acl` drops its raw-httpx block and drives the SDK surface. Row-level policies stay deferred ([[fut-python-sdk-policy-admin]]).
- [ ] **Self-referential `Link` in `loom-sdk`'s pydantic layer** `{#road-python-sdk-link-self-ref area:devx status:planned from:2026-07-21-python-sdk-v1-design pr:- spec:2026-07-22-python-sdk-link-self-ref-design}`
  Promoted (narrowed) from `fut-python-sdk-link-forward-refs`: support `parent: Link["Node", "parent_id"] | None` inside `class Node` via two-pass resolution in `__pydantic_init_subclass__`, with post-resolution annotation repair + `model_rebuild(force=True)` so the FK stays fully typed; the explicit FK column is required (the one-arg default always collides with the class's own identity). General forward refs and mutual A↔B cycles remain deferred under the narrowed [[fut-python-sdk-link-forward-refs]].

## cross-cutting
