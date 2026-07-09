# Roadmap register

_As of 403f7a6a._

Committed and sequenced work — open (`planned`) items only. Shipped work is
documented per subsystem in [`system-capabilities/`](system-capabilities/README.md)
(the slice-by-slice history lives in git). Deferred ideas live in
[`FUTURE.md`](FUTURE.md); known defects in [`ISSUES.md`](ISSUES.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## ontology

- [ ] **Custom-logic actions slice 4 — enqueue-downstream (write-then-derive)** `{#road-action-enqueue-downstream area:ontology status:planned from:2026-07-01-action-enqueue-downstream-design pr:- spec:2026-07-01-action-enqueue-downstream-design}`
  Slice 4 (write-then-derive): on commit, a governed action **atomically enqueues a downstream job** in the **same** unit of work (the atomic-enqueue point is `CommitExtras.jobs` → `apply_commit_extras` → `pg_insert_if_absent`, inside the write's commit tx). **Phase 1 (insert path) landed:** `ActionDef.downstream: Vec<JobTemplate{kind, payload}>` (`kind` validated against a job-kind allowlist at define time, `payload` leaves are `@self.<prop>` refs validated against the primary target's properties), resolved to `NewJob`s on the write path and threaded through a `jobs_json` wire channel (all 4 write RPCs) into the engine's insert commit tx (`IcebergActionWriter::write_object` → `land_cdc` → `inline_append_decl` / `land_parquet` / `land_parquet_stream`, minimal-ripple — the public 9-arg `land` untouched) — atomic commit-or-neither, proven by e2e. A define-time `validate_downstream_scope` gate restricts `downstream` to single-step Insert until phase 2 (no silent drop). **Phase 2 remains:** engine consumption on `write_delta` (Update/Delete) + `write_steps` (multi-step) + the `overwrite_parquet_snapshot` rebuild-jobs merge — the wire already carries `jobs` on all 4 RPCs, so phase 2 is the engine-method/handler layer + lifting the scope gate. Pairs with [[fut-scheduled-jobs]]. Out: conditional enqueue, general hook bus, new job kinds/handlers.

## acl

- [ ] **Comprehensive auth capability — build-vs-adopt a fuller identity layer** `{#road-auth-comprehensive area:acl status:planned from:2026-06-23-auth-password-session-design pr:- spec:2026-07-01-auth-comprehensive-adopt-design}`
  The committed decision to grow loom's hand-rolled `#road-auth-password-session` foundation into a **fuller identity layer**, taken as one unit rather than a scatter of independent slices. The spec's **first job is the build-vs-adopt fork**: continue extending loom's own `auth` concern, or integrate an established auth library/service — evaluated against loom's constraints (hermetic buck2 build, no external mail infra today, `SubjectId`-centric ACL model, self-hosted deploy). Whichever path wins, its **acceptance surface** is the deferred identity follow-ons this subsumes: TOTP/OTP second factor [[fut-auth-totp-mfa]], passkeys/WebAuthn [[fut-auth-passkeys]], SAML federation [[fut-auth-saml]], per-IP login rate-limiting [[fut-auth-login-rate-limit]], password policy (forced rotation + strength) [[fut-auth-password-policy]], and session refresh / sliding expiry [[fut-auth-session-refresh]] — those `fut-auth-*` ideas stay in FUTURE as the detailed surface this item delivers against. **Out (stay deferred, not part of this item):** email reset ([[fut-auth-email-reset]], hard-blocked on mail infra), service-token scoping ([[fut-auth-token-scoping]]) and the first-class auth-admin capability ([[fut-auth-admin-capability]]) (both gate on a real least-privilege / multi-operator need), and the `Debug`-redaction hygiene fix ([[fut-auth-credential-debug-redact]]). Depends on `#road-auth-password-lifecycle` landing first. Spec to follow — expect it to decompose into slices once the build-vs-adopt call is made.

## query

- [ ] **Stream engine — Subscribe / tail feed (slice 3)** `{#road-stream-subscribe area:query status:planned from:2026-07-08-stream-subscribe-design pr:- spec:2026-07-08-stream-subscribe-design}`
  A governed `GET /objects/{type}/changes` streaming feed (chunked NDJSON) of ordered change events over a CDC table's changelog, resumable from a client-held opaque cursor (multi-consumer; server stateless). The feed is the disjoint UNION ALL of the changelog Iceberg files ∪ the base's live inline tail — flush appends-and-end-caps on one tx, so an event is inline XOR in files (no dedup, and no flush watermark needed for correctness). Sub-second freshness via a `pg_notify('loom_changelog:{tid}')`-on-commit wakeup + poll fallback (the queue's proven pattern); per-batch `GovernedTableProvider` enforces row/column ACL on the stream. The event+cursor contract is transport-agnostic (a future gRPC duplex wraps it). Deferred: server-side consumer-offset storage, the flush watermark, log-table subscribe. Folds in [[fut-serving-stream-to-http]].

## transform

- [ ] **Stream engine — Continuous / standing queries (slice 4)** `{#road-stream-continuous area:transform status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-09-stream-continuous-design}`
  Offset-watermarked micro-batch transform mode; MV output committed as its own subscribable changelog. Extends [[fut-transform-followups]].
- [ ] **Stream engine — Stream joins / delta-join analog (slice 5)** `{#road-stream-joins area:transform status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-09-stream-joins-design}`
  Lookup-join (point-lookup against a PK index, [[fut-stream-pk-index]]) + micro-batch stream-stream join. True stateful incremental join deferred ([[fut-stream-incremental-join]]).
