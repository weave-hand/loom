# Roadmap register

_As of 403f7a6a._

Committed and sequenced work — open (`planned`) items only. Shipped work is
documented per subsystem in [`system-capabilities/`](system-capabilities/README.md)
(the slice-by-slice history lives in git). Deferred ideas live in
[`FUTURE.md`](FUTURE.md); known defects in [`ISSUES.md`](ISSUES.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## ontology

- [ ] **Custom-logic actions slice 4 — enqueue-downstream (write-then-derive)** `{#road-action-enqueue-downstream area:ontology status:planned from:2026-07-01-action-enqueue-downstream-design pr:- spec:2026-07-01-action-enqueue-downstream-design}`
  Slice 4: on commit, a governed action **atomically enqueues a downstream job** in the **same** unit of work (write-then-derive). The atomic-enqueue point already exists — `append_parquet_snapshot` (`iceberg_landing.rs:149`) already takes `jobs: &[NewJob]` and enqueues them in the write's `pool.begin()` tx, and `Tx::enqueue` (`transaction.rs:41`) is its seam. The gap is action-level: `ActionDef` gains optional `downstream: Vec<JobTemplate{kind, payload}>` where `kind` is validated at define time against a job-kind allowlist and `payload` leaves may reference params + the **written identity** (`@self.id`, reusing `#road-action-computed-assignments`'s resolver). The write path resolves the templates to `NewJob`s and threads `&[NewJob]` through `write_object`/`overwrite_table` → `land` → the existing enqueue — so the job is visible to a worker **iff** the write commits (rollback ⇒ no job). Insert+Update+Delete. Promotes `#fut-action-enqueue-downstream`. Smallest of the three (plumbing largely landed); pairs with [[fut-scheduled-jobs]]. Out: conditional enqueue, general hook bus, new job kinds/handlers.
- [ ] **Stream engine — PK / CDC tables (slice 2)** `{#road-stream-pk-tables area:ontology status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-06-stream-engine-design}`
  Emit +I/−U/+U/−D change events on the mutation path; write the dual Iceberg tables (changelog + current-state base). Builds on the shipped `road-cow-inline-shadow` (merge-on-read + CAS) and brings `fut-cow-inline-shadow` slice-2 compaction into scope. Merge engine LastRow only; others in [[fut-stream-merge-engines]].

## acl

- [ ] **Comprehensive auth capability — build-vs-adopt a fuller identity layer** `{#road-auth-comprehensive area:acl status:planned from:2026-06-23-auth-password-session-design pr:- spec:2026-07-01-auth-comprehensive-adopt-design}`
  The committed decision to grow loom's hand-rolled `#road-auth-password-session` foundation into a **fuller identity layer**, taken as one unit rather than a scatter of independent slices. The spec's **first job is the build-vs-adopt fork**: continue extending loom's own `auth` concern, or integrate an established auth library/service — evaluated against loom's constraints (hermetic buck2 build, no external mail infra today, `SubjectId`-centric ACL model, self-hosted deploy). Whichever path wins, its **acceptance surface** is the deferred identity follow-ons this subsumes: TOTP/OTP second factor [[fut-auth-totp-mfa]], passkeys/WebAuthn [[fut-auth-passkeys]], SAML federation [[fut-auth-saml]], per-IP login rate-limiting [[fut-auth-login-rate-limit]], password policy (forced rotation + strength) [[fut-auth-password-policy]], and session refresh / sliding expiry [[fut-auth-session-refresh]] — those `fut-auth-*` ideas stay in FUTURE as the detailed surface this item delivers against. **Out (stay deferred, not part of this item):** email reset ([[fut-auth-email-reset]], hard-blocked on mail infra), service-token scoping ([[fut-auth-token-scoping]]) and the first-class auth-admin capability ([[fut-auth-admin-capability]]) (both gate on a real least-privilege / multi-operator need), and the `Debug`-redaction hygiene fix ([[fut-auth-credential-debug-redact]]). Depends on `#road-auth-password-lifecycle` landing first. Spec to follow — expect it to decompose into slices once the build-vs-adopt call is made.

## query

- [ ] **Stream engine — Subscribe / tail feed (slice 3)** `{#road-stream-subscribe area:query status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-06-stream-engine-design}`
  Governed query-api endpoint yielding a per-bucket offset-cursor feed of change events (changelog Iceberg ∪ inline tail), with LISTEN/NOTIFY + polling fallback and column projection. Folds in [[fut-serving-stream-to-http]].

## transform

- [ ] **Stream engine — Continuous / standing queries (slice 4)** `{#road-stream-continuous area:transform status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-06-stream-engine-design}`
  Offset-watermarked micro-batch transform mode; MV output committed as its own subscribable changelog. Extends [[fut-transform-followups]].
- [ ] **Stream engine — Stream joins / delta-join analog (slice 5)** `{#road-stream-joins area:transform status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-06-stream-engine-design}`
  Lookup-join (point-lookup against a PK index, [[fut-stream-pk-index]]) + micro-batch stream-stream join. True stateful incremental join deferred ([[fut-stream-incremental-join]]).
