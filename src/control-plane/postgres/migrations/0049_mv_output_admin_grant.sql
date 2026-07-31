-- Backfill: micro-batch (materialized-view) transform outputs are bare, untyped
-- log-stream tables with no ontology type to inherit catalog/lineage visibility
-- from. Define-time now grants the reserved admin role Read on them (the same
-- treatment a `physical` output has had since the catalog ACL gating landed), but
-- MV outputs defined BEFORE that change carry no grant and would stay invisible
-- until an operator redefined them by hand. Grant them here.
--
-- `join acl.role` is load-bearing, not decoration: role_grant.role_id references
-- acl.role (id), and the 'admin' role row is created by `loom create-admin`, not by
-- a migration. On a database where no admin has been bootstrapped the join yields
-- zero rows and this is a clean no-op; a bare `select 'admin'` would violate the FK
-- and fail the migration.
--
-- 'microbatch' / 'microbatch_join' are the serde tags of TransformBody's MV
-- variants, not the Rust variant names.
--
-- Fail-loud on a malformed body, by design: if an MV row's `body -> 'output'` were
-- absent or not an object, `->> 'schema'`/`->> 'name'` yield NULL and the NOT NULL on
-- target_a/target_b aborts this migration — and with it startup. That cannot happen
-- today (`output: TableRef` is required on both MV variants), and it is deliberately
-- stricter than the runtime, which tolerates an undecodable body with a
-- `tracing::warn!` + skip (src/transforms.rs). The asymmetry is intentional: a skipped
-- row here would leave a permanently invisible table with no operator signal, whereas
-- an aborted migration is loud, immediate, and recoverable by fixing the row.
insert into acl.role_grant (role_id, action, target_kind, target_a, target_b)
select r.id,
       'read',
       'table',
       t.body -> 'output' ->> 'schema',
       t.body -> 'output' ->> 'name'
  from transforms.transform t
  join acl.role r on r.id = 'admin'
 where t.body ->> 'kind' in ('microbatch', 'microbatch_join')
on conflict do nothing;
