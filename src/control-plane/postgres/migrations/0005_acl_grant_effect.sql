-- Deny-override: a grant now carries an effect ('allow' | 'deny'). Deny will win in
-- check() once that precedence is wired (ACL Task 2); this migration only adds the
-- column. Existing rows default to 'allow' (backward-compatible; no backfill needed).
alter table acl.role_grant
    add column effect text not null default 'allow';
