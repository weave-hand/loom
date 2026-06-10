-- Deny-override: a grant now carries an effect ('allow' | 'deny'); deny wins in check().
-- Existing rows default to 'allow' (backward-compatible; no backfill needed).
alter table acl.role_grant
    add column effect text not null default 'allow';
