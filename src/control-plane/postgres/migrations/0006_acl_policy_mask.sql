-- Column masking: a policy can list columns shown-but-redacted (distinct from
-- deny_columns, which removes the column). The query API emits a '***' marker for
-- these. Existing rows default to no masked columns (backward-compatible).
alter table acl.policy
    add column mask_columns text[] not null default '{}';
