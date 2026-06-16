-- Action-scope acl.policy: a policy is keyed by (role, action, target), so a role can
-- hold independent read and write policies on the same target. Mirrors acl.role_grant,
-- which is already action-scoped. Existing rows backfill to 'read' (reads were policy's
-- only enforcer until now); the default is then dropped so the app must always specify.
alter table acl.policy add column action text not null default 'read';
alter table acl.policy drop constraint policy_pkey;
alter table acl.policy add primary key (role_id, action, target_kind, target_a, target_b);
alter table acl.policy alter column action drop default;
