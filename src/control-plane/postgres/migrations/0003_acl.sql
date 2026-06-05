create schema if not exists acl;

create table acl.subject (
    id text primary key
);

create table acl.role (
    id text primary key
);

create table acl.role_member (
    subject_id text not null references acl.subject (id) on delete cascade,
    role_id    text not null references acl.role (id) on delete cascade,
    primary key (subject_id, role_id)
);

-- Coarse allow grants; powers check(). Target encoded as (kind, a, b), all NOT
-- NULL so they sit in the primary key:
--   kind='type'  -> a=TypeName,        b=''
--   kind='table' -> a=TableRef.schema, b=TableRef.name
create table acl.role_grant (
    role_id     text not null references acl.role (id) on delete cascade,
    action      text not null, -- 'read' | 'write'
    target_kind text not null, -- 'type' | 'table'
    target_a    text not null,
    target_b    text not null,
    primary key (role_id, action, target_kind, target_a, target_b)
);

-- Fine row/column policy; powers policies_for(). One row per (role, target).
create table acl.policy (
    role_id      text   not null references acl.role (id) on delete cascade,
    target_kind  text   not null,
    target_a     text   not null,
    target_b     text   not null,
    row_filter   jsonb,                       -- nullable; serialized RowFilter tree
    deny_columns text[] not null default '{}',
    primary key (role_id, target_kind, target_a, target_b)
);
