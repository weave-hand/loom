-- Role hierarchy: `role_id` inherits all grants + policies of `inherits_id`
-- (transitively). check()/policies_for() resolve over the effective-role closure.
-- The graph is kept acyclic by add_role_inheritance (cycle -> Conflict).
create table acl.role_inherits (
    role_id     text not null references acl.role (id) on delete cascade,
    inherits_id text not null references acl.role (id) on delete cascade,
    primary key (role_id, inherits_id)
);
