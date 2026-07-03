-- Multi-step actions: an action is an ordered list of single-target steps.
-- Move target_type/kind from `action` to a new per-step table, and re-key the
-- param/assignment tables by (action_name, step_ordinal, ordinal).

create table ontology.action_step (
    action_name text    not null references ontology.action (name) on delete cascade,
    ordinal     int     not null,
    target_type text    not null references ontology.object_type (name) on delete cascade,
    kind        text    not null,
    bind        text,
    primary key (action_name, ordinal)
);

-- Backfill one implicit step per existing action.
insert into ontology.action_step (action_name, ordinal, target_type, kind, bind)
select name, 0, target_type, kind, null from ontology.action;

-- Re-key params + assignments by step (existing rows belong to step 0).
alter table ontology.action_param add column step_ordinal int not null default 0;
alter table ontology.action_assignment add column step_ordinal int not null default 0;

alter table ontology.action_param drop constraint action_param_pkey;
alter table ontology.action_param add primary key (action_name, step_ordinal, ordinal);
alter table ontology.action_assignment drop constraint action_assignment_pkey;
alter table ontology.action_assignment add primary key (action_name, step_ordinal, ordinal);

-- Cross-step reference source (Task 3): an assignment may be a StepRef(bind, prop).
-- Extend the exactly-one-source check to include the ref pair.
alter table ontology.action_assignment add column ref_bind text;
alter table ontology.action_assignment add column ref_prop text;
alter table ontology.action_assignment drop constraint action_assignment_source_ck;
alter table ontology.action_assignment add constraint action_assignment_source_ck check (
    (value is not null and expr is null and ref_bind is null)
    or (value is null and expr is not null and ref_bind is null)
    or (value is null and expr is null and ref_bind is not null and ref_prop is not null)
);

-- target_type/kind now live on action_step; drop them from action.
alter table ontology.action drop column target_type;
alter table ontology.action drop column kind;
