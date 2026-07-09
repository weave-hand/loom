-- Action-level downstream job templates (slice 4: enqueue-downstream).
-- Mirrors the action_step/action_param/action_assignment per-action child tables.
create table ontology.action_downstream (
    action_name text not null references ontology.action(name) on delete cascade,
    ordinal     int  not null,
    kind        text not null,
    payload     jsonb not null default '{}'::jsonb,
    primary key (action_name, ordinal)
);
