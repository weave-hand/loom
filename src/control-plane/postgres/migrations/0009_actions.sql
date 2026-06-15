create table ontology.action (
    name        text primary key,
    target_type text not null references ontology.object_type (name) on delete cascade
);

create table ontology.action_param (
    action_name text    not null references ontology.action (name) on delete cascade,
    ordinal     int     not null,
    name        text    not null,
    ty          text    not null,
    required    boolean not null,
    primary key (action_name, ordinal)
);
