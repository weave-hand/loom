create schema if not exists lineage;

create table lineage.event (
    event_id   bigserial   primary key,
    run_id     uuid        not null,
    event_type text        not null, -- 'start'|'running'|'complete'|'abort'|'fail'
    event_time timestamptz not null,
    payload    jsonb       not null
);

create index on lineage.event (run_id);

-- Per-event inputs/outputs; powers the one-hop graph. Ordinal preserves emit order.
create table lineage.event_dataset (
    event_id  bigint not null references lineage.event (event_id) on delete cascade,
    direction text   not null, -- 'input' | 'output'
    ordinal   int    not null,
    namespace text   not null,
    name      text   not null,
    primary key (event_id, direction, ordinal)
);

create index on lineage.event_dataset (direction, namespace, name);
