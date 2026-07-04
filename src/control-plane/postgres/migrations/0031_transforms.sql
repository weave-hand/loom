-- The transforms concern: named definitions and their durable runs.
create schema if not exists transforms;

create table transforms.transform (
    name            text primary key,
    body            jsonb   not null,
    schedule        text,
    on_input_commit boolean not null default false
);

create table transforms.run (
    run_id      uuid primary key,
    -- Plain text, no FK: runs survive definition deletion (frozen history).
    transform   text,
    trigger     text not null,
    state       text not null,
    body        jsonb not null,
    queued_at   timestamptz not null,
    started_at  timestamptz,
    finished_at timestamptz,
    snapshot_id bigint,
    error       text
);

create index run_by_transform on transforms.run (transform, queued_at desc, run_id desc);
