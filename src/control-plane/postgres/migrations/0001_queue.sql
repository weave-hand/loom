create schema if not exists queue;

create table queue.jobs (
    id          uuid        primary key,
    kind        text        not null,
    payload     jsonb       not null,
    state       text        not null,
    run_at      timestamptz not null default now(),
    priority    int         not null default 0,
    attempts    int         not null default 0,
    last_error  text,
    locked_at   timestamptz,
    locked_by   text,
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now()
);

create index jobs_dequeue_idx on queue.jobs (state, kind, run_at, priority);
