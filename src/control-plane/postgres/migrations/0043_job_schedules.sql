create table queue.schedule (
    name        text primary key,
    kind        text not null,
    payload     jsonb not null,
    cron        text not null,
    next_run_at timestamptz not null
);

create index schedule_due on queue.schedule (next_run_at);
