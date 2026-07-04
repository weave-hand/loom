-- Slice 2: derived schedule state. next_run_at is computed at define/claim
-- time from the cron expression; the partial index serves the scheduler scan.
alter table transforms.transform add column next_run_at timestamptz;

create index transform_due on transforms.transform (next_run_at)
    where schedule is not null;
