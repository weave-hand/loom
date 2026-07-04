-- Debounce probe for data triggers: "does this transform already have a
-- queued run" is checked once per matched def per commit.
create index run_queued_by_transform on transforms.run (transform)
    where state = 'queued';
