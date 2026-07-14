-- Where an MV was told to BEGIN reading its source, per bucket
-- (iss-mv-register-below-reclaimed-floor).
--
-- `next_offset` alone cannot answer "did this MV ever see offsets 0..N?": a row bootstrapped at
-- N by `define_transform` (because the source's prefix was already reclaimed) is byte-identical
-- to one a run advanced to N. `start_offset` records the difference, so the gap is an auditable
-- fact rather than a lost log line.
--
-- The default is the correct backfill for every pre-existing row AND for every row the watermark
-- CAS creates from 0: such an MV genuinely started at offset 0.
alter table stream.mv_watermark
    add column start_offset bigint not null default 0 check (start_offset >= 0);
