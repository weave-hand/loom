-- A log table must have at least one bucket; a zero/negative count would make
-- the write-path modulo (`row_index % bucket_count`) meaningless. DB-level
-- backstop behind the in-code Validation guard in `inline_append`.
alter table stream.stream_table
    add constraint bucket_count_positive check (bucket_count >= 1);
