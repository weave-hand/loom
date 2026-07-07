-- Per-(table, bucket) monotonic offset allocator: the spine of the stream
-- engine. `next` is the next offset to hand out (offsets start at 0). An
-- allocation of `count` returns the pre-increment value as the first offset of a
-- contiguous run and advances `next` by `count`. The row's lock serializes
-- concurrent allocations per bucket, so offsets are gapless within a bucket;
-- buckets and tables are independent. The allocation runs on the caller's
-- executor, so an offset is assigned iff that write commits.
create schema if not exists stream;

create table stream.bucket_offset (
    table_id bigint not null,
    bucket   int    not null,
    next     bigint not null,
    primary key (table_id, bucket)
);
