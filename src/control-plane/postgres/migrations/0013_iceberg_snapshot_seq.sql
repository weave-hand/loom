-- Catalog-global monotonic snapshot ids, concurrency-safe. Replaces the
-- max(snapshot_id)+1 allocation in next_snapshot (two writers could read the
-- same max and collide on the snapshot PK). nextval is atomic and never reuses
-- a value; gaps (from rolled-back commits) are acceptable — only monotonicity
-- and uniqueness matter to the mirror's MVCC ordering.
create sequence iceberg_mirror.snapshot_seq as bigint start with 1 increment by 1;
