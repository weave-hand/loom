-- runs_for(dataset) (Lineage::runs_for, #616) filters lineage.event_dataset on
-- (namespace, name) alone. The existing (direction, namespace, name) index leads
-- with `direction` — which that read does not constrain — so Postgres cannot
-- prefix-scan it and would scan the append-only edge table. Add a supporting index
-- so the per-dataset run-history read (and any future (namespace, name)-keyed
-- lineage read) uses an index rather than growing linearly with emitted edges.
create index on lineage.event_dataset (namespace, name);
