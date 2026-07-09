-- The replace-class merge engine for a CDC table's current-state fold
-- (road-stream-merge-engines). Default 'last_row' (byte-identical to the
-- pre-engine fold). Immutable after declaration (reconcile_stream_mode rejects
-- a redeclare with a different engine).
alter table stream.stream_table
    add column merge_engine text not null default 'last_row'
        check (merge_engine in ('last_row', 'first_row', 'versioned'));
