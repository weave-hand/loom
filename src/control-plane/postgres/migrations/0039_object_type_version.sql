-- The version/sequence column for a type, used as precedence by the Versioned
-- CDC merge engine (road-stream-merge-engines). NULL = no version column
-- (the default; LastRow/FirstRow engines ignore it). Mirrors `identity`.
alter table ontology.object_type
    add column version text;
