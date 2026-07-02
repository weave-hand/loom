-- One-way bootstrap seal: a single row records when the first admin was created.
-- Absence of the row = Uninitialized; presence = Sealed. There is no unseal path.
create table acl.bootstrap (
    id        smallint primary key default 1 check (id = 1),
    sealed_at timestamptz not null default now()
);
