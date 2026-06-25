-- The auth concern: credential + session store. A loom *user* is an ACL subject
-- (acl.subject) that has credentials; auth and acl join by subject_id. The
-- session primary key is the token HASH (sha-256), never the raw token.
create schema if not exists auth;

create table auth.user (
    subject_id text primary key,
    username   text unique not null,
    created_at timestamptz not null default now()
);

create table auth.password_credential (
    subject_id   text primary key references auth.user (subject_id) on delete cascade,
    password_phc text not null, -- Argon2 PHC string, computed service-side
    updated_at   timestamptz not null default now()
);

create table auth.session (
    token_sha256 bytea primary key,
    subject_id   text not null references auth.user (subject_id) on delete cascade,
    expires_at   timestamptz not null,
    created_at   timestamptz not null default now()
);
