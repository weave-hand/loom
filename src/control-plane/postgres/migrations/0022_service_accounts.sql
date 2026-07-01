-- Service accounts: a machine identity that is an ACL subject (acl.subject) with
-- NO password_credential — it can never password-login. Parallel to auth.user; the
-- two never share a row. Tokens live in auth.service_token, keyed by the token HASH
-- (sha-256), never the raw token, exactly like auth.session. Every token has a
-- mandatory expiry; rotation = mint-new-then-revoke-old, overlapping allowed.
create table auth.service_account (
    subject_id text primary key,
    name       text unique not null,
    created_at timestamptz not null default now()
);

create table auth.service_token (
    token_sha256 bytea primary key,
    subject_id   text not null references auth.service_account (subject_id) on delete cascade,
    label        text not null,
    created_at   timestamptz not null default now(),
    expires_at   timestamptz not null,
    revoked_at   timestamptz
);

create index service_token_subject_id_idx on auth.service_token (subject_id);
