-- Account lockout for online brute-force resistance (see Auth::record_failed_login).
-- Store-backed so the counter is correct across restarts and shared across replicas;
-- an attacker cannot reset it via a pod bounce. All NULL/0 defaults, so existing
-- users are unaffected (unlocked, zero failures).
alter table auth.user
    add column failed_attempt_count integer not null default 0,
    add column last_failed_at       timestamptz,
    add column locked_until         timestamptz;
