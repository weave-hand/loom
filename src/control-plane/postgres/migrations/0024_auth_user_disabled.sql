-- User deactivation for admin-gated provisioning: a disabled user cannot log in
-- and their sessions are revoked immediately (see Auth::set_user_disabled). NULL
-- = active; a timestamp = disabled-at. Defaults NULL so existing users are
-- unaffected.
alter table auth.user
    add column disabled_at timestamptz;
