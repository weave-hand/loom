use async_trait::async_trait;
use control_plane_core::{
    Auth, ControlPlaneError, LockoutPolicy, NewServiceAccount, NewUser, Page, PageReq,
    PasswordCredential, Result, ServiceAccount, ServiceToken, SubjectId, UserSummary,
};
use time::OffsetDateTime;

use crate::{PgControlPlane, backend};

/// Map a unique-violation (SQLSTATE 23505) to `Conflict`, anything else to `Backend`.
fn conflict_or_backend(e: sqlx::Error, what: &str) -> ControlPlaneError {
    if let sqlx::Error::Database(db) = &e
        && db.code().as_deref() == Some("23505")
    {
        return ControlPlaneError::Conflict(what.to_string());
    }
    ControlPlaneError::Backend(Box::new(e))
}

/// Map a foreign-key violation (SQLSTATE 23503) to `NotFound`, anything else to `Backend`.
fn notfound_or_backend(e: sqlx::Error, what: &str) -> ControlPlaneError {
    if let sqlx::Error::Database(db) = &e
        && db.code().as_deref() == Some("23503")
    {
        return ControlPlaneError::NotFound(what.to_string());
    }
    ControlPlaneError::Backend(Box::new(e))
}

#[async_trait]
impl Auth for PgControlPlane {
    #[tracing::instrument(skip(self, user), level = "debug")]
    async fn create_user(&self, user: &NewUser) -> Result<()> {
        // One transaction: ensure the ACL subject, then the user + credential.
        // A duplicate username aborts on the auth.user insert (23505 -> Conflict).
        let mut tx = self.pool().begin().await.map_err(backend)?;
        sqlx::query!(
            "insert into acl.subject (id) values ($1) on conflict (id) do nothing",
            &user.subject_id.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "insert into auth.user (subject_id, username) values ($1, $2)",
            &user.subject_id.0,
            &user.username,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| conflict_or_backend(e, &format!("username {}", user.username)))?;
        sqlx::query!(
            "insert into auth.password_credential (subject_id, password_phc) values ($1, $2)",
            &user.subject_id.0,
            &user.password_phc,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn find_password_credential(&self, username: &str) -> Result<Option<PasswordCredential>> {
        let row = sqlx::query!(
            "select u.subject_id, pc.password_phc, u.locked_until \
             from auth.user u \
             join auth.password_credential pc on pc.subject_id = u.subject_id \
             where u.username = $1 and u.disabled_at is null",
            username,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(row.map(|r| PasswordCredential {
            subject_id: SubjectId(r.subject_id),
            password_phc: r.password_phc,
            locked_until: r.locked_until,
        }))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn create_session(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        expires_at: OffsetDateTime,
    ) -> Result<()> {
        sqlx::query!(
            "insert into auth.session (token_sha256, subject_id, expires_at) \
             values ($1, $2, $3) \
             on conflict (token_sha256) do update set \
                 subject_id = excluded.subject_id, expires_at = excluded.expires_at",
            &token_sha256[..],
            &subject.0,
            expires_at,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn resolve_session(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>> {
        let row = sqlx::query_scalar!(
            "select s.subject_id from auth.session s \
             where s.token_sha256 = $1 and s.expires_at > $2 \
               and not exists ( \
                 select 1 from auth.user u \
                 where u.subject_id = s.subject_id and u.disabled_at is not null \
               )",
            &token_sha256[..],
            now,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(row.map(SubjectId))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn revoke_session(&self, token_sha256: &[u8; 32]) -> Result<()> {
        sqlx::query!(
            "delete from auth.session where token_sha256 = $1",
            &token_sha256[..],
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn has_any_user(&self) -> Result<bool> {
        let exists = sqlx::query_scalar!("select exists (select 1 from auth.user)")
            .fetch_one(self.pool())
            .await
            .map_err(backend)?
            .unwrap_or(false);
        Ok(exists)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn is_bootstrap_sealed(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar!("select exists (select 1 from acl.bootstrap where id = 1)")
                .fetch_one(self.pool())
                .await
                .map_err(backend)?
                .unwrap_or(false),
        )
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn seal_bootstrap(&self) -> Result<()> {
        let inserted =
            sqlx::query!("insert into acl.bootstrap (id) values (1) on conflict (id) do nothing")
                .execute(self.pool())
                .await
                .map_err(backend)?
                .rows_affected();
        if inserted == 0 {
            return Err(ControlPlaneError::Conflict(
                "bootstrap already sealed".into(),
            ));
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_users(&self, _page: PageReq) -> Result<Page<UserSummary>> {
        // Select `disabled_at` directly and derive the bool in Rust — avoids a
        // computed-column nullability override (`as "x!"`) for a plain read.
        let rows = sqlx::query!(
            "select subject_id, username, disabled_at, created_at \
             from auth.user order by username",
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        let out = rows
            .into_iter()
            .map(|r| UserSummary {
                subject_id: SubjectId(r.subject_id),
                username: r.username,
                disabled: r.disabled_at.is_some(),
                created_at: r.created_at,
            })
            .collect();
        Ok(Page::from_full(out))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn set_user_disabled(&self, username: &str, disabled: bool) -> Result<()> {
        // Set/clear the flag and, on disable, revoke the user's sessions in one
        // transaction. `create_user` is non-idempotent, but this is: re-disabling
        // just refreshes disabled_at; re-enabling clears it.
        let mut tx = self.pool().begin().await.map_err(backend)?;
        let row = sqlx::query!(
            "update auth.user \
             set disabled_at = case when $2 then now() else null end \
             where username = $1 \
             returning subject_id",
            username,
            disabled,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?;
        let Some(row) = row else {
            return Err(ControlPlaneError::NotFound(format!("user {username}")));
        };
        if disabled {
            sqlx::query!(
                "delete from auth.session where subject_id = $1",
                &row.subject_id,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn update_password(&self, subject: &SubjectId, new_phc: &str) -> Result<()> {
        let res = sqlx::query!(
            "update auth.password_credential set password_phc = $2, updated_at = now() \
             where subject_id = $1",
            &subject.0,
            new_phc,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        if res.rows_affected() == 0 {
            return Err(ControlPlaneError::NotFound(format!(
                "credential for subject {}",
                subject.0
            )));
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn password_phc_for_subject(&self, subject: &SubjectId) -> Result<Option<String>> {
        let phc = sqlx::query_scalar!(
            "select password_phc from auth.password_credential where subject_id = $1",
            &subject.0,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(phc)
    }

    #[tracing::instrument(skip(self, keep), level = "debug")]
    async fn revoke_subject_sessions(
        &self,
        subject: &SubjectId,
        keep: Option<&[u8; 32]>,
    ) -> Result<()> {
        match keep {
            Some(k) => {
                sqlx::query!(
                    "delete from auth.session where subject_id = $1 and token_sha256 <> $2",
                    &subject.0,
                    &k[..],
                )
                .execute(self.pool())
                .await
                .map_err(backend)?;
            }
            None => {
                sqlx::query!(
                    "delete from auth.session where subject_id = $1",
                    &subject.0,
                )
                .execute(self.pool())
                .await
                .map_err(backend)?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn record_failed_login(
        &self,
        username: &str,
        now: OffsetDateTime,
        policy: LockoutPolicy,
    ) -> Result<()> {
        // Interval math is done in Rust so the SQL binds only concrete instants —
        // no PgInterval. `new_count` is computed in the subquery from the current
        // stored count and written back in a single statement. (Under READ
        // COMMITTED two racing failures can under-count; acceptable for lockout.
        // Store-backed so the counter survives restarts and is shared across
        // replicas, unlike an in-memory counter an attacker could reset.)
        let window_cutoff = now - policy.window;
        let locked_until = now + policy.lockout_duration;
        let threshold = i32::try_from(policy.threshold).unwrap_or(i32::MAX);
        sqlx::query!(
            "update auth.user u \
             set failed_attempt_count = nc.new_count, \
                 last_failed_at = $2, \
                 locked_until = case when nc.new_count >= $4 then $5 else u.locked_until end \
             from ( \
               select case \
                   when last_failed_at is null or last_failed_at < $3 then 1 \
                   else failed_attempt_count + 1 \
                 end as new_count \
               from auth.user where username = $1 \
             ) nc \
             where u.username = $1",
            username,
            now,
            window_cutoff,
            threshold,
            locked_until,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn reset_failed_logins(&self, username: &str) -> Result<()> {
        sqlx::query!(
            "update auth.user \
             set failed_attempt_count = 0, locked_until = null, last_failed_at = null \
             where username = $1",
            username,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, account), level = "debug")]
    async fn create_service_account(&self, account: &NewServiceAccount) -> Result<()> {
        // One transaction: ensure the ACL subject, then the service account.
        // A duplicate name aborts on the service_account insert (23505 -> Conflict).
        let mut tx = self.pool().begin().await.map_err(backend)?;
        // Reject a subject_id that is already a human user: the machine- and
        // human-identity namespaces must not overlap (else a minted token could
        // authenticate as an existing user's subject).
        let clashes_user = sqlx::query_scalar!(
            "select exists (select 1 from auth.user where subject_id = $1)",
            &account.subject_id.0
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if clashes_user {
            return Err(ControlPlaneError::Conflict(format!(
                "subject {} already belongs to a user",
                account.subject_id.0
            )));
        }
        sqlx::query!(
            "insert into acl.subject (id) values ($1) on conflict (id) do nothing",
            &account.subject_id.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "insert into auth.service_account (subject_id, name) values ($1, $2)",
            &account.subject_id.0,
            &account.name,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| conflict_or_backend(e, &format!("service account name {}", account.name)))?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn create_service_token(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        label: &str,
        expires_at: OffsetDateTime,
    ) -> Result<()> {
        sqlx::query!(
            "insert into auth.service_token (token_sha256, subject_id, label, expires_at) \
             values ($1, $2, $3, $4)",
            &token_sha256[..],
            &subject.0,
            label,
            expires_at,
        )
        .execute(self.pool())
        .await
        .map_err(|e| notfound_or_backend(e, &format!("service account {}", subject.0)))?;
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn resolve_service_token(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>> {
        let row = sqlx::query_scalar!(
            "select subject_id from auth.service_token \
             where token_sha256 = $1 and revoked_at is null and expires_at > $2",
            &token_sha256[..],
            now,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(row.map(SubjectId))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn revoke_service_token(&self, token_sha256: &[u8; 32]) -> Result<()> {
        sqlx::query!(
            "update auth.service_token set revoked_at = now() \
             where token_sha256 = $1 and revoked_at is null",
            &token_sha256[..],
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_service_tokens(
        &self,
        subject: &SubjectId,
        _page: PageReq,
    ) -> Result<Page<ServiceToken>> {
        let rows = sqlx::query!(
            "select token_sha256, subject_id, label, created_at, expires_at, revoked_at \
             from auth.service_token where subject_id = $1 \
             order by created_at, token_sha256",
            &subject.0,
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        let items = rows
            .into_iter()
            .map(|r| {
                // The bytea is always 32 bytes (every insert writes a [u8; 32]), but
                // convert fallibly — no panic path — to satisfy the panic-safety lints.
                // Carry the source error rather than discarding it (map_err_ignore).
                let hash: [u8; 32] = r.token_sha256.as_slice().try_into().map_err(backend)?;
                Ok(ServiceToken {
                    token_sha256: hash,
                    subject_id: SubjectId(r.subject_id),
                    label: r.label,
                    created_at: r.created_at,
                    expires_at: r.expires_at,
                    revoked_at: r.revoked_at,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Page::from_full(items))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_service_accounts(&self, _page: PageReq) -> Result<Page<ServiceAccount>> {
        let rows = sqlx::query!(
            "select subject_id, name, created_at from auth.service_account \
             order by created_at, subject_id",
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        let items = rows
            .into_iter()
            .map(|r| ServiceAccount {
                subject_id: SubjectId(r.subject_id),
                name: r.name,
                created_at: r.created_at,
            })
            .collect();
        Ok(Page::from_full(items))
    }
}
