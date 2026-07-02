//! First-admin bootstrap logic shared by the `loom create-admin` CLI. Seeds the
//! first admin and seals the instance in a single logical sequence; refuses once
//! sealed. No HTTP path — the caller is the host CLI only.
use control_plane_core::{ADMIN_ROLE, Acl, Auth, ControlPlaneError, NewUser, RoleId, SubjectId};

/// Failure modes of [`run_create_admin`].
#[derive(Debug, thiserror::Error)]
pub enum CreateAdminError {
    #[error("an admin already exists; re-bootstrap requires direct DB/host access")]
    AlreadySealed,
    #[error("username must not be empty")]
    EmptyUsername,
    #[error("password must not be empty")]
    EmptyPassword,
    #[error("password hashing failed: {0}")]
    Hash(String),
    #[error(transparent)]
    Backend(#[from] ControlPlaneError),
}

/// Create the first admin (`username`/`password`) and seal the instance. Refuses
/// with [`CreateAdminError::AlreadySealed`] if already sealed. Steps: guard →
/// create_user → define_subject → define_role(admin) → assign_role → seal.
pub async fn run_create_admin<CP: Auth + Acl + Sync>(
    cp: &CP,
    username: &str,
    password: &str,
) -> Result<(), CreateAdminError> {
    if username.trim().is_empty() {
        return Err(CreateAdminError::EmptyUsername);
    }
    if password.trim().is_empty() {
        return Err(CreateAdminError::EmptyPassword);
    }
    if cp.is_bootstrap_sealed().await? {
        return Err(CreateAdminError::AlreadySealed);
    }
    let phc = crate::hash_password(password).map_err(|e| CreateAdminError::Hash(e.to_string()))?;
    let subject = SubjectId(username.to_string());
    match cp
        .create_user(&NewUser {
            subject_id: subject.clone(),
            username: username.to_string(),
            password_phc: phc,
        })
        .await
    {
        Ok(()) | Err(ControlPlaneError::Conflict(_)) => {}
        Err(e) => return Err(e.into()),
    }
    cp.define_subject(&subject).await?;
    let admin_role = RoleId(ADMIN_ROLE.to_string());
    cp.define_role(&admin_role).await?;
    cp.assign_role(&subject, &admin_role).await?;
    cp.seal_bootstrap().await?;
    Ok(())
}
