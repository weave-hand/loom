//! Shared bearer-token authentication for query-api's Flight surfaces: the governed
//! export (`flight_export`) and the external SQL wire. Resolves the gRPC
//! `authorization` metadata to a verified `SubjectId` via
//! `service_runtime::resolve_bearer` — session token first, then service token — so
//! both surfaces accept either credential kind through one seam.

use control_plane_core::{Auth, SubjectId};
use time::OffsetDateTime;
use tonic::Status;

/// Resolve the bearer token in the gRPC `authorization` metadata to a verified subject.
/// Missing/invalid/expired → `Unauthenticated`; an auth-store fault → `Internal`.
pub(crate) async fn authenticate(
    auth: &(dyn Auth + Send + Sync),
    md: &tonic::metadata::MetadataMap,
) -> Result<SubjectId, Status> {
    let token = md
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
    let hash = service_runtime::token_sha256(token);
    match service_runtime::resolve_bearer(auth, &hash, OffsetDateTime::now_utc()).await {
        Ok(Some(sid)) => Ok(sid),
        Ok(None) => Err(Status::unauthenticated("invalid or expired token")),
        Err(e) => {
            tracing::error!(error = %e, "flight auth store fault");
            Err(Status::internal("internal error"))
        }
    }
}
