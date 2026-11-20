//! The sense log, signer and verifier of an ant's items over the machine's host connection ([`Link`]): each reaches
//! the hub through the connection current at the call, refused while there is none.

use std::sync::Arc;

use ants_proto::{SenseId, UserId};
use ants_sense::log::{
    AccountVerifier, AppendError, Event, EventLog, LogError, Revocations, Signature, Signer, Stored, Verifier,
    VerifyError,
};

use super::{Link, Route};
use crate::sense::log::{AntSigner, DeviceRevocations, JetStreamLog};

pub(super) fn route(route: &Route) -> std::sync::MutexGuard<'_, Option<Arc<Link>>> {
    route.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn connected(route_: &Route) -> Result<Arc<Link>, LogError> {
    let link = route(route_).clone().ok_or_else(|| LogError::unreachable("hub unavailable: no host connection"))?;
    if link.js.client().connection_state() != async_nats::connection::State::Connected {
        return Err(LogError::unreachable("hub unavailable: host disconnected"));
    }
    Ok(link)
}

pub(super) struct HostLog {
    pub(super) route: Route,
    pub(super) incarnation: String,
    pub(super) accounts: Vec<String>,
}

impl HostLog {
    async fn log(&self) -> Result<JetStreamLog, LogError> {
        let link = connected(&self.route)?;
        if link.accounts != self.accounts {
            return Err(LogError::refused("host authentication accounts changed; cached sense writes retained"));
        }
        let log = link.log.get_or_try_init(|| JetStreamLog::open(&link.js)).await?;
        if log.incarnation() != self.incarnation {
            return Err(LogError::incarnation("sense stream incarnation changed; cached sense writes retained"));
        }
        Ok(log.clone())
    }
}

impl EventLog for HostLog {
    async fn append(&self, sense: &SenseId, payload: Vec<u8>, expected_last: u64) -> Result<u64, AppendError> {
        self.log().await?.append(sense, payload, expected_last).await
    }

    async fn read_after(&self, sense: &SenseId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        self.log().await?.read_after(sense, after, limit).await
    }

    async fn last_sequence(&self, sense: &SenseId) -> Result<u64, LogError> {
        self.log().await?.last_sequence(sense).await
    }

    fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

pub(super) struct HostSigner {
    pub(super) signer: AntSigner,
    pub(super) ant: UserId,
    pub(super) accounts: Vec<String>,
}

impl Signer for HostSigner {
    fn sign(&self, bytes: &[u8]) -> Result<Option<Signature>, String> {
        let signature = self.signer.sign(bytes)?;
        if self.accounts.is_empty() {
            return Ok(signature);
        }
        let signature = signature.ok_or("no signing credentials")?;
        let now = u64::try_from(chrono::Utc::now().timestamp()).unwrap_or_default();
        let user = ants_proto::verify_user_jwt(&signature.credentials, self.accounts.iter().map(String::as_str), now)
            .map_err(|error| error.to_string())?;
        if user.id != self.ant {
            return Err(format!("credentials belong to {}, not {}", user.id, self.ant));
        }
        user.check_signature(bytes, &signature.signature).map_err(|error| error.to_string())?;
        Ok(Some(signature))
    }
}

pub(super) struct HostVerifier(pub(super) Route);

impl Verifier for HostVerifier {
    async fn verify(&self, event: &Event, stored_at: chrono::DateTime<chrono::Utc>) -> Result<(), VerifyError> {
        let link = connected(&self.0).map_err(|error| VerifyError::Unavailable(error.to_string()))?;
        AccountVerifier::new(link.accounts.clone(), HostRevocations(link)).verify(event, stored_at).await
    }
}

struct HostRevocations(Arc<Link>);

impl Revocations for HostRevocations {
    async fn revoked_at(&self, key: &str) -> Result<Option<u64>, String> {
        self.0
            .revocations
            .get_or_try_init(|| DeviceRevocations::new(&self.0.js, self.0.js_domain.as_ref()))
            .await
            .map_err(|error| format!("{error:#}"))?
            .revoked_at(key)
            .await
    }
}
