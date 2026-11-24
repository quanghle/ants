//! Asking for enrollment over NATS: a new ant's machine asks for its credentials with enrollment credentials from
//! `ants auth enroll` ([`request`]), and `ants enroll` on the machine holding the account key lists, approves or
//! rejects what it hears ([`Approver`]).
//!
//! Requests are core NATS messages and stored nowhere: the asking machine repeats its request every
//! [`ants_proto::ENROLL_REPEAT`] until an answer comes or its enrollment credentials expire, and `ants enroll`
//! listens for [`LISTEN`](crate::credentials::approver::LISTEN), long enough to hear every machine still asking.
//!
//! A machine behind a leaf node asks through the leaf's uplink, which carries enrollment requests and their answers
//! ([`ants_proto::Permissions::for_leaf`]). Its request names the leaf ([`EnrollLeaf`]), signed with the uplink key, and
//! the approval renews the leaf's uplink credentials to carry the ant too, keeps the ant among the leaf's members in
//! the auth directory ([`crate::node::members::enrolled`]), and admits the ant into the hub's stream from the leaf's
//! outbox ([`crate::node::outbox::admit_ant`]).
//!
//! A machine without host credentials asks for its own in the same request ([`HostAsk`], [`ants_proto::EnrollHost`]),
//! which the approval of the ant grants too, after checks that bind them to the asking machine
//! ([`Approver::approve_with`]).
//!
//! [`Approver`]: crate::credentials::approver::Approver
//! [`Approver::approve_with`]: crate::credentials::approver::Approver::approve_with

use ants_daemon::bus::relayed;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ants_daemon::{bus, colony};
use ants_proto::{EnrollHost, EnrollLeaf, EnrollReply, EnrollRequest, Machine, UserId, host_user, subject};
use anyhow::{Context as _, Result, bail};
use async_nats::{ClientError, Event, ServerError};
use futures::StreamExt as _;
use nkeys::KeyPair;

use crate::auth;

/// The leaf node an asking machine reaches the hub through: its machine name and the path of its uplink credentials,
/// read again for each repeat of the request, so that it carries the latest.
pub struct Leaf {
    pub machine: Machine,
    pub creds: std::path::PathBuf,
}

/// The host credentials an asking machine without any asks for too: the machine's name and the key generated for
/// them, whose seed stays there.
pub struct HostAsk {
    pub machine: Machine,
    pub key: KeyPair,
}

/// An approval: the user JWT of the ant, for a request naming a leaf node the renewed uplink JWT of the leaf, and for
/// one asking for host credentials the host's user JWT, when they were granted too.
#[derive(Debug)]
pub struct Approval {
    pub jwt: String,
    pub leaf_jwt: Option<String>,
    pub host_jwt: Option<String>,
}

/// What a machine asks along with the credentials of an ant: through its `leaf` node if it has one, and the `host`
/// credentials it lacks.
#[derive(Clone, Copy, Default)]
pub struct Alongside<'a> {
    pub leaf: Option<&'a Leaf>,
    pub host: Option<&'a HostAsk>,
}

/// What a machine asks for: the credentials of `ant` for the public key of `user`, with the enrollment credentials at
/// `creds`, and what goes `alongside`.
#[derive(Clone, Copy)]
pub struct Enrollment<'a> {
    pub creds: &'a Path,
    pub ant: &'a UserId,
    pub user: &'a KeyPair,
    pub alongside: Alongside<'a>,
}

impl<'a> Enrollment<'a> {
    /// The credentials of `ant` alone.
    pub fn new(creds: &'a Path, ant: &'a UserId, user: &'a KeyPair) -> Self {
        Self { creds, ant, user, alongside: Alongside::default() }
    }

    /// Asked through the machine's `leaf` node, if it has one.
    #[cfg(test)]
    pub fn through(self, leaf: Option<&'a Leaf>) -> Self {
        Self { alongside: Alongside { leaf, ..self.alongside }, ..self }
    }
}

/// Ask for the `enrollment` over the server at `url`: repeat the request every `repeat` until an answer for the key
/// arrives, the enrollment credentials expire or the server refuses them, or `stop` completes. Returns the user JWT
/// of an approval, checked to be the account's for that key and ant, the renewed uplink JWT of the leaf, checked
/// likewise, and the host JWT, checked to be the account's for the host's key and machine.
pub async fn request(
    url: &str,
    enrollment: Enrollment<'_>,
    repeat: Duration,
    stop: impl Future<Output = ()>,
) -> Result<Approval> {
    let Enrollment { creds, ant, user, alongside: Alongside { leaf, host } } = enrollment;
    let (enrollment_jwt, enrollment_key) = auth::read_creds(creds)?;
    let (enrollment, left) = check_enrollment(&enrollment_jwt, creds, ant)?;
    let (events_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
    let (client, _) = bus::connect_watched(url, Some(creds), None, ant, "ants-enroll", events_tx).await?;
    let replies = relayed::subscribe(&client, subject::enroll_reply(ant)).await?;
    let public_key = user.public_key();
    let mut request = EnrollRequest::new(ant.clone(), &public_key, colony::machine().await);
    request.enrollment = enrollment_jwt;
    let asking = Asking {
        client: &client,
        ant,
        public_key: &public_key,
        enrollment: &enrollment,
        enrollment_key: &enrollment_key,
        leaf,
        host,
    };
    // Raced against the expiry, a refusal and `stop`: a flush waits for a reconnect, which the client retries forever,
    // with the server gone or the credentials refused alike.
    let asking = asking.until_answered(request, replies, repeat);
    let expiry = async {
        match left {
            Some(left) => tokio::time::sleep(left.min(Duration::from_hours(365 * 24))).await,
            None => std::future::pending().await,
        }
    };
    let refused = refused(&mut events);
    let expired = || anyhow::anyhow!("the enrollment credentials {} expired without an answer", creds.display());
    tokio::select! {
        approval = asking => approval,
        () = expiry => Err(expired()),
        refusal = refused => {
            // The server ends the connection at the expiry, up to a second before the local timer does.
            if refusal.contains("expired") || enrollment.exp.is_some_and(|exp| exp <= now().unwrap_or(u64::MAX)) {
                return Err(expired());
            }
            bail!(
                "the NATS server refused the enrollment credentials {} ({refusal}); issue new ones with `ants auth \
                 enroll`",
                creds.display()
            )
        }
        () = stop => bail!("stopped before an answer came"),
    }
}

/// The claims of the enrollment credentials at `creds`, whose user JWT is `jwt`, checked to be `ant`'s, and how long
/// they have left when they expire.
fn check_enrollment(jwt: &str, creds: &Path, ant: &UserId) -> Result<(auth::Claims, Option<Duration>)> {
    let enrollment = auth::verified(jwt).with_context(|| format!("enrollment credentials {}", creds.display()))?;
    let name = ants_proto::enrollment_user(ant);
    if enrollment.name != name {
        bail!("{} are the credentials of {}, not {name} (`ants auth enroll {ant}`)", creds.display(), enrollment.name);
    }
    let Some(exp) = enrollment.exp else { return Ok((enrollment, None)) };
    let left = exp.checked_sub(now()?).filter(|left| *left > 0).with_context(|| {
        format!("the enrollment credentials {} have expired; issue new ones with `ants auth enroll`", creds.display())
    })?;
    Ok((enrollment, Some(Duration::from_secs(left))))
}

/// An enrollment request being asked: who asks, and what an approval is checked against.
struct Asking<'a> {
    client: &'a async_nats::Client,
    ant: &'a UserId,
    public_key: &'a str,
    enrollment: &'a auth::Claims,
    enrollment_key: &'a KeyPair,
    leaf: Option<&'a Leaf>,
    host: Option<&'a HostAsk>,
}

impl Asking<'_> {
    /// Repeat `request` every `repeat` until an answer to it comes on `replies`: an approval passing its checks, or a
    /// rejection, an error.
    async fn until_answered(
        &self,
        mut request: EnrollRequest,
        mut replies: ants_daemon::bus::relayed::Heard,
        repeat: Duration,
    ) -> Result<Approval> {
        let mut uplink: Option<KeyPair> = None;
        let mut repeats = tokio::time::interval(repeat);
        loop {
            tokio::select! {
                _ = repeats.tick() => {
                    uplink = sign_request(&mut request, self.enrollment_key, self.leaf, self.host)?;
                    self.client.publish(request.subject(), serde_json::to_vec(&request)?.into()).await?;
                    self.client.flush().await?;
                }
                msg = replies.next() => {
                    let msg = msg.context("the NATS connection closed")?;
                    if let Some(approval) = self.answer(&msg.payload, uplink.as_ref())? {
                        return Ok(approval);
                    }
                }
            }
        }
    }

    /// The approval `payload` carries, if an answer to the request whose checks pass, the uplink key `uplink` signed
    /// it with; a rejection is an error.
    fn answer(&self, payload: &[u8], uplink: Option<&KeyPair>) -> Result<Option<Approval>> {
        let reply = match serde_json::from_slice::<EnrollReply>(payload) {
            Ok(reply) if reply.ant() == self.ant && reply.answers(self.public_key) => reply,
            Ok(_) => return Ok(None),
            Err(e) => {
                tracing::warn!(error = %e, "ignoring a malformed enrollment answer");
                return Ok(None);
            }
        };
        let ant = self.ant;
        match reply {
            EnrollReply::Approved { jwt, leaf_jwt, host_jwt, .. } => {
                let checked = check_approval(&jwt, self.enrollment, ant, self.public_key)
                    .and_then(|()| check_leaf_approval(leaf_jwt.as_deref(), self.enrollment, self.leaf, uplink))
                    .and_then(|()| check_host_approval(host_jwt.as_deref(), self.enrollment, self.host));
                match checked {
                    Ok(()) => Ok(Some(Approval { jwt, leaf_jwt, host_jwt })),
                    Err(e) => {
                        tracing::warn!(error = format!("{e:#}"), "ignoring an enrollment approval");
                        Ok(None)
                    }
                }
            }
            EnrollReply::Rejected { reason, .. } => match reason {
                Some(reason) => bail!("the enrollment of {ant} was rejected: {reason}"),
                None => bail!("the enrollment of {ant} was rejected"),
            },
            EnrollReply::Retired { .. } => Ok(None),
        }
    }
}

/// The first refusal of the connection among `events`; never without one.
async fn refused(events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>) -> String {
    while let Some(event) = events.recv().await {
        if let Some(refusal) = refusal(&event) {
            return refusal;
        }
    }
    std::future::pending().await
}

/// Sign `request`, asking for the `host` credentials too, with the enrollment key and, for a machine behind `leaf`, with
/// the leaf's uplink key, read again so that the request carries the latest uplink credentials; returns the uplink key.
fn sign_request(
    request: &mut EnrollRequest,
    enrollment_key: &KeyPair,
    leaf: Option<&Leaf>,
    host: Option<&HostAsk>,
) -> Result<Option<KeyPair>> {
    let mut uplink = None;
    request.host_creds =
        host.map(|host| EnrollHost { machine: host.machine.clone(), public_key: host.key.public_key() });
    if let Some(leaf) = leaf {
        let (jwt, key) = auth::read_creds(&leaf.creds).context("the leaf node's uplink credentials")?;
        request.leaf = Some(EnrollLeaf { machine: leaf.machine.clone(), jwt, signature: String::new() });
        let signature = auth::sign(&key, &request.signed())?;
        request.leaf.as_mut().expect("the leaf just set").signature = signature;
        uplink = Some(key);
    }
    request.signature = auth::sign(enrollment_key, &request.signed())?;
    Ok(uplink)
}

/// What `event` says if it tells of the server refusing the connection's credentials: their expiry or revocation,
/// which ends the connection, or a reconnect refused, which the client retries forever.
fn refusal(event: &Event) -> Option<String> {
    let text = match event {
        Event::ServerError(ServerError::AuthorizationViolation) => return Some("authorization violation".to_owned()),
        Event::ServerError(ServerError::Other(text)) | Event::ClientError(ClientError::Other(text)) => {
            text.to_lowercase()
        }
        _ => return None,
    };
    ["authentication expired", "authentication revoked", "authorization violation"]
        .iter()
        .any(|refused| text.contains(refused))
        .then_some(text)
}

/// Check that `jwt` is the user JWT of `ant` for `public_key`, signed by the account that signed the enrollment
/// credentials.
fn check_approval(jwt: &str, enrollment: &auth::Claims, ant: &UserId, public_key: &str) -> Result<()> {
    let claims = auth::verified(jwt)?;
    if claims.iss != enrollment.iss {
        bail!("signed by {}, not by the account {}", claims.iss, enrollment.iss);
    }
    if claims.sub != public_key || claims.name != ant.as_str() {
        bail!("issued to {} ({}), not to {ant} ({public_key})", claims.name, claims.sub);
    }
    Ok(())
}

/// Check that `leaf_jwt`, the renewed uplink JWT of an approval, is there only when the request named `leaf`, with
/// the uplink key `uplink`, and is then the account's for that key and the leaf's machine.
fn check_leaf_approval(
    leaf_jwt: Option<&str>,
    enrollment: &auth::Claims,
    leaf: Option<&Leaf>,
    uplink: Option<&KeyPair>,
) -> Result<()> {
    let (jwt, leaf, uplink) = match (leaf_jwt, leaf, uplink) {
        (None, None, _) => return Ok(()),
        (Some(jwt), Some(leaf), Some(uplink)) => (jwt, leaf, uplink),
        (Some(_), _, _) => bail!("renews uplink credentials no leaf node asked for"),
        (None, Some(leaf), _) => bail!("renews no uplink credentials of {}, whose leaf node asked", leaf.machine),
    };
    let claims = auth::check_leaf(&enrollment.iss, jwt, &leaf.machine).context("the renewed uplink credentials")?;
    if claims.sub != uplink.public_key() {
        bail!("the renewed uplink credentials are for the key {}, not for this leaf node's", claims.sub);
    }
    Ok(())
}

/// Check that `host_jwt`, the host JWT of an approval, is there only when the request asked for `host` credentials, and
/// is then the account's for the host's key and machine. None while they were asked for is no error: the approver
/// granted the ant alone.
fn check_host_approval(host_jwt: Option<&str>, enrollment: &auth::Claims, host: Option<&HostAsk>) -> Result<()> {
    let (jwt, host) = match (host_jwt, host) {
        (None, _) => return Ok(()),
        (Some(jwt), Some(host)) => (jwt, host),
        (Some(_), None) => bail!("grants host credentials no one asked for"),
    };
    let claims = auth::verified(jwt).context("the host credentials")?;
    if claims.iss != enrollment.iss {
        bail!("the host credentials are signed by {}, not by the account {}", claims.iss, enrollment.iss);
    }
    let name = host_user(&host.machine);
    if claims.sub != host.key.public_key() || claims.name != name {
        bail!(
            "the host credentials are issued to {} ({}), not to {name} ({})",
            claims.name,
            claims.sub,
            host.key.public_key()
        );
    }
    Ok(())
}

fn now() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

#[cfg(test)]
mod tests;
