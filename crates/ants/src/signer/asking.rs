//! The manager's side of the signer: asking `ants signer` on the machine holding the account key for an ant
//! ([`ask`]) or to retire one ([`retire`]), repeating the request until an answer comes, and checking the credentials
//! an approval carries.

use std::time::Duration;

use ants_daemon::colony;
use ants_proto::{AntReply, AntRequest, AskedAnt, EnrollLeaf, ThreadId, UserId, subject};
use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use nkeys::KeyPair;

use crate::auth;
use crate::enroll::Leaf;

/// Who asks the signer, and how: over the manager's connection `client`, as the manager its credentials' user JWT
/// `jwt` and key `key` name, through the machine's `leaf` node if it has one, repeating the request every `repeat`
/// until an answer arrives or `until` passes.
pub struct Requester<'a> {
    pub client: &'a async_nats::Client,
    pub jwt: &'a str,
    pub key: &'a KeyPair,
    pub leaf: Option<&'a Leaf>,
    pub repeat: Duration,
    pub until: DateTime<Utc>,
}

impl Requester<'_> {
    /// Sign `asking`, with the leaf's uplink key too when through one, its credentials read again so that the request
    /// carries the latest, and publish it; returns the uplink key and JWT it carried.
    async fn publish(&self, asking: &mut AntRequest) -> Result<Option<(KeyPair, String)>> {
        let mut uplink = None;
        if let Some(leaf) = self.leaf {
            let (jwt, key) = auth::read_creds(&leaf.creds).context("the leaf node's uplink credentials")?;
            asking.leaf =
                Some(EnrollLeaf { machine: leaf.machine.clone(), jwt: jwt.clone(), signature: String::new() });
            let signature = auth::sign(&key, &asking.signed())?;
            asking.leaf.as_mut().expect("the leaf just set").signature = signature;
            uplink = Some((key, jwt));
        }
        asking.signature = auth::sign(self.key, &asking.signed())?;
        self.client.publish(asking.subject(), serde_json::to_vec(asking)?.into()).await?;
        self.client.flush().await?;
        Ok(uplink)
    }
}

/// What [`ask`] got: the ant's user JWT, and for a request through a leaf node, the renewed uplink JWT of the leaf.
#[derive(Debug)]
pub struct Signed {
    pub jwt: String,
    pub leaf_jwt: Option<String>,
}

/// Ask, as `requester`, for ant `name` from `template` with the settings `request` (JSON), for the public key of
/// `user`. Returns the ant's user JWT of an approval, checked to be the account's for that key and ant, and the leaf's
/// renewed uplink JWT, checked likewise; a refusal is an error carrying the signer's reason.
pub async fn ask(
    requester: &Requester<'_>,
    template: &str,
    request: &str,
    reason: &str,
    thread: &ThreadId,
    name: &UserId,
    user: &KeyPair,
) -> Result<Signed> {
    let Requester { client, jwt, leaf, until, .. } = *requester;
    let claims = auth::verified(jwt).context("the manager's credentials")?;
    let manager: UserId = claims.name.parse().with_context(|| format!("credentials named {}", claims.name))?;
    let mut replies = client.subscribe(subject::antreq_reply(&manager)).await?;
    let public_key = user.public_key();
    let host = colony::machine().await;
    let asked = AskedAnt {
        name: name.clone(),
        template: template.to_owned(),
        request: request.to_owned(),
        reason: reason.to_owned(),
    };
    let mut asking = AntRequest::new(manager.clone(), asked, thread.clone(), &public_key, host, until);
    asking.credentials = jwt.to_owned();
    let mut uplink: Option<KeyPair> = None;
    let mut repeats = tokio::time::interval(requester.repeat);
    let left = (until - Utc::now()).to_std().unwrap_or_default();
    let deadline = tokio::time::sleep(left);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = repeats.tick() => {
                uplink = requester.publish(&mut asking).await?.map(|(key, _)| key);
            }
            msg = replies.next() => {
                let msg = msg.context("the NATS connection closed")?;
                let reply = match serde_json::from_slice::<AntReply>(&msg.payload) {
                    Ok(reply) if reply.manager() == &manager && reply.answers(name, &public_key) => reply,
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!(error = %e, "ignoring a malformed answer to an ant request");
                        continue;
                    }
                };
                match reply {
                    AntReply::Approved { jwt, leaf_jwt, .. } => {
                        match check_signed(&claims.iss, &jwt, name, &public_key, leaf_jwt.as_deref(), leaf, uplink.as_ref()) {
                            Ok(()) => return Ok(Signed { jwt, leaf_jwt }),
                            Err(e) => tracing::warn!(error = format!("{e:#}"), "ignoring an approval of an ant request"),
                        }
                    }
                    AntReply::Refused { reason, .. } => bail!("the signer refused {name}: {reason}"),
                    AntReply::Retired { .. } => continue,
                }
            }
            () = &mut deadline => bail!("no answer from the signer (`ants signer` on the machine holding the team's account key) in time"),
        }
    }
}

/// What [`retire`] got: the keys the signer revoked, and the error of pushing them to the hub, if it failed; through
/// a leaf node, its uplink JWT renewed without the ant, checked to be the account's for the uplink key and the
/// leaf's machine, or why there is none.
#[derive(Debug)]
pub struct Retired {
    pub revoked: Vec<String>,
    pub push_error: Option<String>,
    pub leaf_jwt: Option<String>,
    pub leaf_error: Option<String>,
    /// The uplink JWT the requests carried, which the signer renewed; none when they carried several.
    pub leaf_asked: Option<String>,
}

/// Ask, as `requester`, to retire ant `name`, added from `template`, whose credentials on this machine have
/// `public_key`. A refusal is an error carrying the signer's reason.
pub async fn retire(
    requester: &Requester<'_>,
    template: &str,
    thread: &ThreadId,
    name: &UserId,
    public_key: &str,
) -> Result<Retired> {
    let Requester { client, jwt, leaf, until, .. } = *requester;
    let claims = auth::verified(jwt).context("the manager's credentials")?;
    let manager: UserId = claims.name.parse().with_context(|| format!("credentials named {}", claims.name))?;
    let mut replies = client.subscribe(subject::antreq_reply(&manager)).await?;
    let host = colony::machine().await;
    let asked =
        AskedAnt { name: name.clone(), template: template.to_owned(), request: "{}".to_owned(), reason: String::new() };
    let mut asking = AntRequest::new(manager.clone(), asked, thread.clone(), public_key, host, until);
    asking.retire = true;
    asking.credentials = jwt.to_owned();
    let mut uplink: Option<KeyPair> = None;
    let mut carried: Vec<String> = Vec::new();
    let mut repeats = tokio::time::interval(requester.repeat);
    let left = (until - Utc::now()).to_std().unwrap_or_default();
    let deadline = tokio::time::sleep(left);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = repeats.tick() => {
                if let Some((key, jwt)) = requester.publish(&mut asking).await? {
                    if !carried.contains(&jwt) {
                        carried.push(jwt);
                    }
                    uplink = Some(key);
                }
            }
            msg = replies.next() => {
                let msg = msg.context("the NATS connection closed")?;
                let reply = match serde_json::from_slice::<AntReply>(&msg.payload) {
                    Ok(reply) if reply.manager() == &manager && reply.answers(name, public_key) => reply,
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!(error = %e, "ignoring a malformed answer to an ant request");
                        continue;
                    }
                };
                match reply {
                    AntReply::Retired { revoked, push_error, leaf_jwt, mut leaf_error, .. } => {
                        // The keys are revoked whatever came of the uplink: renewed credentials failing their checks
                        // are only not used.
                        let leaf_jwt = leaf_jwt.filter(|jwt| {
                            let checked = check_uplink(&claims.iss, jwt, leaf, uplink.as_ref());
                            checked.map_err(|e| leaf_error = Some(format!("{e:#}"))).is_ok()
                        });
                        let leaf_asked = (carried.len() == 1).then(|| carried.remove(0));
                        return Ok(Retired { revoked, push_error, leaf_jwt, leaf_error, leaf_asked });
                    }
                    AntReply::Refused { reason, .. } => bail!("the signer refused to retire {name}: {reason}"),
                    AntReply::Approved { .. } => continue,
                }
            }
            () = &mut deadline => bail!("no answer from the signer (`ants signer` on the machine holding the team's account key) in time"),
        }
    }
}

/// Check that `jwt`, a renewed uplink JWT, is the account's for the uplink key `uplink` of `leaf`, which the request
/// named.
fn check_uplink(account: &str, jwt: &str, leaf: Option<&Leaf>, uplink: Option<&KeyPair>) -> Result<()> {
    let (Some(leaf), Some(uplink)) = (leaf, uplink) else { bail!("renews uplink credentials no leaf node asked for") };
    let claims = auth::check_leaf(account, jwt, &leaf.machine).context("the renewed uplink credentials")?;
    if claims.sub != uplink.public_key() {
        bail!("the renewed uplink credentials are for the key {}, not for this leaf node's", claims.sub);
    }
    Ok(())
}

/// Check that `jwt` is the user JWT of ant `name` for `public_key`, signed by `account`, and that `leaf_jwt` is there
/// only when the request named `leaf`, and is then the account's for the uplink key `uplink` and the leaf's machine.
fn check_signed(
    account: &str,
    jwt: &str,
    name: &UserId,
    public_key: &str,
    leaf_jwt: Option<&str>,
    leaf: Option<&Leaf>,
    uplink: Option<&KeyPair>,
) -> Result<()> {
    let claims = auth::verified(jwt)?;
    if claims.iss != account {
        bail!("signed by {}, not by the account {account}", claims.iss);
    }
    if claims.sub != public_key || claims.name != name.as_str() {
        bail!("issued to {} ({}), not to {name} ({public_key})", claims.name, claims.sub);
    }
    match (leaf_jwt, leaf, uplink) {
        (None, None, _) => Ok(()),
        (None, Some(leaf), _) => bail!("no renewed uplink credentials for the leaf node {}", leaf.machine),
        (Some(jwt), leaf, uplink) => check_uplink(account, jwt, leaf, uplink),
    }
}
