//! Enrollment over NATS: a new ant's machine asks for its credentials with enrollment credentials from
//! `ants auth enroll` ([`request`]), and `ants enroll` on the machine holding the account key lists, approves or
//! rejects what it hears ([`Approver`]). The approver also renews credentials whose holders ask over NATS
//! ([`crate::renew`]).
//!
//! Requests are core NATS messages and stored nowhere: the asking machine repeats its request every
//! [`ENROLL_REPEAT`] until an answer comes or its enrollment credentials expire, and `ants enroll` listens for
//! [`LISTEN`], long enough to hear every machine still asking.
//!
//! A machine behind a leaf node asks through the leaf's uplink, which carries enrollment requests and their answers
//! ([`ants_proto::Permissions::for_leaf`]). Its request names the leaf ([`EnrollLeaf`]), signed with the uplink key, and
//! the approval renews the leaf's uplink credentials to carry the ant too, keeps the ant among the leaf's members in
//! the team directory ([`members::enrolled`]), and admits the ant into the hub's stream from the leaf's outbox
//! ([`outbox::admit_ant`]).

use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ants_daemon::{bus, roster};
use ants_proto::{
    Destination, ENROLL_INBOX, ENROLL_REPEAT, EnrollLeaf, EnrollReply, EnrollRequest, JsDomain, Machine, Permissions,
    Role, UserId, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::{ClientError, Event, ServerError};
use chrono::Utc;
use futures::StreamExt as _;
use nkeys::KeyPair;

use crate::{auth, members, outbox, revoke};

/// How long `ants enroll` listens for requests: one repeat of every machine still asking, and some slack.
pub const LISTEN: Duration = ENROLL_REPEAT.saturating_add(Duration::from_secs(1));

/// The leaf node an asking machine reaches the hub through: its machine name and the path of its uplink credentials,
/// read again for each repeat of the request, so that it carries the latest.
pub struct Leaf {
    pub machine: Machine,
    pub creds: std::path::PathBuf,
}

/// An approval: the user JWT of the ant, and for a request naming a leaf node, the renewed uplink JWT of the leaf.
#[derive(Debug)]
pub struct Approval {
    pub jwt: String,
    pub leaf_jwt: Option<String>,
}

/// Ask for the credentials of `ant` for the public key of `user`, with the enrollment credentials at `creds`, over
/// the server at `url`, for the machine's `leaf` node if it has one: repeat the request every `repeat` until an answer
/// for the key arrives, the enrollment credentials expire or the server refuses them, or `stop` completes. Returns the
/// user JWT of an approval, checked to be the account's for that key and ant, and the renewed uplink JWT of the leaf,
/// checked likewise.
pub async fn request(
    url: &str,
    creds: &Path,
    ant: &UserId,
    user: &KeyPair,
    leaf: Option<&Leaf>,
    repeat: Duration,
    stop: impl Future<Output = ()>,
) -> Result<Approval> {
    let (enrollment_jwt, enrollment_key) = auth::read_creds(creds)?;
    let enrollment =
        auth::verified(&enrollment_jwt).with_context(|| format!("enrollment credentials {}", creds.display()))?;
    let name = ants_proto::enrollment_user(ant);
    if enrollment.name != name {
        bail!("{} are the credentials of {}, not {name} (`ants auth enroll {ant}`)", creds.display(), enrollment.name);
    }
    let left = match enrollment.exp {
        Some(exp) => {
            Some(Duration::from_secs(exp.checked_sub(now()?).filter(|left| *left > 0).with_context(|| {
                format!(
                    "the enrollment credentials {} have expired; issue new ones with `ants auth enroll`",
                    creds.display()
                )
            })?))
        }
        None => None,
    };
    let (events_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
    let (client, _) = bus::connect_watched(url, Some(creds), None, ant, "ants-enroll", events_tx).await?;
    let mut replies = client.subscribe(subject::enroll_reply(ant)).await?;
    let public_key = user.public_key();
    let mut request = EnrollRequest::new(ant.clone(), &public_key, roster::machine().await);
    request.enrollment = enrollment_jwt;
    // Raced against the expiry, a refusal and `stop`: a flush waits for a reconnect, which the client retries forever,
    // with the server gone or the credentials refused alike.
    let asking = async {
        let mut uplink: Option<KeyPair> = None;
        let mut repeats = tokio::time::interval(repeat);
        loop {
            tokio::select! {
                _ = repeats.tick() => {
                    uplink = sign_request(&mut request, &enrollment_key, leaf)?;
                    client.publish(request.subject(), serde_json::to_vec(&request)?.into()).await?;
                    client.flush().await?;
                }
                msg = replies.next() => {
                    let msg = msg.context("the NATS connection closed")?;
                    let reply = match serde_json::from_slice::<EnrollReply>(&msg.payload) {
                        Ok(reply) if reply.ant() == ant && reply.answers(&public_key) => reply,
                        Ok(_) => continue,
                        Err(e) => {
                            tracing::warn!(error = %e, "ignoring a malformed enrollment answer");
                            continue;
                        }
                    };
                    match reply {
                        EnrollReply::Approved { jwt, leaf_jwt, .. } => {
                            let checked = check_approval(&jwt, &enrollment, ant, &public_key).and_then(|()| {
                                check_leaf_approval(leaf_jwt.as_deref(), &enrollment, leaf, uplink.as_ref())
                            });
                            match checked {
                                Ok(()) => return Ok(Approval { jwt, leaf_jwt }),
                                Err(e) => tracing::warn!(error = format!("{e:#}"), "ignoring an enrollment approval"),
                            }
                        }
                        EnrollReply::Rejected { reason, .. } => match reason {
                            Some(reason) => bail!("the enrollment of {ant} was rejected: {reason}"),
                            None => bail!("the enrollment of {ant} was rejected"),
                        },
                    }
                }
            }
        }
    };
    let expiry = async {
        match left {
            Some(left) => tokio::time::sleep(left.min(Duration::from_hours(365 * 24))).await,
            None => std::future::pending().await,
        }
    };
    let refused = async {
        while let Some(event) = events.recv().await {
            if let Some(refusal) = refusal(&event) {
                return refusal;
            }
        }
        std::future::pending().await
    };
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

/// Sign `request` with the enrollment key and, for a machine behind `leaf`, with the leaf's uplink key, read again so
/// that the request carries the latest uplink credentials; returns the uplink key.
fn sign_request(request: &mut EnrollRequest, enrollment_key: &KeyPair, leaf: Option<&Leaf>) -> Result<Option<KeyPair>> {
    let mut uplink = None;
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
    let (Some(jwt), Some(leaf), Some(uplink)) = (leaf_jwt, leaf, uplink) else {
        if leaf_jwt.is_some() {
            bail!("renews uplink credentials no leaf node asked for");
        }
        // A request through no leaf node, or an approver from before leaf nodes were named: no uplink to renew.
        return Ok(());
    };
    let claims = auth::check_leaf(&enrollment.iss, jwt, &leaf.machine).context("the renewed uplink credentials")?;
    if claims.sub != uplink.public_key() {
        bail!("the renewed uplink credentials are for the key {}, not for this leaf node's", claims.sub);
    }
    Ok(())
}

fn now() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

/// `ants enroll` on the machine holding the account key, connected with short-lived credentials it issues itself
/// ([`auth::approver_creds`]).
pub struct Approver {
    pub(crate) client: async_nats::Client,
    hub: async_nats::jetstream::Context,
    js_domain: Option<JsDomain>,
    /// The team's account, which must have issued the credentials a request proves.
    pub(crate) account: String,
    /// User keys revoked in the team directory (`ants auth revoke`), whose credentials prove nothing.
    pub(crate) revoked: HashSet<String>,
    pub(crate) listen: Duration,
}

/// What [`Approver::approve`] did.
#[derive(Debug)]
pub struct Approved {
    /// The asking machine's key.
    pub public_key: String,
    /// The leaf node the request came through, if it named one.
    pub leaf: Option<LeafApproved>,
}

#[derive(Debug)]
pub struct LeafApproved {
    pub machine: Machine,
    /// Whether the hub's stream now takes the ant's messages from the leaf's outbox; false when the hub sources no
    /// outbox of the machine.
    pub outbox: bool,
}

impl Approver {
    /// Connect to the server at `url`, signing the credentials with the account key in the team directory `dir`, for
    /// the hub's JetStream domain if it has one, and listen for `listen` each time requests are gathered.
    pub async fn connect(url: &str, dir: &Path, js_domain: Option<&JsDomain>, listen: Duration) -> Result<Self> {
        bus::check_url(url)?;
        let creds = auth::approver_creds(dir, js_domain)?;
        let client = async_nats::ConnectOptions::with_credentials(&creds)?
            .name("ants-enroll")
            .custom_inbox_prefix(ENROLL_INBOX)
            .connect(url)
            .await
            .context("connecting to NATS")?;
        let hub = match js_domain {
            Some(domain) => async_nats::jetstream::with_domain(client.clone(), domain.as_str()),
            None => async_nats::jetstream::new(client.clone()),
        };
        let account = auth::account_key(dir)?;
        let revoked = revoke::revoked(dir)?.into_iter().map(|r| r.key).collect();
        Ok(Self { client, hub, js_domain: js_domain.cloned(), account, revoked, listen })
    }

    /// The hub's JetStream.
    pub(crate) fn hub(&self) -> &async_nats::jetstream::Context {
        &self.hub
    }

    /// Check that `request` proves its enrollment credentials of the team's account for its ant, unexpired, and if it
    /// names a leaf node, the leaf's uplink credentials.
    fn check(&self, request: &EnrollRequest) -> Result<()> {
        auth::check_user_key(&request.public_key)?;
        let signed = request.signed();
        let enrollment = auth::verified(&request.enrollment).context("the enrollment credentials")?;
        if enrollment.iss != self.account {
            bail!("enrollment credentials issued by {}, not by the team's account", enrollment.iss);
        }
        let name = ants_proto::enrollment_user(&request.ant);
        if enrollment.name != name {
            bail!("enrollment credentials of {}, not {name}", enrollment.name);
        }
        if enrollment.exp.is_some_and(|exp| exp <= now().unwrap_or(u64::MAX)) {
            bail!("expired enrollment credentials");
        }
        if self.revoked.contains(&enrollment.sub) {
            bail!("revoked enrollment credentials");
        }
        auth::check_signature(&enrollment.sub, &signed, &request.signature).context("the enrollment credentials")?;
        if let Some(leaf) = &request.leaf {
            let uplink = auth::check_leaf(&self.account, &leaf.jwt, &leaf.machine)
                .with_context(|| format!("the uplink credentials of {}", leaf.machine))?;
            if self.revoked.contains(&uplink.sub) {
                bail!("revoked uplink credentials of {}", leaf.machine);
            }
            auth::check_signature(&uplink.sub, &signed, &leaf.signature)
                .with_context(|| format!("the uplink credentials of {}", leaf.machine))?;
        }
        Ok(())
    }

    /// The requests heard, for `ant` or for every ant: the latest of each ant and key, oldest first.
    pub async fn pending(&self, ant: Option<&UserId>) -> Result<Vec<EnrollRequest>> {
        let filter = ant.map_or_else(|| subject::ALL_ENROLL_REQUESTS.to_owned(), subject::enroll);
        let mut requests = self.client.subscribe(filter).await?;
        self.client.flush().await?;
        let mut heard: Vec<EnrollRequest> = Vec::new();
        let end = tokio::time::sleep(self.listen);
        tokio::pin!(end);
        loop {
            tokio::select! {
                msg = requests.next() => {
                    let msg = msg.context("the NATS connection closed")?;
                    let request = match serde_json::from_slice::<EnrollRequest>(&msg.payload) {
                        Ok(request) => request,
                        Err(e) => {
                            tracing::warn!(subject = %msg.subject, error = %e, "ignoring a malformed enrollment request");
                            continue;
                        }
                    };
                    if let Err(e) = request.check_subject(&msg.subject) {
                        tracing::warn!("{e}");
                        continue;
                    }
                    if let Err(e) = self.check(&request) {
                        tracing::warn!(ant = %request.ant, "ignoring an enrollment request: {e:#}");
                        continue;
                    }
                    heard.retain(|r| (&r.ant, &r.public_key) != (&request.ant, &request.public_key));
                    heard.push(request);
                }
                () = &mut end => break,
            }
        }
        requests.unsubscribe().await?;
        heard.sort_by_key(|r| r.sent_at);
        Ok(heard)
    }

    /// Print the requests heard.
    pub async fn list(&self) -> Result<()> {
        let pending = self.pending(None).await?;
        if pending.is_empty() {
            println!("no enrollment requests heard in {}s", self.listen.as_secs());
        }
        for r in pending {
            let age = (Utc::now() - r.sent_at).num_seconds().max(0);
            let leaf = r.leaf.map_or_default(|l| format!("  leaf {}", l.machine));
            println!("{}  {}  host {}{leaf}  asked {age}s ago", r.ant, r.public_key, r.host);
        }
        Ok(())
    }

    /// Sign the credentials `ant` asks for, with the account key in `dir`, for the hub's JetStream domain if it
    /// has one, and send them. `public_key` picks the request when several machines ask for the ant. A request
    /// through a leaf node also gets the leaf's uplink credentials renewed to carry the ant, the ant kept among the
    /// leaf's members in `dir`, for later `ants auth leaf` runs, and admitted into the hub's stream from the leaf's
    /// outbox, if the hub sources it.
    pub async fn approve(&self, dir: &Path, ant: &UserId, public_key: Option<&str>) -> Result<Approved> {
        if ant.is_human() {
            bail!("{ant} is the default human; issue its credentials with `ants auth user`");
        }
        let request = self.one(ant, public_key).await?;
        let domain = self.js_domain.as_ref();
        let jwt = auth::sign_user(dir, ant, Role::Ant, domain, &request.public_key)?;
        let (leaf_jwt, leaf) = match &request.leaf {
            Some(leaf) => {
                let more = Permissions::for_leaf(&leaf.machine, [(ant, Role::Ant)]).with_js_domain(domain);
                let renewed = auth::extend_leaf(dir, &leaf.jwt, &leaf.machine, more)?;
                members::enrolled(dir, &leaf.machine, &renewed, ant)
                    .with_context(|| format!("keeping {ant} among the members of {}", leaf.machine))?;
                // Before the answer: a machine that heard none asks again, and this runs again.
                let outbox = outbox::admit_ant(&self.hub, &leaf.machine, ant)
                    .await
                    .with_context(|| format!("admitting {ant} from the outbox of {}", leaf.machine))?;
                (Some(renewed), Some(LeafApproved { machine: leaf.machine.clone(), outbox }))
            }
            None => (None, None),
        };
        let public_key = self.send(&request, jwt, leaf_jwt).await?;
        Ok(Approved { public_key, leaf })
    }

    /// Sign the credentials webhook `webhook` asks for (`ants webhook add --enroll`), with the rights
    /// `ants auth user --webhook` gives for `destinations`, and send them, as [`Approver::approve`] does. Returns the
    /// public key approved.
    pub async fn approve_webhook(
        &self,
        dir: &Path,
        webhook: &UserId,
        destinations: &[Destination],
        public_key: Option<&str>,
    ) -> Result<String> {
        if webhook.is_human() {
            bail!("{webhook} is the default human, no webhook");
        }
        let request = self.one(webhook, public_key).await?;
        if let Some(leaf) = &request.leaf {
            bail!("{webhook} asks through the leaf node of {}; a webhook connects to the hub", leaf.machine);
        }
        let permissions = Permissions::for_webhook(webhook, destinations).with_js_domain(self.js_domain.as_ref());
        let jwt = auth::sign_with(dir, webhook, revoke::Kind::Webhook, permissions, &request.public_key)?;
        self.send(&request, jwt, None).await
    }

    /// Send the approval of `request`, its user JWT `jwt` and the leaf's renewed uplink JWT `leaf_jwt`, until the
    /// machine stops asking. Returns the public key approved.
    async fn send(&self, request: &EnrollRequest, jwt: String, leaf_jwt: Option<String>) -> Result<String> {
        let ant = &request.ant;
        let public_key = request.public_key.clone();
        let reply = EnrollReply::Approved { ant: ant.clone(), public_key: public_key.clone(), jwt, leaf_jwt };
        // An answer lost on the way leaves the machine asking: answer again while it does.
        for _ in 0..3 {
            self.answer(&reply).await?;
            let still = self.pending(Some(ant)).await?;
            if !still.iter().any(|r| r.public_key == request.public_key) {
                return Ok(public_key);
            }
        }
        bail!("{ant} on {} keeps asking; the answer did not get through", request.host)
    }

    /// Refuse the credentials `ant` asks for, to the machine of `public_key`, or to every machine asking for it,
    /// with `reason` if given. Returns the public keys refused.
    pub async fn reject(&self, ant: &UserId, public_key: Option<&str>, reason: Option<&str>) -> Result<Vec<String>> {
        let keys: Vec<String> = match public_key {
            Some(key) => vec![self.one(ant, Some(key)).await?.public_key],
            None => self.heard(ant).await?.into_iter().map(|r| r.public_key).collect(),
        };
        for key in &keys {
            let reply = EnrollReply::Rejected {
                ant: ant.clone(),
                public_key: Some(key.clone()),
                reason: reason.map(str::to_owned),
            };
            self.answer(&reply).await?;
        }
        Ok(keys)
    }

    /// The requests heard for `ant`; an error when there are none.
    async fn heard(&self, ant: &UserId) -> Result<Vec<EnrollRequest>> {
        let heard = self.pending(Some(ant)).await?;
        if heard.is_empty() {
            bail!(
                "no enrollment request for {ant} heard in {}s; run `ants add {ant} --enroll ...` on its \
                 machine and leave it waiting",
                self.listen.as_secs()
            );
        }
        Ok(heard)
    }

    /// The request for `ant` from `public_key`, or the only one heard.
    async fn one(&self, ant: &UserId, public_key: Option<&str>) -> Result<EnrollRequest> {
        let heard = self.heard(ant).await?;
        if let Some(key) = public_key {
            return heard
                .into_iter()
                .find(|r| r.public_key == key)
                .with_context(|| format!("no enrollment request for {ant} from {key} heard"));
        }
        if let [one] = heard.as_slice() {
            return Ok(one.clone());
        }
        let keys: Vec<String> = heard.iter().map(|r| format!("{} (host {})", r.public_key, r.host)).collect();
        bail!("several machines ask for {ant}; pick one with --public-key: {}", keys.join(", "))
    }

    async fn answer(&self, reply: &EnrollReply) -> Result<()> {
        self.client.publish(reply.subject(), serde_json::to_vec(reply)?.into()).await?;
        self.client.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
