//! The enrollment requests an [`Approver`] hears, lists, approves and rejects: those of ants, through a leaf node or
//! not, with the host credentials a machine asks for too, and those of webhooks.

use std::path::Path;

use ants_daemon::bus::relayed;
use ants_proto::{
    Destination, EnrollHost, EnrollReply, EnrollRequest, Machine, Permissions, Role, UserId, host_user, subject,
};
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use futures::StreamExt as _;

use super::Approver;
use crate::auth;
use crate::credentials::issued::{self, Kind};
use crate::credentials::revoke;
use crate::node::{members, outbox};

/// What [`Approver::approve`] did.
#[derive(Debug)]
pub struct Approved {
    /// The asking machine's key.
    pub public_key: String,
    /// The leaf node the request came through, if it named one.
    pub leaf: Option<LeafApproved>,
    /// What came of the host credentials the request asked for too, if it did.
    pub host: Option<HostApproved>,
}

/// What came of the host credentials of `machine` an enrollment request asked for too.
#[derive(Debug, PartialEq, Eq)]
pub enum HostApproved {
    Granted(Machine),
    /// Not granted, and why.
    Refused(Machine, String),
}

impl HostApproved {
    /// What came of them, in words.
    pub fn describe(&self) -> String {
        match self {
            Self::Granted(machine) => format!("host credentials of {machine} granted"),
            Self::Refused(machine, why) => format!(
                "host credentials of {machine} not granted: {why}; issue them with `ants auth host {machine}` if \
                 the machine needs them"
            ),
        }
    }
}

#[derive(Debug)]
pub struct LeafApproved {
    pub machine: Machine,
    /// Whether the hub's stream now takes the ant's messages from the leaf's outbox; false when the hub sources no
    /// outbox of the machine.
    pub outbox: bool,
}

impl Approver {
    /// Check that `request` proves its enrollment credentials of the bus's account for its ant, unexpired, and if it
    /// names a leaf node, the leaf's uplink credentials.
    fn check(&self, request: &EnrollRequest) -> Result<()> {
        auth::check_user_key(&request.public_key)?;
        let signed = request.signed();
        let enrollment = auth::verified(&request.enrollment).context("the enrollment credentials")?;
        if enrollment.iss != self.account {
            bail!("enrollment credentials issued by {}, not by the bus's account", enrollment.iss);
        }
        let name = ants_proto::enrollment_user(&request.ant);
        if enrollment.name != name {
            bail!("enrollment credentials of {}, not {name}", enrollment.name);
        }
        if enrollment.exp.is_some_and(|exp| exp <= auth::now().unwrap_or(u64::MAX)) {
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
        let mut requests = relayed::subscribe(&self.client, filter).await?;
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
                    // `ants signer` answers those carrying an admin's order, with no human asked.
                    if request.order.is_some() {
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
            let host_creds = r.host_creds.map_or_default(|h| format!("  with host creds of {}", h.machine));
            println!("{}  {}  host {}{leaf}{host_creds}  asked {age}s ago", r.ant, r.public_key, r.host);
        }
        Ok(())
    }

    /// Sign the credentials `ant` asks for, with the account key in `dir`, for the hub's JetStream domain if it
    /// has one, and send them. `public_key` picks the request when several machines ask for the ant. A request
    /// through a leaf node also gets the leaf's uplink credentials renewed to carry the ant, the ant kept among the
    /// leaf's members in `dir`, for later `ants auth leaf` runs, and admitted into the hub's stream from the leaf's
    /// outbox, if the hub sources it; uplink credentials the hub would refuse fail the approval before anything is
    /// signed or recorded. Host credentials the request asks for too are granted when they pass the checks of
    /// [`Approver::approve_with`].
    pub async fn approve(&self, dir: &Path, ant: &UserId, public_key: Option<&str>) -> Result<Approved> {
        self.approve_with(dir, ant, public_key, true).await
    }

    /// [`Approver::approve`], granting the host credentials the request asks for too only with `host`, and only when
    /// nothing tells they are another machine's: through a leaf node, only the leaf's machine's; for a machine with
    /// live uplink credentials, only through its leaf node; never while the team holds live host credentials of the
    /// machine for another key, nor for a key revoked or issued to other credentials. Refused, they leave the ant
    /// approved, telling why ([`HostApproved::Refused`]).
    pub async fn approve_with(
        &self,
        dir: &Path,
        ant: &UserId,
        public_key: Option<&str>,
        host: bool,
    ) -> Result<Approved> {
        if ant.is_human() {
            bail!("{ant} is the default human; issue its credentials with `ants auth user`");
        }
        let request = self.one(ant, public_key).await?;
        let domain = self.js_domain.as_ref();
        // First, as it records nothing: an uplink the hub would refuse leaves no ant signed.
        let renewed = match &request.leaf {
            Some(leaf) => {
                let more = Permissions::for_leaf(&leaf.machine, [(ant, Role::Ant)]).with_uplink_js_domain(domain);
                let renewed = auth::extend_leaf(dir, &leaf.jwt, &leaf.machine, more)
                    .with_context(|| format!("the uplink of the leaf node {} cannot carry {ant}", leaf.machine))?;
                Some(renewed)
            }
            None => None,
        };
        let jwt = auth::sign_user(dir, ant, Role::Ant, domain, &request.public_key)?;
        let (host, host_jwt) = match &request.host_creds {
            None => (None, None),
            Some(asked) if !host => (
                Some(HostApproved::Refused(asked.machine.clone(), "approved without them (--no-host)".to_owned())),
                None,
            ),
            Some(asked) => match self.host_grantable(dir, &request, asked) {
                Ok(()) => {
                    let jwt = auth::sign_host(dir, &asked.machine, domain, &asked.public_key)?;
                    (Some(HostApproved::Granted(asked.machine.clone())), Some(jwt))
                }
                Err(e) => (Some(HostApproved::Refused(asked.machine.clone(), format!("{e:#}"))), None),
            },
        };
        let leaf = match (&request.leaf, &renewed) {
            (Some(leaf), Some(_)) => {
                members::enrolled(dir, &leaf.machine, ant)
                    .with_context(|| format!("keeping {ant} among the members of {}", leaf.machine))?;
                // Before the answer: a machine that heard none asks again, and this runs again.
                let outbox = outbox::admit_ant(&self.hub, &leaf.machine, ant)
                    .await
                    .with_context(|| format!("admitting {ant} from the outbox of {}", leaf.machine))?;
                Some(LeafApproved { machine: leaf.machine.clone(), outbox })
            }
            _ => None,
        };
        let public_key = self.send(&request, jwt, renewed, host_jwt).await?;
        Ok(Approved { public_key, leaf, host })
    }

    /// Whether the host credentials `asked` for with `request` may be granted, as [`Approver::approve_with`] tells.
    fn host_grantable(&self, dir: &Path, request: &EnrollRequest, asked: &EnrollHost) -> Result<()> {
        let (machine, key) = (&asked.machine, asked.public_key.as_str());
        auth::check_user_key(key)?;
        if key == request.public_key {
            bail!("the request asks for them with the ant's own key");
        }
        let live = crate::signing::signer::live(dir)?;
        match &request.leaf {
            Some(leaf) if leaf.machine != *machine => {
                bail!("the request asks for those of {machine} through the leaf node of {}", leaf.machine)
            }
            Some(_) => {}
            None => {
                let leaf = auth::leaf_user(machine);
                if live.iter().any(|i| i.kind == Kind::Leaf && i.name == leaf) {
                    bail!("{machine} runs a leaf node, and the request did not come through it");
                }
            }
        }
        if self.revoked.contains(key) || revoke::revoked(dir)?.iter().any(|r| r.key == key) {
            bail!("the key {key} is revoked");
        }
        let name = host_user(machine);
        if let Some(other) =
            issued::issued(dir)?.iter().find(|i| i.key == key && (i.name != name || i.kind != Kind::Host))
        {
            bail!("the key {key} was issued to {} already", other.name);
        }
        if let Some(other) = live.iter().find(|i| i.name == name && i.kind == Kind::Host && i.key != key) {
            bail!(
                "the bus holds live host credentials of {machine} (key {}); revoke them first with `ants auth \
                 revoke --key {}` if the machine lost them",
                other.key,
                other.key
            );
        }
        Ok(())
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
        let jwt = auth::sign_with(dir, webhook, issued::Kind::Webhook, permissions, &request.public_key)?;
        self.send(&request, jwt, None, None).await
    }

    /// Send the approval of `request`, its user JWT `jwt`, the leaf's renewed uplink JWT `leaf_jwt` and the host's
    /// `host_jwt`, until the machine stops asking. Returns the public key approved.
    async fn send(
        &self,
        request: &EnrollRequest,
        jwt: String,
        leaf_jwt: Option<String>,
        host_jwt: Option<String>,
    ) -> Result<String> {
        let ant = &request.ant;
        let public_key = request.public_key.clone();
        let reply = EnrollReply::Approved { ant: ant.clone(), public_key: public_key.clone(), jwt, leaf_jwt, host_jwt };
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
