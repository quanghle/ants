//! Enrollment over NATS: a new ant's machine asks for its credentials with enrollment credentials from
//! `ants auth enroll` ([`request`]), and `ants enroll` on the machine holding the account key lists, approves or
//! rejects what it hears ([`Approver`]).
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
    ENROLL_INBOX, ENROLL_REPEAT, EnrollLeaf, EnrollReply, EnrollRequest, JsDomain, Machine, Permissions, Role, UserId,
    subject,
};
use anyhow::{Context as _, Result, bail};
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
/// for the key arrives, the enrollment credentials expire or `stop` completes. Returns the user JWT of an approval,
/// checked to be the account's for that key and ant, and the renewed uplink JWT of the leaf, checked likewise.
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
    let (client, _) = bus::connect(url, Some(creds), None, ant, "ants-enroll").await?;
    let mut replies = client.subscribe(subject::enroll_reply(ant)).await?;
    let public_key = user.public_key();
    let mut request = EnrollRequest::new(ant.clone(), &public_key, roster::machine().await);
    request.enrollment = enrollment_jwt;
    let mut uplink: Option<KeyPair> = None;
    let mut repeats = tokio::time::interval(repeat);
    let expiry = async {
        match left {
            Some(left) => tokio::time::sleep(left.min(Duration::from_secs(365 * 24 * 3600))).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(expiry, stop);
    loop {
        tokio::select! {
            _ = repeats.tick() => {
                if let Some(leaf) = leaf {
                    let (jwt, key) = auth::read_creds(&leaf.creds).context("the leaf node's uplink credentials")?;
                    let unsigned = EnrollLeaf { machine: leaf.machine.clone(), jwt, signature: String::new() };
                    request.leaf = Some(unsigned);
                    let signature = auth::sign(&key, &request.signed())?;
                    request.leaf.as_mut().expect("the leaf just set").signature = signature;
                    uplink = Some(key);
                }
                request.signature = auth::sign(&enrollment_key, &request.signed())?;
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
                        let checked = check_approval(&jwt, &enrollment, ant, &public_key)
                            .and_then(|()| check_leaf_approval(leaf_jwt.as_deref(), &enrollment, leaf, uplink.as_ref()));
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
            () = &mut expiry => bail!("the enrollment credentials {} expired without an answer", creds.display()),
            () = &mut stop => bail!("stopped before an answer came"),
        }
    }
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
    client: async_nats::Client,
    hub: async_nats::jetstream::Context,
    js_domain: Option<JsDomain>,
    /// The team's account, which must have issued the credentials a request proves.
    account: String,
    /// User keys revoked in the team directory (`ants auth revoke`), whose credentials prove nothing.
    revoked: HashSet<String>,
    listen: Duration,
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
            let leaf = r.leaf.map(|l| format!("  leaf {}", l.machine)).unwrap_or_default();
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
        let public_key = request.public_key.clone();
        let reply = EnrollReply::Approved { ant: ant.clone(), public_key: public_key.clone(), jwt, leaf_jwt };
        // An answer lost on the way leaves the machine asking: answer again while it does.
        for _ in 0..3 {
            self.answer(&reply).await?;
            let still = self.pending(Some(ant)).await?;
            if !still.iter().any(|r| r.public_key == request.public_key) {
                return Ok(Approved { public_key, leaf });
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
mod tests {
    use std::future::pending;

    use ants_proto::{Machine, UserId};
    use async_nats::Event;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::*;
    use crate::auth::tests::Server;

    const WAIT: Duration = Duration::from_secs(5);
    const REPEAT: Duration = Duration::from_millis(100);
    const LISTEN: Duration = Duration::from_millis(400);

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn enrollment(server: &Server, ant: &UserId, ttl: Duration) -> std::path::PathBuf {
        let out = server.dir().join(format!("enroll-{ant}.creds"));
        auth::issue_enrollment(&server.dir().join("auth"), ant, ttl, &out).unwrap();
        out
    }

    fn enrollment_in(team: &Path, dir: &Path, ant: &UserId) -> std::path::PathBuf {
        let out = dir.join(format!("enroll-{ant}.creds"));
        auth::issue_enrollment(team, ant, Duration::from_secs(60), &out).unwrap();
        out
    }

    #[tokio::test]
    async fn enrollment_credentials_ask_only_for_their_ant() {
        let Some(server) = Server::start() else { return };
        let creds = enrollment(&server, &p("dev-a"), Duration::from_secs(60));
        let (tx, mut errors) = mpsc::unbounded_channel();
        let client = async_nats::ConnectOptions::with_credentials_file(&creds)
            .await
            .unwrap()
            .event_callback(move |event| {
                let tx = tx.clone();
                async move {
                    if let Event::ServerError(e) = event {
                        let _ = tx.send(e.to_string());
                    }
                }
            })
            .connect(&server.url)
            .await
            .unwrap();
        let mut violation = async |what: &str| {
            client.flush().await.unwrap();
            let error = timeout(WAIT, errors.recv()).await.expect(what).unwrap();
            assert!(error.to_lowercase().contains("permissions violation"), "{what}: {error}");
        };
        client.publish("ants.enroll.dev-b", "{}".into()).await.unwrap();
        violation("asking for another ant").await;
        let _all = client.subscribe(subject::ALL_ENROLL_REPLIES).await.unwrap();
        violation("hearing the answers to others").await;
        let _inbox = client.subscribe("ants.inbox.dev-a").await.unwrap();
        violation("reading the ant's inbox").await;

        let other = enrollment(&server, &p("dev-b"), Duration::from_secs(60));
        let user = KeyPair::new_user();
        let err = request(&server.url, &other, &p("dev-a"), &user, None, REPEAT, pending()).await.unwrap_err();
        assert!(err.to_string().contains("not enroll-dev-a"), "{err:#}");
    }

    #[tokio::test]
    async fn an_approval_completes_the_credentials_of_the_asking_key() {
        let Some(server) = Server::start() else { return };
        let (team, dev_a) = (server.dir().join("auth"), p("dev-a"));
        // With no expiry, as `--ttl 0` issues them.
        let creds = enrollment(&server, &dev_a, Duration::ZERO);
        let user = KeyPair::new_user();
        let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();

        let asking = request(&server.url, &creds, &dev_a, &user, None, REPEAT, pending());
        let approving = async {
            let heard = approver.pending(None).await.unwrap();
            assert_eq!(heard.len(), 1, "{heard:?}");
            assert_eq!((&heard[0].ant, &heard[0].public_key), (&dev_a, &user.public_key()));
            approver.approve(&team, &dev_a, None).await.unwrap()
        };
        let (jwt, approved) = Box::pin(timeout(WAIT * 2, async { tokio::join!(asking, approving) })).await.unwrap();
        assert_eq!(approved.public_key, user.public_key());
        assert!(approved.leaf.is_none());
        let approval = jwt.unwrap();
        assert_eq!(approval.leaf_jwt, None);
        let jwt = approval.jwt;

        let out = server.dir().join("dev-a.creds");
        auth::write_creds(&out, &jwt, &user).unwrap();
        let (client, _) = bus::connect(&server.url, Some(&out), None, &dev_a, "test").await.unwrap();
        client.flush().await.unwrap();
    }

    /// Publish `request` for dev-a with the enrollment credentials at `creds`, as a machine would, signed as given.
    async fn forge(server: &Server, creds: &Path, request: &EnrollRequest) {
        let client = async_nats::ConnectOptions::with_credentials_file(creds).await.unwrap().connect(&server.url).await;
        let client = client.unwrap();
        for _ in 0..8 {
            client.publish(request.subject(), serde_json::to_vec(request).unwrap().into()).await.unwrap();
            client.flush().await.unwrap();
            tokio::time::sleep(REPEAT).await;
        }
    }

    #[tokio::test]
    async fn requests_must_prove_their_enrollment_and_leaf_credentials() {
        let Some(server) = Server::start() else { return };
        let (team, dev_a) = (server.dir().join("auth"), p("dev-a"));
        let creds = enrollment(&server, &dev_a, Duration::from_secs(60));
        let (enrollment_jwt, enrollment_key) = auth::read_creds(&creds).unwrap();
        let m1: Machine = "m1".parse().unwrap();
        let leaf_creds = server.dir().join("leaf-m1.creds");
        auth::issue_leaf(&team, &m1, &[(p("dev-b"), Role::Ant)], &[], false, None, &leaf_creds).unwrap();
        let (leaf_jwt, leaf_key) = auth::read_creds(&leaf_creds).unwrap();
        // An ant named like a leaf's uplink user, which may connect as a client only.
        let posing = server.dir().join("leaf-m1-ant.creds");
        auth::issue(&team, &p("leaf-m1"), Role::Ant, None, &posing).unwrap();
        let (posing_jwt, posing_key) = auth::read_creds(&posing).unwrap();
        let revoked = server.dir().join("leaf-m1-revoked.creds");
        auth::issue_leaf(&team, &m1, &[(p("dev-b"), Role::Ant)], &[], false, None, &revoked).unwrap();
        let (revoked_jwt, revoked_key) = auth::read_creds(&revoked).unwrap();
        revoke::revoke(&team, None, &[revoked_key.public_key()]).unwrap();
        let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();

        let signed = |leaf: Option<(&str, &KeyPair)>, enrollment: &KeyPair| {
            let mut request = EnrollRequest::new(dev_a.clone(), KeyPair::new_user().public_key(), "host");
            request.enrollment = enrollment_jwt.clone();
            if let Some((jwt, leaf_key)) = leaf {
                request.leaf = Some(EnrollLeaf { machine: m1.clone(), jwt: jwt.to_owned(), signature: String::new() });
                let signature = auth::sign(leaf_key, &request.signed()).unwrap();
                request.leaf.as_mut().unwrap().signature = signature;
            }
            request.signature = auth::sign(enrollment, &request.signed()).unwrap();
            request
        };
        let stranger = KeyPair::new_user();
        let good = signed(Some((&leaf_jwt, &leaf_key)), &enrollment_key);
        let mut unsigned = good.clone();
        unsigned.signature = String::new();
        let mut moved = good.clone();
        moved.leaf.as_mut().unwrap().machine = "m2".parse().unwrap();
        let forged = [
            signed(None, &stranger),
            unsigned,
            moved,
            signed(Some((&leaf_jwt, &stranger)), &enrollment_key),
            signed(Some((&posing_jwt, &posing_key)), &enrollment_key),
            signed(Some((&revoked_jwt, &revoked_key)), &enrollment_key),
        ];
        let sending = async {
            for request in forged.iter().chain([&good]) {
                forge(&server, &creds, request).await;
            }
        };
        let listening = async {
            let mut heard = Vec::new();
            while !heard.contains(&good) {
                heard.extend(approver.pending(Some(&dev_a)).await.unwrap());
            }
            heard
        };
        let ((), heard) = Box::pin(timeout(WAIT * 4, async { tokio::join!(sending, listening) })).await.unwrap();
        assert!(heard.contains(&good), "the proven request is heard");
        assert!(heard.iter().all(|r| *r == good), "{heard:#?}");

        let approving = approver.approve(&team, &dev_a, None);
        let ((), approved) =
            Box::pin(timeout(WAIT * 2, async { tokio::join!(forge(&server, &creds, &good), approving) }))
                .await
                .unwrap();
        let approved = approved.unwrap_or_else(|e| panic!("{e:#}"));
        let leaf = approved.leaf.unwrap();
        assert_eq!(leaf.machine, m1);
        assert!(!leaf.outbox, "no outbox of m1 to admit dev-a from");
    }

    /// A real hub and two leaf nodes: an ant new to m1 asks through m1's uplink, `ants enroll` approves it through
    /// m2's (issued with `--enroll-approver`), and once m1's leaf restarts with the renewed uplink credentials, the
    /// ant reaches the hub and what it keeps in m1's outbox reaches the hub's stream.
    #[tokio::test]
    async fn an_ant_new_to_a_leaf_enrolls_through_it_and_reaches_the_hub() {
        use ants_proto::{JsDomain, STREAM, ThreadId};
        use ants_test_support::NatsServer;

        use crate::leaf;
        use crate::outbox::tests::{eventually, free_port, spawn_hub};

        let Some(hub) = spawn_hub(free_port(), "feature_flags { js_api_reply_v2: true }") else { return };
        let leaf_port = {
            let conf = std::fs::read_to_string(hub.dir().join("nats.conf")).unwrap();
            let at = conf.find("leafnodes { listen: \"127.0.0.1:").unwrap() + "leafnodes { listen: \"127.0.0.1:".len();
            conf[at..].split('"').next().unwrap().to_owned()
        };
        let team = hub.dir().join("auth");
        let domain: JsDomain = "hub".parse().unwrap();
        let (human, dev_a, dev_c) = (UserId::human(), p("dev-a"), p("dev-c"));
        let creds = |user: &UserId| {
            let out = hub.dir().join(format!("{user}.creds"));
            auth::issue(&team, user, Role::by_default(user), Some(&domain), &out).unwrap();
            out
        };
        let (h, h_js) = bus::connect(&hub.url, Some(&creds(&human)), Some(&domain), &human, "test").await.unwrap();
        bus::init_stream(&h_js).await.unwrap();
        bus::ensure_links(&h_js).await.unwrap();

        let spawn_leaf = |machine: &Machine, members: &[(UserId, Role)], approver: bool| {
            // Its configuration, outbox and uplink credentials outlive each of its runs.
            let dir = hub.dir().join(machine.as_str());
            std::fs::create_dir_all(&dir).unwrap();
            let leaf_creds = dir.join("uplink.creds");
            if !leaf_creds.exists() {
                auth::issue_leaf(&team, machine, members, &[], approver, Some(&domain), &leaf_creds).unwrap();
            }
            let uplinks = [format!("nats-leaf://127.0.0.1:{leaf_port}")];
            let listen = format!("127.0.0.1:{}", free_port());
            let config = leaf::Leaf {
                machine,
                hub_domain: Some(&domain),
                uplinks: &uplinks,
                creds: &leaf_creds,
                system_creds: None,
                server_conf: &team.join(auth::SERVER_CONF),
                ca_file: None,
                proxy: None,
                listen: &listen,
            };
            leaf::write(&config, &dir.join("leaf.conf")).unwrap();
            let hub_dir = hub.dir().file_name().unwrap().to_string_lossy().into_owned();
            let node = NatsServer::spawn_with(|_| format!("include \"../{hub_dir}/{machine}/leaf.conf\"\n")).unwrap();
            (node, dir.join("leaf.conf"))
        };
        let (m1, m2): (Machine, Machine) = ("m1".parse().unwrap(), "m2".parse().unwrap());
        let on_m1 = [(dev_a.clone(), Role::Ant), (human.clone(), Role::Human)];
        let (m1_node, m1_conf) = spawn_leaf(&m1, &on_m1, false);
        let (m2_node, _) = spawn_leaf(&m2, &[(human.clone(), Role::Human)], true);
        let (_a, a_js) = bus::connect(&m1_node.url, Some(&creds(&dev_a)), Some(&domain), &dev_a, "test").await.unwrap();
        eventually("m1 reaches the hub", async || bus::open_stream(&a_js, &dev_a).await.is_ok()).await;
        let setup = crate::outbox::connect(&hub.url, Some(&creds(&human)), &human, &m1).await.unwrap();
        crate::outbox::admit(&setup, &h_js, &m1, &on_m1).await.unwrap();

        let enroll_creds = enrollment_in(&team, hub.dir(), &dev_c);
        let (machine, uplink_creds) = leaf::read_uplink(&m1_conf).unwrap();
        let m1_leaf = Leaf { machine, creds: uplink_creds.clone() };
        let user = KeyPair::new_user();
        let approver =
            Approver::connect(&m2_node.url, &team, Some(&domain), Duration::from_millis(1500)).await.unwrap();
        let asking = request(&m1_node.url, &enroll_creds, &dev_c, &user, Some(&m1_leaf), REPEAT, pending());
        let approving = async {
            loop {
                match approver.approve(&team, &dev_c, None).await {
                    Ok(approved) => break approved,
                    Err(e) if e.to_string().contains("no enrollment request") => continue,
                    Err(e) => panic!("{e:#}"),
                }
            }
        };
        let (approval, approved) =
            Box::pin(timeout(WAIT * 6, async { tokio::join!(asking, approving) })).await.unwrap();
        let approval = approval.unwrap();
        let leaf_approved = approved.leaf.unwrap();
        assert_eq!(leaf_approved.machine, m1);
        assert!(leaf_approved.outbox, "the hub sources m1's outbox");
        let kept = members::issued(&team, &m1, &uplink_creds).unwrap().unwrap();
        assert_eq!(kept.ants, [dev_a.clone(), dev_c.clone()], "a later `ants auth leaf m1` keeps dev-c");

        let (old, key) = auth::read_creds(&uplink_creds).unwrap();
        let renewed = approval.leaf_jwt.unwrap();
        assert!(auth::allows_all(&renewed, &old).unwrap() && !auth::allows_all(&old, &renewed).unwrap());
        let c_creds = hub.dir().join("dev-c-enrolled.creds");
        auth::write_creds(&c_creds, &approval.jwt, &user).unwrap();
        let (_c, c_js) = bus::connect(&m1_node.url, Some(&c_creds), Some(&domain), &dev_c, "test").await.unwrap();
        assert!(bus::open_stream(&c_js, &dev_c).await.is_err(), "the uplink does not carry dev-c yet");
        auth::write_creds(&uplink_creds, &renewed, &key).unwrap();
        let carried = members::uplink(&m1_conf, &m1).unwrap();
        assert!(carried.ants.contains(&dev_c), "a later `ants leaf outbox m1` admits dev-c: {carried}");
        drop((_a, _c));
        drop(m1_node);
        let (m1_node, _) = spawn_leaf(&m1, &on_m1, false);

        let (_c, c_js) = bus::connect(&m1_node.url, Some(&c_creds), Some(&domain), &dev_c, "test").await.unwrap();
        eventually("dev-c reaches the hub", async || bus::open_stream(&c_js, &dev_c).await.is_ok()).await;
        let direct = ants_proto::Envelope::new(dev_c.clone(), human.clone(), "hello");
        assert_eq!(bus::publish(&c_js, &direct).await.unwrap(), bus::Stored::Hub);
        // Its notes kept in m1's outbox reach the hub's stream: only its own admission takes them.
        let thread: ThreadId = "t1".parse().unwrap();
        let post = ants_proto::NotesPost::new(thread.clone(), dev_c.clone(), "kept");
        let kept = subject::outbox(&post.subject()).unwrap();
        c_js.publish(kept, serde_json::to_vec(&post).unwrap().into()).await.unwrap().await.unwrap();
        let notes = subject::notes(&thread, &dev_c);
        eventually("the outbox's notes reach the hub", async || {
            let stream = h_js.get_stream(STREAM).await.unwrap();
            stream.get_last_raw_message_by_subject(&notes).await.is_ok()
        })
        .await;
        drop(h);
    }

    #[tokio::test]
    async fn a_rejection_or_the_expiry_ends_the_request() {
        let Some(server) = Server::start() else { return };
        let (team, dev_a) = (server.dir().join("auth"), p("dev-a"));
        let creds = enrollment(&server, &dev_a, Duration::from_secs(60));
        let (first, second) = (KeyPair::new_user(), KeyPair::new_user());
        let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();

        let both = async {
            let first = request(&server.url, &creds, &dev_a, &first, None, REPEAT, pending());
            let second = request(&server.url, &creds, &dev_a, &second, None, REPEAT, pending());
            tokio::join!(first, second)
        };
        let answering = async {
            let err = approver.approve(&team, &dev_a, None).await.unwrap_err().to_string();
            assert!(err.contains("several machines ask for dev-a"), "{err}");
            approver.reject(&dev_a, None, Some("unknown machine")).await.unwrap()
        };
        let ((first_err, second_err), rejected) =
            Box::pin(timeout(WAIT * 2, async { tokio::join!(both, answering) })).await.unwrap();
        assert_eq!(rejected.len(), 2);
        for err in [first_err.unwrap_err(), second_err.unwrap_err()] {
            assert!(err.to_string().contains("rejected: unknown machine"), "{err:#}");
        }

        let err = approver.approve(&team, &dev_a, None).await.unwrap_err().to_string();
        assert!(err.contains("no enrollment request for dev-a"), "{err}");
        let short = enrollment(&server, &dev_a, Duration::from_secs(1));
        let err = request(&server.url, &short, &dev_a, &first, None, REPEAT, pending()).await.unwrap_err();
        assert!(format!("{err:#}").contains("expire"), "{err:#}");
    }
}
