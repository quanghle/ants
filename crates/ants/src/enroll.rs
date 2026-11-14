//! Enrollment over NATS: a new persona's machine asks for its credentials with enrollment credentials from
//! `ants auth enroll` ([`request`]), and `ants enroll` on the machine holding the account key lists, approves or
//! rejects what it hears ([`Approver`]).
//!
//! Requests are core NATS messages and stored nowhere: the asking machine repeats its request every
//! [`ENROLL_REPEAT`] until an answer comes or its enrollment credentials expire, and `ants enroll` listens for
//! [`LISTEN`], long enough to hear every machine still asking.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ants_daemon::{bus, roster};
use ants_proto::{ENROLL_REPEAT, EnrollReply, EnrollRequest, JsDomain, PersonaId, Role, subject};
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use futures::StreamExt as _;
use nkeys::KeyPair;

use crate::auth;

/// How long `ants enroll` listens for requests: one repeat of every machine still asking, and some slack.
pub const LISTEN: Duration = ENROLL_REPEAT.saturating_add(Duration::from_secs(1));

/// Ask for the credentials of `persona` for the public key of `user`, with the enrollment credentials at `creds`, over
/// the server at `url`: repeat the request every `repeat` until an answer for the key arrives, the enrollment
/// credentials expire or `stop` completes. Returns the user JWT of an approval, checked to be the account's for that
/// key and persona.
pub async fn request(
    url: &str,
    creds: &Path,
    persona: &PersonaId,
    user: &KeyPair,
    repeat: Duration,
    stop: impl Future<Output = ()>,
) -> Result<String> {
    let text = std::fs::read_to_string(creds).with_context(|| format!("reading {}", creds.display()))?;
    let enrollment = auth::creds_jwt(&text)
        .with_context(|| format!("no user JWT in {}", creds.display()))
        .and_then(auth::verified)
        .with_context(|| format!("enrollment credentials {}", creds.display()))?;
    let name = ants_proto::enrollment_user(persona);
    if enrollment.name != name {
        bail!(
            "{} are the credentials of {}, not {name} (`ants auth enroll {persona}`)",
            creds.display(),
            enrollment.name
        );
    }
    let left = match enrollment.exp {
        Some(exp) => Duration::from_secs(exp.checked_sub(now()?).filter(|left| *left > 0).with_context(|| {
            format!(
                "the enrollment credentials {} have expired; issue new ones with `ants auth enroll`",
                creds.display()
            )
        })?),
        None => Duration::MAX,
    };
    let (client, _) = bus::connect(url, Some(creds), None, persona, "ants-enroll").await?;
    let mut replies = client.subscribe(subject::enroll_reply(persona)).await?;
    let public_key = user.public_key();
    let request = EnrollRequest::new(persona.clone(), &public_key, roster::machine().await);
    let payload = serde_json::to_vec(&request)?;
    let mut repeats = tokio::time::interval(repeat);
    let expiry = tokio::time::sleep(left.min(Duration::from_secs(365 * 24 * 3600)));
    tokio::pin!(expiry, stop);
    loop {
        tokio::select! {
            _ = repeats.tick() => {
                client.publish(request.subject(), payload.clone().into()).await?;
                client.flush().await?;
            }
            msg = replies.next() => {
                let msg = msg.context("the NATS connection closed")?;
                let reply = match serde_json::from_slice::<EnrollReply>(&msg.payload) {
                    Ok(reply) if reply.persona() == persona && reply.answers(&public_key) => reply,
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!(error = %e, "ignoring a malformed enrollment answer");
                        continue;
                    }
                };
                match reply {
                    EnrollReply::Approved { jwt, .. } => match check_approval(&jwt, &enrollment, persona, &public_key) {
                        Ok(()) => return Ok(jwt),
                        Err(e) => tracing::warn!(error = format!("{e:#}"), "ignoring an enrollment approval"),
                    },
                    EnrollReply::Rejected { reason, .. } => match reason {
                        Some(reason) => bail!("the enrollment of {persona} was rejected: {reason}"),
                        None => bail!("the enrollment of {persona} was rejected"),
                    },
                }
            }
            () = &mut expiry => bail!("the enrollment credentials {} expired without an answer", creds.display()),
            () = &mut stop => bail!("stopped before an answer came"),
        }
    }
}

/// Check that `jwt` is the user JWT of `persona` for `public_key`, signed by the account that signed the enrollment
/// credentials.
fn check_approval(jwt: &str, enrollment: &auth::Claims, persona: &PersonaId, public_key: &str) -> Result<()> {
    let claims = auth::verified(jwt)?;
    if claims.iss != enrollment.iss {
        bail!("signed by {}, not by the account {}", claims.iss, enrollment.iss);
    }
    if claims.sub != public_key || claims.name != persona.as_str() {
        bail!("issued to {} ({}), not to {persona} ({public_key})", claims.name, claims.sub);
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
    listen: Duration,
}

impl Approver {
    /// Connect to the server at `url`, signing the credentials with the account key in the team directory `dir`, and
    /// listen for `listen` each time requests are gathered.
    pub async fn connect(url: &str, dir: &Path, listen: Duration) -> Result<Self> {
        let creds = auth::approver_creds(dir)?;
        let client = async_nats::ConnectOptions::with_credentials(&creds)?
            .name("ants-enroll")
            .connect(url)
            .await
            .context("connecting to NATS")?;
        Ok(Self { client, listen })
    }

    /// The requests heard, for `persona` or for every persona: the latest of each persona and key, oldest first.
    pub async fn pending(&self, persona: Option<&PersonaId>) -> Result<Vec<EnrollRequest>> {
        let filter = persona.map_or_else(|| subject::ALL_ENROLL_REQUESTS.to_owned(), subject::enroll);
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
                    if let Err(e) = auth::check_user_key(&request.public_key) {
                        tracing::warn!(persona = %request.persona, "ignoring an enrollment request: {e:#}");
                        continue;
                    }
                    heard.retain(|r| (&r.persona, &r.public_key) != (&request.persona, &request.public_key));
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
            println!("{}  {}  host {}  asked {age}s ago", r.persona, r.public_key, r.host);
        }
        Ok(())
    }

    /// Sign the credentials `persona` asks for, with the account key in `dir`, for the hub's JetStream domain if it
    /// has one, and send them. `public_key` picks the request when several machines ask for the persona. Returns the
    /// public key approved.
    pub async fn approve(
        &self,
        dir: &Path,
        persona: &PersonaId,
        public_key: Option<&str>,
        js_domain: Option<&JsDomain>,
    ) -> Result<String> {
        if persona.is_human() {
            bail!("{persona} is the default human; issue its credentials with `ants auth user`");
        }
        let request = self.one(persona, public_key).await?;
        let jwt = auth::sign_user(dir, persona, Role::Persona, js_domain, &request.public_key)?;
        let reply = EnrollReply::Approved { persona: persona.clone(), public_key: request.public_key.clone(), jwt };
        // An answer lost on the way leaves the machine asking: answer again while it does.
        for _ in 0..3 {
            self.answer(&reply).await?;
            let still = self.pending(Some(persona)).await?;
            if !still.iter().any(|r| r.public_key == request.public_key) {
                return Ok(request.public_key);
            }
        }
        bail!("{persona} on {} keeps asking; the answer did not get through", request.host)
    }

    /// Refuse the credentials `persona` asks for, to the machine of `public_key`, or to every machine asking for it,
    /// with `reason` if given. Returns the public keys refused.
    pub async fn reject(
        &self,
        persona: &PersonaId,
        public_key: Option<&str>,
        reason: Option<&str>,
    ) -> Result<Vec<String>> {
        let keys: Vec<String> = match public_key {
            Some(key) => vec![self.one(persona, Some(key)).await?.public_key],
            None => self.heard(persona).await?.into_iter().map(|r| r.public_key).collect(),
        };
        for key in &keys {
            let reply = EnrollReply::Rejected {
                persona: persona.clone(),
                public_key: Some(key.clone()),
                reason: reason.map(str::to_owned),
            };
            self.answer(&reply).await?;
        }
        Ok(keys)
    }

    /// The requests heard for `persona`; an error when there are none.
    async fn heard(&self, persona: &PersonaId) -> Result<Vec<EnrollRequest>> {
        let heard = self.pending(Some(persona)).await?;
        if heard.is_empty() {
            bail!(
                "no enrollment request for {persona} heard in {}s; run `ants persona add {persona} --enroll ...` on its \
                 machine and leave it waiting",
                self.listen.as_secs()
            );
        }
        Ok(heard)
    }

    /// The request for `persona` from `public_key`, or the only one heard.
    async fn one(&self, persona: &PersonaId, public_key: Option<&str>) -> Result<EnrollRequest> {
        let heard = self.heard(persona).await?;
        if let Some(key) = public_key {
            return heard
                .into_iter()
                .find(|r| r.public_key == key)
                .with_context(|| format!("no enrollment request for {persona} from {key} heard"));
        }
        if let [one] = heard.as_slice() {
            return Ok(one.clone());
        }
        let keys: Vec<String> = heard.iter().map(|r| format!("{} (host {})", r.public_key, r.host)).collect();
        bail!("several machines ask for {persona}; pick one with --public-key: {}", keys.join(", "))
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

    use ants_proto::PersonaId;
    use async_nats::Event;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::*;
    use crate::auth::tests::Server;

    const WAIT: Duration = Duration::from_secs(5);
    const REPEAT: Duration = Duration::from_millis(100);
    const LISTEN: Duration = Duration::from_millis(400);

    fn p(s: &str) -> PersonaId {
        s.parse().unwrap()
    }

    fn enrollment(server: &Server, persona: &PersonaId, ttl: Duration) -> std::path::PathBuf {
        let out = server.dir().join(format!("enroll-{persona}.creds"));
        auth::issue_enrollment(&server.dir().join("auth"), persona, ttl, &out).unwrap();
        out
    }

    #[tokio::test]
    async fn enrollment_credentials_ask_only_for_their_persona() {
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
        violation("asking for another persona").await;
        let _all = client.subscribe(subject::ALL_ENROLL_REPLIES).await.unwrap();
        violation("hearing the answers to others").await;
        let _inbox = client.subscribe("ants.inbox.dev-a").await.unwrap();
        violation("reading the persona's inbox").await;

        let other = enrollment(&server, &p("dev-b"), Duration::from_secs(60));
        let user = KeyPair::new_user();
        let err = request(&server.url, &other, &p("dev-a"), &user, REPEAT, pending()).await.unwrap_err();
        assert!(err.to_string().contains("not enroll-dev-a"), "{err:#}");
    }

    #[tokio::test]
    async fn an_approval_completes_the_credentials_of_the_asking_key() {
        let Some(server) = Server::start() else { return };
        let (team, dev_a) = (server.dir().join("auth"), p("dev-a"));
        let creds = enrollment(&server, &dev_a, Duration::from_secs(60));
        let user = KeyPair::new_user();
        let approver = Approver::connect(&server.url, &team, LISTEN).await.unwrap();

        let asking = request(&server.url, &creds, &dev_a, &user, REPEAT, pending());
        let approving = async {
            let heard = approver.pending(None).await.unwrap();
            assert_eq!(heard.len(), 1, "{heard:?}");
            assert_eq!((&heard[0].persona, &heard[0].public_key), (&dev_a, &user.public_key()));
            approver.approve(&team, &dev_a, None, None).await.unwrap()
        };
        let (jwt, approved) = timeout(WAIT * 2, async { tokio::join!(asking, approving) }).await.unwrap();
        assert_eq!(approved, user.public_key());
        let jwt = jwt.unwrap();

        let out = server.dir().join("dev-a.creds");
        auth::write_creds(&out, &jwt, &user).unwrap();
        let (client, _) = bus::connect(&server.url, Some(&out), None, &dev_a, "test").await.unwrap();
        client.flush().await.unwrap();
    }

    #[tokio::test]
    async fn a_rejection_or_the_expiry_ends_the_request() {
        let Some(server) = Server::start() else { return };
        let (team, dev_a) = (server.dir().join("auth"), p("dev-a"));
        let creds = enrollment(&server, &dev_a, Duration::from_secs(60));
        let (first, second) = (KeyPair::new_user(), KeyPair::new_user());
        let approver = Approver::connect(&server.url, &team, LISTEN).await.unwrap();

        let both = async {
            let first = request(&server.url, &creds, &dev_a, &first, REPEAT, pending());
            let second = request(&server.url, &creds, &dev_a, &second, REPEAT, pending());
            tokio::join!(first, second)
        };
        let answering = async {
            let err = approver.approve(&team, &dev_a, None, None).await.unwrap_err().to_string();
            assert!(err.contains("several machines ask for dev-a"), "{err}");
            approver.reject(&dev_a, None, Some("unknown machine")).await.unwrap()
        };
        let ((first_err, second_err), rejected) =
            timeout(WAIT * 2, async { tokio::join!(both, answering) }).await.unwrap();
        assert_eq!(rejected.len(), 2);
        for err in [first_err.unwrap_err(), second_err.unwrap_err()] {
            assert!(err.to_string().contains("rejected: unknown machine"), "{err:#}");
        }

        let err = approver.approve(&team, &dev_a, None, None).await.unwrap_err().to_string();
        assert!(err.contains("no enrollment request for dev-a"), "{err}");
        let short = enrollment(&server, &dev_a, Duration::from_secs(1));
        let err = request(&server.url, &short, &dev_a, &first, REPEAT, pending()).await.unwrap_err();
        assert!(format!("{err:#}").contains("expire"), "{err:#}");
    }
}
