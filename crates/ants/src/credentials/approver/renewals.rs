//! The renewal requests an [`Approver`] hears, lists and answers, one by one or, with `ants enroll renew --auto`, as
//! they arrive.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use ants_daemon::bus::relayed;
use ants_daemon::secrets::creds::Finding;
use ants_proto::{JsDomain, RenewReply, RenewRequest, subject};
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use futures::StreamExt as _;

use super::Approver;
use crate::auth;
use crate::credentials::issued;

impl Approver {
    /// Check that `request` proves credentials of the team's account named as it says, unexpired and not revoked:
    /// signed with their key. Returns their claims.
    fn check_renewal(&self, request: &RenewRequest) -> Result<auth::Claims> {
        auth::check_user_key(&request.public_key)?;
        let current = auth::verified(&request.current).context("the credentials to renew")?;
        if current.iss != self.account {
            bail!("credentials issued by {}, not by the team's account", current.iss);
        }
        if current.name != request.name {
            bail!("credentials of {}, not {}", current.name, request.name);
        }
        if current.exp.is_some_and(|exp| exp <= auth::now().unwrap_or(u64::MAX)) {
            bail!("expired credentials");
        }
        if self.revoked.contains(&current.sub) {
            bail!("revoked credentials");
        }
        auth::check_signature(&current.sub, &request.signed(), &request.signature).context("the credentials")?;
        Ok(current)
    }

    /// A checked renewal request of `msg`, with the claims of the credentials it renews, or why it is ignored.
    pub(crate) fn checked_renewal(&self, msg: &async_nats::Message) -> Result<(RenewRequest, auth::Claims)> {
        let request: RenewRequest = serde_json::from_slice(&msg.payload).context("a malformed renewal request")?;
        request.check_subject(&msg.subject)?;
        let current = self.check_renewal(&request).with_context(|| format!("renewal request of {}", request.name))?;
        Ok((request, current))
    }

    /// A checked renewal request of `msg` for `ants enroll renew`, or why it is ignored: credentials a registration
    /// granted are `ants signer`'s to renew ([`issued::registered`]).
    pub(crate) fn renewal(&self, msg: &async_nats::Message) -> Result<RenewRequest> {
        let (request, current) = self.checked_renewal(msg)?;
        if issued::registered(&issued::issued(&self.dir)?, &current.sub).is_some() {
            bail!(
                "renewal request of {}: a registration granted the credentials, which ants signer renews",
                request.name
            );
        }
        Ok(request)
    }

    /// The renewal requests heard, for `name` or for every name: the latest of each name and key, oldest first.
    pub async fn renewals(&self, name: Option<&str>) -> Result<Vec<RenewRequest>> {
        let filter = name.map_or_else(|| subject::ALL_RENEW_REQUESTS.to_owned(), subject::renew);
        let mut requests = relayed::subscribe(&self.client, filter).await?;
        self.client.flush().await?;
        let mut heard: Vec<RenewRequest> = Vec::new();
        let end = tokio::time::sleep(self.listen);
        tokio::pin!(end);
        loop {
            tokio::select! {
                msg = requests.next() => {
                    let msg = msg.context("the NATS connection closed")?;
                    match self.renewal(&msg) {
                        Ok(request) => {
                            heard.retain(|r| (&r.name, &r.public_key) != (&request.name, &request.public_key));
                            heard.push(request);
                        }
                        Err(e) => tracing::warn!("ignoring a renewal request: {e:#}"),
                    }
                }
                () = &mut end => break,
            }
        }
        requests.unsubscribe().await?;
        heard.sort_by_key(|r| r.sent_at);
        Ok(heard)
    }

    /// Print the renewal requests heard.
    pub async fn list_renewals(&self) -> Result<()> {
        let heard = self.renewals(None).await?;
        if heard.is_empty() {
            println!("no renewal requests heard in {}s", self.listen.as_secs());
        }
        for r in heard {
            let age = (Utc::now() - r.sent_at).num_seconds().max(0);
            let holder = Finding::from_jwt(&r.current).map_or_else(|_| r.name.clone(), |f| f.holder());
            println!("{}  {holder}  new key {}  host {}  asked {age}s ago", r.name, r.public_key, r.host);
        }
        Ok(())
    }

    /// Renew the credentials `name` asks for, with the account key in `dir` ([`auth::renew`]), and send them.
    /// `public_key` picks the request when several holders ask for `name`.
    pub async fn renew(
        &self,
        dir: &Path,
        name: &str,
        public_key: Option<&str>,
    ) -> Result<(RenewRequest, auth::Renewed)> {
        let heard = self.renewals(Some(name)).await?;
        let request = match (public_key, heard.as_slice()) {
            (_, []) => bail!(
                "no renewal request for {name} heard in {}s; run `ants renew FILE` where its credentials are and \
                 leave it waiting",
                self.listen.as_secs()
            ),
            (Some(key), heard) => heard
                .iter()
                .find(|r| r.public_key == key)
                .with_context(|| format!("no renewal request for {name} from {key} heard"))?,
            (None, [one]) => one,
            (None, several) => {
                let keys: Vec<String> = several.iter().map(|r| format!("{} (host {})", r.public_key, r.host)).collect();
                bail!("several holders ask to renew {name}; pick one with --public-key: {}", keys.join(", "))
            }
        };
        let renewed = auth::renew(dir, &request.current, &request.public_key, request.xkey.as_ref())?;
        let reply = RenewReply::signed(name, &request.public_key, renewed.jwt.clone());
        // An answer lost on the way leaves the holder asking: answer again while it does.
        for _ in 0..3 {
            self.answer_renewal(&reply).await?;
            let still = self.renewals(Some(name)).await?;
            if !still.iter().any(|r| r.public_key == request.public_key) {
                return Ok((request.clone(), renewed));
            }
        }
        bail!("{name} on {} keeps asking; the answer did not get through", request.host)
    }

    pub(crate) async fn answer_renewal(&self, reply: &RenewReply) -> Result<()> {
        self.client.publish(reply.subject(), serde_json::to_vec(reply)?.into()).await?;
        self.client.flush().await?;
        Ok(())
    }
}

/// `ants enroll renew --auto`: renew every credentials asking ([`auth::renew`]) as their requests arrive, over the
/// server at `url`, with the account key in `dir`, until `stop` completes. Each run connects afresh every
/// [`super::AUTO_RECONNECT`], with credentials it issues itself, and reads the revocations again. Calls `renewed` with
/// what each renewal did.
pub async fn auto(
    url: &str,
    dir: &Path,
    js_domain: Option<&JsDomain>,
    listen: Duration,
    reconnect: Duration,
    mut renewed: impl FnMut(&RenewRequest, &auth::Renewed),
    stop: impl Future<Output = ()>,
) -> Result<()> {
    // The answer to each name and key: a holder that missed it asks again and gets the same.
    let mut answered: HashMap<(String, String), RenewReply> = HashMap::new();
    tokio::pin!(stop);
    loop {
        let approver = Approver::connect(url, dir, js_domain, listen).await?;
        let mut requests = relayed::subscribe(&approver.client, subject::ALL_RENEW_REQUESTS).await?;
        approver.client.flush().await?;
        let until = tokio::time::sleep(reconnect);
        tokio::pin!(until);
        loop {
            tokio::select! {
                msg = requests.next() => {
                    let msg = msg.context("the NATS connection closed")?;
                    let request = match approver.renewal(&msg) {
                        Ok(request) => request,
                        Err(e) => {
                            tracing::warn!("ignoring a renewal request: {e:#}");
                            continue;
                        }
                    };
                    let key = (request.name.clone(), request.public_key.clone());
                    if let Some(reply) = answered.get(&key) {
                        approver.answer_renewal(reply).await?;
                        continue;
                    }
                    match auth::renew(dir, &request.current, &request.public_key, request.xkey.as_ref()) {
                        Ok(done) => {
                            let reply = RenewReply::signed(&request.name, &request.public_key, done.jwt.clone());
                            approver.answer_renewal(&reply).await?;
                            answered.insert(key, reply);
                            renewed(&request, &done);
                        }
                        Err(e) => tracing::warn!(name = %request.name, "not renewing: {e:#}"),
                    }
                }
                () = &mut until => break,
                () = &mut stop => return Ok(()),
            }
        }
    }
}

/// Print what renewing `request` did, and how to retire the earlier key.
pub fn print_renewed(request: &RenewRequest, renewed: &auth::Renewed) {
    println!(
        "{} renewed for {} (host {}); once the new credentials are in use, retire the earlier key: \
         `ants auth revoke --key {} --earlier`, then `ants auth push`",
        renewed.finding.holder(),
        request.public_key,
        request.host,
        renewed.previous
    );
}
