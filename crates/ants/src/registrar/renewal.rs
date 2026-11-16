//! Renewals of the credentials a registration granted: renewed while their grant holds, checked again on GitHub for
//! an organization or team grant, else refused, revoking the name.

use ants_daemon::config::registration::{GithubAccount, Grant, signer_registration};
use ants_proto::{MAX_REGISTRATIONS, RenewReply, RenewRequest};
use anyhow::{Context as _, Result};
use tokio::time::Instant;

use super::offboard::{Offboarder, gone};
use super::{Progress, Registrar, Renewal, Update};
use crate::auth;
use crate::enroll::Approver;
use crate::github::Github;
use crate::revoke;
use crate::signer::AbortOnDrop;

impl Registrar<'_> {
    /// Answer the renewal request `msg` if it is one of credentials a registration granted ([`revoke::registered`]):
    /// a repeat with the answer it had, a new one by renewing them while their grant holds, else refusing. An
    /// organization or team grant is checked again on GitHub first, by a task reporting [`Progress::Member`].
    pub async fn renewal(&mut self, approver: &Approver, msg: &async_nats::Message) -> Result<()> {
        let (request, current) = match approver.checked_renewal(msg) {
            Ok(checked) => checked,
            Err(e) => {
                tracing::warn!("ignoring a renewal request: {e:#}");
                return Ok(());
            }
        };
        let key = (request.name.clone(), request.public_key.clone());
        match self.renewals.get(&key) {
            Some((_, Renewal::Answered(reply))) => return approver.answer_renewal(reply).await,
            Some((_, Renewal::Checking { .. })) => return Ok(()),
            None => {}
        }
        let issued = revoke::issued(self.dir)?;
        let Some(record) = revoke::registered(&issued, &current.sub) else { return Ok(()) };
        let (Some(account), Some(grant)) = (record.github.clone(), record.grant.clone()) else { return Ok(()) };
        let name = request.name.as_str();
        let reply = match grant {
            Grant::Approval(_) => match revoke::standing_approval(&issued, name, account.id) {
                Some(grant) => self.renewed(&request, account, grant),
                None => self.refused(
                    &request,
                    "the approval that registered the device was withdrawn: register it again with `ants login`".into(),
                ),
            },
            Grant::EmailDomain(domain) => self.refused(
                &request,
                format!(
                    "a verified email address in {domain} registered the device, which only signing in again shows: \
                     register it again with `ants login`"
                ),
            ),
            Grant::Org(_) | Grant::Team(_) => {
                let checking = self.renewals.values().filter(|(_, r)| matches!(r, Renewal::Checking { .. })).count();
                if checking >= MAX_REGISTRATIONS {
                    // The holder asks again.
                    return Ok(());
                }
                match self.check_member(&request, &account) {
                    Ok(task) => {
                        print_renewal(&request, &format!("checking the memberships of GitHub user {}", account.login));
                        let checking = Renewal::Checking { request, account, _task: task };
                        self.renewals.insert(key, (Instant::now(), checking));
                        return Ok(());
                    }
                    Err(e) => self.refused(&request, format!("{e:#}: try again later")),
                }
            }
        };
        self.renewals.insert(key, (Instant::now(), Renewal::Answered(reply.clone())));
        approver.answer_renewal(&reply).await
    }

    /// Begin checking on GitHub, with the signer's token, whether the GitHub user `account` is still a member under
    /// the policy, for the renewal `request`: a task reporting [`Progress::Member`].
    fn check_member(&self, request: &RenewRequest, account: &GithubAccount) -> Result<AbortOnDrop> {
        let registration =
            signer_registration(self.config).context("the signer's registration policy does not load")?.with_context(
                || format!("registration is no longer enabled on this team to check GitHub user {}", account.login),
            )?;
        let github = Github::new(Some(&registration))?;
        let (id, tx) = (account.id, self.progress.0.clone());
        let (key, name) = (request.public_key.clone(), request.name.clone());
        Ok(AbortOnDrop(tokio::spawn(async move {
            let found = github.member(id, &registration).await.map(|user| {
                user.map(|user| {
                    let grant = registration.grant(&user);
                    (user.account(), grant)
                })
            });
            let _ = tx.send(Update(key, Progress::Member { name, found }));
        })))
    }

    /// Act on what the membership check of the renewal `key` found, and answer: renew the credentials under the
    /// policy's grant; refuse, then revoke every key of the name and push the account, when the user is no member, or
    /// no GitHub user any more; refuse alone when GitHub could not tell. The refusal goes first, as the push closes
    /// the asking connection.
    pub(super) async fn member(
        &mut self,
        approver: &Approver,
        key: (String, String),
        found: Result<Option<(GithubAccount, Option<Grant>)>>,
    ) -> Result<()> {
        let Some((since, Renewal::Checking { request, account, .. })) = self.renewals.remove(&key) else {
            return Ok(());
        };
        let left = |why: &str| format!("{why}: every key of {} is revoked", request.name);
        let (reply, revoking) = match found {
            Ok(Some((now, Some(grant)))) => (self.renewed(&request, now, grant), None),
            Ok(found) => {
                let why = gone(&account, found.as_ref().map(|(now, _)| now));
                (self.refused(&request, left(&why)), Some(why))
            }
            Err(e) => {
                let why = format!("the memberships of GitHub user {} could not be checked ({e:#})", account.login);
                (self.refused(&request, format!("{why}: try again later")), None)
            }
        };
        self.renewals.insert(key, (since, Renewal::Answered(reply.clone())));
        approver.answer_renewal(&reply).await?;
        if let Some(why) = revoking {
            let what = self.offboard(&request.name, &account, &why, Offboarder::Renewal).await;
            print_renewal(&request, &what);
        }
        Ok(())
    }

    /// The answer renewing the credentials of `request` for the GitHub user `account` under `grant`
    /// ([`auth::renew_registered`]), or refusing it.
    fn renewed(&self, request: &RenewRequest, account: GithubAccount, grant: Grant) -> RenewReply {
        let (login, why) = (account.login.clone(), grant.to_string());
        let renewed = auth::renew_registered(
            self.dir,
            &request.current,
            &request.public_key,
            request.xkey.as_ref(),
            account,
            grant,
        );
        match renewed {
            Ok(renewed) => {
                print_renewal(request, &format!("renewed: GitHub user {login}, {why}"));
                RenewReply::signed(&request.name, &request.public_key, renewed.jwt)
            }
            Err(e) => self.refused(request, format!("{e:#}")),
        }
    }

    /// The answer refusing to renew the credentials of `request`, and why.
    fn refused(&self, request: &RenewRequest, reason: String) -> RenewReply {
        print_renewal(request, &format!("refused: {reason}"));
        RenewReply::refused(&request.name, &request.public_key, reason)
    }
}

/// Print what the signer did with the renewal `request`.
pub(super) fn print_renewal(request: &RenewRequest, what: &str) {
    println!("renewal of {} (new key {}, host {}): {what}", request.name, request.public_key, request.host);
}
