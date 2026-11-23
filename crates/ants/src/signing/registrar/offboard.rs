//! Offboarding: revoking every key of a name whose GitHub user left the registration policy, recorded in
//! [`OFFBOARDED`], and the membership sweep finding such users between renewals.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write as _;
use std::path::Path;
use std::time::Duration;

use ants_daemon::config::registration::{GithubAccount, Grant, signer_registration};
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use super::{OFFBOARDED, Progress, Registrar, SWEEP_PACE, SWEEP_RECHECK, SWEEP_RESERVE, Update};
use crate::auth;
use crate::credentials::issued;
use crate::credentials::push;
use crate::credentials::revoke;
use crate::people::devices;
use crate::signing::github::Github;
use crate::signing::signer::AbortOnDrop;

impl Registrar<'_> {
    /// Revoke every key of `name`, bound to the GitHub user `account`, which left the registration policy for the
    /// reason `why`, as `ants auth revoke` does: record it in [`OFFBOARDED`], push the account, and record the revoked
    /// devices in the hub's directory ([`devices::record_revocations`]). Returns what happened, in words.
    pub(super) async fn offboard(&self, name: &str, account: &GithubAccount, why: &str, by: Offboarder) -> String {
        let revoked = match revoke::revoke(self.dir, Some(name), &[]) {
            Ok(revoked) => revoked,
            Err(e) => return format!("revoking the keys of {name} failed ({e:#}): `ants auth revoke {name}`"),
        };
        let keys: Vec<String> = revoked.into_iter().map(|r| r.key).collect();
        let n = keys.len();
        tracing::warn!(name, github = %account.login, by = ?by, keys = n, "revoked as {why}");
        let at = auth::now().unwrap_or_default();
        let line = Offboarded { at, name: name.to_owned(), github: account.clone(), by, reason: why.to_owned(), keys };
        if let Err(e) = record_offboarded(self.dir, &line) {
            tracing::warn!(name, "recording the revocation in {OFFBOARDED}: {e:#}");
        }
        let pushed = match push::push(self.url, self.dir).await {
            Ok(_) => match devices::record_revocations(self.url, self.js_domain, self.dir).await {
                Ok(_) => String::new(),
                Err(e) => format!(", but recording the revoked devices failed ({e:#}): `ants auth push`"),
            },
            Err(e) => format!(", but pushing the account failed ({e:#}): `ants auth push`"),
        };
        format!("{n} keys of {name} revoked{pushed}")
    }

    /// Begin a membership sweep if one is due and may run: a task checking on GitHub, one at a time, the users the
    /// live credentials an organization or team granted are bound to ([`swept`]), reporting [`Progress::Swept`] for
    /// each. A sweep runs every `[registration] sweep`, the first at once; while one may not, the signer says why once
    /// and looks again every [`SWEEP_RECHECK`].
    pub fn sweep(&mut self) {
        let now = Instant::now();
        if now < self.sweep.next || self.sweep.task.as_ref().is_some_and(|task| !task.0.is_finished()) {
            return;
        }
        match self.start_sweep() {
            Ok((every, task)) => {
                if self.sweep.off.take().is_some() {
                    print_sweep(&format!("on, every {}", ants_proto::duration::format(every)));
                }
                (self.sweep.next, self.sweep.task) = (now + every, task);
            }
            Err(why) => {
                if self.sweep.off.as_ref() != Some(&why) {
                    print_sweep(&format!("off: {why}"));
                }
                (self.sweep.next, self.sweep.task, self.sweep.off) = (now + SWEEP_RECHECK, None, Some(why));
            }
        }
    }

    /// The task of a membership sweep, if there is anyone to check, and when the next is due; else why none may run.
    pub(super) fn start_sweep(&self) -> Result<(Duration, Option<AbortOnDrop>), String> {
        let registration = match signer_registration(self.config) {
            Ok(Some(registration)) => registration,
            Ok(None) => return Err(format!("{} has no [registration] table", self.config.display())),
            Err(e) => return Err(format!("the registration policy does not load ({e:#})")),
        };
        if registration.sweep.is_zero() {
            return Err("[registration] sweep is 0".to_owned());
        }
        if registration.orgs.is_empty() && registration.teams.is_empty() {
            return Err("the registration policy names no organization or team".to_owned());
        }
        let github = Github::new(Some(&registration)).map_err(|e| format!("{e:#}"))?;
        if !github.has_token() {
            return Err(
                "no GitHub token of the signer reads memberships ([registration] token_file or token_store)".to_owned()
            );
        }
        let users = swept(self.dir).map_err(|e| format!("{e:#}"))?;
        if users.is_empty() {
            return Ok((registration.sweep, None));
        }
        let (every, tx) = (registration.sweep, self.progress.0.clone());
        Ok((
            every,
            Some(AbortOnDrop(tokio::spawn(async move {
                let rate_wait = async || {
                    if let Some(wait) = github.rate_wait(SWEEP_RESERVE) {
                        tracing::info!("membership sweep: waiting {wait:?} for GitHub's rate limit");
                        tokio::time::sleep(wait).await;
                    }
                };
                rate_wait().await;
                if let Err(e) = github.sees_policy(&registration).await {
                    print_sweep(&format!("skipped, as the signer's token cannot tell who is a member: {e:#}"));
                    return;
                }
                print_sweep(&format!("checking the GitHub users of {} names", users.len()));
                for (i, (name, account)) in users.into_iter().enumerate() {
                    if i > 0 {
                        tokio::time::sleep(SWEEP_PACE).await;
                    }
                    rate_wait().await;
                    let found = github.member(account.id, &registration).await.map(|user| {
                        user.map(|user| {
                            let grant = registration.grant(&user);
                            (user.account(), grant)
                        })
                    });
                    let _ = tx.send(Update(name.clone(), Progress::Swept { name, account, found }));
                }
            }))),
        ))
    }

    /// Act on what the membership sweep found of the GitHub user `account` the live credentials of `name` are bound
    /// to: revoke every key of the name ([`Registrar::offboard`]) when GitHub definitely finds the user outside the
    /// policy, or gone, and the sweep still covers the name; keep it when GitHub could not tell.
    pub(super) async fn swept(
        &self,
        name: &str,
        account: &GithubAccount,
        found: Result<Option<(GithubAccount, Option<Grant>)>>,
    ) {
        let why = match found {
            Ok(Some((_, Some(_)))) => return,
            Ok(found) => gone(account, found.as_ref().map(|(now, _)| now)),
            Err(e) => {
                print_sweep(&format!(
                    "{name} kept, as the memberships of GitHub user {} could not be checked ({e:#})",
                    account.login
                ));
                return;
            }
        };
        let still = signer_registration(self.config).ok().flatten().is_some_and(|r| !r.sweep.is_zero())
            && swept(self.dir).is_ok_and(|users| users.get(name).is_some_and(|bound| bound.id == account.id));
        if still {
            let what = self.offboard(name, account, &why, Offboarder::Sweep).await;
            print_sweep(&format!("{why}: {what}"));
        }
    }
}

/// What revoked a name as its GitHub user left the registration policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Offboarder {
    /// A renewal refused.
    Renewal,
    /// The membership sweep.
    Sweep,
}

/// A line of [`OFFBOARDED`]: the keys of `name`, bound to the GitHub user `github`, revoked at `at` (seconds since the
/// epoch) by `by`, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Offboarded {
    pub at: u64,
    pub name: String,
    pub github: GithubAccount,
    pub by: Offboarder,
    pub reason: String,
    pub keys: Vec<String>,
}

/// Why the GitHub user `account` falls outside the registration policy: GitHub finds the user, as `now`, no member of
/// its organizations or teams, or no user any more.
pub(super) fn gone(account: &GithubAccount, now: Option<&GithubAccount>) -> String {
    match now {
        Some(now) => format!("GitHub user {} is no member of the team's organizations or teams any more", now.login),
        None => format!("GitHub has no user {} (id {}) any more", account.login, account.id),
    }
}

/// The names a membership sweep covers, each with the GitHub user it is bound to: those with live credentials (neither
/// revoked nor expired) an organization or team of the policy granted.
pub(super) fn swept(dir: &Path) -> Result<BTreeMap<String, GithubAccount>> {
    let revoked: HashSet<String> = revoke::revoked(dir)?.into_iter().map(|r| r.key).collect();
    let now = auth::now()?;
    let mut latest: HashMap<String, issued::Issued> = HashMap::new();
    for one in issued::issued(dir)? {
        latest.insert(one.key.clone(), one);
    }
    let live = latest.into_values().filter(|i| !revoked.contains(&i.key) && i.expires_at.is_none_or(|exp| exp > now));
    let granted = live.filter(|i| matches!(i.grant, Some(Grant::Org(_) | Grant::Team(_))));
    Ok(granted.filter_map(|i| Some((i.name, i.github?))).collect())
}

/// Append `line` to [`OFFBOARDED`] in `dir`.
pub(super) fn record_offboarded(dir: &Path, line: &Offboarded) -> Result<()> {
    let path = dir.join(OFFBOARDED);
    let mut text = serde_json::to_string(line)?;
    text.push('\n');
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    file.write_all(text.as_bytes()).with_context(|| format!("writing {}", path.display()))
}

/// Print what the membership sweep did.
pub(super) fn print_sweep(what: &str) {
    println!("membership sweep: {what}");
}
