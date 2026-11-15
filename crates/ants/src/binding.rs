//! Bindings of human names to GitHub users: a name registered with a GitHub sign-in (`ants login`, answered by
//! `ants signer`) or bound by `ants auth user NAME --human --github LOGIN` belongs to that GitHub user's numeric id for
//! good. The registry ([`revoke::ISSUED`]) holds them, on the record of each device signed for the name, so they are
//! kept and backed up with it.

use ants_daemon::config::registration::GithubAccount;
use ants_proto::UserId;
use anyhow::{Result, bail};

use crate::revoke::{self, Issued, Kind};

/// Who binds: `ants signer` registering a device, or the operator with `ants auth user --github`, who may also bind a
/// name already holding a human's keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Binder {
    Signer,
    Operator,
}

/// Whether `name` is already bound to `github`: false when signing for it binds it, true when it adds a device to it.
/// Fails, naming why, for `human`, a reserved name, a name any key other than a human's was issued to, a name bound to
/// another GitHub user, a GitHub user bound to another name, and, for the signer, a name holding keys of a human but no
/// binding.
pub fn check(issued: &[Issued], name: &UserId, github: &GithubAccount, binder: Binder) -> Result<bool> {
    let login = &github.login;
    check_name(issued, name)?;
    if let Some(other) =
        issued.iter().find(|i| i.github.as_ref().is_some_and(|g| g.id == github.id) && i.name != name.as_str())
    {
        bail!("GitHub user {login} is bound to {}, not {name}", other.name);
    }
    let mine: Vec<&Issued> = issued.iter().filter(|i| i.name == name.as_str()).collect();
    let bound = mine.iter().filter_map(|i| i.github.as_ref());
    if let Some(other) = bound.clone().find(|g| g.id != github.id) {
        bail!("{name} is bound to another GitHub user ({})", other.login);
    }
    if bound.count() > 0 {
        return Ok(true);
    }
    if !mine.is_empty() && binder == Binder::Signer {
        bail!(
            "{name} holds credentials bound to no GitHub user: the team binds it with `ants auth user {name} --human \
             --github LOGIN`"
        );
    }
    Ok(false)
}

/// The part of [`check`] no GitHub user changes: fails for `human`, a reserved name, and a name any key other than a
/// human's was issued to.
pub fn check_name(issued: &[Issued], name: &UserId) -> Result<()> {
    if name.is_human() {
        bail!("{name} is the default human, which no one registers");
    }
    if *name == UserId::channel_manager() || *name == UserId::signer() || name.as_str() == ants_proto::BOOTSTRAP_USER {
        bail!("{name} is a reserved name");
    }
    if let Some(other) = issued.iter().find(|i| i.name == name.as_str() && i.kind != Kind::Human) {
        bail!("{name} is taken: {} credentials were issued to it", kind(other.kind));
    }
    Ok(())
}

fn kind(kind: Kind) -> &'static str {
    match kind {
        Kind::Ant => "an ant's",
        Kind::Human => "a human's",
        Kind::Webhook => "a webhook's",
        Kind::Leaf => "a leaf node's",
        Kind::ChannelManager => "the channel manager's",
        Kind::Enrollment => "enrollment",
        Kind::Observer => "an observer's",
        Kind::Bootstrap => "bootstrap",
    }
}

/// [`check`] against the registry of `dir`.
pub fn check_in(dir: &std::path::Path, name: &UserId, github: &GithubAccount, binder: Binder) -> Result<bool> {
    check(&revoke::issued(dir)?, name, github, binder)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str, kind: Kind, github: Option<u64>) -> Issued {
        let mut issued = Issued::new("UKEY", name, kind, None).unwrap();
        issued.github = github.map(|id| GithubAccount { id, login: format!("user{id}") });
        issued
    }

    #[test]
    fn a_name_belongs_to_the_github_user_it_was_first_bound_to() {
        let account = |id| GithubAccount { id, login: format!("user{id}") };
        let p = |s: &str| -> UserId { s.parse().unwrap() };
        let issued = [
            record("alice", Kind::Human, Some(1)),
            record("alice", Kind::Human, None),
            record("bob", Kind::Human, None),
            record("dev-a", Kind::Ant, None),
            record("enroll-x", Kind::Enrollment, None),
        ];
        let check = |name: &str, id, binder| check(&issued, &p(name), &account(id), binder).map_err(|e| e.to_string());
        assert_eq!(check("alice", 1, Binder::Signer), Ok(true), "the same user adds a device");
        assert_eq!(check("carol", 3, Binder::Signer), Ok(false), "a free name is bound on first signing");
        let refused = [
            ("alice", 2, Binder::Operator, "alice is bound to another GitHub user (user1)"),
            ("carol", 1, Binder::Operator, "GitHub user user1 is bound to alice, not carol"),
            ("bob", 2, Binder::Signer, "bob holds credentials bound to no GitHub user"),
            ("dev-a", 2, Binder::Operator, "dev-a is taken: an ant's credentials"),
            ("enroll-x", 2, Binder::Signer, "enroll-x is taken: enrollment credentials"),
            ("human", 2, Binder::Operator, "human is the default human"),
            ("channel-manager", 2, Binder::Signer, "reserved"),
            ("bootstrap", 2, Binder::Signer, "reserved"),
            ("signer", 2, Binder::Signer, "reserved"),
        ];
        for (name, id, binder, why) in refused {
            let err = check(name, id, binder).unwrap_err();
            assert!(err.contains(why), "{name}: {err}");
        }
        assert_eq!(check("bob", 2, Binder::Operator), Ok(false), "the operator binds a name holding a human's keys");
    }
}
