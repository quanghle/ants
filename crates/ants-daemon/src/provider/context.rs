//! The [`CallContext`] of a provider call, as the daemon serving the ant knows it: never taken from the ant's text.
//! Tool calls and brief requests alike take who calls from [`Callers`], where the thread came from from [`origin`],
//! kept per thread by [`settled`], and the repository the session works in from [`repository`].

use std::path::Path;

use ants_proto::provider::{CallContext, Origin, Starter, normalize_remote, remote_url};
use ants_proto::{ChannelId, Envelope, Humans, ThreadId, UserId};
use chrono::Utc;

use crate::profile::identity::Identity;

/// Where the daemon finds who the provider calls of its ant are for: the ant, the machine serving it, and its identity
/// record, whose id is read when a call needs it ([`Identity::id`] keeps it once read).
#[derive(Clone)]
pub struct Callers {
    ant: UserId,
    machine: String,
    record: Record,
}

#[derive(Clone)]
#[cfg_attr(test, expect(clippy::large_enum_variant, reason = "the small variant serves tests only"))]
enum Record {
    Stored(Identity),
    #[cfg(test)]
    Known(String),
}

impl Callers {
    pub fn new(ant: UserId, machine: String, identity: Identity) -> Self {
        Self { ant, machine, record: Record::Stored(identity) }
    }

    /// Callers whose identity record has the id `identity`, without reading it.
    #[cfg(test)]
    pub(crate) fn known(ant: UserId, machine: &str, identity: &str) -> Self {
        Self { ant, machine: machine.to_owned(), record: Record::Known(identity.to_owned()) }
    }

    /// Who a call now is for: an error saying why when the identity record is absent or cannot be read.
    pub async fn caller(&self) -> Result<Caller, String> {
        let identity = match &self.record {
            Record::Stored(identity) => match identity.id().await {
                Ok(Some(id)) => id,
                Ok(None) => return Err("the ant has no identity record".to_owned()),
                Err(e) => {
                    tracing::warn!(error = format!("{e:#}"), "reading the ant's identity for a provider call");
                    return Err(CANNOT_READ.to_owned());
                }
            },
            #[cfg(test)]
            Record::Known(id) => id.clone(),
        };
        Ok(Caller { ant: self.ant.clone(), identity, machine: self.machine.clone() })
    }
}

/// Why a provider call names no caller when the ant's identity record cannot be read.
pub const CANNOT_READ: &str = "the ant's identity record cannot be read";

/// Who a provider call is for: the ant, the id of its identity record ([`crate::profile::identity::Identity::id`])
/// and the machine serving it, as its colony entry names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub ant: UserId,
    pub identity: String,
    pub machine: String,
}

impl Caller {
    /// The context of a call now on `thread`, which `origin` started, from a session of `harness` running `model`
    /// (None for the harness's default) in the repository `repo` ([`repository`]).
    pub fn context(
        &self,
        thread: ThreadId,
        origin: Origin,
        harness: &str,
        model: Option<&str>,
        repo: Option<String>,
    ) -> CallContext {
        CallContext {
            ant: self.ant.clone(),
            identity: self.identity.clone(),
            thread,
            origin,
            harness: harness.to_owned(),
            model: model.map(str::to_owned),
            machine: self.machine.clone(),
            repo,
            at: Utc::now(),
        }
    }
}

/// The repository a session working in `dir` works in: the remote of the git repository holding `dir` (`origin`,
/// else the first with a URL), normalized ([`normalize_remote`]). Read from the repository's config file without
/// running git, so `url.<base>.insteadOf` and included files are not applied; None outside a git repository, or one
/// without a remote.
pub fn repository(dir: &Path) -> Option<String> {
    let dir = std::path::absolute(dir).ok()?;
    let dot_git = dir.ancestors().map(|dir| dir.join(".git")).find(|path| path.exists())?;
    let git_dir = if dot_git.is_file() {
        // A linked worktree's or a submodule's `.git` file: `gitdir: <path>`, relative to the file's directory.
        let text = std::fs::read_to_string(&dot_git).ok()?;
        dot_git.parent()?.join(text.trim().strip_prefix("gitdir:")?.trim())
    } else {
        dot_git
    };
    let common = match std::fs::read_to_string(git_dir.join("commondir")) {
        Ok(text) => git_dir.join(text.trim()),
        Err(_) => git_dir,
    };
    let config = std::fs::read_to_string(common.join("config")).ok()?;
    remote_url(&config).and_then(normalize_remote)
}

/// Where the thread of `envelope`, a message as the daemon took it on arrival, came from, on `channel` when a post on
/// it is the message, in this order:
/// - a provider's wake request ([`Envelope::wake_provider`]): the provider's, as a thread the wake starts is; a wake on
///   an existing thread leaves it the origin [`settled`] kept;
/// - an ant's message, not a post, narrowed to a review ([`ants_proto::REVIEW_ALLOW`]): the review of the sender, the ant
///   under review;
/// - a webhook's or schedule's message, or one on a thread narrowed to other tool rules ([`Envelope::allow`]), which
///   only those start and only ants carry on: the webhook's, the sender standing for a webhook it passes on;
/// - the ant that started the thread with a channel post (its creator);
/// - the human who started it (its owner), or the human an assistant acts for;
/// - else the message's sender, a human or an ant.
pub fn origin(envelope: &Envelope, channel: Option<ChannelId>, humans: &Humans) -> Origin {
    let by = if let Some(name) = envelope.wake_provider() {
        Starter::Provider { name: name.to_owned() }
    } else if channel.is_none()
        && !envelope.webhook
        && ants_proto::is_review(envelope.allow.as_deref())
        && !envelope.sender_role(humans).is_human()
    {
        Starter::Review { user: envelope.from.clone() }
    } else if envelope.webhook || envelope.allow.is_some() {
        Starter::Webhook { user: envelope.from.clone() }
    } else if let Some(creator) = &envelope.creator {
        Starter::Ant { user: creator.clone() }
    } else if let Some(owner) = envelope.owner.as_ref().or(envelope.on_behalf_of.as_ref()) {
        Starter::Human { user: owner.clone() }
    } else if envelope.sender_role(humans).is_human() {
        Starter::Human { user: envelope.from.clone() }
    } else {
        Starter::Ant { user: envelope.from.clone() }
    };
    Origin { by, channel }
}

/// The origin of a thread `earlier` gave, now that a message on it gives `now`: the first stays, its channel set when
/// it had none, unless `now` is from outside the bus and `earlier` is not, so a thread a webhook ever reached stays
/// untrusted.
pub fn settled(earlier: Option<Origin>, now: Origin) -> Origin {
    match earlier {
        None => now,
        Some(earlier) if earlier.trusted() && !now.trusted() => {
            Origin { channel: earlier.channel.or(now.channel), ..now }
        }
        Some(Origin { by, channel }) => Origin { by, channel: channel.or(now.channel) },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ants_test_support::user;

    fn message(from: &str) -> Envelope {
        Envelope::on_thread("t1".parse().unwrap(), from.parse().unwrap(), "dev-a".parse().unwrap(), "hi")
    }

    fn by(envelope: &Envelope) -> Starter {
        origin(envelope, None, &Humans::of([ants_test_support::human()])).by
    }

    #[test]
    fn the_origin_is_the_provider_then_the_review_then_the_webhook_then_the_creator_then_the_owner_then_the_sender() {
        let wake = Envelope { owner: Some(ants_test_support::human()), ..message("dev-a").woken_by("sense") };
        assert_eq!(by(&wake), Starter::Provider { name: "sense".to_owned() });
        assert_eq!(by(&message("dev-b").woken_by("sense")), Starter::Ant { user: user("dev-b") }, "no wake");
        let webhook = Envelope { owner: Some(ants_test_support::human()), ..message("hook").from_webhook() };
        assert_eq!(by(&webhook), Starter::Webhook { user: user("hook") });
        assert!(!origin(&webhook, None, &Humans::default()).trusted());
        let created =
            Envelope { creator: Some(user("dev-c")), owner: Some(ants_test_support::human()), ..message("dev-b") };
        assert_eq!(by(&created), Starter::Ant { user: user("dev-c") });
        let owned = Envelope { owner: Some(ants_test_support::human()), ..message("dev-b") };
        assert_eq!(by(&owned), Starter::Human { user: ants_test_support::human() });
        let assisted = message("assistant").on_behalf(Some(ants_test_support::human()));
        assert_eq!(by(&assisted), Starter::Human { user: ants_test_support::human() });
        assert_eq!(by(&message("human")), Starter::Human { user: ants_test_support::human() });
        assert_eq!(by(&message("dev-b")), Starter::Ant { user: user("dev-b") });
        let narrowed = message("dev-b").with_allow(Some(vec!["read:*".to_owned()]));
        assert_eq!(by(&narrowed), Starter::Webhook { user: user("dev-b") }, "carrying on a webhook's thread");
        let review = message("dev-b").with_allow(Some(ants_proto::REVIEW_ALLOW.map(str::to_owned).to_vec()));
        assert_eq!(by(&review), Starter::Review { user: user("dev-b") }, "the ant under review");
        assert!(!origin(&review, None, &Humans::default()).trusted());
        let channel: ChannelId = "ops".parse().unwrap();
        let posted = origin(&review, Some(channel.clone()), &Humans::default());
        assert_eq!(posted.by, Starter::Webhook { user: user("dev-b") }, "a post asks no review");
        let relayed = Envelope { owner: Some(ants_test_support::human()), ..review.from_webhook() };
        assert_eq!(by(&relayed), Starter::Webhook { user: user("dev-b") }, "a webhook asks no review");
        assert_eq!(origin(&message("dev-b"), Some(channel.clone()), &Humans::default()).channel, Some(channel));
    }

    #[test]
    fn the_repository_is_the_origin_remote_else_the_first_normalized() {
        let config = "[core]\n\tbare = false\n[remote \"upstream\"]\n\turl = https://github.com/up/repo.git\n\
                      [Remote \"origin\"]\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n\tURL = \"git@GitHub.com:o/r.git\"\n";
        let dir = ants_test_support::scratch_dir("repository");
        let main = dir.join("main");
        std::fs::create_dir_all(main.join(".git/worktrees/w")).unwrap();
        std::fs::create_dir_all(main.join("src/deep")).unwrap();
        std::fs::write(main.join(".git/config"), config).unwrap();
        std::fs::write(main.join(".git/worktrees/w/commondir"), "../..\n").unwrap();
        assert_eq!(repository(&main.join("src/deep")).as_deref(), Some("github.com/o/r"));
        let linked = dir.join("linked");
        std::fs::create_dir_all(&linked).unwrap();
        std::fs::write(linked.join(".git"), "gitdir: ../main/.git/worktrees/w\n").unwrap();
        assert_eq!(repository(&linked).as_deref(), Some("github.com/o/r"), "a linked worktree reads the common config");
        let bare = dir.join("bare");
        std::fs::create_dir_all(bare.join(".git")).unwrap();
        std::fs::write(bare.join(".git/config"), "[core]\n\tbare = false\n").unwrap();
        assert_eq!(repository(&bare), None, "no remote");
        assert_eq!(repository(Path::new("/")), None, "no repository");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_first_origin_stays_until_one_from_outside_the_bus() {
        let ops: ChannelId = "ops".parse().unwrap();
        let human = Origin { by: Starter::Human { user: ants_test_support::human() }, channel: None };
        let ant = Origin { by: Starter::Ant { user: user("dev-b") }, channel: Some(ops.clone()) };
        let hook = Origin { by: Starter::Webhook { user: user("hook") }, channel: None };
        assert_eq!(settled(None, ant.clone()), ant);
        assert_eq!(settled(Some(human.clone()), ant.clone()), Origin { channel: Some(ops.clone()), ..human.clone() });
        let untrusted = settled(Some(ant), hook.clone());
        assert_eq!(untrusted, Origin { channel: Some(ops), ..hook.clone() });
        assert_eq!(settled(Some(hook.clone()), human.clone()), hook, "a human's message keeps it untrusted");
        let woken = Origin { by: Starter::Provider { name: "sense".to_owned() }, channel: None };
        assert_eq!(settled(None, woken.clone()), woken, "a thread a wake starts is the provider's");
        assert_eq!(settled(Some(human.clone()), woken.clone()), human, "a wake on a thread leaves its origin");
        assert_eq!(settled(Some(hook.clone()), woken.clone()), hook, "and leaves it untrusted");
        assert_eq!(settled(Some(woken), hook.clone()), hook);
    }
}
