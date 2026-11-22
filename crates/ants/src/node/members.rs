//! The members of a machine's leaf node, its ants, humans and webhooks, kept between runs of `ants auth leaf` and
//! `ants leaf outbox`: a run naming none takes those of the last one, and `--add-*` and `--remove-*` change them.
//!
//! `ants auth leaf` runs where the team keys are. It keeps the members in the line of the registry
//! ([`revoke::ISSUED`]) recording the uplink key it issues, and `ants enroll approve`, there too, adds an ant enrolled
//! through the leaf node with a later line for that key.
//!
//! `ants leaf outbox` runs on the machine itself: the uplink credentials of its leaf node configuration name its
//! members, enrolled ants included, as an enrollment through the leaf renews them there.

use std::fmt;
use std::path::Path;

use ants_daemon::secrets::creds::{self, Finding};
use ants_proto::{Machine, Role, UserId};
use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

use crate::auth;
use crate::credentials::revoke::{self, Issued, Kind};
use crate::node::leaf;

/// The members of a machine's leaf node, as a line of the registry keeps them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Members {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ants: Vec<UserId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub humans: Vec<UserId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub webhooks: Vec<UserId>,
    /// The uplink also carries what `ants enroll` on the machine needs.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub enroll_approver: bool,
}

/// Whether a leaf node's uplink also carries what `ants enroll` on the machine needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrollApprover {
    Without,
    With,
}

impl From<bool> for EnrollApprover {
    fn from(enroll_approver: bool) -> Self {
        if enroll_approver { Self::With } else { Self::Without }
    }
}

impl Members {
    /// The members of an uplink issued for `members`, ants and humans each in its role, and `webhooks`.
    pub fn new(members: &[(UserId, Role)], webhooks: &[UserId], approver: EnrollApprover) -> Self {
        let of = |human: bool| members.iter().filter(move |(_, role)| role.is_human() == human).map(|(u, _)| u.clone());
        Self {
            ants: of(false).collect(),
            humans: of(true).collect(),
            webhooks: webhooks.to_vec(),
            enroll_approver: approver == EnrollApprover::With,
        }
    }

    /// The ants and humans, each in its role ([`auth::members`]).
    pub fn roles(&self) -> Vec<(UserId, Role)> {
        auth::members(&self.ants, &self.humans)
    }

    /// The members leaf uplink credentials of `machine` carry, as `finding` tells; none for other credentials.
    fn carried(finding: Finding, machine: &Machine) -> Option<Self> {
        match finding.kind {
            creds::Kind::Leaf { machine: m, ants, humans, webhooks, enroll_approver } if m == *machine => {
                Some(Self { ants, humans, webhooks, enroll_approver })
            }
            _ => None,
        }
    }

    fn holds(&self, user: &UserId) -> bool {
        [&self.ants, &self.humans, &self.webhooks].iter().any(|list| list.contains(user))
    }
}

impl fmt::Display for Members {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let list = |users: &[UserId]| users.iter().map(UserId::as_str).collect::<Vec<_>>().join(", ");
        let mut parts: Vec<String> = [("ants", &self.ants), ("humans", &self.humans), ("webhooks", &self.webhooks)]
            .into_iter()
            .filter(|(_, users)| !users.is_empty())
            .map(|(what, users)| format!("{what} {}", list(users)))
            .collect();
        if self.enroll_approver {
            parts.push("enroll approver".to_owned());
        }
        if parts.is_empty() { f.write_str("no members") } else { f.write_str(&parts.join("; ")) }
    }
}

/// A change of a machine's members, as given on the command line.
#[derive(Debug, Default)]
pub struct Change {
    /// The members instead of those kept, when any is named: the lists replace them.
    pub set: Members,
    pub add: Members,
    pub remove: Members,
    /// Make the uplink the enrollment approver's or not; none keeps what it was.
    pub enroll_approver: Option<bool>,
}

impl Change {
    /// Whether it names the members in full.
    pub fn lists(&self) -> bool {
        !(self.set.ants.is_empty() && self.set.humans.is_empty() && self.set.webhooks.is_empty())
    }

    /// The members of `machine` after the change to `kept`, the members of its last run if known; `unknown` is the
    /// error when they are needed but not known.
    pub fn apply(
        self,
        machine: &Machine,
        kept: Option<Members>,
        unknown: impl FnOnce() -> anyhow::Error,
    ) -> Result<Members> {
        let lists = self.lists();
        let approver = kept.as_ref().is_some_and(|k| k.enroll_approver);
        let mut members = match (lists, kept) {
            (true, _) => Members { enroll_approver: approver, ..self.set },
            (false, Some(kept)) => kept,
            (false, None) => return Err(unknown()),
        };
        let add = [(self.add.ants, "ant"), (self.add.humans, "human"), (self.add.webhooks, "webhook")];
        for (n, (users, what)) in add.into_iter().enumerate() {
            for user in users {
                let lists = [&mut members.ants, &mut members.humans, &mut members.webhooks];
                if lists.iter().enumerate().any(|(other, list)| other != n && list.contains(&user)) {
                    bail!("{user} is a member of {machine} already, not as {what}: remove it first");
                }
                if !lists[n].contains(&user) {
                    lists[n].push(user);
                }
            }
        }
        let remove = [
            (&self.remove.ants, &mut members.ants, "ant"),
            (&self.remove.humans, &mut members.humans, "human"),
            (&self.remove.webhooks, &mut members.webhooks, "webhook"),
        ];
        for (users, list, what) in remove {
            for user in users {
                let Some(at) = list.iter().position(|u| u == user) else {
                    bail!("{user} is no {what} of {machine}");
                };
                list.remove(at);
            }
        }
        if let Some(approver) = self.enroll_approver {
            members.enroll_approver = approver;
        }
        Ok(members)
    }

    /// Take out of the ants to remove those `kept` does not hold that `left` tells left the members already, and
    /// return them: a retire order drops a retired ant from the members in the registry, and the hub's outbox
    /// sourcing, before the machine writes the uplink renewed without it, which may fail; and `ants auth leaf
    /// --remove-ant` renews the uplink without it before `ants leaf outbox --remove-ant` runs. Issuing the uplink, or
    /// admitting the members to the outbox, anew leaves them out all the same.
    pub fn left_already(&mut self, kept: Option<&Members>, left: impl Fn(&UserId) -> bool) -> Vec<UserId> {
        let (gone, remove) = std::mem::take(&mut self.remove.ants)
            .into_iter()
            .partition(|ant| !kept.is_some_and(|k| k.ants.contains(ant)) && left(ant));
        self.remove.ants = remove;
        gone
    }
}

/// The members `ants auth leaf` names on its command line.
#[derive(clap::Args, Debug)]
pub struct UplinkArgs {
    /// Ant served on the machine (repeatable). `human` counts as a human. With any of --ant, --human or --webhook,
    /// the members are exactly those named; without, those of the last run, which the team directory keeps (with
    /// the ants enrolled through the leaf node since).
    #[arg(long = "ant", value_name = "ANT")]
    ants: Vec<UserId>,
    /// Human running the CLI on the machine (repeatable), as issued with `ants auth user HUMAN --human`.
    #[arg(long = "human", value_name = "HUMAN")]
    humans: Vec<UserId>,
    /// Webhook `ants webhook serve` runs on the machine (repeatable): the uplink also allows what its credentials,
    /// <dir>/<WEBHOOK>.creds from `ants auth user WEBHOOK --webhook`, allow.
    #[arg(long = "webhook", value_name = "WEBHOOK")]
    webhooks: Vec<UserId>,
    /// Add an ant to the members of the last run (repeatable).
    #[arg(long = "add-ant", value_name = "ANT", conflicts_with_all = UPLINK_LISTS)]
    add_ants: Vec<UserId>,
    /// Add a human to the members of the last run (repeatable).
    #[arg(long = "add-human", value_name = "HUMAN", conflicts_with_all = UPLINK_LISTS)]
    add_humans: Vec<UserId>,
    /// Add a webhook to the members of the last run (repeatable).
    #[arg(long = "add-webhook", value_name = "WEBHOOK", conflicts_with_all = UPLINK_LISTS)]
    add_webhooks: Vec<UserId>,
    /// Remove an ant from the members of the last run (repeatable). An ant an earlier run had that left them since,
    /// as a retire order drops a retired ant, stays out.
    #[arg(long = "remove-ant", value_name = "ANT", conflicts_with_all = UPLINK_LISTS)]
    remove_ants: Vec<UserId>,
    /// Remove a human from the members of the last run (repeatable).
    #[arg(long = "remove-human", value_name = "HUMAN", conflicts_with_all = UPLINK_LISTS)]
    remove_humans: Vec<UserId>,
    /// Remove a webhook from the members of the last run (repeatable).
    #[arg(long = "remove-webhook", value_name = "WEBHOOK", conflicts_with_all = UPLINK_LISTS)]
    remove_webhooks: Vec<UserId>,
    /// The machine holds the account key and runs `ants enroll` through its leaf node: the uplink also carries
    /// every enrollment request, the answers to them and the admission of a new ant into the hub's stream. Later
    /// runs keep it until --no-enroll-approver.
    #[arg(long, conflicts_with = "no_enroll_approver")]
    enroll_approver: bool,
    /// The uplink no longer carries what `ants enroll` needs.
    #[arg(long)]
    no_enroll_approver: bool,
}

const UPLINK_LISTS: [&str; 3] = ["ants", "humans", "webhooks"];

impl From<UplinkArgs> for Change {
    fn from(args: UplinkArgs) -> Self {
        let members = |ants, humans, webhooks| Members { ants, humans, webhooks, enroll_approver: false };
        Self {
            set: members(args.ants, args.humans, args.webhooks),
            add: members(args.add_ants, args.add_humans, args.add_webhooks),
            remove: members(args.remove_ants, args.remove_humans, args.remove_webhooks),
            enroll_approver: (args.enroll_approver || args.no_enroll_approver).then_some(args.enroll_approver),
        }
    }
}

/// The members `ants leaf outbox` names on its command line.
#[derive(clap::Args, Debug)]
pub struct OutboxArgs {
    /// Ant served on the machine (repeatable). `human` counts as a human. With any of --ant or --human, the members
    /// are exactly those named; without, those the uplink credentials of the machine's leaf node carry.
    #[arg(long = "ant", value_name = "ANT")]
    ants: Vec<UserId>,
    /// Human running the CLI on the machine (repeatable).
    #[arg(long = "human", value_name = "HUMAN")]
    humans: Vec<UserId>,
    /// Add an ant to the members the uplink credentials carry (repeatable).
    #[arg(long = "add-ant", value_name = "ANT", conflicts_with_all = OUTBOX_LISTS)]
    add_ants: Vec<UserId>,
    /// Add a human to the members the uplink credentials carry (repeatable).
    #[arg(long = "add-human", value_name = "HUMAN", conflicts_with_all = OUTBOX_LISTS)]
    add_humans: Vec<UserId>,
    /// Remove an ant from the members the uplink credentials carry (repeatable). An ant they no longer carry that
    /// the hub still takes from the outbox for, or that the team directory kept among the members once, as a retired
    /// ant, stays out.
    #[arg(long = "remove-ant", value_name = "ANT", conflicts_with_all = OUTBOX_LISTS)]
    remove_ants: Vec<UserId>,
    /// Remove a human from the members the uplink credentials carry (repeatable).
    #[arg(long = "remove-human", value_name = "HUMAN", conflicts_with_all = OUTBOX_LISTS)]
    remove_humans: Vec<UserId>,
}

const OUTBOX_LISTS: [&str; 2] = ["ants", "humans"];

impl From<OutboxArgs> for Change {
    fn from(args: OutboxArgs) -> Self {
        let members = |ants, humans| Members { ants, humans, ..Members::default() };
        Self {
            set: members(args.ants, args.humans),
            add: members(args.add_ants, args.add_humans),
            remove: members(args.remove_ants, args.remove_humans),
            enroll_approver: None,
        }
    }
}

/// The latest line of the registry of `dir` for an uplink of `machine`'s leaf node.
fn latest(dir: &Path, machine: &Machine) -> Result<Option<Issued>> {
    let name = auth::leaf_user(machine);
    Ok(revoke::issued(dir)?.into_iter().rev().find(|i| i.name == name && i.kind == Kind::Leaf))
}

/// The members `ants auth leaf` last issued the uplink of `machine` for, from the team directory `dir`, with the ants
/// enrolled through the leaf node since; none when it issued none.
pub fn issued(dir: &Path, machine: &Machine) -> Result<Option<Members>> {
    Ok(latest(dir, machine)?.and_then(|line| line.members))
}

/// Keep `ant`, enrolled through the leaf node of `machine`, among its members in the registry of `dir`, with a later
/// line for the latest uplink key issued there.
pub fn enrolled(dir: &Path, machine: &Machine, ant: &UserId) -> Result<()> {
    let latest = latest(dir, machine)?;
    let mut line = latest.with_context(|| format!("{} keeps no uplink of {machine}", dir.display()))?;
    let members = line.members.get_or_insert_default();
    if !members.holds(ant) {
        members.ants.push(ant.clone());
    }
    revoke::record(dir, &line)
}

/// Drop `ant`, retired, from the members of `machine`'s leaf node in the registry of `dir`, with a later line for the
/// latest uplink key issued there; nothing to do when the members kept do not hold it. Returns whether they did.
pub fn retired(dir: &Path, machine: &Machine, ant: &UserId) -> Result<bool> {
    let Some(mut line) = latest(dir, machine)? else { return Ok(false) };
    let Some(members) = line.members.as_mut().filter(|m| m.ants.contains(ant)) else { return Ok(false) };
    members.ants.retain(|a| a != ant);
    revoke::record(dir, &line)?;
    Ok(true)
}

/// The ants any line of the registry of `dir` kept among the members of `machine`'s leaf node.
pub fn ever(dir: &Path, machine: &Machine) -> Result<Vec<UserId>> {
    let name = auth::leaf_user(machine);
    let mut ants: Vec<UserId> = Vec::new();
    for line in revoke::issued(dir)?.into_iter().filter(|i| i.name == name && i.kind == Kind::Leaf) {
        for ant in line.members.into_iter().flat_map(|m| m.ants) {
            if !ants.contains(&ant) {
                ants.push(ant);
            }
        }
    }
    Ok(ants)
}

/// The machines whose leaf node's members in the registry of `dir`, kept with the latest uplink key issued there,
/// hold the ant `ant`.
pub fn behind(dir: &Path, ant: &UserId) -> Result<Vec<Machine>> {
    let mut seen = Vec::new();
    let mut behind = Vec::new();
    for line in revoke::issued(dir)?.into_iter().rev().filter(|i| i.kind == Kind::Leaf) {
        if seen.contains(&line.name) {
            continue;
        }
        if line.members.as_ref().is_some_and(|m| m.ants.contains(ant)) {
            let machine = line.name.strip_prefix("leaf-").and_then(|m| m.parse().ok());
            behind.push(machine.with_context(|| format!("{} names no leaf node", line.name))?);
        }
        seen.push(line.name);
    }
    Ok(behind)
}

/// The ants and humans the uplink credentials of the leaf node configuration at `leaf_conf` carry, which must be
/// that of `machine`.
pub fn uplink(leaf_conf: &Path, machine: &Machine) -> Result<Members> {
    let (configured, creds) = leaf::read_uplink(leaf_conf)?;
    if configured != *machine {
        bail!("{} configures the leaf node of {configured}, not of {machine}", leaf_conf.display());
    }
    let finding = creds::check(&creds)?;
    let holder = finding.holder();
    let carried = Members::carried(finding, machine)
        .with_context(|| format!("{} are the credentials of {holder}, not the uplink of {machine}", creds.display()))?;
    Ok(Members { ants: carried.ants, humans: carried.humans, ..Members::default() })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn users(names: &[&str]) -> Vec<UserId> {
        names.iter().map(|n| p(n)).collect()
    }

    fn m1() -> Machine {
        "m1".parse().unwrap()
    }

    #[test]
    fn a_change_replaces_or_edits_the_kept_members() {
        let kept = Members {
            ants: users(&["dev-a", "dev-b"]),
            humans: users(&["alice"]),
            webhooks: users(&["github"]),
            enroll_approver: true,
        };
        let unknown = || anyhow::anyhow!("unknown");
        let keep = Change::default().apply(&m1(), Some(kept.clone()), unknown).unwrap();
        assert_eq!(keep, kept, "no lists: the kept members");
        assert_eq!(Change::default().apply(&m1(), None, unknown).unwrap_err().to_string(), "unknown");

        let set = Members { ants: users(&["dev-c"]), ..Members::default() };
        let replaced = Change { set: set.clone(), ..Change::default() }.apply(&m1(), Some(kept.clone()), unknown);
        assert_eq!(replaced.unwrap(), Members { enroll_approver: true, ..set.clone() }, "lists replace, approver kept");
        let fresh = Change { set: set.clone(), ..Change::default() }.apply(&m1(), None, unknown).unwrap();
        assert_eq!(fresh, set);

        let edit = Change {
            add: Members { ants: users(&["dev-c", "dev-a"]), humans: users(&["bob"]), ..Members::default() },
            remove: Members { ants: users(&["dev-b"]), webhooks: users(&["github"]), ..Members::default() },
            enroll_approver: Some(false),
            ..Change::default()
        };
        let edited = edit.apply(&m1(), Some(kept.clone()), unknown).unwrap();
        let want = Members { ants: users(&["dev-a", "dev-c"]), humans: users(&["alice", "bob"]), ..Members::default() };
        assert_eq!(edited, want);

        let absent =
            Change { remove: Members { humans: users(&["dev-a"]), ..Members::default() }, ..Change::default() };
        let err = absent.apply(&m1(), Some(kept.clone()), unknown).unwrap_err().to_string();
        assert_eq!(err, "dev-a is no human of m1");
        let twice = Change { add: Members { humans: users(&["dev-a"]), ..Members::default() }, ..Change::default() };
        let err = twice.apply(&m1(), Some(kept), unknown).unwrap_err().to_string();
        assert!(err.contains("dev-a is a member of m1 already"), "{err}");
    }

    #[test]
    fn the_team_directory_keeps_a_leaf_s_members_and_its_enrolled_ants() {
        let dir = ants_test_support::scratch_dir("leaf-members");
        let (team, out, m1) = (dir.join("auth"), dir.join("leaf-m1.creds"), m1());
        auth::init(&team).unwrap();
        assert_eq!(issued(&team, &m1).unwrap(), None, "nothing issued yet");

        let on_m1 = auth::members(&users(&["dev-a", "human"]), &users(&["alice"]));
        auth::issue_leaf(&team, &m1, &on_m1, &[], EnrollApprover::With, None, &out).unwrap();
        let kept = Members {
            ants: users(&["dev-a"]),
            humans: users(&["alice", "human"]),
            webhooks: Vec::new(),
            enroll_approver: true,
        };
        assert_eq!(issued(&team, &m1).unwrap(), Some(kept.clone()));
        assert_eq!(issued(&team, &"m2".parse().unwrap()).unwrap(), None);

        enrolled(&team, &m1, &p("dev-c")).unwrap();
        let with_c = Members { ants: users(&["dev-a", "dev-c"]), ..kept.clone() };
        assert_eq!(issued(&team, &m1).unwrap(), Some(with_c), "the enrolled ant joins");
        let listed = revoke::listing(&team, auth::now().unwrap()).unwrap();
        let uplinks: Vec<_> = listed.iter().filter(|l| l.issued.kind == Kind::Leaf).collect();
        assert!(uplinks.len() == 1 && !uplinks[0].reissued, "the same key, once: {listed:?}");
        assert!(enrolled(&team, &"m2".parse().unwrap(), &p("dev-c")).is_err(), "no uplink of m2 issued");

        assert!(retired(&team, &m1, &p("dev-c")).unwrap());
        assert_eq!(issued(&team, &m1).unwrap(), Some(kept.clone()), "the retired ant leaves");
        assert!(!retired(&team, &m1, &p("dev-c")).unwrap(), "gone already");
        assert!(!retired(&team, &"m2".parse().unwrap(), &p("dev-a")).unwrap(), "no uplink of m2 issued");

        let mut remove = Change {
            remove: Members { ants: users(&["dev-c", "dev-a", "dev-x"]), ..Members::default() },
            ..Change::default()
        };
        let ever = ever(&team, &m1).unwrap();
        assert_eq!(ever, users(&["dev-a", "dev-c"]));
        assert_eq!(remove.left_already(Some(&kept), |a| ever.contains(a)), users(&["dev-c"]), "only one that left");
        assert_eq!(remove.remove.ants, users(&["dev-a", "dev-x"]));
        let err = remove.apply(&m1, Some(kept.clone()), || anyhow::anyhow!("unknown")).unwrap_err().to_string();
        assert_eq!(err, "dev-x is no ant of m1", "an ant never a member");
        let mut again =
            Change { remove: Members { ants: users(&["dev-c"]), ..Members::default() }, ..Change::default() };
        assert_eq!(again.left_already(Some(&kept), |a| ever.contains(a)), users(&["dev-c"]));
        assert_eq!(again.apply(&m1, Some(kept.clone()), || anyhow::anyhow!("unknown")).unwrap(), kept);
    }
}
