//! The ant's entry in the colony, kept alive by heartbeats.

mod pick;
mod registration;
mod workload;

use ants_proto::{ANTS_VERSION, Activity, AntModel, ColonyEntry, PERMISSIONS_VERSION};
use chrono::Utc;

use crate::daemon::Config;

pub(crate) use pick::pick;
pub use pick::{STALE_AFTER, has_capability, is_live, list_entries, naming_as_sage};
pub use registration::{
    LOOK_AGAIN, Registration, hold, leave_unserved, may_start, register, stand_unserved, wait_for_turn,
};
pub use workload::{Working, Workload};

/// The colony entry of the ant of `config` on this machine, started now, naming the machine as `config` does, else by
/// its host name.
pub async fn entry(config: &Config) -> ColonyEntry {
    let now = Utc::now();
    let (presence, harness) = (&config.presence, &config.harness);
    let machine = match &presence.machine {
        Some(machine) => machine.clone(),
        None => machine().await,
    };
    ColonyEntry {
        ant: config.ant.clone(),
        machine,
        instance: uuid::Uuid::new_v4().to_string(),
        harness: harness.harness.to_string(),
        agent: None,
        capabilities: presence.capabilities.clone(),
        whoami: presence.whoami.clone(),
        profile: None,
        sense_unkept: None,
        workdir: Some(harness.workdir.display().to_string()),
        started_at: now,
        heartbeat_at: now,
        activity: Activity::Idle,
        handling: 0,
        harnesses: 0,
        escalate_to: config.policy.escalation.escalate_to.clone(),
        sage: config.policy.sage.clone(),
        version: ANTS_VERSION.to_owned(),
        perms: PERMISSIONS_VERSION,
        signed_out: None,
        limited: None,
        failing: None,
        unserved: None,
        fallback: None,
        memory_low: None,
        paused: config.pause.get(),
        drift: config.drift.get(),
        mcp: harness.harness.reports_mcp(),
        model: AntModel::of(
            harness.model_settings.model.clone(),
            harness.model_settings.reasoning_effort.clone(),
            harness.model_settings.context.clone(),
        ),
    }
}

/// Host name of this machine, as `hostname` prints it.
pub async fn machine() -> String {
    let output = tokio::process::Command::new("hostname").output().await;
    match output {
        Ok(out) if out.status.success() => {
            let name = String::from_utf8_lossy(&out.stdout).trim().to_owned();
            if !name.is_empty() {
                return name;
            }
        }
        _ => {}
    }
    std::env::var("HOSTNAME").or_else(|_| std::env::var("COMPUTERNAME")).unwrap_or_else(|_| "unknown".to_owned())
}

#[cfg(test)]
mod tests {
    use ants_proto::UserId;

    use super::*;

    pub(super) fn entry(ant: &str, capabilities: &[&str]) -> ColonyEntry {
        entry_on(ant, "m", capabilities)
    }

    pub(super) fn entry_on(ant: &str, machine: &str, capabilities: &[&str]) -> ColonyEntry {
        let now = Utc::now();
        ColonyEntry {
            ant: ant.parse().unwrap(),
            machine: machine.to_owned(),
            instance: format!("{ant}@{machine}"),
            harness: "copilot".to_owned(),
            agent: None,
            capabilities: capabilities.iter().map(|c| (*c).to_owned()).collect(),
            whoami: None,
            profile: None,
            sense_unkept: None,
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: Activity::Idle,
            handling: 0,
            harnesses: 0,
            escalate_to: Vec::new(),
            sage: None,
            version: ANTS_VERSION.to_owned(),
            perms: PERMISSIONS_VERSION,
            signed_out: None,
            limited: None,
            failing: None,
            unserved: None,
            fallback: None,
            memory_low: None,
            model: None,
            paused: None,
            drift: Vec::new(),
            mcp: false,
        }
    }

    #[tokio::test]
    async fn the_entry_names_the_sage_the_ant_s_config_and_shared_settings_resolve() {
        use crate::config::{Ant, Sage};
        use crate::profile::shared::Roles;

        let harnesses = crate::harness::Harnesses::builtin();
        let sage_of = async |ant: Ant| super::entry(&ant.into_config("nats://unused", &harnesses).unwrap()).await.sage;
        let mut dev_a = Ant::new("dev-a".parse().unwrap(), "/".into());
        dev_a.harness = Some("copilot".to_owned());
        dev_a.machine = Some("m1".to_owned());
        assert_eq!(sage_of(dev_a.clone()).await, Some(UserId::sage()), "the default");
        let shared = Roles { sage: Some(Sage::Ant("lead".parse().unwrap())), ..Roles::default() };
        assert_eq!(sage_of(shared.overlay(dev_a.clone())).await, Some("lead".parse().unwrap()), "shared settings'");
        dev_a.sage = Some(Sage::None);
        assert_eq!(sage_of(dev_a.clone()).await, None, "reviews off");
        dev_a.sage = None;
        dev_a.is_sage = true;
        assert_eq!(sage_of(dev_a).await, None, "a sage has none");
    }
}
