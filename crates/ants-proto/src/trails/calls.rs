//! The arguments of a script's calls, as the trail engine hands them out in a call's `args`
//! ([`PendingCall::args`](crate::PendingCall::args), [`JournalEntry::args`](crate::JournalEntry::args)), and the label
//! a call's step carries.

use serde::Deserialize;
use serde_json::Value;

use super::{TrailFunction, check_tip_key};

/// The arguments of an agent() call.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AgentArgs {
    pub prompt: String,
    #[serde(default)]
    pub ant: Option<String>,
    #[serde(default)]
    pub capability: Option<String>,
    /// The team whose lead answers the step; `ant`, `capability` and `team` go one at a time.
    #[serde(default)]
    pub team: Option<String>,
    #[serde(default)]
    pub schema: Option<Value>,
    #[serde(default)]
    pub label: Option<String>,
    /// The tip key naming the tips the step carries first and a tip left answering it goes under.
    #[serde(default)]
    pub tips: Option<String>,
}

impl AgentArgs {
    /// The arguments in `args`, refused without a prompt, with more than one of an ant, a capability and a team, or
    /// with a tips key [`check_tip_key`] refuses.
    pub fn of(args: &Value) -> Result<Self, String> {
        let args: Self = serde_json::from_value(args.clone()).map_err(|e| format!("agent() takes a prompt: {e}"))?;
        let named = [args.ant.is_some(), args.capability.is_some(), args.team.is_some()];
        if named.into_iter().filter(|&named| named).count() > 1 {
            return Err("agent() takes at most one of ant=, capability= and team=".to_owned());
        }
        if let Some(key) = &args.tips {
            check_tip_key(key).map_err(|e| format!("agent() names no tip key as {key:?}: {e}"))?;
        }
        Ok(args)
    }
}

/// The arguments of a trail() call.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TrailArgs {
    pub name: String,
    #[serde(default)]
    pub args: Value,
}

impl TrailArgs {
    pub fn of(args: &Value) -> Result<Self, String> {
        serde_json::from_value(args.clone()).map_err(|e| format!("trail() takes a name: {e}"))
    }
}

/// The arguments of a goto() call.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct GotoArgs {
    #[serde(default)]
    pub trail: Option<String>,
    #[serde(default)]
    pub at: Option<String>,
    #[serde(default)]
    pub state: Value,
}

impl GotoArgs {
    pub fn of(args: &Value) -> Result<Self, String> {
        serde_json::from_value(args.clone()).map_err(|e| format!("goto() takes a trail and a waypoint by name: {e}"))
    }
}

impl TrailFunction {
    /// The label of a call of the function with `args`, as its step's `step` status updates and its
    /// [`TrailStepThread`](crate::TrailStepThread) carry it: an agent() call's `label`, `trail NAME` for a trail()
    /// call, read from those fields alone ([`AgentArgs::label`], [`TrailArgs::name`]); none for others.
    pub fn label(self, args: &Value) -> Option<String> {
        match self {
            Self::Agent => args.get("label")?.as_str().map(str::to_owned),
            Self::Trail => args.get("name")?.as_str().map(|name| format!("trail {name}")),
            Self::Human | Self::Goto | Self::Command | Self::ReadFile | Self::Bd | Self::Thread => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::AgentArgs;

    #[test]
    fn an_agent_call_s_tip_key_is_one_line() {
        let args = AgentArgs::of(&json!({"prompt": "fix it", "tips": "fix the build"})).unwrap();
        assert_eq!(args.tips.as_deref(), Some("fix the build"));
        for bad in ["", "two\nlines"] {
            let e = AgentArgs::of(&json!({"prompt": "fix it", "tips": bad})).expect_err("refused");
            assert!(e.starts_with("agent() names no tip key as "), "{e}");
        }
    }

    #[test]
    fn an_agent_call_names_at_most_one_of_an_ant_a_capability_and_a_team() {
        for one in [json!({"ant": "dev"}), json!({"capability": "rust"}), json!({"team": "reviewers"}), json!({})] {
            let mut args = one.clone();
            args["prompt"] = json!("fix it");
            assert!(AgentArgs::of(&args).is_ok(), "{one}");
        }
        for two in [
            json!({"ant": "dev", "capability": "rust"}),
            json!({"ant": "dev", "team": "reviewers"}),
            json!({"capability": "rust", "team": "reviewers"}),
        ] {
            let mut args = two.clone();
            args["prompt"] = json!("fix it");
            let e = AgentArgs::of(&args).expect_err("refused");
            assert_eq!(e, "agent() takes at most one of ant=, capability= and team=", "{two}");
        }
    }
}
