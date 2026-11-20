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
    #[serde(default)]
    pub schema: Option<Value>,
    #[serde(default)]
    pub label: Option<String>,
    /// The tip key naming the tips the step carries first and a tip left answering it goes under.
    #[serde(default)]
    pub tips: Option<String>,
}

impl AgentArgs {
    /// The arguments in `args`, refused without a prompt or with a tips key [`check_tip_key`] refuses.
    pub fn of(args: &Value) -> Result<Self, String> {
        let args: Self = serde_json::from_value(args.clone()).map_err(|e| format!("agent() takes a prompt: {e}"))?;
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
            Self::Human | Self::Goto => None,
        }
    }
}
