//! The limits a run's start asks for where no human gives `ants trail run`'s limit flags: `run_trail`'s `limits` and a
//! trail schedule's ([`crate::schedule`]), each the flags' counterpart.

use ants_proto::TrailLimits;
use rmcp::schemars;
use serde::Deserialize;

/// What a start asks a run may spend: each limit given replaces its default ([`TrailLimits::DEFAULT`]) and is at most
/// its maximum ([`TrailLimits::MAX`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AskedLimits {
    /// Steps the run sends in all: 50 by default, at most 500.
    pub steps: Option<u32>,
    /// Steps waiting on an answer at once: 4 by default, at most 16.
    pub at_once: Option<u32>,
    /// Turns the run's steps spend in all, as ants' messages spend them: 200 by default, at most 2000.
    pub turns: Option<u32>,
    /// Time from the run's start to its end, stopped time included, such as `2h` or `3d`: 24h by default, at most 7d.
    pub wall: Option<String>,
}

impl AskedLimits {
    /// The run's limits: the defaults, with those asked for instead; refused when one is 0 or over its maximum.
    pub fn limits(&self) -> Result<TrailLimits, String> {
        let wall = match &self.wall {
            Some(wall) => Some(ants_proto::duration::parse(wall.trim()).map_err(|e| format!("wall: {e}"))?.as_secs()),
            None => None,
        };
        let max = TrailLimits::MAX;
        let asked = [
            ("steps", self.steps.map(u64::from), u64::from(max.steps), max.steps.to_string()),
            ("at_once", self.at_once.map(u64::from), u64::from(max.concurrency), max.concurrency.to_string()),
            ("turns", self.turns.map(u64::from), u64::from(max.turns), max.turns.to_string()),
            ("wall", wall, max.wall_secs, ants_proto::duration::format(max.wall_time())),
        ];
        for (limit, value, most, shown) in asked {
            match value {
                Some(0) => return Err(format!("{limit} must be more than 0")),
                Some(value) if value > most => return Err(format!("{limit} is over the most a run may take, {shown}")),
                _ => {}
            }
        }
        let base = TrailLimits::DEFAULT;
        Ok(TrailLimits {
            steps: self.steps.unwrap_or(base.steps),
            concurrency: self.at_once.unwrap_or(base.concurrency),
            turns: self.turns.unwrap_or(base.turns),
            wall_secs: wall.unwrap_or(base.wall_secs),
            ..base
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_limit_asked_replaces_its_default_up_to_its_maximum() {
        assert_eq!(AskedLimits::default().limits(), Ok(TrailLimits::DEFAULT));
        let asked = AskedLimits { steps: Some(500), at_once: Some(8), turns: None, wall: Some("7d".to_owned()) };
        let limits = TrailLimits { steps: 500, concurrency: 8, wall_secs: 7 * 24 * 3600, ..TrailLimits::DEFAULT };
        assert_eq!(asked.limits(), Ok(limits));
        for (asked, why) in [
            (AskedLimits { steps: Some(501), ..AskedLimits::default() }, "steps is over the most a run may take, 500"),
            (
                AskedLimits { at_once: Some(17), ..AskedLimits::default() },
                "at_once is over the most a run may take, 16",
            ),
            (AskedLimits { turns: Some(0), ..AskedLimits::default() }, "turns must be more than 0"),
            (
                AskedLimits { wall: Some("8d".to_owned()), ..AskedLimits::default() },
                "wall is over the most a run may take, 7d",
            ),
            (AskedLimits { wall: Some("soon".to_owned()), ..AskedLimits::default() }, "wall: "),
        ] {
            let refused = asked.limits().unwrap_err();
            assert!(refused.starts_with(why), "{asked:?}: {refused}");
        }
    }
}
