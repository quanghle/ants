//! Saved trails, in [`TRAILS_BUCKET`].

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{MAX_TRAIL_DESCRIPTION, MAX_TRAIL_PHASES, MAX_TRAIL_SCRIPT, MAX_TRAIL_WAYPOINTS};
use crate::questions::breaks_line;
use crate::{Error, MAX_TITLE, RunId, TrailName, UserId};
#[cfg(doc)]
use crate::{SaveTrailRequest, TRAIL_RUNS_BUCKET, TRAILS_BUCKET};

/// Who saved a trail: a human, with `ants trail save`, or an ant, with its `save_trail` tool.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Saver {
    Human { user: UserId },
    Ant { user: UserId },
}

impl Saver {
    pub fn user(&self) -> &UserId {
        match self {
            Self::Human { user } | Self::Ant { user } => user,
        }
    }
}

impl std::fmt::Display for Saver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Human { user } => write!(f, "{user}"),
            Self::Ant { user } => write!(f, "ant {user}"),
        }
    }
}

/// A saved trail in [`TRAILS_BUCKET`], under the key of its name. Humans put and delete any. An ant saves new ones and
/// replaces or removes only those it saved ([`Saver`]), through `ants signer` ([`SaveTrailRequest`]), as ant
/// credentials write none. Every ant and human reads them. Readers should treat unknown fields as additions, not
/// errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedTrail {
    pub name: TrailName,
    #[serde(default)]
    pub description: String,
    /// The phases the script reports, in order, for showing a run's progress.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phases: Vec<String>,
    /// The top-level async defs a run enters the trail at, the first its start; none for a trail entered only at its
    /// start.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waypoints: Vec<String>,
    /// The script's source text.
    pub script: String,
    /// The human or ant who saved it.
    pub saved_by: Saver,
    /// The run whose inline script an ant saved, conducted by that ant, so its record is `<saver>.<run>` in
    /// [`TRAIL_RUNS_BUCKET`]; none for a script saved as given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub saved_from: Option<RunId>,
    pub saved_at: DateTime<Utc>,
}

impl SavedTrail {
    pub fn key(&self) -> &str {
        self.name.as_str()
    }

    /// Check what a reader takes from the bucket: the trail is stored under its own name and within the bounds of
    /// [`SavedTrail::check`].
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key != self.key() {
            return Err(Error::ForeignTrail { name: self.name.clone(), at: key.to_owned() });
        }
        self.check()
    }

    /// Check the trail's bounds: a name not reading as a run's id, a script of 1 to [`MAX_TRAIL_SCRIPT`] bytes, a
    /// description of one line of at most [`MAX_TRAIL_DESCRIPTION`] characters, at most [`MAX_TRAIL_PHASES`] phases,
    /// each one line of 1 to [`MAX_TITLE`] characters (no control characters in either), and at most
    /// [`MAX_TRAIL_WAYPOINTS`] waypoints, each a name of 1 to [`MAX_TITLE`] letters, digits and `_`.
    pub fn check(&self) -> Result<(), Error> {
        let invalid = |why: String| Err(Error::InvalidTrail { name: self.name.clone(), why });
        if Uuid::try_parse(self.name.as_str()).is_ok() {
            return invalid("a trail's name reads as a run's id, which `ants trail show` takes for a run".to_owned());
        }
        if self.script.trim().is_empty() || self.script.len() > MAX_TRAIL_SCRIPT {
            return invalid(format!("a script has 1-{MAX_TRAIL_SCRIPT} bytes"));
        }
        if self.description.chars().count() > MAX_TRAIL_DESCRIPTION || self.description.chars().any(breaks_line) {
            return invalid(format!("a description is one line of at most {MAX_TRAIL_DESCRIPTION} characters"));
        }
        if self.phases.len() > MAX_TRAIL_PHASES {
            return invalid(format!("a trail has at most {MAX_TRAIL_PHASES} phases"));
        }
        let bad_phase = |p: &str| p.trim().is_empty() || p.chars().any(breaks_line) || p.chars().count() > MAX_TITLE;
        if self.phases.iter().any(|p| bad_phase(p)) {
            return invalid(format!("a phase is one line of 1-{MAX_TITLE} characters"));
        }
        if self.waypoints.len() > MAX_TRAIL_WAYPOINTS {
            return invalid(format!("a trail has at most {MAX_TRAIL_WAYPOINTS} waypoints"));
        }
        let bad_waypoint =
            |w: &str| w.is_empty() || w.len() > MAX_TITLE || !w.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if self.waypoints.iter().any(|w| bad_waypoint(w)) {
            return invalid(format!("a waypoint is a name of 1-{MAX_TITLE} letters, digits and '_'"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::p;

    fn saved() -> SavedTrail {
        SavedTrail {
            name: "release".parse().unwrap(),
            description: String::new(),
            phases: vec!["build".to_owned()],
            waypoints: Vec::new(),
            script: "agent('dev-a', 'build')".to_owned(),
            saved_by: Saver::Human { user: p("alice") },
            saved_from: None,
            saved_at: Utc::now(),
        }
    }

    #[test]
    fn a_saved_trail_is_checked_against_its_key_and_bounds() {
        let trail = saved();
        assert_eq!(trail.check_key("release"), Ok(()));
        assert!(matches!(trail.check_key("other"), Err(Error::ForeignTrail { .. })));
        let bad = [
            SavedTrail { script: " ".to_owned(), ..saved() },
            SavedTrail { script: "x".repeat(MAX_TRAIL_SCRIPT + 1), ..saved() },
            SavedTrail { description: "x".repeat(MAX_TRAIL_DESCRIPTION + 1), ..saved() },
            SavedTrail { description: "a\nb".to_owned(), ..saved() },
            SavedTrail { description: "a\u{1b}[8m".to_owned(), ..saved() },
            SavedTrail { phases: vec!["a\nb".to_owned()], ..saved() },
            SavedTrail { phases: vec!["a\u{1b}]0;title\u{7}".to_owned()], ..saved() },
            SavedTrail { phases: vec!["p".to_owned(); MAX_TRAIL_PHASES + 1], ..saved() },
            SavedTrail { waypoints: vec!["a b".to_owned()], ..saved() },
            SavedTrail { waypoints: vec!["w".to_owned(); MAX_TRAIL_WAYPOINTS + 1], ..saved() },
        ];
        for trail in bad {
            assert!(matches!(trail.check(), Err(Error::InvalidTrail { .. })), "{trail:?}");
        }
    }

    #[test]
    fn a_trail_name_reading_as_a_run_id_fails_the_check() {
        let trail = SavedTrail { name: RunId::generate().as_str().parse().unwrap(), ..saved() };
        let error = trail.check().unwrap_err().to_string();
        assert!(error.contains("reads as a run's id"), "{error}");
        let saver = Saver::Ant { user: p("dev-a") };
        assert_eq!((saver.to_string(), saver.user()), ("ant dev-a".to_owned(), &p("dev-a")));
        assert_eq!(Saver::Human { user: p("alice") }.to_string(), "alice");
    }
}
