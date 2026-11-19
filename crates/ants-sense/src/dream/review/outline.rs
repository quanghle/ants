//! The saved trail a sage's [`super::TRAIL`] finding suggests (requirement ants-x39hk.12.54): its name, what it does,
//! the specifics it takes as args with an example of each, and its phases. No script: the ant writes it.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use ants_proto::TrailName;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::clean;

/// The most characters of a suggested trail's name: a [`TrailName`]'s.
const MAX_NAME_CHARS: usize = 64;
/// The most characters of what a suggested trail does.
pub(in crate::dream) const MAX_DESCRIPTION_CHARS: usize = 200;
/// The most args of a suggested trail.
pub(in crate::dream) const MAX_ARGS: usize = 8;
/// The most characters of an arg's name.
pub(in crate::dream) const MAX_ARG_NAME_CHARS: usize = 40;
/// The most characters of an arg's example, or of a phase.
pub(in crate::dream) const MAX_PART_CHARS: usize = 120;
/// The fewest phases of a suggested trail: one phase is no multi-step approach.
pub(in crate::dream) const MIN_PHASES: usize = 2;
/// The most phases of a suggested trail.
pub(in crate::dream) const MAX_PHASES: usize = 8;

/// The bounds [`Outline::read`] holds an outline to, as a sage's packet tells them.
pub(in crate::dream) fn bounds() -> String {
    format!(
        "a name made 1 to {MAX_NAME_CHARS} of a-z, 0-9 and '-'; a description of at most {MAX_DESCRIPTION_CHARS} \
         characters; at most {MAX_ARGS} args, each named by 1 to {MAX_ARG_NAME_CHARS} of a-z, 0-9 and '_' (other \
         characters made '_') with a string, number or boolean example of at most {MAX_PART_CHARS} characters; \
         {MIN_PHASES} to {MAX_PHASES} phases, each of at most {MAX_PART_CHARS} characters; longer text is cut, and an \
         outline with more args, fewer or more phases, or args whose names clash, is refused with its finding"
    )
}

/// A saved trail, as a sage's finding suggests it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outline {
    pub name: TrailName,
    pub description: String,
    /// Its args, each with an example taken from the threads the finding cites.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub args: BTreeMap<String, String>,
    pub phases: Vec<String>,
}

impl Outline {
    /// The outline `given` as a sage's answer holds it, `{"name", "description", "args": {"key": "example"},
    /// "phases": ["…"]}`, the name made a [`TrailName`] and each arg's an arg name, an example given as a number or a
    /// boolean taken as its text, and text past its bounds cut ([`bounds`]): none when it holds no name, description or
    /// phases, more than [`MAX_ARGS`] args or args whose names clash, or phases fewer than [`MIN_PHASES`] or more than
    /// [`MAX_PHASES`], so it is kept or refused whole.
    pub fn read(given: &Value) -> Option<Self> {
        let object = given.as_object()?;
        let name = name(object.get("name")?.as_str()?)?;
        let description = clean(object.get("description")?.as_str()?, MAX_DESCRIPTION_CHARS)?;
        let args = match object.get("args") {
            None | Some(Value::Null) => BTreeMap::new(),
            Some(Value::Object(given)) if given.len() <= MAX_ARGS => {
                let mut args = BTreeMap::new();
                for (key, example) in given {
                    let example = match example {
                        Value::String(text) => clean(text, MAX_PART_CHARS)?,
                        Value::Number(_) | Value::Bool(_) => clean(&example.to_string(), MAX_PART_CHARS)?,
                        _ => return None,
                    };
                    if args.insert(arg_name(key)?, example).is_some() {
                        return None;
                    }
                }
                args
            }
            Some(_) => return None,
        };
        let phases = object.get("phases")?.as_array()?;
        if !(MIN_PHASES..=MAX_PHASES).contains(&phases.len()) {
            return None;
        }
        let phases = phases.iter().map(|phase| clean(phase.as_str()?, MAX_PART_CHARS)).collect::<Option<_>>()?;
        Some(Self { name, description, args, phases })
    }

    /// The outline on one line: `<name>: <description>; args key (example), …; phases a; b; c`.
    pub fn line(&self) -> String {
        let mut line = format!("{}: {}", self.name, self.description);
        if !self.args.is_empty() {
            let args: Vec<_> = self.args.iter().map(|(key, example)| format!("{key} ({example})")).collect();
            let _ = write!(line, "; args {}", args.join(", "));
        }
        let _ = write!(line, "; phases {}", self.phases.join("; "));
        line
    }
}

/// The sage's `given` name as a [`TrailName`]: lowercased, each run of other characters than `a-z` and `0-9` made a
/// `-`, its ends trimmed of `-`, cut to [`MAX_NAME_CHARS`].
fn name(given: &str) -> Option<TrailName> {
    normalized(given, '-', MAX_NAME_CHARS).parse().ok()
}

/// The sage's `key` as an arg's name: lowercased, each run of other characters than `a-z`, `0-9` and `_` made a `_`,
/// its ends trimmed of `_`, cut to [`MAX_ARG_NAME_CHARS`]; none when nothing is left.
fn arg_name(key: &str) -> Option<String> {
    Some(normalized(key, '_', MAX_ARG_NAME_CHARS)).filter(|name| !name.is_empty())
}

/// `given` lowercased, each run of other characters than `a-z`, `0-9` and `joiner` made one `joiner`, its ends trimmed
/// of it, cut to `max` characters.
fn normalized(given: &str, joiner: char, max: usize) -> String {
    let mut normalized = String::new();
    for c in given.to_lowercase().chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() || c == joiner {
            normalized.push(c);
        } else if !normalized.ends_with(joiner) {
            normalized.push(joiner);
        }
    }
    let mut normalized = normalized.trim_matches(joiner).to_owned();
    normalized.truncate(max);
    normalized.trim_end_matches(joiner).to_owned()
}
