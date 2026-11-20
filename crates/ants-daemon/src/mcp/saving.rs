//! The trails ants save for the colony ([`crate::trail::Trails::save`]): `save_trail` saves a script, or a finished
//! inline run's, under a name as the ant's own, and `remove_trail` removes one the ant saved. Both are refused on a
//! thread a webhook or schedule started, which a step of a run such a thread started is too. A save on a dream's thread
//! or in answer to a dream finding is checked against the ant's dreams too ([`crate::sense::Dreams::ground`]).

use std::fmt::Write as _;

use ants_proto::{Envelope, RunState, SavedTrail, ThreadId, TrailName};
use chrono::Utc;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use super::Hub;
use super::tools::{Tools, text};
use crate::sense::Grounded;
use crate::trail::{SaveFrom, SaveOutcome, Trails, age, from_run};

/// Why saving and removing are refused when the ant conducts no runs.
const NO_TRAILS: &str = "this ant saves no trails: the bus keeps no trail runs";
/// Why saving and removing are refused on an untrusted thread.
const UNTRUSTED_SAVE: &str = "this thread is untrusted: a webhook or schedule started it, or the trail run whose step \
                              it answers; trails are saved and removed only from trusted threads";

#[derive(Deserialize, schemars::JsonSchema)]
struct SaveTrail {
    /// The name to save the trail under: 1-64 of a-z, 0-9 and '-', not a run's id.
    name: String,
    /// A trail script starting with its `meta` literal; give it or `run`.
    #[serde(default)]
    script: Option<String>,
    /// The id run_trail returned for a finished run of an inline script this ant conducted, whose script to save,
    /// recorded as its source; give it or `script`.
    #[serde(default)]
    run: Option<String>,
    /// Replace the trail this ant saved before under `name`; without it, that name is refused.
    #[serde(default)]
    replace: bool,
    /// On a dream thread: the dream finding this save answers, such as 3.1.
    #[serde(default)]
    finding: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RemoveTrail {
    /// The name of a trail this ant saved for the colony.
    name: String,
}

impl Hub {
    /// Save the trail `args` gives for the colony as the ant's, from `thread`.
    async fn save_trail(&self, thread: &ThreadId, args: SaveTrail) -> Result<String, String> {
        let trails = self.trails.get().ok_or(NO_TRAILS)?;
        let current = self
            .handling(thread)
            .ok_or("no message is being handled on this thread; trails are saved only while one is")?;
        self.refuse_saving(thread, &current).await?;
        let name = trail_name(&args.name)?;
        let finding = args.finding.as_deref().map(str::trim).filter(|finding| !finding.is_empty());
        let from = match (args.script, args.run) {
            (Some(script), None) => SaveFrom::Script(script),
            (None, Some(run)) => SaveFrom::Run(run.trim().parse().map_err(|_| format!("{run:?} is no run id"))?),
            (Some(_), Some(_)) => return Err("give `script` or `run`, not both".to_owned()),
            (None, None) => return Err("give `script` or `run`".to_owned()),
        };
        let ask = crate::trail::SaveTrail { name: name.clone(), from, replace: args.replace };
        let prepared = trails.prepare_save(ask).await?;
        // Checked and stored apart, so that what grounds a save can be checked between the two.
        let grounded = self.ground_save(thread, finding).await?;
        if let (Some(task), Some(finding)) = (grounded.as_ref().and_then(|g| g.suggested.as_deref()), finding) {
            refuse_served(trails, task, thread, &name, finding).await?;
        }
        let outcome = trails.commit_save(prepared).await?;
        tracing::info!(%thread, trail = %outcome.trail.name, finding, "trail saved from a tool call");
        let mut text = saved_text(&outcome);
        if let Some(dream) = match grounded {
            Some(grounded) => grounded.saved(&name).await,
            None => None,
        } {
            let _ = write!(text, "\n{dream}");
        }
        Ok(text)
    }

    /// Check a save on `thread`, in answer to `finding` if it names one, against the ant's dreams, which only sense
    /// keeps ([`crate::sense::Dreams::ground`]): without them, a save naming a finding is refused and one naming none
    /// passes.
    async fn ground_save(&self, thread: &ThreadId, finding: Option<&str>) -> Result<Option<Grounded>, String> {
        let Some(dreams) = self.dreams.get() else {
            return match finding {
                Some(finding) => Err(format!(
                    "No dream has a finding {finding:?}: dreams are sense's, and sense is not this ant's provider."
                )),
                None => Ok(None),
            };
        };
        let context = self.call_context(thread).await;
        dreams.ground(&self.ant, context, finding).await.map(Some).map_err(|refusal| refusal.text)
    }

    /// Remove the colony's trail `name`, which the ant saved, from `thread`.
    async fn remove_trail(&self, thread: &ThreadId, name: &str) -> Result<String, String> {
        let trails = self.trails.get().ok_or(NO_TRAILS)?;
        let current = self
            .handling(thread)
            .ok_or("no message is being handled on this thread; trails are removed only while one is")?;
        self.refuse_saving(thread, &current).await?;
        let removed = trails.remove_saved(&trail_name(name)?).await?;
        tracing::info!(%thread, trail = %removed.name, "trail removed from a tool call");
        Ok(format!(
            "Removed trail {}, which this ant saved {} ago{}. Its tips stay: a trail saved later under that name \
             inherits them, marked of an older script. Runs already started keep the script they started with.",
            removed.name,
            age(removed.saved_at, Utc::now()),
            from_run(&removed)
        ))
    }

    /// The one gate of saving and removing trails: refused unless `thread`, handling `current`, is trusted.
    async fn refuse_saving(&self, thread: &ThreadId, current: &Envelope) -> Result<(), String> {
        if self.trusted(thread, current).await? { Ok(()) } else { Err(UNTRUSTED_SAVE.to_owned()) }
    }
}

/// Refuse saving trail `name` for `finding`, a sage's trail finding suggesting a trail for `task`, when a saved trail
/// other than `name` already serves such tasks ([`Trails::serving`]): the finding is then rejected naming that trail.
/// Ratings that cannot be read refuse nothing.
async fn refuse_served(
    trails: &Trails,
    task: &str,
    thread: &ThreadId,
    name: &TrailName,
    finding: &str,
) -> Result<(), String> {
    let serving = match trails.serving(task, thread).await {
        Ok(serving) => serving,
        Err(e) => {
            tracing::warn!(%thread, finding, error = e, "finding the saved trails serving a trail finding's task");
            return Ok(());
        }
    };
    match serving.iter().find(|found| found.name != *name) {
        Some(found) => Err(format!(
            "Refused: saved trail {other} already serves such tasks ({counts}): reject finding {finding} with \
             sense_dream naming {other}, and run it with run_trail(name: \"{other}\").",
            other = found.name,
            counts = found.matching.counts
        )),
        None => Ok(()),
    }
}

fn trail_name(name: &str) -> Result<TrailName, String> {
    name.trim().parse().map_err(|_| format!("{name:?} is no trail name: 1-64 of a-z, 0-9 and '-'"))
}

/// What `save_trail` tells of a trail it saved.
fn saved_text(outcome: &SaveOutcome) -> String {
    let SaveOutcome { trail, replaced, run_state } = outcome;
    let failed = match run_state {
        Some(RunState::Failed { error, .. }) => format!(" (which failed: {error})"),
        _ => String::new(),
    };
    let name = &trail.name;
    let rate = match &trail.saved_from {
        Some(run) => format!(
            " A rate_trail of run {run} made from now on rates trail {name} too: rate the run again, replacing its \
             earlier rating, for the saved trail to start rated."
        ),
        None => String::new(),
    };
    let saved = match replaced {
        None => format!(
            "Saved trail {name} for the colony as this ant's{}{failed}. Every ant runs it by name with run_trail; this \
             ant replaces it only with `replace` or removes it with remove_trail, and humans replace or remove any \
             trail.",
            from_run(trail)
        ),
        Some(before) => format!(
            "Replaced trail {name}, which this ant saved {} ago{}, with {}{failed}. Runs already started keep the script \
             they started with.",
            age(before.saved_at, trail.saved_at),
            from_run(before),
            source(trail)
        ),
    };
    saved + &rate
}

/// Where the script of `trail` came from.
fn source(trail: &SavedTrail) -> String {
    match &trail.saved_from {
        Some(run) => format!("the script of run {run}"),
        None => "the script given".to_owned(),
    }
}

#[tool_router(router = saving_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "Save a trail for the colony under `name`, as this ant's: a `script`, or the script of a \
                       finished `run` of an inline script this ant conducted, recorded as its source. The script is \
                       checked as `ants trail save` checks a file, not run; every ant then runs it by name with \
                       run_trail. A name a human or another ant saved is refused: save a changed copy under a new \
                       name. A trail this ant saved is replaced only with `replace`; runs already started keep their \
                       script. Refused on a thread a webhook or schedule started. Save a script that worked and whose \
                       steps recur, with its specifics taken as `args`. On a dream thread, `finding` names the dream \
                       finding the save answers, such as a sage's trail finding: refused when it rests on untrusted \
                       text, when the dream covers untrusted text and it is no trail finding, or when a saved trail \
                       already serves the task it suggests; a save there counts as one of the dream's writes.")]
    async fn save_trail(
        &self,
        Parameters(args): Parameters<SaveTrail>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.save_trail(&thread, args).await))
    }

    #[tool(
        description = "Remove a trail this ant saved for the colony, by `name`: run_trail no longer finds it there. A \
                       trail a human or another ant saved is refused: ask a human. Its tips stay, for a trail saved \
                       later under that name to inherit, marked of an older script; runs already started keep their \
                       script. Refused on a thread a webhook or schedule started."
    )]
    async fn remove_trail(
        &self,
        Parameters(args): Parameters<RemoveTrail>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.remove_trail(&thread, &args.name).await))
    }
}
