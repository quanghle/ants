//! Trails: scripts an ant, the conductor, runs to conduct other ants and humans, one step at a time. A step is a
//! message carrying its [`TrailStep`]; a saved trail is a [`SavedTrail`] in [`TRAILS_BUCKET`]; a run is kept in
//! [`TRAIL_RUNS_BUCKET`] under its conductor's keys: its [`TrailRun`], its [`TrailSession`], one [`JournalEntry`] per
//! answered call, the conductor's [`TrailRunIndex`] and a [`TrailStepThread`] per step's thread.
//!
//! A step goes on a thread of its own, which the conductor's [`TrailSession`] maps to the call
//! ([`PendingCall::thread`]). The reply resolving the call carries nothing of the step: the first message on that
//! thread from the step's recipient ([`PendingCall::to`]), or from the assistant of a human recipient answering for it,
//! to the conductor that carries no [`TrailStep`], no notice, no escalation and no run to rate answers it, as the
//! conductor's daemon tells. Only the conductor's own records say which threads are steps, so no other user can
//! answer a call by naming it. A step the conductor sends itself tells its reply apart the same way: the step carries
//! its [`TrailStep`], the reply none, and the conductor's invitation to rate a run started on the step's thread names
//! the run ([`Envelope::rating_of`]).

use std::time::Duration;

#[cfg(doc)]
use crate::{Envelope, TRAIL_RUNS_BUCKET, TRAILS_BUCKET};

mod calls;
mod control;
mod ratings;
mod runs;
mod saved;
mod saving;
mod steps;
mod tips;

pub use calls::{AgentArgs, GotoArgs, TrailArgs};
pub use control::{TrailAction, TrailOutcome, TrailReply, TrailRequest, TrailSource, TrailStart, WaitingStep};
pub use ratings::{
    Counts, FIND_TRAILS_LIMIT, Finder, Found, LATEST_RATINGS, MAX_FIND_TRAILS, MAX_RATING_TASK, MAX_RATING_WHY, Query,
    RATING_FULL_WEIGHT, RATING_MATCH_WORDS, RatingOutcome, RatingSummary, TrailRating, serves, stem,
};
pub use runs::{
    CallAnswer, JournalEntry, PendingCall, QueuedCall, RunLimit, RunState, TrailFunction, TrailLeg, TrailLimits,
    TrailParent, TrailRun, TrailRunIndex, TrailSession, TrailStepThread, Unresumable, leg_entry,
};
pub use saved::{SavedTrail, Saver};
pub use saving::{SAVE_TRAIL_REPEAT, SAVE_TRAIL_WAIT, SaveTrailOutcome, SaveTrailReply, SaveTrailRequest, TrailChange};
pub use steps::{SpendsFrom, StepBounds, StepGrant, TrailAnswering, TrailSpent, TrailStep};
pub use tips::{
    DEFAULT_TIPS_FOUND, MAX_LIVE_TIPS, MAX_TIP_REASON, MAX_TIP_TEXT, MAX_TIPS_BLOCK, MAX_TIPS_CARRIED, MAX_TIPS_FOUND,
    Ranked, Retirement, Similarity, TIP_HALF_LIFE, TIP_RELEVANCE, TIP_RELEVANT_WORDS, TIP_SIMILARITY, TIP_UNUSED,
    Tally, TipBoard, TipRetired, TipStatus, TipVerdict, TipVote, TrailTip, check_tip_key, script_sha256, search_words,
    tip_words,
};

/// Most bytes a trail's script has: a saved trail's and a run's.
pub const MAX_TRAIL_SCRIPT: usize = 256 * 1024;
/// Most bytes a run's arguments have, as JSON.
pub const MAX_TRAIL_ARGS: usize = 64 * 1024;
/// Most characters a saved trail's description has.
pub const MAX_TRAIL_DESCRIPTION: usize = 1000;
/// Most characters of a step's label ([`TrailStep::label`]), an ellipsis marking a cut aside.
pub const MAX_STEP_LABEL: usize = 200;
/// Most phases a saved trail names.
pub const MAX_TRAIL_PHASES: usize = 32;
/// Most waypoints a saved trail names.
pub const MAX_TRAIL_WAYPOINTS: usize = 32;
/// How long [`TRAIL_RUNS_BUCKET`] keeps a finished run, at least.
pub const TRAIL_RUN_RETENTION: Duration = Duration::from_hours(30 * 24);
/// How far ahead of a reader's clock a tip, a vote or a rating may be dated and still count: one dated further ahead
/// counts for nothing until then, so no date set ahead keeps a record from ageing.
pub const TRAIL_CLOCK_SKEW: chrono::TimeDelta = chrono::TimeDelta::minutes(1);
