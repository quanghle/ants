//! Tool requests in words: the line of one in the pending list, the whole of one, the commands answering it and the
//! allow rule suggested.

use std::fmt::Write as _;

use ants_client::text::spelling;
use ants_proto::{ApprovalRequest, Verdict};
use chrono::{DateTime, Utc};

use super::State;
use super::answer::Suggestion;
use super::assisted::deciders;
use super::requests::qualified;
use crate::text::left;

fn options(request: &ApprovalRequest) -> String {
    request.options.iter().map(|v| spelling(*v)).collect::<Vec<_>>().join("|")
}

/// The command giving `verdict` on `request`.
fn command(request: &ApprovalRequest, verdict: Verdict) -> String {
    let id = qualified(request);
    match verdict {
        Verdict::AllowOnce => format!("ants tools approve {id}"),
        Verdict::AllowAlways => format!("ants tools approve {id} --always"),
        Verdict::Reject => format!("ants tools reject {id}"),
    }
}

/// The commands answering `request` with the verdicts it offers, separated by ` | `.
pub(super) fn commands(request: &ApprovalRequest) -> String {
    request.options.iter().map(|v| command(request, *v)).collect::<Vec<_>>().join(" | ")
}

/// The first line of `text`, cut to `max` characters.
pub(super) fn excerpt(text: &str, max: usize) -> String {
    let line = text.lines().next().unwrap_or("");
    let mut out: String = line.chars().take(max).collect();
    if out.len() < text.len() {
        out.push('…');
    }
    out
}

/// One line of the pending list: `<ant>/<id>`, thread, tool, files, options and time left.
pub(crate) fn row(request: &ApprovalRequest, now: DateTime<Utc>) -> String {
    let files = if request.tool.locations.is_empty() { "-".to_owned() } else { request.tool.locations.join(",") };
    format!(
        "{}\t{}\t{}: {}\t{}\t{}\t{} left",
        qualified(request),
        request.thread_id,
        request.tool.kind,
        excerpt(&request.tool.title, 80),
        files,
        options(request),
        left(request.expires_at, now)
    )
}

/// The whole request, with its tool input and how to answer it.
pub(crate) fn details(request: &ApprovalRequest, state: &State, now: DateTime<Utc>) -> String {
    let mut out = format!(
        "request  {}\nant      {}\nthread   {}\ntool     {}: {}\n",
        request.id, request.ant, request.thread_id, request.tool.kind, request.tool.title
    );
    for location in &request.tool.locations {
        let _ = writeln!(out, "file     {location}");
    }
    if let Some(input) = &request.tool.input {
        let _ = writeln!(out, "input    {input}");
    }
    if let Some(always) = request.label(Verdict::AllowAlways) {
        let _ = writeln!(out, "always   {always}");
    }
    let _ = writeln!(out, "asked    {}", request.requested_at.format("%Y-%m-%d %H:%M:%S"));
    let _ = match state {
        State::Pending => write!(out, "expires  in {}\nanswer   {}", left(request.expires_at, now), commands(request)),
        State::Decided(d) => {
            write!(out, "answered {} at {}", spelling(d.verdict), d.decided_at.format("%Y-%m-%d %H:%M:%S"))
        }
        State::DecidedByAnt(by, d) => {
            let (by, at) = (deciders(by, d), d.decided_at.format("%Y-%m-%d %H:%M:%S"));
            write!(out, "answered {} by {by} at {at}", spelling(d.verdict))
        }
        State::Expired => write!(out, "expired  at {}, rejected", request.expires_at.format("%Y-%m-%d %H:%M:%S")),
        State::Cancelled(at) => write!(out, "gone     at {}, its prompt cancelled", at.format("%Y-%m-%d %H:%M:%S")),
    };
    out
}

/// What `ants tools approve` prints after approving `request`, for which `suggestion` came.
pub(crate) fn suggestion_text(request: &ApprovalRequest, suggestion: &Suggestion) -> String {
    let ant = &request.ant;
    format!(
        "{ant} was allowed this {} times: the allow rule {:?} in its allow list would spare asking; `ants tools \
         approve ID --rule` on the next such request adds it",
        suggestion.approved_before + 1,
        suggestion.rule
    )
}
