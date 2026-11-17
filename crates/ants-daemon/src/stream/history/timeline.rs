//! The tool-call timeline an ant stored on its threads ([`ants_proto::Timeline`]), read back by its daemon through the
//! ant's own [`sent_reader`] for its dreams' review packets ([`super::active`]).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::ops::ControlFlow;
use std::time::SystemTime;

use ants_proto::{NoReplyWhy, ThreadId, Timeline, TimelineCall, Timelines, UserId, Uuid, sent_reader, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};
use chrono::{DateTime, Utc};

use crate::stream::reading::{self, Reading};

/// The timeline of `ant` on a thread, merged from its records.
#[derive(Debug, Default)]
pub(super) struct Thread {
    timelines: Timelines,
    /// The messages whose prompts the records cover, in the order of their first record, with its time.
    prompts: Vec<(Uuid, DateTime<Utc>)>,
}

/// The timeline `ant` stored from `start` up to `until` on each thread `wanted` takes, read as its daemon reads
/// ([`Reading::daemon`]), one read through the ant's sent reader at a time ([`SENT_READS`](super::SENT_READS)).
pub(super) async fn read(
    js: &jetstream::Context,
    ant: &UserId,
    reading: Reading,
    start: consumer::DeliverPolicy,
    until: SystemTime,
    wanted: impl Fn(&ThreadId) -> bool,
) -> Result<BTreeMap<ThreadId, Thread>> {
    let mut threads: BTreeMap<ThreadId, Thread> = BTreeMap::new();
    let _one = if reading == Reading::Consumer { Some(super::SENT_READS.lock().await) } else { None };
    let (reader, filter) = (sent_reader(ant), subject::timeline_from(ant));
    reading::read_back(js, &reader, reading, &filter, start, |_, at, subject, payload| {
        if at > until {
            return ControlFlow::Break(());
        }
        if let Some((thread, _)) = subject::parse_timeline(subject)
            && wanted(&thread)
            && let Ok(record) = serde_json::from_slice::<Timeline>(payload)
        {
            let (message, at) = (record.message, record.at);
            let thread = threads.entry(thread).or_default();
            if thread.timelines.add(subject, record) && !thread.prompts.iter().any(|(m, _)| *m == message) {
                thread.prompts.push((message, at));
            }
        }
        ControlFlow::Continue(())
    })
    .await
    .context("reading back the timeline")?;
    Ok(threads)
}

impl Thread {
    /// The timeline of `ant`, oldest prompt first: a line naming each prompt's message, then a line for each of its
    /// tool calls, its plan, the calls left out and its end with no reply, whose final text is cut to `most` bytes.
    pub(super) fn show(&self, ant: &UserId, most: usize) -> String {
        let mut text = String::new();
        for (message, first) in &self.prompts {
            let calls: Vec<_> = self.timelines.calls().iter().filter(|c| c.message == *message).collect();
            let began = calls.iter().map(|c| c.call.started).min().map_or(*first, |started| started.min(*first));
            let _ = writeln!(text, "{} UTC, the prompt of message {message}:", began.format("%Y-%m-%d %H:%M"));
            for call in calls {
                let _ = writeln!(text, "- {}", call_line(&call.call));
            }
            if let Some(steps) = self.timelines.plan(ant, *message).filter(|steps| !steps.is_empty()) {
                let steps: Vec<_> =
                    steps.iter().map(|step| format!("{} ({})", one_line(&step.content), step.status)).collect();
                let _ = writeln!(text, "- plan: {}", steps.join("; "));
            }
            match self.timelines.omitted(ant, *message) {
                0 => {}
                1 => text.push_str("- 1 more tool call left out\n"),
                n => {
                    let _ = writeln!(text, "- {n} more tool calls left out");
                }
            }
            for end in self.timelines.no_replies().iter().filter(|end| end.message == *message) {
                let why = match end.why {
                    NoReplyWhy::ReplyToOwn => "the message answered the ant's own",
                    NoReplyWhy::Completed => "the ant called complete",
                    NoReplyWhy::Steered => "a later message took its answer",
                };
                let _ = write!(text, "- {} ended with no reply: {why}", end.at.format("%H:%M"));
                if let Some(reason) = &end.reason {
                    let _ = write!(text, ", {}", one_line(reason));
                }
                let final_text = end.text.trim();
                if final_text.is_empty() {
                    text.push('\n');
                } else if final_text.len() <= most {
                    let _ = writeln!(text, "; its final text, sent to nobody:\n{final_text}");
                } else {
                    let kept = final_text.floor_char_boundary(most);
                    let left = final_text.len() - kept;
                    let _ = writeln!(
                        text,
                        "; its final text, sent to nobody:\n{} … ({left} bytes left out)",
                        &final_text[..kept]
                    );
                }
            }
        }
        text
    }
}

/// A tool call as a timeline line shows it: when it started, its kind and title, status and duration, and paths.
fn call_line(call: &TimelineCall) -> String {
    let kind = call.tool_kind.as_deref().unwrap_or("tool");
    let mut line = format!("{} {kind}: {}", call.started.format("%H:%M"), one_line(&call.title));
    let mut how: Vec<String> = call.status.iter().cloned().collect();
    match call.duration_ms {
        Some(ms) if ms < 1000 => how.push(format!("{ms} ms")),
        Some(ms) => how.push(format!("{} s", ms / 1000)),
        None => {}
    }
    if !how.is_empty() {
        let _ = write!(line, " ({})", how.join(", "));
    }
    if !call.paths.is_empty() {
        let paths: Vec<_> = call.paths.iter().map(|path| one_line(path)).collect();
        let _ = write!(line, "; {}", paths.join(", "));
    }
    line
}

/// `text` on one line, its line breaks as spaces.
fn one_line(text: &str) -> String {
    text.replace(['\n', '\r'], " ")
}

#[cfg(test)]
mod tests {
    use ants_proto::{PlanStep, TimelineEntry};

    use super::*;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn call(id: &str, title: &str, status: &str, minute: i64, duration_ms: Option<u64>) -> TimelineEntry {
        TimelineEntry::Tool(TimelineCall {
            id: id.to_owned(),
            tool_kind: Some("execute".to_owned()),
            title: title.to_owned(),
            status: Some(status.to_owned()),
            started: DateTime::from_timestamp(1_794_646_800 + minute * 60, 0).unwrap(),
            duration_ms,
            paths: Vec::new(),
        })
    }

    #[test]
    fn a_timeline_shows_each_prompt_with_its_calls_plan_and_end() {
        let (ant, thread): (UserId, ThreadId) = (p("dev-a"), "t1".parse().unwrap());
        let (m1, m2) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let mut shown = Thread::default();
        let mut add = |message, entries, omitted| {
            let mut record = Timeline::new(ant.clone(), thread.clone(), message, entries, omitted);
            record.at = DateTime::from_timestamp(1_794_646_800 + 600, 0).unwrap();
            if !shown.prompts.iter().any(|(m, _)| *m == message) {
                shown.prompts.push((message, record.at));
            }
            assert!(shown.timelines.add(&record.subject(), record));
        };
        add(m1, vec![call("c1", "cargo test", "in_progress", 1, None)], 0);
        let plan = vec![
            PlanStep { content: "test".into(), status: "completed".into() },
            PlanStep { content: "fix\nit".into(), status: "pending".into() },
        ];
        let mut edit = call("c2", "Edit a.rs", "failed", 2, Some(40));
        if let TimelineEntry::Tool(edit) = &mut edit {
            edit.tool_kind = None;
            edit.paths = vec!["src/a.rs".into(), "src/b.rs".into()];
        }
        add(
            m1,
            vec![call("c1", "cargo test", "completed", 1, Some(12_345)), edit, TimelineEntry::Plan { steps: plan }],
            3,
        );
        let end = TimelineEntry::NoReply {
            why: NoReplyWhy::Completed,
            reason: Some("nothing to add".into()),
            text: "é".repeat(20),
        };
        add(m2, vec![end], 0);
        assert_eq!(
            shown.show(&ant, 11),
            format!(
                "2026-11-14 09:01 UTC, the prompt of message {m1}:\n\
                 - 09:01 execute: cargo test (completed, 12 s)\n\
                 - 09:02 tool: Edit a.rs (failed, 40 ms); src/a.rs, src/b.rs\n\
                 - plan: test (completed); fix it (pending)\n\
                 - 3 more tool calls left out\n\
                 2026-11-14 09:10 UTC, the prompt of message {m2}:\n\
                 - 09:10 ended with no reply: the ant called complete, nothing to add; its final text, sent to nobody:\n\
                 ééééé … (30 bytes left out)\n"
            )
        );
    }
}
