//! Tests of `ants thread`: how its entries show (those of every kind of stored message are tested where they are made,
//! in ants-client), forged messages, titles, listing, picking by id, replies and forwards, tool calls and requests,
//! prompt ends, and sealed messages.

mod listing;
mod picking;
mod sending;
mod showing;
mod stored;

use std::assert_matches;

use ants_client::entry::last_reopening;
use ants_daemon::channel::CLOCK_SLACK;
use ants_daemon::records::reactions::Reacted;
use ants_proto::{
    Acting, ApprovalRequest, ChannelId, ChannelPost, DeadLetter, DecidedBy, Envelope, Escalation, EscalationReason,
    Humans, NotesPost, SummaryRequest, ToolCall, Verdict,
};
use ants_test_support::user as p;
use serde::Serialize;

use super::pick::{made_within, select, select_any};
use super::text::state;
use super::*;

fn t(s: &str) -> ThreadId {
    s.parse().unwrap()
}

fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
}

fn json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

/// The entries of `messages`, each a subject and a payload, stored a second apart.
fn entries(messages: &[(String, Vec<u8>)]) -> Vec<Entry> {
    let mut parser = Parser::default();
    let parsed = messages.iter().enumerate();
    parsed.filter_map(|(i, (subject, payload))| parser.entry(i as u64 + 1, at(i as i64), subject, payload)).collect()
}
