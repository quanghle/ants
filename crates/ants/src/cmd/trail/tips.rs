//! `ants trail tips`: the tips ants left on a trail's steps, and their votes, in [`TRAIL_TIPS_BUCKET`], ranked as
//! steps carry them ([`TipBoard`]). A human retires a tip under its retirement key, which no ant writes ([`retire`]),
//! and removing a saved trail purges its tips ([`purge`]).

use std::fmt::Write as _;

use ants_proto::{
    MAX_TIP_TEXT, Ranked, Retirement, TRAIL_TIPS_BUCKET, TipBoard, TipId, TipRetired, TipStatus, TipVote, TrailName,
    TrailTip, UserId, script_sha256,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use serde::Serialize;

use super::TipsArgs;
use crate::colony::{short, since};
use crate::machines::latest_in;
use crate::text::one_line;

/// How many characters of a tip key a listing shows.
const KEY_WIDTH: usize = 40;

/// The tips of the trail `name` with their votes, and the [`script_sha256`] of the script the colony saved under that
/// name, against which a tip is of an older script; none when the colony saves no such trail.
pub(crate) async fn read(js: &jetstream::Context, name: &TrailName) -> Result<(TipBoard, Option<String>)> {
    let entries = latest_in(js, TRAIL_TIPS_BUCKET, &format!("$KV.{TRAIL_TIPS_BUCKET}.{name}.>")).await?;
    let script = super::saved::get(js, name).await?.map(|saved| script_sha256(&saved.script));
    Ok((TipBoard::new(name.clone(), entries), script))
}

/// The tips of `board` `args` lists at `now` for the current script `script`: the live ones in rank order, then with
/// `--all` the retired ones; with `--key`, only those under that tip key.
pub(crate) fn listed<'a>(
    board: &'a TipBoard,
    args: &TipsArgs,
    script: Option<&str>,
    now: DateTime<Utc>,
) -> Vec<Ranked<'a>> {
    let mut ranked = keyed(board, args, script, now);
    ranked.retain(|r| args.all || r.status.is_live());
    ranked
}

/// Every tip of `board` under `args`' tip key, if any, ranked.
fn keyed<'a>(board: &'a TipBoard, args: &TipsArgs, script: Option<&str>, now: DateTime<Utc>) -> Vec<Ranked<'a>> {
    let mut ranked = board.ranked(now, script);
    if let Some(key) = &args.key {
        ranked.retain(|r| r.tip.tip_key.as_deref() == Some(key.as_str()));
    }
    ranked
}

/// What `ants trail tips` prints: how many tips are live and retired, then the tips listed, each a row of columns
/// followed by its text.
pub(crate) fn render(board: &TipBoard, args: &TipsArgs, script: Option<&str>, now: DateTime<Utc>) -> String {
    let mut out = summary(board, args, script, now);
    out.push('\n');
    let listed = listed(board, args, script, now);
    if listed.is_empty() {
        return out;
    }
    out.push('\n');
    let header = ["TIP", "KEY", "WRITER", "VOTES", "STATUS", "WRITTEN"].map(str::to_owned);
    let rows: Vec<[String; 6]> = std::iter::once(header).chain(listed.iter().map(|r| row(r, now))).collect();
    let table = super::columns(&rows);
    let mut lines = table.lines();
    let _ = writeln!(out, "{}", lines.next().unwrap_or_default());
    for (line, ranked) in lines.zip(&listed) {
        let _ = writeln!(out, "{line}");
        for text in ranked.tip.text.lines().map(|line| one_line(line, MAX_TIP_TEXT)).filter(|l| !l.is_empty()) {
            let _ = writeln!(out, "  {text}");
        }
    }
    out
}

/// The first line of [`render`]: how many of the trail's tips, under `args`' tip key if any, are live and retired.
pub(crate) fn summary(board: &TipBoard, args: &TipsArgs, script: Option<&str>, now: DateTime<Utc>) -> String {
    let name = board.trail();
    let keyed = keyed(board, args, script, now);
    let key = args.key.as_deref().map(|key| one_line(key, KEY_WIDTH));
    if keyed.is_empty() {
        return match key {
            Some(key) => format!("no tips under key {key} on trail {name}"),
            None => format!("no tips on trail {name}"),
        };
    }
    let live = keyed.iter().filter(|r| r.status.is_live()).count();
    let retired = keyed.len() - live;
    let of = key.map_or_default(|key| format!(", key {key}"));
    let live = match live {
        0 => "no live tips".to_owned(),
        1 => "1 live tip".to_owned(),
        n => format!("{n} live tips"),
    };
    match retired {
        0 => format!("trail {name}{of}: {live}"),
        n if args.all => format!("trail {name}{of}: {live}, {n} retired"),
        n => format!("trail {name}{of}: {live}, {n} retired, which --all lists"),
    }
}

/// A tip's row: id, tip key, writer, votes, status and age.
pub(crate) fn row(ranked: &Ranked<'_>, now: DateTime<Utc>) -> [String; 6] {
    let tip = ranked.tip;
    let mut status = ranked.status.to_string();
    if ranked.older_script {
        status.push_str(", older script");
    }
    let written = format!("{} ago", short(since(tip.written_at, now)));
    [tip.id.to_string(), key(tip), tip.writer.to_string(), votes(ranked), status, written]
}

/// The tip key a tip was left under, or `trail-wide`.
pub(crate) fn key(tip: &TrailTip) -> String {
    tip.tip_key.as_deref().map_or_else(|| "trail-wide".to_owned(), |key| one_line(key, KEY_WIDTH))
}

/// A tip's votes as `+helpful ~stale !wrong`.
pub(crate) fn votes(ranked: &Ranked<'_>) -> String {
    let tally = ranked.tally;
    format!("+{} ~{} !{}", tally.helpful, tally.stale, tally.wrong)
}

/// A tip as `ants trail tips --json` prints it.
#[derive(Serialize)]
pub(crate) struct Json<'a> {
    #[serde(flatten)]
    tip: &'a TrailTip,
    status: String,
    live: bool,
    older_script: bool,
    score: f64,
    last_used: DateTime<Utc>,
    helpful: u32,
    stale: u32,
    wrong: u32,
    votes: &'a [TipVote],
}

impl<'a> From<&Ranked<'a>> for Json<'a> {
    fn from(ranked: &Ranked<'a>) -> Self {
        Self {
            tip: ranked.tip,
            status: ranked.status.to_string(),
            live: ranked.status.is_live(),
            older_script: ranked.older_script,
            score: ranked.score,
            last_used: ranked.last_used,
            helpful: ranked.tally.helpful,
            stale: ranked.tally.stale,
            wrong: ranked.tally.wrong,
            votes: ranked.votes,
        }
    }
}

/// Retire the tip `id` of the trail `name`, `writer`'s when given, as `by`, a human, at `now`: who retired it and when, put under its
/// [`TrailTip::retired_key`], which its writer cannot undo, so steps carry and searches find it no more. Refused for a tip the trail lacks or one retired already.
pub(crate) async fn retire(
    js: &jetstream::Context,
    name: &TrailName,
    id: &TipId,
    writer: Option<&UserId>,
    by: &UserId,
    now: DateTime<Utc>,
) -> Result<TrailTip> {
    let (board, _) = read(js, name).await?;
    let ranked = match board.tip(id, writer, now, None) {
        Ok(ranked) => ranked,
        Err(writers) if writers.is_empty() => {
            bail!("trail {name} has no tip {id}: `ants trail tips {name} --all` lists them")
        }
        Err(_) if let Some(writer) = writer => {
            bail!("trail {name} has no tip {id} by {writer}: `ants trail tips {name} --all` lists them")
        }
        Err(writers) => {
            let writers: Vec<String> = writers.iter().map(ToString::to_string).collect();
            bail!("tips of {} on trail {name} share id {id}: name the tip's writer with --writer", writers.join(", "))
        }
    };
    match ranked.status {
        TipStatus::Live => {}
        TipStatus::Retired(Retirement::ByHuman { by: human, at }) => {
            bail!("tip {id} of trail {name} was retired by {human} {} ago", short(since(at, now)))
        }
        status => bail!("tip {id} of trail {name} is retired already: {status}"),
    }
    let mut tip = ranked.tip.clone();
    let retired = TipRetired { by: by.clone(), at: now };
    let kv = js.get_key_value(TRAIL_TIPS_BUCKET).await.context("opening ants_trail_tips (`ants init` creates it)")?;
    let payload = serde_json::to_vec(&retired)?;
    kv.put(tip.retired_key(), payload.into()).await.with_context(|| format!("retiring tip {id} of trail {name}"))?;
    tip.retired = Some(retired);
    Ok(tip)
}

/// What `ants trail tips --retire` prints of the tip `tip` it retired at `now`.
pub(crate) fn retired(tip: &TrailTip, now: DateTime<Utc>) -> String {
    let ago = short(since(tip.written_at, now));
    let text = one_line(&tip.text, 60);
    format!("retired tip {} of trail {}, left by {} {ago} ago: {text}", tip.id, tip.trail, tip.writer)
}

/// Purge the tips of the trail `name` and their votes: how many entries went.
pub(super) async fn purge(js: &jetstream::Context, name: &TrailName) -> Result<u64> {
    let stream = js
        .get_stream(format!("KV_{TRAIL_TIPS_BUCKET}"))
        .await
        .context("opening ants_trail_tips (`ants init` creates it)")?;
    super::saved::purge_keys(&stream, &format!("$KV.{TRAIL_TIPS_BUCKET}.{name}.>")).await
}

/// What `ants trail rm` adds to its line for the `purged` entries of tips and votes it removed with the trail.
pub(super) fn purged(purged: u64) -> String {
    match purged {
        0 => String::new(),
        1 => " and its tips: 1 entry".to_owned(),
        n => format!(" and its tips: {n} entries, votes included"),
    }
}
