//! A human's reminders ([`Reminders`]), set, listed and cleared outside `ants chat` with `ants remind set|list|clear`,
//! in the same entry `/remind` in `ants chat` changes; and when `DURATION|TIME [NOTE]` says to be reminded, which both
//! take ([`parse`]).

use std::collections::HashMap;
use std::fmt::Write as _;

use ants_proto::{Reminder, Reminders, ThreadId, ThreadTitle, Uuid};
use anyhow::{Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Local, NaiveDateTime, NaiveTime, TimeDelta, TimeZone, Utc};

use crate::{Nats, thread, thread_meta};

/// When `text`, `DURATION|TIME [NOTE]`, says to be reminded from `now`, and the note: a duration such as `1h30m`
/// from now, a time of day `HH:MM` (the next one) or a date and time `YYYY-MM-DDTHH:MM`, both in `zone`.
pub fn parse<Z: TimeZone>(text: &str, now: DateTime<Utc>, zone: &Z) -> Result<(DateTime<Utc>, String), String> {
    let text = text.trim();
    let (when, note) = text.split_once(char::is_whitespace).unwrap_or((text, ""));
    if when.is_empty() {
        return Err("when to remind: a duration such as 1h, a time HH:MM or YYYY-MM-DDTHH:MM".to_owned());
    }
    let local = now.with_timezone(zone).naive_local();
    let at = if let Ok(time) = NaiveTime::parse_from_str(when, "%H:%M") {
        let today = local.date().and_time(time);
        let day = if today > local { today } else { today + TimeDelta::days(1) };
        utc(zone, day, when)?
    } else if let Ok(at) = NaiveDateTime::parse_from_str(when, "%Y-%m-%dT%H:%M") {
        utc(zone, at, when)?
    } else {
        let duration = ants_proto::duration::parse(when)?;
        now + TimeDelta::from_std(duration).map_err(|_| format!("too long: {when}"))?
    };
    if at <= now {
        return Err(format!("{when} is past: a reminder is due later"));
    }
    Ok((at, note.trim().to_owned()))
}

/// `at` in `zone`, in UTC; the earlier of two in a repeated hour.
fn utc<Z: TimeZone>(zone: &Z, at: NaiveDateTime, when: &str) -> Result<DateTime<Utc>, String> {
    let at = zone.from_local_datetime(&at).earliest().ok_or_else(|| format!("no such local time: {when}"))?;
    Ok(at.with_timezone(&Utc))
}

/// `at` on the local clock, as `ants remind` prints it.
fn local(at: DateTime<Utc>) -> String {
    at.with_timezone(&Local).format("%Y-%m-%d %H:%M").to_string()
}

/// The thread `target` names, and the message on it when it names one: a message id or a unique start of one among
/// those the human can read back, else a thread id, a unique start of one, or `last`.
async fn target(nats: &Nats<'_>, js: &jetstream::Context, target: &str) -> Result<(ThreadId, Option<Uuid>)> {
    let message = match target.parse::<thread::MessagePick>() {
        Ok(pick) => match pick.resolve(js, &nats.me).await {
            Ok(id) => thread::find_message(js, id, None).await?,
            Err(e) => {
                tracing::debug!("{target} as a message: {e:#}");
                None
            }
        },
        Err(_) => None,
    };
    match message {
        Some(entry) => Ok((entry.thread_id, entry.id)),
        None => Ok((target.parse::<thread::Pick>()?.resolve(nats, js).await?, None)),
    }
}

/// Connect as the CLI's human, whose reminders these are.
async fn connect(nats: &Nats<'_>) -> Result<(jetstream::Context, thread_meta::Meta)> {
    if !nats.me.role.is_human() {
        bail!("the credentials of {} are an ant's: only a human sets reminders", nats.me.id);
    }
    let js = thread::connect(nats).await?;
    let meta = thread_meta::open(nats, &js).await?;
    Ok((js, meta))
}

/// `ants remind set TARGET WHEN [NOTE]`: remind the human of the thread or message `target` names as `when`
/// (`DURATION|TIME [NOTE]`) says, in place of any reminder of the thread.
pub async fn set(nats: &Nats<'_>, target_text: &str, when: &str) -> Result<()> {
    let now = Utc::now();
    let (due, note) = parse(when, now, &Local).map_err(anyhow::Error::msg)?;
    let (js, meta) = connect(nats).await?;
    let (thread, message) = target(nats, &js, target_text).await?;
    let reminder = Reminder { at: now, due, message, note, pushed: false };
    let replaced = std::cell::Cell::new(false);
    meta.change_reminders(|reminders| {
        replaced.set(reminders.remind.contains_key(&thread));
        reminders.remind(&thread, reminder.clone());
        true
    })
    .await?;
    println!("{}", set_line(&thread, &reminder, replaced.get()));
    Ok(())
}

/// What `ants remind set` prints of `reminder` of `thread`, which `replaced` one it had.
fn set_line(thread: &ThreadId, reminder: &Reminder, replaced: bool) -> String {
    let of = match reminder.message {
        Some(message) => format!("message {message} on thread {thread}"),
        None => format!("thread {thread}"),
    };
    let replaced = if replaced { ", in place of the one it had" } else { "" };
    format!("reminder of {of} due {}{replaced}", local(reminder.due))
}

/// `ants remind list [--json]`: the human's reminders, soonest due first.
pub async fn list(nats: &Nats<'_>, json: bool) -> Result<()> {
    let (_, meta) = connect(nats).await?;
    let reminders = meta.reminders().await?;
    let titles = meta.titles(reminders.remind.keys()).await?;
    let now = Utc::now();
    if json {
        for (thread, reminder) in soonest(&reminders) {
            println!("{}", json_line(thread, reminder, titles.get(thread), now)?);
        }
        return Ok(());
    }
    print!("{}", listing(&reminders, &titles, now, local));
    Ok(())
}

/// The reminders of `reminders`, soonest due first.
fn soonest(reminders: &Reminders) -> Vec<(&ThreadId, &Reminder)> {
    let mut all: Vec<_> = reminders.remind.iter().collect();
    all.sort_by_key(|(_, r)| r.due);
    all
}

/// One reminder as `ants remind list --json` prints it: the stored reminder with its thread, whether it is due at
/// `now`, and the thread's title when it has one.
fn json_line(
    thread: &ThreadId,
    reminder: &Reminder,
    title: Option<&ThreadTitle>,
    now: DateTime<Utc>,
) -> Result<String> {
    let mut value = serde_json::to_value(reminder)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("thread".into(), serde_json::json!(thread));
        object.insert("is_due".into(), serde_json::json!(reminder.due <= now));
        if let Some(title) = title.filter(|t| !t.title.is_empty()) {
            object.insert("title".into(), serde_json::json!(title.title));
        }
    }
    Ok(serde_json::to_string(&value)?)
}

/// The reminders as `ants remind list` prints them, one line each, soonest due first: when due (`at` writes the
/// time), `due` once due at `now` and `pushed` once a notifier pushed it, the thread and message, the note and the
/// thread's title.
fn listing(
    reminders: &Reminders,
    titles: &HashMap<ThreadId, ThreadTitle>,
    now: DateTime<Utc>,
    at: impl Fn(DateTime<Utc>) -> String,
) -> String {
    if reminders.remind.is_empty() {
        return "no reminders\n".to_owned();
    }
    let mut out = String::new();
    for (thread, reminder) in soonest(reminders) {
        let state = match (reminder.due <= now, reminder.pushed) {
            (false, _) => "",
            (true, false) => " due",
            (true, true) => " due, pushed",
        };
        let message = reminder.message.map_or_default(|m| format!(" message {m}"));
        let title = titles.get(thread).map(|t| t.title.as_str()).filter(|t| !t.is_empty());
        let about: Vec<&str> =
            [Some(reminder.note.as_str()).filter(|n| !n.is_empty()), title].into_iter().flatten().collect();
        let about = if about.is_empty() { String::new() } else { format!(": {}", about.join(" · ")) };
        let _ = writeln!(out, "{}{state} thread {thread}{message}{about}", at(reminder.due));
    }
    out
}

/// `ants remind clear TARGET`: clear the reminder of the thread `target` names, or of the thread of the message it
/// names.
pub async fn clear(nats: &Nats<'_>, target_text: &str) -> Result<()> {
    let (js, meta) = connect(nats).await?;
    let (thread, _) = target(nats, &js, target_text).await?;
    if meta.change_reminders(|reminders| reminders.clear(&thread)).await? {
        println!("reminder of thread {thread} cleared");
    } else {
        println!("thread {thread} has no reminder");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::tests::at;

    #[test]
    fn a_reminder_is_due_after_a_duration_at_the_next_time_of_day_or_at_a_date() {
        let now = at(0);
        assert_eq!(parse("90m check CI", now, &Utc), Ok((now + TimeDelta::minutes(90), "check CI".to_owned())));
        let utc = |text: &str| text.parse::<DateTime<Utc>>().unwrap();
        assert_eq!(now, utc("2027-01-15T08:00:00Z"));
        assert_eq!(parse("09:30", now, &Utc).unwrap().0, utc("2027-01-15T09:30:00Z"), "later today");
        assert_eq!(parse("07:00", now, &Utc).unwrap().0, utc("2027-01-16T07:00:00Z"), "tomorrow");
        let east = chrono::FixedOffset::east_opt(2 * 3600).unwrap();
        assert_eq!(parse("09:30", now, &east).unwrap().0, utc("2027-01-16T07:30:00Z"), "in the zone");
        let date = (now + TimeDelta::days(3)).format("%Y-%m-%dT%H:%M").to_string();
        assert_eq!(parse(&date, now, &Utc).unwrap().0, now + TimeDelta::days(3));
        assert!(parse("2000-01-01T00:00", now, &Utc).unwrap_err().contains("past"));
        assert!(parse("", now, &Utc).is_err());
        assert!(parse("soon", now, &Utc).is_err());
    }

    #[test]
    fn the_list_shows_the_soonest_first_with_what_is_due_the_message_note_and_title() {
        let (later, sooner) = (ThreadId::generate(), ThreadId::generate());
        let message = Uuid::now_v7();
        let mut reminders = Reminders::default();
        let reminder =
            |due, message, note: &str, pushed| Reminder { at: at(0), due, message, note: note.into(), pushed };
        reminders.remind(&later, reminder(at(7200), None, "", false));
        reminders.remind(&sooner, reminder(at(60), Some(message), "check CI", true));
        let titles = HashMap::from([(
            sooner.clone(),
            ThreadTitle::new(sooner.clone(), "Deploy", "alice".parse().unwrap()).unwrap(),
        )]);
        let hhmm = |at: DateTime<Utc>| at.format("%H:%M").to_string();
        let listed = listing(&reminders, &titles, at(600), hhmm);
        assert_eq!(
            listed,
            format!("08:01 due, pushed thread {sooner} message {message}: check CI · Deploy\n10:00 thread {later}\n")
        );
        assert_eq!(listing(&Reminders::default(), &titles, at(0), hhmm), "no reminders\n");
        let line = json_line(&sooner, &reminders.remind[&sooner], titles.get(&sooner), at(0)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(value["thread"], sooner.to_string());
        assert_eq!((value["is_due"].as_bool(), value["title"].as_str()), (Some(false), Some("Deploy")));
        assert_eq!(value["note"], "check CI");
        let set = set_line(&later, &reminders.remind[&later], true);
        assert!(
            set.starts_with(&format!("reminder of thread {later} due ")) && set.ends_with("in place of the one it had")
        );
    }
}
