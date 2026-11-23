//! What a human holds back of its notifications, on all its devices: a snooze (`ants notify snooze|resume|status`, and
//! `/notify snooze|resume|status` in `ants chat`), muted threads (`ants thread mute|unmute`) and each channel's notify
//! level (`ants channel notify`), kept as one
//! [`NotifyPrefs`] under the human's key in [`NOTIFY_BUCKET`] and updated with compare-and-set, so devices changing
//! them at once keep each other's changes. Each hold carries its deadline and ends there on its own.

use ants_daemon::bus::relayed::GuardedKv as _;
use std::time::Duration;

use ants_proto::{ChannelId, ChannelNotify, NOTIFY_BUCKET, NotifyPrefs, ThreadId, UserId};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::{self, kv};
use chrono::{DateTime, TimeDelta, Utc};

use crate::cas;
use crate::connection::Nats;
use crate::threads::thread;

/// How long a mute lasts unless given: as long as the stream keeps a thread's messages.
pub const MUTE_FOR: Duration = ants_daemon::bus::RETENTION;
/// The longest snooze or mute.
pub const MAX_HOLD: Duration = Duration::from_hours(365 * 24);
/// How many times a change is tried again when another device changed the holds meanwhile.
const CONFLICTS: usize = 10;

/// The bucket of the holds, created as `ants init` does when missing. Entries never expire: the holds they carry do.
pub async fn ensure_bucket(js: &jetstream::Context) -> Result<kv::Store> {
    Box::pin(ants_daemon::hub::ensure_bucket(js, ants_daemon::hub::Object::Notify))
        .await
        .context("opening the notify bucket (with credentials, `ants init` as a human creates it)")
}

/// The holds of `human` and their revision, 0 when there are none.
async fn load(kv: &kv::Store, human: &UserId) -> Result<(NotifyPrefs, u64)> {
    let Some(entry) = kv.guarded_entry(human.as_str()).await.context("reading the snooze and muted threads")? else {
        return Ok((NotifyPrefs::default(), 0));
    };
    if entry.operation != kv::Operation::Put {
        return Ok((NotifyPrefs::default(), entry.revision));
    }
    let prefs = serde_json::from_slice(&entry.value).unwrap_or_else(|e| {
        tracing::warn!(error = %e, "malformed snooze and muted threads; starting over");
        NotifyPrefs::default()
    });
    Ok((prefs, entry.revision))
}

/// The holds of `human` as stored; none when the bucket does not exist.
pub(crate) async fn prefs(js: &jetstream::Context, human: &UserId) -> Result<NotifyPrefs> {
    match js.get_key_value(NOTIFY_BUCKET).await {
        Ok(kv) => Ok(load(&kv, human).await?.0),
        Err(_) => Ok(NotifyPrefs::default()),
    }
}

/// Apply `change` to the holds of `human`, on top of whatever other devices changed meanwhile, dropping those that
/// ended: the holds as stored.
pub(crate) async fn update(
    js: &jetstream::Context,
    human: &UserId,
    change: impl Fn(&mut NotifyPrefs, DateTime<Utc>),
) -> Result<NotifyPrefs> {
    let kv = ensure_bucket(js).await?;
    for _ in 0..CONFLICTS {
        let (mut prefs, revision) = load(&kv, human).await?;
        let now = Utc::now();
        change(&mut prefs, now);
        prefs.prune(now);
        let stored = cas::update(&kv, human.as_str(), serde_json::to_vec(&prefs)?, revision).await;
        if stored.context("storing the snooze and muted threads")? {
            return Ok(prefs);
        }
    }
    bail!("storing the snooze and muted threads: another device kept changing them")
}

/// `now` plus `hold`, which must be from a second to [`MAX_HOLD`].
pub(crate) fn until(now: DateTime<Utc>, hold: Duration) -> Result<DateTime<Utc>> {
    if hold < Duration::from_secs(1) || hold > MAX_HOLD {
        bail!("from 1s to {}", ants_proto::duration::format(MAX_HOLD));
    }
    Ok(now + TimeDelta::from_std(hold)?)
}

fn local(at: DateTime<Utc>) -> String {
    at.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M").to_string()
}

/// Connect as the CLI's human, which alone holds back its notifications.
async fn connect(nats: &Nats<'_>, what: &str) -> Result<jetstream::Context> {
    if !nats.me.role.is_human() {
        bail!("{what} needs a human's credentials; those of {} are an ant's", nats.me.id);
    }
    Ok(nats.connect("ants-cli").await?.1)
}

/// `ants notify snooze DURATION`.
pub async fn snooze(nats: &Nats<'_>, hold: Duration) -> Result<()> {
    until(Utc::now(), hold).context("snooze")?;
    let js = connect(nats, "snoozing notifications").await?;
    println!("{}", snoozed(&js, &nats.me.id, hold).await?);
    Ok(())
}

/// Snooze the notifications of `human` for `hold`, through `js`; what `ants notify snooze` prints.
pub async fn snoozed(js: &jetstream::Context, human: &UserId, hold: Duration) -> Result<String> {
    until(Utc::now(), hold).context("snooze")?;
    let prefs = update(js, human, |prefs, now| prefs.snoozed_until = until(now, hold).ok()).await?;
    let until = prefs.snoozed_until.map_or_default(local);
    Ok(format!("notifications of {human} snoozed until {until}, on every device"))
}

/// `ants notify resume`.
pub async fn resume(nats: &Nats<'_>) -> Result<()> {
    let js = connect(nats, "resuming notifications").await?;
    println!("{}", resumed(&js, &nats.me.id).await?);
    Ok(())
}

/// End the snooze of `human`'s notifications, through `js`; what `ants notify resume` prints.
pub async fn resumed(js: &jetstream::Context, human: &UserId) -> Result<String> {
    let snoozed = std::cell::Cell::new(false);
    update(js, human, |prefs, now| {
        snoozed.set(prefs.snoozed(now));
        prefs.snoozed_until = None;
    })
    .await?;
    Ok(if snoozed.get() {
        format!("notifications of {human} resumed")
    } else {
        format!("notifications of {human} were not snoozed")
    })
}

/// `ants notify status`.
pub async fn status(nats: &Nats<'_>, json: bool) -> Result<()> {
    let js = connect(nats, "showing held notifications").await?;
    let prefs = holding(&js, &nats.me.id).await?;
    if json {
        println!("{}", serde_json::to_string(&prefs)?);
        return Ok(());
    }
    for line in status_lines(&prefs) {
        println!("{line}");
    }
    Ok(())
}

/// The holds of `human` in force now, through `js`, as `ants notify status` shows them.
pub async fn holding(js: &jetstream::Context, human: &UserId) -> Result<NotifyPrefs> {
    let kv = ensure_bucket(js).await?;
    let (mut prefs, _) = load(&kv, human).await?;
    prefs.prune(Utc::now());
    Ok(prefs)
}

/// What `ants notify status` prints of `prefs`, a line each: the snooze, then each muted thread and each channel's
/// notify level other than the default.
pub fn status_lines(prefs: &NotifyPrefs) -> Vec<String> {
    let snooze = match prefs.snoozed_until {
        Some(until) => format!("snoozed until {}", local(until)),
        None => "not snoozed".to_owned(),
    };
    let muted = prefs.muted.iter().map(|(thread, until)| format!("muted thread {thread} until {}", local(*until)));
    let levels =
        prefs.channels.iter().map(|(channel, level)| format!("#{channel} notifies {level} ({})", level.describe()));
    std::iter::once(snooze).chain(muted).chain(levels).collect()
}

/// Set the notify level of `channel` for `human` to `level`, through `js`; what `ants channel notify` prints.
pub async fn channel_notify(
    js: &jetstream::Context,
    human: &UserId,
    channel: &ChannelId,
    level: ChannelNotify,
) -> Result<String> {
    update(js, human, |prefs, _| prefs.set_channel(channel, level)).await?;
    Ok(format!("#{channel} notifies {human}: {level} ({})", level.describe()))
}

/// `ants thread mute THREAD [--for DURATION]`.
pub async fn mute(nats: &Nats<'_>, pick: thread::Pick, hold: Option<Duration>) -> Result<()> {
    let hold = hold.unwrap_or(MUTE_FOR);
    until(Utc::now(), hold).context("--for")?;
    let js = connect(nats, "muting a thread").await?;
    let thread = pick.resolve(nats, &js).await?;
    println!("{}", muted(&js, &nats.me.id, &thread, hold).await?);
    Ok(())
}

/// Mute `thread` for `human` for `hold`, through `js`; what `ants thread mute` prints.
pub async fn muted(js: &jetstream::Context, human: &UserId, thread: &ThreadId, hold: Duration) -> Result<String> {
    until(Utc::now(), hold).context("--for")?;
    let prefs = update(js, human, |prefs, now| {
        if let Ok(until) = until(now, hold) {
            prefs.muted.insert(thread.clone(), until);
        }
    })
    .await?;
    let until = prefs.muted.get(thread).copied().map_or_default(local);
    Ok(format!("thread {thread} muted until {until}: no notice of it reaches {human}"))
}

/// `ants thread unmute THREAD`.
pub async fn unmute(nats: &Nats<'_>, pick: thread::Pick) -> Result<()> {
    let js = connect(nats, "unmuting a thread").await?;
    let thread: ThreadId = pick.resolve(nats, &js).await?;
    println!("{}", unmuted(&js, &nats.me.id, &thread).await?);
    Ok(())
}

/// Unmute `thread` for `human`, through `js`; what `ants thread unmute` prints.
pub async fn unmuted(js: &jetstream::Context, human: &UserId, thread: &ThreadId) -> Result<String> {
    let was = std::cell::Cell::new(false);
    update(js, human, |prefs, now| was.set(prefs.muted.remove(thread).is_some_and(|until| now < until))).await?;
    Ok(if was.get() { format!("thread {thread} unmuted") } else { format!("thread {thread} was not muted") })
}
