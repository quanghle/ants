//! `/notify snooze DURATION`, `/notify resume` and `/notify status`: the holds on the human's push notifications on
//! every device, changed and shown as `ants notify snooze|resume|status` does with the same code
//! ([`crate::people::quiet`]), beside `/notify all|humans|mentions`, the notify level of the channel on view, which
//! the status lists too.

use std::time::Duration;

use ants_proto::NotifyPrefs;
use anyhow::Result;

use super::exec::{Ctx, Done};
use crate::people::quiet;

/// What `/notify` asks of the human's push notifications, as `ants notify` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Job {
    /// Hold them back on every device this long.
    Snooze(Duration),
    Resume,
    Status,
}

impl Job {
    pub(super) async fn run(self, ctx: &Ctx) -> Result<Done> {
        let me = &ctx.me.id;
        match self {
            Self::Snooze(hold) => Ok(Done::line(quiet::snoozed(&ctx.js, me, hold).await?)),
            Self::Resume => Ok(Done::line(quiet::resumed(&ctx.js, me).await?)),
            Self::Status => status(ctx).await,
        }
    }
}

/// What holds the human's push notifications back now, as `ants notify status` shows it.
async fn status(ctx: &Ctx) -> Result<Done> {
    Ok(shown(&quiet::holding(&ctx.js, &ctx.me.id).await?))
}

/// The holds `prefs` as `/notify status` shows them: the snooze on the status line, with the counts of the muted
/// threads and the channels' notify levels when the pager lists them too.
fn shown(prefs: &NotifyPrefs) -> Done {
    let lines = quiet::status_lines(prefs);
    let snooze = lines[0].clone();
    if lines.len() == 1 {
        return Done { line: snooze, ..Done::default() };
    }
    let counts = [
        (prefs.muted.len(), "muted thread", "muted threads"),
        (prefs.channels.len(), "channel's notify level", "channels' notify levels"),
    ];
    let counts = counts.into_iter().filter(|(n, ..)| *n > 0).map(|(n, one, many)| match n {
        1 => format!("1 {one}"),
        n => format!("{n} {many}"),
    });
    let line = std::iter::once(snooze).chain(counts).collect::<Vec<_>>().join(", ");
    let page = lines.iter().map(|line| format!("{line}\n")).collect();
    Done { line: format!("{line}: the pager lists them"), page: Some(page), ..Done::default() }
}

#[cfg(test)]
mod tests;
