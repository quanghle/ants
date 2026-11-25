//! `/assistant enable|show|disable|set|clear|allow` from the command bar: `ants assistant` run with its own code
//! ([`crate::cmd::assistant::act`]), what it prints on the status line, in the pager too when it prints more than two
//! lines. `/assistant disable` without `--retire` first asks in the footer whether to retire the dedicated assistant
//! too, as the CLI asks on a terminal.

use anyhow::Result;
use ratatui::style::{Color, Style};
use ratatui::text::Line;

use super::command::Tone;
use super::exec::{Ctx, Done, Job};
use super::model::{Action, Key, Model};
use crate::cmd::assistant::{self, AssistantCommand};
use crate::people::assistant::DisableArgs;
use crate::text::Say;

/// Run `command` as `ants assistant` does for the human of the chat, `disable` retiring the dedicated assistant only
/// with `--retire`: its lines, or its error after them.
pub(super) async fn run(ctx: &Ctx, command: AssistantCommand) -> Result<Done> {
    let nats = ctx.reach.nats()?;
    let mut say = Say::Gather(Vec::new());
    let done = assistant::act(&nats, command, |_| Ok(false), &mut say).await;
    let lines = say.into_lines();
    match done {
        Ok(()) => Ok(said(lines)),
        Err(e) if lines.is_empty() => Err(e),
        Err(e) => Err(e.context(lines.join(" · "))),
    }
}

/// What the bar shows of `lines` printed: those of `warning: ` as warnings, the others on the status line, else their
/// first there and all of them in the pager.
fn said(lines: Vec<String>) -> Done {
    let page = lines.join("\n");
    let (warnings, lines): (Vec<String>, Vec<String>) = lines.into_iter().partition(|l| l.starts_with("warning: "));
    let warnings = warnings.into_iter().map(|w| w["warning: ".len()..].to_owned()).collect();
    match &lines[..] {
        [] | [_] | [_, _] => Done { line: lines.join(" · "), warnings, ..Done::default() },
        [first, ..] => {
            let line = format!("{first} · the pager shows all ants assistant printed");
            Done { line, warnings, page: Some(page), ..Done::default() }
        }
    }
}

/// `/assistant disable` without `--retire`, waiting for y to retire the dedicated assistant too or n to withdraw the
/// assistant alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disabling(pub DisableArgs);

impl Disabling {
    pub fn line(&self) -> Line<'static> {
        let text = " /assistant disable: y withdraws the assistant and retires the dedicated one enable created (unlisted, \
                    its credentials revoked, its home kept); n withdraws it alone; any other key keeps it";
        Line::styled(text, Style::new().fg(Color::Yellow))
    }

    /// Take `key`: y runs `ants assistant disable --retire`, n runs it without, another key runs nothing.
    pub fn key(self, key: Key, model: &mut Model) -> Option<Action> {
        let Self(mut args) = self;
        match key {
            Key::Char('y') => args.retire = true,
            Key::Char('n') => args.retire = false,
            _ => {
                model.ui.bar.set_status("the assistant stays", Tone::Done);
                return None;
            }
        }
        let doing =
            if args.retire { "withdrawing and retiring the assistant…" } else { "withdrawing the assistant…" };
        model.ui.bar.set_status(doing, Tone::Busy);
        Some(Action::Run(Job::Assistant(AssistantCommand::Disable(args)), String::new()))
    }
}
