//! `ants remind`: set, list and clear the CLI's human's reminders, as `/remind` in `ants chat` does.

use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCompleter;

use crate::connection::Nats;
use crate::{completions, remind};

#[derive(Subcommand, Debug)]
pub(crate) enum RemindCommand {
    /// Remind the human of a message or a thread, on every device of the human, in place of any reminder of the
    /// thread: once due, it shows as a reminder on the home of `ants chat` and `ants webhook serve` pushes it once.
    Set {
        /// A message (its id, or a unique start of one) to be reminded of on its thread, else a thread (its id, a
        /// unique start of one, or `last`).
        #[arg(value_name = "MESSAGE|THREAD", add = ArgValueCompleter::new(completions::messages))]
        target: String,
        /// When: a duration such as 2h or 1d30m from now, the next HH:MM, or YYYY-MM-DDTHH:MM, on the local clock.
        #[arg(value_name = "DURATION|TIME")]
        when: String,
        /// What to note with it.
        #[arg(value_name = "NOTE")]
        note: Vec<String>,
    },
    /// List the human's reminders, the soonest due first: when, whether due (and pushed), the thread and message,
    /// the note and the thread's title.
    List {
        /// One JSON reminder per line.
        #[arg(long)]
        json: bool,
    },
    /// Clear the reminder of a thread, done or cancelled.
    Clear {
        /// The thread (its id, a unique start of one, or `last`), or a message on it (its id, or a unique start of
        /// one).
        #[arg(value_name = "MESSAGE|THREAD", add = ArgValueCompleter::new(completions::messages))]
        target: String,
    },
}

pub(crate) async fn run(nats: &Nats<'_>, command: RemindCommand) -> Result<()> {
    match command {
        RemindCommand::Set { target, when, note } => {
            let when = std::iter::once(when).chain(note).collect::<Vec<_>>().join(" ");
            remind::set(nats, &target, &when).await
        }
        RemindCommand::List { json } => remind::list(nats, json).await,
        RemindCommand::Clear { target } => remind::clear(nats, &target).await,
    }
}
