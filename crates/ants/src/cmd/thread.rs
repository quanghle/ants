//! `ants thread`: list, show, search, title, pin, archive and mute threads.

use std::time::Duration;

use ants_proto::{ChannelId, UserId};
use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::{Nats, completions, quiet, search, thread, thread_meta};

#[derive(Subcommand, Debug)]
pub(crate) enum ThreadCommand {
    /// List the threads with messages stored within --since, one row each: id, owner, users and channels, first and
    /// last time, messages, pinned or archived and title when any is, and the first line of the first message; the
    /// most recently active last, and the pinned after the others. Archived threads are left out, but for those with
    /// a message stored since they were archived (`archived+new`). With `auto_archive` set in the config file's
    /// `[cli]`, such as "14d", a thread neither pinned nor archived with no message stored for that long is left out
    /// as `auto-archived` too, until a message is stored on it.
    List {
        /// Only threads this ant (or human) sent or received a message on.
        #[arg(long, value_name = "ANT", add = ArgValueCandidates::new(completions::ants))]
        ant: Option<UserId>,
        /// Only threads this human owns.
        #[arg(long, value_name = "HUMAN")]
        owner: Option<UserId>,
        /// Only threads with a message whose text contains TEXT, whatever the case; its first matching line is shown
        /// under the thread.
        #[arg(long, value_name = "TEXT")]
        grep: Option<String>,
        /// Only threads whose escalation reached a human with no human's message on the thread since, other than a
        /// tool request's; the escalation is shown under the thread. One sent before --since is not seen.
        #[arg(long)]
        escalated: bool,
        /// Only the threads pinned (`ants thread pin`), by default read from the earliest one's start.
        #[arg(long, conflicts_with = "archived")]
        pinned: bool,
        /// Only the threads archived (`ants thread archive`), by default read from the earliest one's start, or with
        /// `auto_archive` set from all the stream keeps, auto-archived threads included.
        #[arg(long)]
        archived: bool,
        /// How far back to read, such as 90m, 24h or 7d; 0 for everything the stream keeps. Each message in it is
        /// read, so a longer one takes longer. By default 24h.
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse)]
        since: Option<Duration>,
        /// At most this many threads, the most recently active.
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// One JSON thread per line.
        #[arg(long)]
        json: bool,
    },
    /// Show a thread's whole history in the order the stream stored it: kind, time, sender and recipient, and text.
    /// The tool calls its ants made show among them by when each started, as entries of kind tool: the ant, the
    /// call's title and, in brackets, its kind, status and duration.
    Show {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
        /// How far back to read; by default from the thread's start when its id is time-ordered (as generated ids
        /// are), else everything the stream keeps.
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse)]
        since: Option<Duration>,
        /// One JSON entry per line.
        #[arg(long)]
        json: bool,
        /// Leave out the tool calls the thread's ants made.
        #[arg(long)]
        no_tools: bool,
    },
    /// Find the messages stored on any thread that match every filter given, the latest --limit of them, printed
    /// oldest first: time, thread, kind, sender and recipient, message id and a line of text. Times without an offset
    /// are UTC, as printed; --since is inclusive, --until exclusive. Only the subjects the filters can match are read,
    /// from --since to --until.
    Search {
        /// Only messages whose text contains TEXT, whatever the case; its first matching line is shown.
        #[arg(long, value_name = "TEXT")]
        grep: Option<String>,
        /// Only messages this ant or human sent.
        #[arg(long, value_name = "SENDER", add = ArgValueCandidates::new(completions::ants))]
        from: Option<UserId>,
        /// Only messages to this ant or human: inbox messages, and the decisions and dead letters of this ant.
        #[arg(long, value_name = "RECIPIENT", add = ArgValueCandidates::new(completions::ants))]
        to: Option<UserId>,
        /// Only posts to this channel and the mentions telling of them.
        #[arg(long, value_name = "CHANNEL", add = ArgValueCandidates::new(completions::channels))]
        channel: Option<ChannelId>,
        /// Only messages of these kinds (repeat or separate with commas).
        #[arg(long = "type", value_name = "TYPE", value_enum, value_delimiter = ',')]
        kinds: Vec<thread::Kind>,
        /// From when: a duration back from now (90m, 2h, 3d; 0 for everything the stream keeps), an RFC 3339 time, or
        /// a date or date and time in UTC (2026-11-14, 2026-11-14 09:30); by default 24h before --until.
        #[arg(long, value_name = "TIME", value_parser = search::parse_when)]
        since: Option<search::When>,
        /// Until when, as --since takes it; by default now.
        #[arg(long, value_name = "TIME", value_parser = search::parse_when)]
        until: Option<search::When>,
        /// At most this many matches, the latest.
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// One JSON entry per line, as `ants thread show --json` prints them.
        #[arg(long)]
        json: bool,
    },
    /// Print a thread's title, or with TEXT title it, for every human: any human titles or retitles any thread.
    Title {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
        /// The title: one line of up to 120 characters.
        #[arg(conflicts_with = "clear")]
        text: Vec<String>,
        /// Remove the title.
        #[arg(long)]
        clear: bool,
    },
    /// Pin a thread, unarchiving it, for every device of the human: `ants thread list` lists it after the others.
    Pin {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
    },
    /// Unpin a thread.
    Unpin {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
    },
    /// Archive a thread, unpinning it, for every device of the human: `ants thread list` leaves it out until a
    /// message is stored on it. Its messages stay, and its work goes on.
    Archive {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
    },
    /// Unarchive a thread. One auto-archived for having been idle is kept until idle that long again.
    Unarchive {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
    },
    /// Hold back every notification `ants webhook serve` pushes to the CLI's human about a thread, on every device of
    /// the human, whatever the human's `bypass` lets through. Held notices are dropped, not sent later.
    Mute {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
        /// How long (default: 30d).
        #[arg(long = "for", value_name = "DURATION", value_parser = ants_proto::duration::parse)]
        duration: Option<Duration>,
    },
    /// Notify the CLI's human about a muted thread again.
    Unmute {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
    },
}

pub(crate) async fn run(nats: &Nats<'_>, command: ThreadCommand) -> Result<()> {
    match command {
        ThreadCommand::List { ant, owner, grep, escalated, pinned, archived, since, limit, json } => {
            let select =
                thread::Select { ant, owner, grep, limit: Some(limit), escalated, pinned, archived, idle_before: None };
            thread::list(nats, since, &select, json).await
        }
        ThreadCommand::Title { thread, text, clear } => thread_meta::title(nats, thread, &text, clear).await,
        ThreadCommand::Pin { thread } => thread_meta::mark(nats, thread, thread_meta::Marking::Pin).await,
        ThreadCommand::Unpin { thread } => thread_meta::mark(nats, thread, thread_meta::Marking::Unpin).await,
        ThreadCommand::Archive { thread } => thread_meta::mark(nats, thread, thread_meta::Marking::Archive).await,
        ThreadCommand::Unarchive { thread } => thread_meta::mark(nats, thread, thread_meta::Marking::Unarchive).await,
        ThreadCommand::Show { thread, since, json, no_tools } => {
            thread::show(nats, thread, since, json, no_tools).await
        }
        ThreadCommand::Search { grep, from, to, channel, kinds, since, until, limit, json } => {
            let kinds = kinds.into_iter().collect();
            let search = search::Search { from, to, channel, kinds, grep, limit, ..Default::default() };
            search::search(nats, search, since, until, json).await
        }
        ThreadCommand::Mute { thread, duration } => quiet::mute(nats, thread, duration).await,
        ThreadCommand::Unmute { thread } => quiet::unmute(nats, thread).await,
    }
}
