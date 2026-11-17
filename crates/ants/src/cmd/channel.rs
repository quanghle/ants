//! `ants channel`: list, create, read and set channels.

use std::collections::{HashMap, HashSet};

use ants_proto::{ChannelId, ChannelNotify, UserId};
use anyhow::{Result, bail};
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::channel::reads;
use crate::connection::{Connection, Nats};
use crate::people::{humans, quiet};
use crate::release::upgrade;
use crate::threads::{thread, thread_meta};
use crate::{channel, colony, completions};

#[derive(Subcommand, Debug)]
pub(crate) enum ChannelCommand {
    /// Create a channel; fails when the channel exists.
    Create {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Member, an ant or a human (repeatable, or comma-separated).
        #[arg(long = "member", value_name = "USER", value_delimiter = ',', value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        members: Vec<UserId>,
        /// Ant that triages the posts of humans that @mention nobody (default: `messenger`).
        #[arg(long, value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        messenger: Option<UserId>,
        /// Ant that owns the channel: it changes the members and the messenger through `ants channel serve`.
        #[arg(long, value_name = "ANT", value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        owner: Option<UserId>,
        /// What the channel is for, up to 1000 characters: each member ant's session on a thread of the channel
        /// starts with it.
        #[arg(long, value_name = "TEXT")]
        description: Option<String>,
    },
    /// Replace the members, the messenger, the owner or the description of a channel; what is not given stays.
    Set {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
        /// Member, an ant or a human (repeatable, or comma-separated): together, the new members.
        #[arg(long = "member", value_name = "USER", value_delimiter = ',', value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        members: Option<Vec<UserId>>,
        /// Ant that triages the posts of humans that @mention nobody.
        #[arg(long, value_parser = channel::parse_ant, conflicts_with = "default_messenger", add = ArgValueCandidates::new(completions::ants))]
        messenger: Option<UserId>,
        /// Hand the triage back to the default messenger, the ant `messenger` (triage stays on).
        #[arg(long)]
        default_messenger: bool,
        /// Ant that owns the channel: it changes the members and the messenger through `ants channel serve`.
        #[arg(long, value_name = "ANT", value_parser = channel::parse_ant, conflicts_with = "no_owner", add = ArgValueCandidates::new(completions::ants))]
        owner: Option<UserId>,
        /// Leave the channel without an owner.
        #[arg(long)]
        no_owner: bool,
        /// What the channel is for, up to 1000 characters: sessions started from then on take it in, while running
        /// ones keep the one they started with.
        #[arg(long, value_name = "TEXT", conflicts_with = "no_description")]
        description: Option<String>,
        /// Leave the channel without a description.
        #[arg(long)]
        no_description: bool,
    },
    /// Add members, ants or humans, to a channel. An @mention of a human member puts a notice in the human's inbox.
    Add {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
        /// Ants or humans: `dev-a` or `@dev-a`.
        #[arg(required = true, num_args = 1.., value_name = "USER", value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        users: Vec<UserId>,
    },
    /// Remove members from a channel.
    Remove {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
        /// Ants or humans: `dev-a` or `@dev-a`.
        #[arg(required = true, num_args = 1.., value_name = "USER", value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        users: Vec<UserId>,
    },
    /// List the channels with the members, the messenger and the owner of each; for a human member of any, also the
    /// unread posts its notify level of each channel it is a member of counts (by default, those by humans or
    /// @mentioning it), with how many @mention it, and the other unread posts, those on archived threads left out.
    List,
    /// Show or set which unread posts of a channel count for the CLI's human, on all its devices: `all`, `humans`
    /// (posts by humans and @mentions, the default) or `mentions` (@mentions only). The others show apart and dim.
    Notify {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
        /// The level: `all`, `humans` or `mentions`; without one, show the channel's.
        level: Option<ChannelNotify>,
    },
    /// Print the posts of a channel kept in the stream, by thread: the post that started each thread, then the
    /// replies on the thread, indented. A human's printed posts are marked read on all its devices. The threads the
    /// human archived (`ants thread archive`) are left out, but for those with a post since; with `auto_archive` set
    /// in the config file's `[cli]`, so are those with no post for that long.
    Read {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
        /// Only this thread: its id, a unique start of one, or `last`.
        #[arg(long, value_name = "THREAD")]
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: Option<thread::Pick>,
        /// Only the last this many threads.
        #[arg(long, conflicts_with = "thread")]
        limit: Option<usize>,
        /// Only the threads left out as archived.
        #[arg(long, conflicts_with = "thread")]
        archived: bool,
        /// Print without marking read.
        #[arg(long)]
        peek: bool,
    },
    /// Delete a channel. The posts to the channel stay in the stream.
    Delete {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
    },
    /// Run the channel manager, where no ant runs (on the hub, say), with the credentials of
    /// `ants auth channel-manager` as --creds: it applies the changes owners ask for, refuses the others, and posts
    /// each change in its channel.
    Serve {
        #[command(flatten)]
        upgrade: upgrade::UpgradeArgs,
    },
}

/// `ants channel serve`, which runs as the channel manager rather than as a human.
async fn serve(conn: &Connection<'_>, upgrade: &upgrade::UpgradeArgs) -> Result<()> {
    Box::pin(upgrade::or_exit(channel::serve(conn.nats, conn.creds, conn.js_domain), upgrade)).await
}

pub(crate) async fn run(conn: &Connection<'_>, command: ChannelCommand) -> Result<()> {
    match command {
        ChannelCommand::Serve { upgrade } => serve(conn, &upgrade).await,
        command => manage(&conn.nats()?, command).await,
    }
}

/// Every `ants channel` subcommand but `serve`, as the CLI's user.
async fn manage(nats: &Nats<'_>, command: ChannelCommand) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    let kv = channel::open(&js).await?;
    let humans = humans::load(&js, &nats.me).await;
    match command {
        ChannelCommand::Create { channel, members, messenger, owner, description } => {
            let change = channel::Change {
                add: members,
                messenger: Some(messenger),
                owner: Some(owner),
                description: Some(description),
                ..channel::Change::default()
            };
            change.check(&channel, &humans)?;
            let mut entry = ants_proto::ChannelEntry::new(channel);
            change.apply(&mut entry);
            channel::create(&kv, &entry).await?;
            warn_unserved(&js, entry.members.iter().chain(&entry.messenger).chain(&entry.owner), &humans).await;
            println!("created {}", channel::describe(&entry));
        }
        command @ ChannelCommand::Set { .. } => set(&js, &kv, &humans, command).await?,
        ChannelCommand::Add { channel, users } => {
            write(&js, &kv, &humans, &channel, &channel::Change { add: users, ..channel::Change::default() }).await?;
        }
        ChannelCommand::Remove { channel, users } => {
            write(&js, &kv, &humans, &channel, &channel::Change { remove: users, ..channel::Change::default() })
                .await?;
        }
        ChannelCommand::List => {
            let entries = channel::list(&kv).await?;
            if entries.is_empty() {
                println!("no channels");
            } else {
                let unread = unread(nats, &js, &entries).await;
                println!("{}", channel::table(&entries, unread.as_ref()));
            }
        }
        ChannelCommand::Notify { channel, level } => {
            if !nats.me.role.is_human() {
                bail!("a channel's notify level needs a human's credentials; those of {} are an ant's", nats.me.id);
            }
            if channel::get(&kv, &channel).await?.is_none() {
                tracing::warn!("no channel #{channel}");
            }
            match level {
                Some(level) => println!("{}", quiet::channel_notify(&js, &nats.me.id, &channel, level).await?),
                None => {
                    let level = quiet::prefs(&js, &nats.me.id).await?.channel(&channel);
                    println!("#{channel} notifies {}: {level} ({})", nats.me.id, level.describe());
                }
            }
        }
        ChannelCommand::Read { channel, thread, limit, archived, peek } => {
            read(nats, &js, &kv, &channel, Pick { thread, limit, archived }, peek).await?;
        }
        ChannelCommand::Delete { channel } => {
            channel::delete(&kv, &channel).await?;
            println!("deleted #{channel}; the posts to #{channel} stay in the stream");
        }
        ChannelCommand::Serve { .. } => unreachable!("`serve` runs the channel manager"),
    }
    Ok(())
}

/// `ants channel set`: change what `command` gives of a channel.
async fn set(
    js: &async_nats::jetstream::Context,
    kv: &async_nats::jetstream::kv::Store,
    humans: &ants_proto::Humans,
    command: ChannelCommand,
) -> Result<()> {
    let ChannelCommand::Set {
        channel,
        members,
        messenger,
        default_messenger,
        owner,
        no_owner,
        description,
        no_description,
    } = command
    else {
        unreachable!("`set` runs only `ants channel set`");
    };
    let change = channel::Change {
        members,
        messenger: (messenger.is_some() || default_messenger).then_some(messenger),
        owner: (owner.is_some() || no_owner).then_some(owner),
        description: (description.is_some() || no_description).then_some(description),
        ..channel::Change::default()
    };
    if change.is_empty() {
        bail!(
            "nothing to change: give --member, --messenger, --default-messenger, --owner, --no-owner, \
             --description or --no-description"
        );
    }
    write(js, kv, humans, &channel, &change).await
}

/// Write `change` to `channel`, warning of what it warns of, and print the channel as it stored it.
async fn write(
    js: &async_nats::jetstream::Context,
    kv: &async_nats::jetstream::kv::Store,
    humans: &ants_proto::Humans,
    channel: &ChannelId,
    change: &channel::Change,
) -> Result<()> {
    let changed = change.write(js, kv, channel, humans).await?;
    for warning in &changed.warnings {
        tracing::warn!("{warning}");
    }
    println!("{}", channel::describe(&changed.entry));
    Ok(())
}

/// Which threads `ants channel read` prints.
struct Pick {
    thread: Option<thread::Pick>,
    limit: Option<usize>,
    /// Only those left out as archived, rather than only the others.
    archived: bool,
}

/// `ants channel read`: print the posts of `channel` by thread, as `pick` picks the threads, marking those printed
/// read unless `peek`.
async fn read(
    nats: &Nats<'_>,
    js: &async_nats::jetstream::Context,
    kv: &async_nats::jetstream::kv::Store,
    channel: &ChannelId,
    pick: Pick,
    peek: bool,
) -> Result<()> {
    if channel::get(kv, channel).await?.is_none() {
        tracing::warn!("no channel #{channel}; showing the posts kept for #{channel} anyway");
    }
    let thread = thread::resolve(pick.thread, nats, js).await?;
    let posts = channel::read_from(js, &nats.me, channel, 0).await?;
    let mut threads = channel::threads(posts.iter().map(|(_, post)| post.clone()).collect());
    let mut left_out = 0;
    if let Some(thread) = &thread {
        threads.retain(|t| t.id == *thread);
    } else if let Some(archive) = archive(nats, js, pick.archived).await? {
        let hidden = archive.hidden(posts.iter().map(|(seq, post)| (*seq, post)));
        let all = threads.len();
        threads.retain(|t| hidden.contains(&t.id) == pick.archived);
        left_out = all - threads.len();
    }
    if let Some(limit) = pick.limit {
        threads.drain(..threads.len().saturating_sub(limit));
    }
    if threads.is_empty() {
        match thread {
            Some(thread) => println!("no posts on thread {thread} of #{channel}"),
            None if pick.archived => println!("no archived threads in #{channel}"),
            None if left_out > 0 => println!("no posts in #{channel} but on {left_out} archived threads (--archived)"),
            None => println!("no posts in #{channel}"),
        }
    }
    let behalf = crate::people::behalf::Checker::new(nats, js);
    let mut by = std::collections::HashMap::new();
    for post in threads.iter().flat_map(|t| &t.posts) {
        by.insert(post.id, behalf.label(&post.from, post.on_behalf_of.as_ref()).await);
    }
    print!("{}", channel::render(&threads, &|post| by.get(&post.id).cloned().unwrap_or_else(|| post.from.to_string())));
    if !peek && reads::allowed(nats) {
        mark_read(js, &nats.me.id, channel, &posts, &threads).await;
    }
    Ok(())
}

/// What leaves the archived and idle threads of the CLI's human out of `ants channel`: none for an ant's credentials,
/// or, with a warning, when the human's marks cannot be read, unless `required`, which fails then.
async fn archive(
    nats: &Nats<'_>,
    js: &async_nats::jetstream::Context,
    required: bool,
) -> Result<Option<channel::Archive>> {
    if !nats.me.role.is_human() {
        if required {
            bail!("archived threads are a human's; the credentials of {} are an ant's", nats.me.id);
        }
        return Ok(None);
    }
    let idle_before = thread::idle_before(crate::defaults::auto_archive()?, chrono::Utc::now());
    match async { thread_meta::open(nats, js).await?.marks().await }.await {
        Ok(marks) => Ok(Some(channel::Archive { marks, idle_before })),
        Err(e) if !required => {
            tracing::warn!("not leaving archived threads out: {e:#}");
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// The unread posts of each channel among `entries` the human of `nats` is a member of, but for those on its
/// archived threads, if its credentials may read its channel read markers and it is a member of any; none, with a
/// warning, when reading them fails.
async fn unread(
    nats: &Nats<'_>,
    js: &async_nats::jetstream::Context,
    entries: &[ants_proto::ChannelEntry],
) -> Option<HashMap<ChannelId, reads::Unread>> {
    if !entries.iter().any(|e| e.is_member(&nats.me.id)) || !reads::allowed(nats) {
        return None;
    }
    let me = &nats.me;
    let archive = archive(nats, js, false).await.inspect_err(|e| tracing::warn!("{e:#}")).ok().flatten();
    let counted = async {
        let (markers, _) = reads::ChannelReads::open(js, &me.id).await?.markers().await?;
        let humans = crate::people::humans::read(js).await?;
        let prefs = quiet::prefs(js, &me.id).await?;
        let mut unread = HashMap::new();
        for entry in entries.iter().filter(|e| e.is_member(&me.id)) {
            let level = prefs.channel(&entry.channel);
            let counted = reads::unread(js, me, &humans, &markers, &entry.channel, level, archive.as_ref()).await?;
            unread.insert(entry.channel.clone(), counted);
        }
        anyhow::Ok(unread)
    };
    counted.await.inspect_err(|e| tracing::warn!("not counting the unread posts: {e:#}")).ok()
}

/// Mark the posts of `threads`, printed from `posts` of `channel`, every post of it kept, read for `human`; warns when
/// that fails.
async fn mark_read(
    js: &async_nats::jetstream::Context,
    human: &UserId,
    channel: &ChannelId,
    posts: &[(u64, ants_proto::ChannelPost)],
    threads: &[channel::Thread],
) {
    let printed: HashSet<&ants_proto::ThreadId> = threads.iter().map(|t| &t.id).collect();
    let marks: Vec<u64> = posts.iter().filter(|(_, p)| printed.contains(&p.thread_id)).map(|(seq, _)| *seq).collect();
    let marked = async {
        reads::ChannelReads::open(js, human)
            .await?
            .change(|markers| {
                for seq in &marks {
                    markers.mark(channel, *seq);
                }
                // Every post kept was read, so what the marker covers has no gap.
                markers.compact(channel, posts.iter().map(|(seq, _)| *seq));
            })
            .await
    };
    if let Err(e) = marked.await {
        tracing::warn!("not marking the posts of #{channel} read: {e:#}");
    }
}

/// Warn about members, messengers and owners no daemon serves now ([`colony::unserved`]), rather than refuse them:
/// one may start later. `humans` need none.
pub(crate) async fn warn_unserved<'a>(
    js: &async_nats::jetstream::Context,
    users: impl IntoIterator<Item = &'a UserId>,
    humans: &ants_proto::Humans,
) {
    let users: Vec<&UserId> = users.into_iter().filter(|u| !humans.contains(u)).collect();
    for warning in colony::unserved_warnings(js, &users).await {
        tracing::warn!("{warning}");
    }
}
