//! `ants channel`: list, create, read and set channels.

use std::collections::{HashMap, HashSet};

use ants_proto::{ChannelId, ChannelNotify, UserId};
use anyhow::{Result, bail};
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::channel::reads;
use crate::{Connection, Nats, channel, colony, completions, humans, thread, upgrade};

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
    },
    /// Replace the members or the messenger of a channel; what is not given stays.
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
    /// @mentioning it), with how many @mention it, and the other unread posts.
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
    /// replies on the thread, indented. A human's printed posts are marked read on all its devices.
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
pub(crate) async fn serve(conn: &Connection<'_>, upgrade: &upgrade::UpgradeArgs) -> Result<()> {
    Box::pin(upgrade::or_exit(channel::serve(conn.nats, conn.creds, conn.js_domain), upgrade)).await
}

pub(crate) async fn run(nats: &Nats<'_>, command: ChannelCommand) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    let kv = channel::open(&js).await?;
    let humans = humans::load(&js, &nats.me).await;
    match command {
        ChannelCommand::Create { channel, members, messenger, owner } => {
            channel::check_messenger_not_human(messenger.as_ref(), &humans)?;
            channel::check_owner_not_human(owner.as_ref(), &humans)?;
            let mut entry = ants_proto::ChannelEntry::new(channel);
            for member in members {
                entry.add_member(member);
            }
            entry.messenger = messenger;
            entry.owner = owner;
            channel::create(&kv, &entry).await?;
            warn_unserved(&js, entry.members.iter().chain(&entry.messenger).chain(&entry.owner), &humans).await;
            println!("created {}", channel::describe(&entry));
        }
        ChannelCommand::Set { channel, members, messenger, default_messenger, owner, no_owner } => {
            if members.is_none() && messenger.is_none() && !default_messenger && owner.is_none() && !no_owner {
                bail!("nothing to change: give --member, --messenger, --default-messenger, --owner or --no-owner");
            }
            channel::check_messenger_not_human(messenger.as_ref(), &humans)?;
            channel::check_owner_not_human(owner.as_ref(), &humans)?;
            let (entry, ()) = channel::update(&kv, &channel, |entry| {
                if let Some(members) = &members {
                    entry.members.clear();
                    for member in members {
                        entry.add_member(member.clone());
                    }
                }
                if messenger.is_some() || default_messenger {
                    entry.messenger.clone_from(&messenger);
                }
                if owner.is_some() || no_owner {
                    entry.owner.clone_from(&owner);
                }
            })
            .await?;
            warn_unserved(&js, members.iter().flatten().chain(&messenger).chain(&owner), &humans).await;
            println!("{}", channel::describe(&entry));
        }
        ChannelCommand::Add { channel, users } => {
            let (entry, added) = channel::update(&kv, &channel, |entry| {
                users.iter().filter(|p| entry.add_member((*p).clone())).cloned().collect::<Vec<_>>()
            })
            .await?;
            for user in users.iter().filter(|p| !added.contains(p)) {
                tracing::warn!("{user} is already a member of #{channel}");
            }
            warn_unserved(&js, &added, &humans).await;
            println!("{}", channel::describe(&entry));
        }
        ChannelCommand::Remove { channel, users } => {
            let (entry, removed) = channel::update(&kv, &channel, |entry| {
                users.iter().filter(|p| entry.remove_member(p)).cloned().collect::<Vec<_>>()
            })
            .await?;
            for user in users.iter().filter(|p| !removed.contains(p)) {
                tracing::warn!("{user} is no member of #{channel}");
            }
            println!("{}", channel::describe(&entry));
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
                Some(level) => println!("{}", crate::quiet::channel_notify(&js, &nats.me.id, &channel, level).await?),
                None => {
                    let level = crate::quiet::prefs(&js, &nats.me.id).await?.channel(&channel);
                    println!("#{channel} notifies {}: {level} ({})", nats.me.id, level.describe());
                }
            }
        }
        ChannelCommand::Read { channel, thread, limit, peek } => {
            read(nats, &js, &kv, &channel, thread, limit, peek).await?;
        }
        ChannelCommand::Delete { channel } => {
            channel::delete(&kv, &channel).await?;
            println!("deleted #{channel}; the posts to #{channel} stay in the stream");
        }
        ChannelCommand::Serve { .. } => unreachable!("`serve` runs the channel manager"),
    }
    Ok(())
}

/// `ants channel read`: print the posts of `channel` by thread, marking those printed read unless `peek`.
async fn read(
    nats: &Nats<'_>,
    js: &async_nats::jetstream::Context,
    kv: &async_nats::jetstream::kv::Store,
    channel: &ChannelId,
    thread: Option<thread::Pick>,
    limit: Option<usize>,
    peek: bool,
) -> Result<()> {
    if channel::get(kv, channel).await?.is_none() {
        tracing::warn!("no channel #{channel}; showing the posts kept for #{channel} anyway");
    }
    let thread = thread::resolve(thread, nats, js).await?;
    let posts = channel::read_from(js, &nats.me, channel, 0).await?;
    let mut threads = channel::threads(posts.iter().map(|(_, post)| post.clone()).collect());
    if let Some(thread) = &thread {
        threads.retain(|t| t.id == *thread);
    }
    if let Some(limit) = limit {
        threads.drain(..threads.len().saturating_sub(limit));
    }
    if threads.is_empty() {
        match thread {
            Some(thread) => println!("no posts on thread {thread} of #{channel}"),
            None => println!("no posts in #{channel}"),
        }
    }
    let behalf = crate::behalf::Checker::new(nats, js);
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

/// The unread posts of each channel among `entries` the human of `nats` is a member of, if its credentials may read
/// its channel read markers and it is a member of any; none, with a warning, when reading them fails.
async fn unread(
    nats: &Nats<'_>,
    js: &async_nats::jetstream::Context,
    entries: &[ants_proto::ChannelEntry],
) -> Option<HashMap<ChannelId, reads::Unread>> {
    if !entries.iter().any(|e| e.is_member(&nats.me.id)) || !reads::allowed(nats) {
        return None;
    }
    let me = &nats.me;
    let counted = async {
        let (markers, _) = reads::ChannelReads::open(js, &me.id).await?.markers().await?;
        let humans = crate::humans::read(js).await?;
        let prefs = crate::quiet::prefs(js, &me.id).await?;
        let mut unread = HashMap::new();
        for entry in entries.iter().filter(|e| e.is_member(&me.id)) {
            let level = prefs.channel(&entry.channel);
            unread
                .insert(entry.channel.clone(), reads::unread(js, me, &humans, &markers, &entry.channel, level).await?);
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
