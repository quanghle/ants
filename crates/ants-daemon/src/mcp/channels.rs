//! The channel tools: `post_channel` starts a channel thread the ant creates, whose escalations go up the ant's
//! chain, and `manage_channel` asks the channel manager to change a channel the ant owns (see [`crate::chanadm`]).

use crate::bus::relayed::GuardedKv as _;
use std::sync::Arc;

use ants_proto::{
    CHANADM_WAIT, CHANNELS_BUCKET, ChannelAdminOutcome, ChannelAdminRequest, ChannelEntry, ChannelId, ChannelOp,
    ChannelPost, JsDomain, SESSIONS_BUCKET, ThreadId, UserId,
};
use anyhow::{Context as _, Result};
use futures::future::BoxFuture;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use super::Hub;
use super::messages::no_turns;
use super::tools::{Tools, text};
use crate::bus;
use crate::records::direct_kv::DirectKv;
use crate::sync::MutexExt as _;

/// What `post_channel` needs: a channel's entry, None when there is no such channel, and a store for posts, which
/// resolves once JetStream has the post.
#[derive(Clone)]
pub struct ChannelAccess {
    pub entry: Arc<dyn Fn(ChannelId) -> BoxFuture<'static, Result<Option<ChannelEntry>>> + Send + Sync>,
    pub post: ChannelWriter,
}

/// Store a channel post, with the originating inbox thread as private review evidence when given.
pub type ChannelWriter = Arc<dyn Fn(ChannelPost, Option<ThreadId>) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Reads the channels bucket and posts through JetStream.
pub fn jetstream_channels(js: async_nats::jetstream::Context, domain: Option<JsDomain>) -> ChannelAccess {
    let reader = js.clone();
    ChannelAccess {
        entry: Arc::new(move |channel| {
            let js = reader.clone();
            Box::pin(async move {
                let kv = js.get_key_value(CHANNELS_BUCKET).await.context("opening the channels")?;
                let Some(value) = kv.guarded_get(channel.as_str()).await.context("reading the channel")? else {
                    return Ok(None);
                };
                let entry: ChannelEntry = serde_json::from_slice(&value).context("reading the channel")?;
                entry.check_key(channel.as_str())?;
                Ok(Some(entry))
            })
        }),
        post: Arc::new(move |post, source| {
            let js = js.clone();
            let domain = domain.clone();
            Box::pin(async move {
                if let Some(thread) = source {
                    let sessions = DirectKv::new(&js, domain.as_ref(), SESSIONS_BUCKET).await?;
                    let key = crate::channel::review_source_key(&post.from, post.id);
                    sessions.put(key, thread.to_string()).await.context("storing the post's review evidence link")?;
                }
                bus::store(&js, post.subject(), &post, &post.id.to_string()).await.map(drop)
            })
        }),
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct PostChannel {
    /// Channel to post to, such as `ops` or `#ops`.
    channel: String,
    /// The post; an @mention in it wakes that member of the channel, or notifies it if a human.
    text: String,
    /// Files to attach: paths in the working directory, at most 8 files of up to 8 MiB each; secrets in text files are
    /// redacted.
    #[serde(default)]
    attach: Vec<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ManageChannel {
    /// The channel, such as `ops`; omitted to list the channels this ant owns.
    #[serde(default)]
    channel: Option<String>,
    /// Users to make members: ants in the colony, or humans.
    #[serde(default)]
    add: Vec<String>,
    /// Members to remove.
    #[serde(default)]
    remove: Vec<String>,
    /// The ant to triage humans' posts that mention nobody; it must be in the colony.
    #[serde(default)]
    messenger: Option<String>,
    /// Give the channel the default messenger again.
    #[serde(default)]
    clear_messenger: bool,
}

impl Hub {
    /// Post `text` to `channel` as the ant, a member or the channel's messenger: a top-level post starting a thread
    /// the ant creates, with no owner and the turns the message `thread` is handling leaves, as `send_message` spends
    /// them. Returns the new thread and whom the post wakes or notifies (its daemon sends the notices).
    async fn post_channel(
        &self,
        thread: &ThreadId,
        channel: &str,
        text: String,
        attach: &[String],
    ) -> Result<String, String> {
        let (current, source) = {
            let sessions = self.sessions.locked();
            let current = sessions.current.get(thread).cloned();
            let channel = sessions.origins.get(thread).is_some_and(|origin| origin.channel.is_some());
            (current, (!channel).then(|| thread.clone()))
        };
        let current = current.ok_or("no message is being handled on this thread; posts go out only while one is")?;
        let channel: ChannelId = channel.trim().trim_start_matches('#').parse().map_err(|e| format!("{e}"))?;
        let access = self.channels.get().ok_or("channels are not served here; post_channel is unavailable")?;
        let entry = (access.entry)(channel.clone())
            .await
            .map_err(|e| format!("cannot read #{channel}: {e:#}"))?
            .ok_or_else(|| format!("there is no channel #{channel}"))?;
        if !entry.is_member(&self.ant) && entry.messenger() != self.ant {
            return Err(format!(
                "{} is neither a member nor the messenger of #{channel}; only members and the messenger start \
                 threads there",
                self.ant
            ));
        }
        let left = current
            .reply(self.ant.clone(), "", self.humans.sender_role(&current))
            .map_err(|_| no_turns(thread, &current.from))?
            .turn_budget;
        let loaded = self.load(thread, attach).await?;
        let mut post = ChannelPost::new(channel.clone(), self.ant.clone(), text)
            .with_creator(Some(self.ant.clone()))
            .with_allow(current.allow.clone());
        post.turn_budget = left;
        let attachments = self.store(loaded, |id| post.attachment_handle(id)).await?;
        post.attachments = attachments;
        (access.post)(post.clone(), source).await.map_err(|e| format!("not posted: {e:#}"))?;
        let humans = self.humans.humans();
        let names = |users: Vec<UserId>| users.iter().map(UserId::as_str).collect::<Vec<_>>().join(", ");
        let (woken, notified) = (entry.woken(&post, &humans), entry.notified(&post, &humans));
        let wakes = match (woken.is_empty(), notified.is_empty()) {
            (true, true) => "it wakes nobody: only @mentions of members reach anyone".to_owned(),
            (false, true) => format!("it wakes {}", names(woken)),
            (true, false) => format!("it notifies {}", names(notified)),
            (false, false) => format!("it wakes {} and notifies {}", names(woken), names(notified)),
        };
        Ok(format!(
            "Posted to #{channel}, starting thread {} that {} created ({left} turns left); {wakes}.",
            post.thread_id, self.ant
        ))
    }

    /// What `manage_channel` does: list the channels the ant owns, show one, or ask the channel manager to change one.
    async fn manage_channel(&self, thread: &ThreadId, args: ManageChannel) -> Result<String, String> {
        let admin = self.admin.get().ok_or("channels cannot be managed from this daemon")?;
        let channels = async || admin.channels().await.map_err(|e| format!("cannot read the channels: {e:#}"));
        let Some(channel) = args.channel.as_deref().map(str::trim).filter(|c| !c.is_empty()) else {
            let owned: Vec<String> = channels()
                .await?
                .iter()
                .filter(|e| e.owner.as_ref() == Some(&self.ant))
                .map(describe_channel)
                .collect();
            if owned.is_empty() {
                return Ok(format!("{} owns no channel; a human makes an ant a channel's owner.", self.ant));
            }
            return Ok(format!("{} owns:\n{}", self.ant, owned.join("\n")));
        };
        let channel: ChannelId = channel.trim_start_matches('#').parse().map_err(|e| format!("{e}"))?;
        let ant = |s: &String| s.trim().trim_start_matches('@').parse::<UserId>().map_err(|e| format!("{e}"));
        let mut ops = Vec::new();
        for added in &args.add {
            ops.push(ChannelOp::AddMember { ant: ant(added)? });
        }
        for removed in &args.remove {
            ops.push(ChannelOp::RemoveMember { ant: ant(removed)? });
        }
        match (&args.messenger, args.clear_messenger) {
            (Some(_), true) => return Err("give either messenger or clear_messenger, not both".to_owned()),
            (Some(messenger), false) => ops.push(ChannelOp::SetMessenger { ant: ant(messenger)? }),
            (None, true) => ops.push(ChannelOp::ClearMessenger),
            (None, false) => {}
        }
        if ops.is_empty() {
            let entries = channels().await?;
            let entry = entries.iter().find(|e| e.channel == channel).ok_or(format!("no channel #{channel}"))?;
            return Ok(describe_channel(entry));
        }
        let request = ChannelAdminRequest::new(channel.clone(), self.ant.clone(), thread.clone(), ops);
        let reply = admin.request(request).await.map_err(|e| format!("cannot ask the channel manager: {e:#}"))?;
        let Some(reply) = reply else {
            return Err(format!(
                "no channel manager answered in {}s, so the change may not have been made; a human can make it with \
                 `ants channel add`, `remove` or `set`",
                CHANADM_WAIT.as_secs()
            ));
        };
        match reply.outcome {
            ChannelAdminOutcome::Applied { entry, changed, .. } if changed.is_empty() => {
                Ok(format!("Nothing to change. {}", describe_channel(&entry)))
            }
            ChannelAdminOutcome::Applied { entry, changed, .. } => {
                Ok(format!("Changed #{channel}: {}. {}", changed.join("; "), describe_channel(&entry)))
            }
            ChannelAdminOutcome::Refused { refusal } => Err(format!("#{channel} was not changed: {refusal}")),
        }
    }
}

#[tool_router(router = channels_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "Start a thread in `channel` with a top-level post, as its creator: the @mentioned \
        members of the channel wake and answer on the thread, and an escalation on the thread goes up this ant's \
        escalation chain, whichever ant gets stuck. Only a member or the messenger of the channel may post. The \
        thread has no owner and gets the turns the current message leaves, one fewer unless it is a human's; with \
        none left it is refused. `attach` lists files in the working directory to attach, as for send_message. \
        Returns the thread id.")]
    async fn post_channel(
        &self,
        Parameters(args): Parameters<PostChannel>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.post_channel(&thread, &args.channel, args.text, &args.attach).await))
    }

    #[tool(description = "Manage a channel this ant owns (a human makes an ant a channel's owner): `add` and \
        `remove` change its members, `messenger` sets the ant that triages humans' posts that mention nobody, and \
        `clear_messenger` gives it the default messenger again. Members are ants in the colony or humans; the \
        messenger is an ant in the colony. \
        The channel manager applies the change, refuses it unless this ant owns the channel, and announces it in \
        the channel. With only `channel` it shows the channel; with nothing it lists the channels this ant owns.")]
    async fn manage_channel(
        &self,
        Parameters(args): Parameters<ManageChannel>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.manage_channel(&thread, args).await))
    }
}

/// A channel's members, messenger, owner and description.
pub(super) fn describe_channel(entry: &ChannelEntry) -> String {
    let members = if entry.members.is_empty() {
        "none".to_owned()
    } else {
        entry.members.iter().map(UserId::as_str).collect::<Vec<_>>().join(", ")
    };
    let messenger = match &entry.messenger {
        Some(m) => m.to_string(),
        None => format!("{} (default)", UserId::messenger()),
    };
    let owner = entry.owner.as_ref().map_or_else(|| "none".to_owned(), UserId::to_string);
    let described = format!("#{}: members {members}; messenger {messenger}; owner {owner}.", entry.channel);
    match &entry.description {
        Some(description) => format!("{described}\nDescription: {description}"),
        None => described,
    }
}
