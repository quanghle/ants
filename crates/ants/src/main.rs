//! `ants`: talk to agent teams over NATS from the terminal, and run the ant daemon.

//! `ants`: talk to agent teams over NATS from the terminal, and run the ant daemon.

mod add;
mod age;
mod ant;
mod assistant;
mod attachments;
mod auth;
mod backup;
mod binding;
mod bridge;
mod channel;
mod cli;
mod cmd;
mod completions;
mod compose;
mod dash;
mod defaults;
mod devices;
mod doctor;
mod enroll;
mod github;
mod hook;
mod http;
mod humans;
mod inbox;
mod keys;
mod leaf;
mod live;
mod login;
mod manager;
mod members;
mod notify;
mod outbox;
mod presence;
mod progress;
mod queue;
mod quiet;
mod reacted;
mod registrar;
mod reload;
mod renew;
mod retire;
mod revoke;
mod roster;
mod scheduler;
mod sealed;
mod search;
mod serve;
mod service;
mod setup;
mod signer;
mod spend;
mod summary;
mod thread;
mod thread_meta;
mod tools;
mod tui;
mod upgrade;
mod versions;
mod webhook;
mod webhook_add;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ants_daemon::{bus, hub};
use ants_proto::{DeviceId, Escalation, JsDomain, UserId};
use anyhow::{Context as _, Result};

use crate::cli::{Cli, Command, HookCommand, NotifyCommand};
use crate::cmd::channel::ChannelCommand;
use crate::cmd::send;

fn main() -> Result<()> {
    completions::answer();
    run()
}

#[tokio::main(flavor = "current_thread")]
async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("ANTS_LOG").unwrap_or_else(|_| "info,rmcp=warn".into()),
        )
        .with_writer(progress::log_writer)
        .init();
    let cli = defaults::parse()?;
    let domain = cli.js_domain.as_ref();
    let conn = Connection { nats: &cli.nats, creds: cli.creds.as_deref(), js_domain: domain };
    let nats = || conn.nats();
    let Some(command) = cli.command else {
        tui::usage_without_terminal();
        return Box::pin(tui::run(&nats()?, tui::SINCE, cli.compact)).await;
    };
    match command {
        Command::Ask { thread, sending, replies, input, attach, words } => {
            Box::pin(send::ask(&nats()?, thread, sending, &replies, &input, &attach, words)).await
        }
        Command::Inbox { peek, thread, limit, follow, json } => {
            let select = inbox::Select { thread: None, limit, peek, summaries: false };
            Box::pin(cmd::inbox::show(&nats()?, select, thread, Listing { follow, json })).await
        }
        Command::Roster { json } => roster::show(&nats()?, json).await,
        Command::Ant(command) => Box::pin(cmd::ant::run(&nats()?, command)).await,
        Command::Spend { ant, thread, days, json, config } => {
            let path = match config {
                Some(path) => path,
                None => ants_daemon::config::default_path()?,
            };
            let caps = ants_daemon::config::Summary::load(&path)?.caps;
            spend::show(&nats()?, &spend::Select { ant, thread, days }, &caps, json).await
        }
        Command::Post { channel, thread, replies, input, attach, words } => {
            Box::pin(send::post(&nats()?, channel, thread, &replies, &input, &attach, words)).await
        }
        Command::Channel(ChannelCommand::Serve { upgrade }) => {
            Box::pin(upgrade::or_exit(channel::serve(&cli.nats, cli.creds.as_deref(), domain), &upgrade)).await
        }
        Command::Channel(command) => cmd::channel::run(&nats()?, command).await,
        Command::Notes { thread, attach, text } => send::thread_notes(&nats()?, thread, &attach, text).await,
        Command::React { message, reaction, remove } => send::react(&nats()?, message, reaction, remove).await,
        Command::Answer { message, answer } => send::answer(&nats()?, message, answer).await,
        Command::Thread(command) => cmd::thread::run(&nats()?, command).await,
        Command::Digest { since } => summary::digest(&nats()?, since).await,
        Command::Attachment(command) => Box::pin(cmd::attachment::run(&nats()?, command)).await,
        Command::Notify(NotifyCommand::Snooze { duration }) => quiet::snooze(&nats()?, duration).await,
        Command::Notify(NotifyCommand::Resume) => quiet::resume(&nats()?).await,
        Command::Notify(NotifyCommand::Status { json }) => quiet::status(&nats()?, json).await,
        Command::Assistant(command) => assistant::run(&nats()?, command).await,
        Command::Watch { subject, thread } => cmd::watch::watch(&nats()?, &subject, thread).await,
        Command::Chat { since, compact } => Box::pin(tui::run(&nats()?, since, compact)).await,
        Command::Status { tmux, json } => Box::pin(tui::status(&nats()?, tmux, json)).await,
        Command::Dash { since } => Box::pin(dash::run(&nats()?, since)).await,
        Command::Cancel { ant, thread } => cmd::cancel::cancel(&nats()?, ant, thread).await,
        Command::Steer { ant, thread, replies, input, attach, words } => {
            Box::pin(send::steer(&nats()?, &ant, thread, &replies, &input, &attach, words)).await
        }
        Command::Queue { ant, json } => queue::list(&nats()?, ant, json).await,
        Command::Withdraw { ant, message } => queue::withdraw(&nats()?, ant, message).await,
        Command::Init => {
            for line in init_hub(&nats()?).await? {
                println!("{line}");
            }
            Ok(())
        }
        Command::Setup(args) => Box::pin(setup::run(&args, &conn)).await,
        Command::Completions { shell, install } => completions::run(shell, install),
        Command::Tools(command) => cmd::tools::run(command, &conn).await,
        Command::Dead(command) => Box::pin(cmd::dead::run(&nats()?, command)).await,
        Command::Daemon(args) => cmd::daemon::run(&args, &conn).await,
        Command::Mcp { socket, token } => ants_daemon::mcp::bridge_stdio(&socket, &token).await,
        Command::Harness(command) => cmd::harness::run(command).await,
        Command::Doctor { config, leaf_config, no_harness, json, team, webhook_config } => {
            cmd::doctor::run(config, leaf_config, no_harness, json, team, webhook_config, &conn).await
        }
        Command::Auth(command) => cmd::auth::run(command, &conn).await,
        Command::Add(args) => add::add(*args, &cli.nats, domain).await,
        Command::Retire(args) => {
            let roster = retire_roster(&conn, &args.name).await;
            retire::retire(&args, roster.as_ref(), retire::STOP_WAIT, &cli.nats, domain).await
        }
        Command::Login(args) => login::run(args).await,
        Command::Renew(args) => renew::run(args, &cli.nats, cli.creds.as_deref()).await,
        Command::Keys(command) => keys::run(command, &cli.nats, cli.creds.as_deref()).await,
        Command::Signer { config, approval_timeout, dir, upgrade } => {
            cmd::signer::run(config, approval_timeout, dir, upgrade, &conn).await
        }
        Command::Enroll(command) => cmd::enroll::run(command, &conn).await,
        Command::Leaf(command) => cmd::leaf::run(command, &conn).await,
        Command::Bridge(command) => cmd::bridge::run(command).await,
        Command::Hook(HookCommand::Run { on, concurrency, timeout, command }) => {
            let nats = nats()?;
            let (_, js) = nats.connect_only("ants-hook").await?;
            hook::Hook { event: on, command, timeout, concurrency }.run(&js, &nats.me.id).await
        }
        Command::Webhook(command) => cmd::webhook::run(command, &conn).await,
    }
}

/// The roster `ants retire` waits on for `name` to stop, None (said) when it cannot be read.
async fn retire_roster(conn: &Connection<'_>, name: &impl std::fmt::Display) -> Option<async_nats::jetstream::Context> {
    let connected = match conn.nats() {
        Ok(nats) => nats.connect("ants-retire").await,
        Err(e) => Err(e),
    };
    match connected {
        Ok((_, js)) => Some(js),
        Err(e) => {
            println!("cannot read the roster ({e:#}): not waiting for {name} to stop");
            None
        }
    }
}

/// How the CLI reaches NATS.
pub struct Connection<'a> {
    pub nats: &'a str,
    pub creds: Option<&'a Path>,
    pub js_domain: Option<&'a JsDomain>,
}

impl<'a> Connection<'a> {
    fn nats(&self) -> Result<Nats<'a>> {
        Nats::new(self.nats, self.creds, self.js_domain)
    }
}

/// How a listing command prints: once or following new entries until interrupted, as text or one JSON value a line.
#[derive(Debug, Clone, Copy)]
pub struct Listing {
    pub follow: bool,
    pub json: bool,
}

pub struct Nats<'a> {
    url: &'a str,
    creds: Option<&'a Path>,
    domain: Option<&'a JsDomain>,
    pub me: humans::Me,
    /// The device of the human the credentials were issued for; none for an ant's, and without credentials.
    pub device: Option<DeviceId>,
    rights: humans::Rights,
    /// The account that signed the credentials, whose devices' keys alone the CLI takes from the directory
    /// ([`devices::Directory`]); none without credentials.
    pub account: Option<String>,
    /// The human's device of the credentials, if they bind an encryption key, which a connection puts in the directory.
    own_device: Option<devices::Own>,
    /// Whether a connection checked the hub's stream and buckets already ([`create_missing`]).
    hub_checked: AtomicBool,
    /// Whether a connection put [`Nats::own_device`] in the directory already.
    device_published: AtomicBool,
}

impl<'a> Nats<'a> {
    fn new(url: &'a str, creds: Option<&'a Path>, domain: Option<&'a JsDomain>) -> Result<Self> {
        let me = humans::Me::from_creds(creds)?;
        let device = humans::device(creds)?;
        let rights = humans::Rights::from_creds(creds)?;
        let account = devices::account_of(creds)?;
        // Credentials issued before the directory existed may not put in it: asking would only time out.
        let own_device = devices::Own::from_creds(creds)?
            .filter(|own| rights.may_publish(&ants_proto::subject::devices_of(&own.human)));
        Ok(Self {
            url,
            creds,
            domain,
            me,
            device,
            rights,
            account,
            own_device,
            hub_checked: AtomicBool::new(false),
            device_published: AtomicBool::new(false),
        })
    }

    /// Connect as [`Nats::me`], and register it in the humans bucket ([`humans::register_or_warn`]). The first
    /// connection of a human also creates the hub's stream and buckets that are missing ([`create_missing`]), and puts
    /// the user JWT of its device in the directory of encryption keys when it differs ([`devices::publish_or_warn`]).
    pub async fn connect(&self, name: &str) -> Result<(async_nats::Client, async_nats::jetstream::Context)> {
        let (client, js) = self.connect_only(name).await?;
        self.prepare(&js).await;
        Ok((client, js))
    }

    /// What [`Nats::connect`] does once connected: register [`Nats::me`], create what the hub lacks, and put the
    /// device's user JWT in the directory of encryption keys.
    async fn prepare(&self, js: &async_nats::jetstream::Context) {
        let create = async {
            if self.me.role.is_human() && !self.hub_checked.swap(true, Ordering::Relaxed) {
                create_missing(js, &self.rights).await;
            }
        };
        let publish = async {
            if let Some(own) = &self.own_device
                && !self.device_published.swap(true, Ordering::Relaxed)
            {
                devices::publish_or_warn(js, own).await;
            }
        };
        tokio::join!(humans::register_or_warn(js, &self.me), create, publish);
    }

    /// Connect as [`Nats::me`] without registering it, naming the connection after its device, if any, too.
    async fn connect_only(&self, name: &str) -> Result<(async_nats::Client, async_nats::jetstream::Context)> {
        let name = match &self.device {
            Some(device) => format!("{name} {device}"),
            None => name.to_owned(),
        };
        bus::connect(self.url, self.creds, self.domain, &self.me.id, &name).await
    }
}

/// `ants init`: create the hub's stream and buckets that are missing, register the human of the credentials and put
/// the user JWT of its device in the directory of encryption keys.
/// Returns what was done, a line each.
async fn init_hub(nats: &Nats<'_>) -> Result<Vec<String>> {
    let (_, js) = nats.connect_only("ants-cli").await?;
    let fixed = hub::init(&js, |subject| nats.rights.may_publish(subject)).await;
    let fixed = fixed.context("`ants init` needs a human's credentials (`ants auth user NAME --human`)")?;
    let mut lines: Vec<String> = fixed.iter().map(hub::Finding::fixed).collect();
    humans::register_or_warn(&js, &nats.me).await;
    if let Some(own) = &nats.own_device {
        devices::publish_or_warn(&js, own).await;
    }
    lines.push(format!(
        "stream {}, buckets {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} and {}, and store {} ready",
        ants_proto::STREAM,
        ants_proto::ROSTER_BUCKET,
        ants_proto::SESSIONS_BUCKET,
        ants_proto::ATTEMPTS_BUCKET,
        ants_proto::CHANNELS_BUCKET,
        ants_proto::HUMANS_BUCKET,
        ants_proto::CHAINS_BUCKET,
        ants_proto::LINKS_BUCKET,
        ants_proto::OWNERS_BUCKET,
        ants_proto::WEBHOOKS_BUCKET,
        ants_proto::REACTIONS_BUCKET,
        ants_proto::THREADS_BUCKET,
        ants_proto::READ_BUCKET,
        ants_proto::PRESENCE_BUCKET,
        ants_proto::NOTIFY_BUCKET,
        ants_proto::DEVICES_BUCKET,
        ants_proto::FILES_BUCKET
    ));
    Ok(lines)
}

/// Serve health and metrics from `source` on `addr`, if given, for as long as the result is kept.
async fn serve_metrics(
    addr: Option<SocketAddr>,
    source: Arc<dyn ants_daemon::metrics::Source>,
) -> Result<Option<ants_daemon::metrics::Listening>> {
    match addr {
        Some(addr) => Ok(Some(ants_daemon::metrics::start(addr, source).await?)),
        None => Ok(None),
    }
}

/// Create the hub's stream and buckets that are missing, add the subjects the stream lacks and widen a duplicate window
/// too short, as `ants init` does ([`hub::check`]), saying what was fixed; an object otherwise stays as it is. Those
/// the credentials may not fix are reported as needing `ants init`; those whose stream info the credentials do not
/// get, or that cannot be checked (the hub out of reach), are left alone. Returns what was fixed.
async fn create_missing(js: &async_nats::jetstream::Context, rights: &humans::Rights) -> Vec<hub::Finding> {
    let mut created = Vec::new();
    for finding in hub::check(js, |subject| rights.may_publish(subject)).await {
        if let hub::Problem::Unchecked(_) = finding.problem {
            tracing::debug!("{finding}");
        } else if !finding.rights().iter().all(|subject| rights.may_publish(subject)) {
            tracing::warn!("{finding}; `ants init` with a human's current credentials (`ants auth user`) fixes it");
        } else {
            match hub::apply(js, &finding).await {
                Ok(()) => {
                    tracing::info!("{}", finding.fixed());
                    created.push(finding);
                }
                Err(e) => tracing::warn!("{finding}; `ants init` as a human fixes it: {e:#}"),
            }
        }
    }
    created
}

fn team_dir(dir: Option<PathBuf>) -> Result<PathBuf> {
    match dir {
        Some(dir) => Ok(dir),
        None => auth::default_dir(),
    }
}

/// The first line of `text`, cut to `max` characters.
fn excerpt(text: &str, max: usize) -> String {
    let line = text.lines().next().unwrap_or("");
    let mut out: String = line.chars().take(max).collect();
    if out.len() < text.len() {
        out.push('…');
    }
    out
}

/// An escalation in one line: `escalation level 1, turn_budget: dev-a stuck on t1, leader lead, visited dev-a > lead`.
fn escalation_note(x: &Escalation) -> String {
    let reason = match x.request_id {
        Some(id) => format!("{} {}/{id}", x.reason.as_str(), x.stuck),
        None => x.reason.as_str().to_owned(),
    };
    let visited = x.visited.iter().map(UserId::as_str).collect::<Vec<_>>().join(" > ");
    let (level, stuck, thread, leader) = (x.level, &x.stuck, &x.thread_id, &x.leader);
    let away = x.away.as_ref().map_or_default(|a| format!(", relayed while {} is away: answer {}", a.human, a.ant));
    format!("escalation level {level}, {reason}: {stuck} stuck on {thread}, leader {leader}, visited {visited}{away}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A human's credentials create what the hub lacks, leaving what is there as it is; an ant's ask for nothing they
    /// are denied, which would only time out, and create nothing.
    #[tokio::test]
    async fn a_human_creates_what_the_hub_lacks() {
        let Some(server) = auth::tests::Server::start() else { return };
        let connect = async |user: &UserId, role| {
            let creds = server.creds_as(user, role);
            let (_, js) = bus::connect(&server.url, Some(&creds), None, user, "test").await.unwrap();
            (js, humans::Rights::from_creds(Some(&creds)).unwrap())
        };
        let (a_js, a_rights) = connect(&"dev-a".parse().unwrap(), ants_proto::Role::Ant).await;
        assert!(!a_rights.may_publish(&hub::Object::Sessions.info_subject()));
        let started = tokio::time::Instant::now();
        assert!(create_missing(&a_js, &a_rights).await.is_empty());
        assert!(started.elapsed() < Duration::from_secs(2), "no request was denied");

        let (h_js, h_rights) = connect(&"alice".parse().unwrap(), ants_proto::Role::Human).await;
        bus::ensure_stream(&h_js).await.unwrap();
        let created: Vec<_> = create_missing(&h_js, &h_rights).await.into_iter().map(|f| f.object).collect();
        let seen: Vec<_> = hub::Object::ALL
            .into_iter()
            .filter(|o| ![hub::Object::Stream, hub::Object::Presence].contains(o))
            .collect();
        assert_eq!(created, seen, "every missing bucket a human may look up created");
        for watched in [hub::Object::Channels, hub::Object::Humans, hub::Object::Roster] {
            let config = h_js.get_stream(watched.stream_name()).await.unwrap().cached_info().config.clone();
            assert_eq!(config.max_consumers, hub::MAX_CONSUMERS, "{watched}");
        }
        assert!(hub::check(&h_js, |s| h_rights.may_publish(s)).await.is_empty());
        assert!(create_missing(&h_js, &h_rights).await.is_empty());
    }
}
