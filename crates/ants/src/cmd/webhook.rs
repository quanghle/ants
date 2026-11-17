//! `ants webhook`: add webhooks, serve them, and test the notifications of a human.

mod add;

use std::net::SocketAddr;
use std::path::PathBuf;

use ants_proto::UserId;
use anyhow::{Context as _, Result, bail};
use clap::Subcommand;

use crate::connection::Connection;
use crate::serve::Ended;
use crate::{bridge, notify, reload, upgrade, webhook};

#[derive(Subcommand, Debug)]
pub(crate) enum WebhookCommand {
    /// On the hub, behind the TLS-terminating reverse proxy: take `POST <path>/<webhook>` for each webhook of the
    /// config file and deliver its events, each webhook as itself with its own credentials. Also push the tool
    /// requests and escalations of each `[[human]]` to its ntfy, Slack, Telegram, command (exec) or signed HTTP
    /// endpoint (http) sinks, with approve and reject links answered at `<path>/_approval`.
    Serve {
        /// The webhooks' config file (default: ~/.ants/webhooks.toml).
        #[arg(long)]
        config: Option<PathBuf>,
        /// Address for the reverse proxy (default: the file's `listen`, else 127.0.0.1:8424).
        #[arg(long)]
        listen: Option<String>,
        /// Serve GET /healthz and GET /metrics (Prometheus text format) on this address, such as 127.0.0.1:9464
        /// (default: the file's `metrics`, else off).
        #[arg(long, value_name = "ADDR")]
        metrics: Option<SocketAddr>,
        #[command(flatten)]
        upgrade: upgrade::UpgradeArgs,
    },
    /// Send a test notice through each sink (ntfy, slack, telegram, exec, http) of HUMAN in the config file, those of
    /// a device too, and print how each went; exits with status 1 when one fails. Needs no NATS.
    NotifyTest {
        /// A `[[human]]` of the config file.
        human: UserId,
        /// The webhooks' config file (default: ~/.ants/webhooks.toml).
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// List a new webhook in the config file in one step: issue its credentials (or ask for them over NATS with
    /// --enroll), write a new secret (or the one given, such as GitLab's signing token) to <name>.secret beside the
    /// config file (mode 0600), add its [[webhook]] entry, reload the webhook service, and print the URL and secret to
    /// give the sender.
    Add(Box<add::AddArgs>),
}

pub(crate) async fn run(command: WebhookCommand, conn: &Connection<'_>) -> Result<()> {
    let domain = conn.js_domain;
    match command {
        WebhookCommand::Add(args) => add::add(*args, conn.nats, domain).await,
        WebhookCommand::Serve { config, listen, metrics, upgrade } => {
            let path = match config {
                Some(path) => path,
                None => webhook::default_path()?,
            };
            let load = || {
                let mut config = webhook::Config::load(&path)?;
                config.metrics = metrics.or(config.metrics);
                config.listen = listen.clone().or(config.listen);
                Ok(config)
            };
            let config = load()?;
            let addr = config.listen.as_deref().unwrap_or(webhook::DEFAULT_LISTEN);
            let addr = bridge::listen_addr(addr, bridge::Listen::Any)?;
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("listening on {addr}"))?;
            tracing::info!("webhooks on http://{addr}{}/<webhook>", config.path);
            if config.links.is_some() {
                tracing::info!("approval links on http://{addr}{}/_approval", config.path);
            }
            let reload =
                webhook::Reload { triggers: reload::Triggers::new(path.clone(), None, reload::CHECK_EVERY)?, load };
            let (stopping, stopped) = tokio::sync::oneshot::channel();
            let stop = async move {
                let signal = async {
                    if let Err(e) = ants_daemon::daemon::stop_signal().await {
                        tracing::warn!(error = %e, "waiting for Ctrl-C or SIGTERM");
                        std::future::pending::<()>().await;
                    }
                };
                let ended = tokio::select! {
                    () = signal => Ended::Stopped,
                    () = upgrade::replaced(upgrade.upgrade_check) => Ended::Replaced,
                };
                let _ = stopping.send(());
                ended
            };
            // While the requests under way finish, another Ctrl-C or SIGTERM stops at once.
            let again = async {
                if stopped.await.is_err() || ants_daemon::daemon::stop_signal().await.is_err() {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                ended = webhook::serve(config, conn.nats, domain, listener, reload, stop) => match ended? {
                    Ended::Stopped => Ok(()),
                    Ended::Replaced => upgrade::exit(),
                },
                () = again => {
                    tracing::info!("stopping at once, cutting off the requests under way");
                    Ok(())
                }
            }
        }
        WebhookCommand::NotifyTest { human, config } => {
            let path = match config {
                Some(path) => path,
                None => webhook::default_path()?,
            };
            let config = webhook::Config::read(&path)?;
            let Some(entry) = config.humans.iter().find(|h| h.name == human) else {
                let names: Vec<&str> = config.humans.iter().map(|h| h.name.as_str()).collect();
                if names.is_empty() {
                    bail!("{} lists no [[human]]", path.display())
                } else {
                    bail!("{} lists no human {human}, only {}", path.display(), names.join(", "))
                }
            };
            let machine = ants_daemon::colony::machine().await;
            let results = notify::test(entry, &machine).await;
            for (sink, sent) in &results {
                match sent {
                    Ok(()) => println!("ok    {sink}"),
                    Err(e) => println!("fail  {sink}: {e:#}"),
                }
            }
            if let Some(quiet) = entry.quiet_hours(chrono::Utc::now()) {
                println!("{quiet}; `ants notify status` shows the snooze and muted threads");
            }
            let failed = results.iter().filter(|(_, sent)| sent.is_err()).count();
            match failed {
                0 => Ok(()),
                _ => bail!("{failed} of {} sinks of {human} failed", results.len()),
            }
        }
    }
}
