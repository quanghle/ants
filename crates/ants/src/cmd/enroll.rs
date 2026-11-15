//! `ants enroll`: approve, reject and renew the credentials ants and webhooks ask for.

use std::path::PathBuf;

use ants_proto::{ChannelId, Destination, UserId};
use anyhow::{Context as _, Result};
use clap::Subcommand;

use crate::{Connection, channel, enroll, renew, team_dir};

#[derive(Subcommand, Debug)]
pub(crate) enum EnrollCommand {
    /// List the enrollment requests heard, the latest of each ant and key; machines repeat theirs every few
    /// seconds while they wait.
    List {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Sign the credentials an ant asks for, with the rights `ants auth user` gives, for the hub's --js-domain if
    /// it has one, and send them to the asking machine. With --webhook, those of a webhook asking with
    /// `ants webhook add --enroll`.
    #[command(group = clap::ArgGroup::new("targets").args(["to", "channels"]).multiple(true))]
    Approve {
        /// The ant, or with --webhook the webhook.
        ant: UserId,
        /// The asking machine's public key, needed when several ask for the ant.
        #[arg(long, value_name = "KEY")]
        public_key: Option<String>,
        /// Sign a webhook's credentials, with the rights `ants auth user --webhook` gives: it may only message the
        /// ants of --to and post to the channels of --channel.
        #[arg(long, requires = "targets")]
        webhook: bool,
        /// With --webhook: an ant the webhook may message (repeatable).
        #[arg(long, value_name = "ANT", requires = "webhook")]
        to: Vec<UserId>,
        /// With --webhook: a channel the webhook may post to (repeatable).
        #[arg(long = "channel", value_name = "CHANNEL", value_parser = channel::parse_channel, requires = "webhook")]
        channels: Vec<ChannelId>,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// List the renewal requests heard (`ants renew`), the latest of each name and key.
    Renewals {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Renew the credentials NAME asks for (`ants renew`): valid, unrevoked credentials of the team, for the new key,
    /// with the same name, kind, members, device and --js-domain, and the permissions their kind has in this version.
    /// With --auto, renew every credentials asking, as they ask, until stopped.
    Renew {
        /// The name the credentials carry: an ant, a human, a webhook, an observer, channel-manager or leaf-<machine>.
        #[arg(required_unless_present = "auto")]
        name: Option<String>,
        /// The asking holder's new public key, needed when several ask for NAME.
        #[arg(long, value_name = "KEY", conflicts_with = "auto")]
        public_key: Option<String>,
        /// Keep running and renew every credentials whose holder asks, without an approval each.
        #[arg(long, conflicts_with = "name")]
        auto: bool,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Refuse the credentials an ant asks for, to every machine asking for it or the one of --public-key.
    Reject {
        ant: UserId,
        /// The asking machine's public key.
        #[arg(long, value_name = "KEY")]
        public_key: Option<String>,
        /// Reason shown to the asking machine.
        #[arg(long)]
        reason: Option<String>,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
}

pub(crate) async fn run(command: EnrollCommand, conn: &Connection<'_>) -> Result<()> {
    let domain = conn.js_domain;
    let dir = |dir| team_dir(dir);
    match command {
        EnrollCommand::List { dir: d } => {
            enroll::Approver::connect(conn.nats, &dir(d)?, domain, enroll::LISTEN).await?.list().await
        }
        EnrollCommand::Approve { ant, public_key, webhook: true, to, channels, dir: d } => {
            let dir = dir(d)?;
            let approver = enroll::Approver::connect(conn.nats, &dir, domain, enroll::LISTEN).await?;
            let destinations: Vec<Destination> =
                to.into_iter().map(Destination::Ant).chain(channels.into_iter().map(Destination::Channel)).collect();
            let key = approver.approve_webhook(&dir, &ant, &destinations, public_key.as_deref()).await?;
            println!("webhook {ant} approved for {key}");
            Ok(())
        }
        EnrollCommand::Approve { ant, public_key, dir: d, .. } => {
            let dir = dir(d)?;
            let approver = enroll::Approver::connect(conn.nats, &dir, domain, enroll::LISTEN).await?;
            let approved = approver.approve(&dir, &ant, public_key.as_deref()).await?;
            println!("{ant} approved for {}", approved.public_key);
            if let Some(leaf) = &approved.leaf {
                println!("the uplink credentials of {} renewed to carry {ant}", leaf.machine);
                match leaf.outbox {
                    true => {
                        println!("the hub's stream takes {ant}'s messages from the outbox of {}", leaf.machine)
                    }
                    false => {
                        println!("{} has no outbox (`ants leaf outbox`) to admit {ant} from", leaf.machine)
                    }
                }
            }
            Ok(())
        }
        EnrollCommand::Renewals { dir: d } => {
            let approver = enroll::Approver::connect(conn.nats, &dir(d)?, domain, enroll::LISTEN).await?;
            approver.list_renewals().await
        }
        EnrollCommand::Renew { name: None, auto: true, dir: d, .. } => {
            let dir = dir(d)?;
            eprintln!("renewing credentials as their holders ask (Ctrl-C stops)");
            let stop = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let (listen, reconnect) = (enroll::LISTEN, renew::AUTO_RECONNECT);
            renew::auto(conn.nats, &dir, domain, listen, reconnect, renew::print_renewed, stop).await
        }
        EnrollCommand::Renew { name, public_key, dir: d, .. } => {
            let dir = dir(d)?;
            let name = name.context("name the credentials to renew, or give --auto")?;
            let approver = enroll::Approver::connect(conn.nats, &dir, domain, enroll::LISTEN).await?;
            let (request, renewed) = approver.renew(&dir, &name, public_key.as_deref()).await?;
            renew::print_renewed(&request, &renewed);
            Ok(())
        }
        EnrollCommand::Reject { ant, public_key, reason, dir: d } => {
            let approver = enroll::Approver::connect(conn.nats, &dir(d)?, domain, enroll::LISTEN).await?;
            for key in approver.reject(&ant, public_key.as_deref(), reason.as_deref()).await? {
                println!("{ant} rejected for {key}");
            }
            Ok(())
        }
    }
}
