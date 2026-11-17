//! `ants leaf`: a machine's leaf node, and its outbox on the hub.

use std::path::PathBuf;

use ants_proto::Machine;
use anyhow::{Context as _, Result};
use clap::Subcommand;

use crate::auth::team_dir;
use crate::connection::Connection;
use crate::{auth, leaf, members, natsd, outbox};

#[derive(Subcommand, Debug)]
pub(crate) enum LeafCommand {
    /// Write the nats-server configuration of the machine's leaf node: listening on localhost only, with one uplink
    /// to the hub, and a JetStream of its own (domain: the machine name) for the outbox. Run it again to switch the
    /// uplink: it reloads the leaf node service (ants.nats, ants-nats) running the file, or says how.
    Config {
        /// Machine name, as given to `ants auth leaf`.
        machine: Machine,
        /// Hub URL: nats-leaf:// or tls:// for its leafnode port (also through `devtunnel connect`), ws:// or wss://
        /// for its websocket listener (a leaf node asks for <path>/leafnode). Repeatable, all of one scheme.
        #[arg(long = "uplink", value_name = "URL", required = true)]
        uplinks: Vec<String>,
        /// Uplink credentials from `ants auth leaf` (default: <dir>/leaf-<machine>.creds).
        #[arg(long)]
        uplink_creds: Option<PathBuf>,
        /// System account credentials from `ants auth leaf`, for a second uplink bringing the account updates
        /// `ants auth push` sends the hub, revocations included (default: leaf-<machine>.system.creds beside the
        /// uplink credentials, when there).
        #[arg(long)]
        system_creds: Option<PathBuf>,
        /// The team's server.conf from `ants auth init` (default: <dir>/server.conf).
        #[arg(long)]
        server_conf: Option<PathBuf>,
        /// Team directory holding server.conf and the credentials (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// CA certificate of a hub the platform does not trust (tls:// and wss:// uplinks).
        #[arg(long)]
        ca_file: Option<PathBuf>,
        /// HTTP CONNECT forward proxy for the uplink, http://host:port or https://host:port.
        #[arg(long, value_name = "URL")]
        proxy: Option<String>,
        /// Address for the machine's ants processes; loopback only.
        #[arg(long, default_value = leaf::DEFAULT_LISTEN)]
        listen: String,
        /// Where to write the configuration (default: ~/.config/ants/leaf.conf).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// As a human, with the leaf node of the machine up and connected: create the machine's outbox, which keeps
    /// what its ants and humans send while the hub is out of reach, and have the hub's stream take from it what
    /// they may send once the uplink is back, through the outbox's durable consumer, so the hub goes on within
    /// seconds of the leaf node or the hub returning. Run it again when the machine's ants or humans change; without
    /// members, it takes those its uplink credentials carry, and to have the hub ask for the outbox at once after it
    /// stopped hearing from it. Fails if the hub does not hear from the outbox within 10 s, as without
    /// `feature_flags { js_api_reply_v2: true }` on the hub.
    Outbox {
        /// Machine name, as given to `ants leaf config`.
        machine: Machine,
        #[command(flatten)]
        members: members::OutboxArgs,
        /// Leaf node configuration whose uplink credentials name the members when --ant and --human do not
        /// (default: ~/.config/ants/leaf.conf).
        #[arg(long, value_name = "FILE")]
        leaf_config: Option<PathBuf>,
        /// Stop taking anything from the machine's outbox instead.
        #[arg(long, conflicts_with_all = ["ants", "humans", "add_ants", "add_humans", "remove_ants", "remove_humans", "leaf_config"])]
        remove: bool,
    },
}

pub(crate) async fn run(conn: &Connection<'_>, command: LeafCommand) -> Result<()> {
    let domain = conn.js_domain;
    let nats = || conn.nats();
    match command {
        LeafCommand::Config {
            machine,
            uplinks,
            uplink_creds,
            system_creds,
            server_conf,
            dir,
            ca_file,
            proxy,
            listen,
            out,
        } => {
            let dir = team_dir(dir)?;
            let creds = uplink_creds.unwrap_or_else(|| dir.join(format!("leaf-{machine}.creds")));
            let system_creds = system_creds.or_else(|| Some(auth::leaf_system_creds(&creds)).filter(|p| p.is_file()));
            if system_creds.is_none() {
                println!(
                    "no system account credentials at {}: revocations pushed to the hub do not reach this leaf; issue \
                     them with `ants auth leaf` and run this again",
                    auth::leaf_system_creds(&creds).display()
                );
            }
            let server_conf = server_conf.unwrap_or_else(|| dir.join(auth::SERVER_CONF));
            let default_out = out.is_none();
            let out = match out {
                Some(out) => out,
                None => auth::default_dir()?.with_file_name("leaf.conf"),
            };
            let config = leaf::Leaf {
                machine: &machine,
                hub_domain: domain,
                uplinks: &uplinks,
                creds: &creds,
                system_creds: system_creds.as_deref(),
                server_conf: &server_conf,
                ca_file: ca_file.as_deref(),
                proxy: proxy.as_deref(),
                listen: &listen,
            };
            let old = std::fs::read_to_string(&out).ok();
            leaf::write(&config, &out)?;
            ants_daemon::creds::warn(&creds);
            system_creds.as_deref().map(ants_daemon::creds::warn);
            let new = std::fs::read_to_string(&out).with_context(|| format!("reading {}", out.display()))?;
            let taken = leaf::take(&out, old.as_deref(), &new, |action| natsd::apply(&out, action));
            println!(
                "{}\n{taken}\n\
                 switch the uplink: run this again\n\
                 clients on this machine: nats://{listen} (the default) with the hub's --js-domain\n\
                 once the leaf is up: ants leaf outbox {machine}{1}",
                out.display(),
                if default_out { String::new() } else { format!(" --leaf-config {}", out.display()) }
            );
            Ok(())
        }
        LeafCommand::Outbox { machine, members, leaf_config, remove } => {
            if remove {
                let (_, js) = nats()?.connect("ants-cli").await?;
                outbox::remove(&js, &machine).await?;
                println!("the hub no longer takes anything from the outbox of {machine}");
                return Ok(());
            }
            let mut change = members::Change::from(members);
            let kept = if change.lists() {
                None
            } else {
                let conf = match leaf_config {
                    Some(path) => path,
                    None => auth::default_dir()?.with_file_name("leaf.conf"),
                };
                let members = members::uplink(&conf, &machine).with_context(|| {
                    format!("no --ant or --human given: reading the members of {machine} from {}", conf.display())
                })?;
                Some(members)
            };
            let nats = nats()?;
            let (_, js) = nats.connect("ants-cli").await?;
            let dir = auth::default_dir()?;
            for ant in outbox::left_already(&js, &machine, &dir, &mut change, kept.as_ref()).await? {
                println!(
                    "{ant} left the members of {machine} already: the hub takes nothing more of it from the outbox"
                );
            }
            let set = change.apply(&machine, kept, || anyhow::anyhow!("no members of {machine}"))?;
            let leaf = outbox::connect(conn.nats, conn.creds, &nats.me.id, &machine).await?;
            outbox::admit(&leaf, &js, &machine, &set.roles()).await?;
            outbox::sourced(&js, &machine, outbox::SOURCE_WAIT).await?;
            println!("outbox of {machine} ready; the hub takes from it what its members send ({set})");
            Ok(())
        }
    }
}
