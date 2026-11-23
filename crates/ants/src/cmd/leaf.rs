//! `ants leaf`: a machine's leaf node, and its outbox on the hub.

use std::path::PathBuf;

use ants_proto::{JsDomain, Machine};
use anyhow::{Context as _, Result};
use clap::Subcommand;

use crate::auth;
use crate::auth::team_dir;
use crate::connection::Connection;
use crate::node::{leaf, members, natsd, outbox, web as node_web};

#[derive(Subcommand, Debug)]
pub(crate) enum LeafCommand {
    /// Write the nats-server configuration of the machine's leaf node: listening on localhost only, with one uplink
    /// to the hub, and a JetStream of its own (domain: the machine name) for the outbox. Run it again to switch the
    /// uplink: it reloads the leaf node service (ants.nats, ants-nats) running the file, or says how.
    Config(ConfigArgs),
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

/// `ants leaf config`.
#[derive(clap::Args, Debug)]
pub(crate) struct ConfigArgs {
    /// Machine name, as given to `ants auth leaf`.
    pub(crate) machine: Machine,
    /// Hub URL: nats-leaf:// or tls:// for its leafnode port (also through `devtunnel connect`), ws:// or wss://
    /// for its websocket listener (a leaf node asks for <path>/leafnode). Repeatable, all of one scheme.
    #[arg(long = "uplink", value_name = "URL", required = true)]
    pub(crate) uplinks: Vec<String>,
    /// Uplink credentials from `ants auth leaf` (default: <dir>/leaf-<machine>.creds).
    #[arg(long)]
    pub(crate) uplink_creds: Option<PathBuf>,
    /// System account credentials from `ants auth leaf`, for a second uplink bringing the account updates
    /// `ants auth push` sends the hub, revocations included (default: leaf-<machine>.system.creds beside the
    /// uplink credentials, when there).
    #[arg(long)]
    pub(crate) system_creds: Option<PathBuf>,
    /// The team's server.conf from `ants auth init` (default: <dir>/server.conf).
    #[arg(long)]
    pub(crate) server_conf: Option<PathBuf>,
    /// Team directory holding server.conf and the credentials (default: ~/.config/ants/auth).
    #[arg(long, env = "ANTS_AUTH_DIR")]
    pub(crate) dir: Option<PathBuf>,
    /// CA certificate of a hub the platform does not trust (tls:// and wss:// uplinks).
    #[arg(long)]
    pub(crate) ca_file: Option<PathBuf>,
    /// HTTP CONNECT forward proxy for the uplink, http://host:port or https://host:port.
    #[arg(long, value_name = "URL")]
    pub(crate) proxy: Option<String>,
    /// Address for the machine's ants processes; loopback only.
    #[arg(long, default_value = leaf::DEFAULT_LISTEN)]
    pub(crate) listen: String,
    /// Also listen for the page of `ants web`: a WebSocket listener on port 4281 of the --listen host, taking
    /// pages of the origin `ants web` serves (http://127.0.0.1:4280) only. A run without it drops the listener.
    #[arg(long)]
    pub(crate) web: bool,
    /// Where to write the configuration (default: ~/.config/ants/leaf.conf).
    #[arg(long)]
    pub(crate) out: Option<PathBuf>,
}

/// The line saying where the page of `ants web` connects on a leaf node whose clients connect to `listen`.
fn pages(listen: &str) -> Result<String> {
    Ok(format!("the page of `ants web`: ws://{}, taken from {} only\n", node_web::listen(listen)?, node_web::origin()))
}

pub(crate) async fn run(conn: &Connection<'_>, command: LeafCommand) -> Result<()> {
    match command {
        LeafCommand::Config(args) => config(conn.js_domain, args),
        LeafCommand::Outbox { machine, members, leaf_config, remove } => {
            outbox(conn, machine, members, leaf_config, remove).await
        }
    }
}

/// `ants leaf config`, for the hub's JetStream `domain` if it has one.
fn config(domain: Option<&JsDomain>, args: ConfigArgs) -> Result<()> {
    let ConfigArgs { machine, uplinks, uplink_creds, system_creds, server_conf, dir, ca_file, proxy, listen, web, out } =
        args;
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
    let write = if web { leaf::write_with_web } else { leaf::write };
    write(&config, &out)?;
    ants_daemon::secrets::creds::warn(&creds);
    ants_daemon::secrets::creds::warn(&auth::leaf_store_creds(&creds));
    system_creds.as_deref().map(ants_daemon::secrets::creds::warn);
    let new = std::fs::read_to_string(&out).with_context(|| format!("reading {}", out.display()))?;
    let taken = leaf::take(&out, old.as_deref(), &new, |action| natsd::apply(&out, action));
    let pages = if web { pages(&listen)? } else { String::new() };
    println!(
        "{}\n{taken}\n\
         switch the uplink: run this again\n\
         clients on this machine: nats://{listen} (the default) with the hub's --js-domain\n\
         {pages}once the leaf is up: ants leaf outbox {machine}{1}",
        out.display(),
        if default_out { String::new() } else { format!(" --leaf-config {}", out.display()) }
    );
    Ok(())
}

/// `ants leaf outbox`.
async fn outbox(
    conn: &Connection<'_>,
    machine: Machine,
    members: members::OutboxArgs,
    leaf_config: Option<PathBuf>,
    remove: bool,
) -> Result<()> {
    if remove {
        let (_, js) = conn.nats()?.connect("ants-cli").await?;
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
    let nats = conn.nats()?;
    let (_, js) = nats.connect("ants-cli").await?;
    let dir = auth::default_dir()?;
    for ant in outbox::left_already(&js, &machine, &dir, &mut change, kept.as_ref()).await? {
        println!("{ant} left the members of {machine} already: the hub takes nothing more of it from the outbox");
    }
    let set = change.apply(&machine, kept, || anyhow::anyhow!("no members of {machine}"))?;
    let leaf = outbox::connect(conn.nats, conn.creds, &nats.me.id).await?;
    outbox::admit(&leaf, &js, &machine, &set.roles()).await?;
    outbox::sourced(&js, &machine, outbox::SOURCE_WAIT).await?;
    println!("outbox of {machine} ready; the hub takes from it what its members send ({set})");
    Ok(())
}
