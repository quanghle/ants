//! `ants`: talk to agent teams over NATS from the terminal, and run the persona daemon.

mod auth;
mod inbox;
mod serve;

use std::path::{Path, PathBuf};

use ants_daemon::{HarnessKind, Launch, PermissionPolicy, bus, harness};
use ants_proto::{Envelope, PersonaId, ThreadId, subject};
use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use futures::StreamExt as _;

#[derive(Parser)]
#[command(name = "ants", version, about = "Agent teams messaging over NATS")]
struct Cli {
    /// NATS server: nats://, tls://, ws:// or wss://. Point it at the local leaf node where there is one.
    #[arg(long, global = true, env = "ANTS_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats: String,

    /// NATS credentials file (`ants auth user` writes one). Needed when the server enforces per-persona permissions.
    #[arg(long, global = true, env = "ANTS_CREDS")]
    creds: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Send a message. Starts with @persona to pick the recipient; the messenger gets it otherwise.
    Ask {
        /// Continue an existing thread.
        #[arg(long)]
        thread: Option<ThreadId>,
        /// Wait for the first reply on the thread, print it and mark it read.
        #[arg(long)]
        wait: bool,
        /// `@persona` (optional), then the message.
        #[arg(required = true, num_args = 1..)]
        words: Vec<String>,
    },
    /// Print the replies not read yet, including those that arrived while no CLI was running, and mark them read.
    Inbox {
        /// Show without marking read.
        #[arg(long)]
        peek: bool,
        /// Only replies on this thread; the others stay unread.
        #[arg(long)]
        thread: Option<ThreadId>,
        /// At most this many replies; the rest stay unread.
        #[arg(long)]
        limit: Option<usize>,
        /// Keep waiting for new replies until Ctrl-C.
        #[arg(long, short)]
        follow: bool,
        /// One JSON envelope per line.
        #[arg(long)]
        json: bool,
    },
    /// Print ants traffic as it happens.
    Watch {
        /// Subject filter.
        #[arg(default_value = subject::ALL)]
        subject: String,
    },
    /// Create the ANTS stream on the server (the daemon also does this on start).
    Init,
    /// Serve persona inboxes through agent harnesses: those of the config file, or one described by flags.
    Daemon(serve::DaemonArgs),
    /// Talk to a harness directly, without NATS.
    #[command(subcommand)]
    Harness(HarnessCommand),
    /// Issue NATS credentials: one operator and account for the team, one user per persona.
    #[command(subcommand)]
    Auth(AuthCommand),
}

#[derive(Subcommand)]
enum AuthCommand {
    /// Create the team operator and account, and the server.conf that makes nats-server trust them.
    Init {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Issue credentials for a persona, allowing only the subjects of its role. `human` is the CLI user.
    User {
        persona: PersonaId,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the credentials (default: <dir>/<persona>.creds).
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum HarnessCommand {
    /// Start the harness and complete the ACP handshake.
    Check {
        #[arg(long, default_value_t = HarnessKind::Copilot)]
        harness: HarnessKind,
    },
    /// Run one prompt in the current directory and print the reply. Tool requests are rejected.
    Prompt {
        #[arg(long, default_value_t = HarnessKind::Copilot)]
        harness: HarnessKind,
        #[arg(required = true, num_args = 1..)]
        words: Vec<String>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_env("ANTS_LOG").unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Ask { thread, wait, words } => ask(&cli.nats, cli.creds.as_deref(), thread, wait, words).await,
        Command::Inbox { peek, thread, limit, follow, json } => {
            let select = inbox::Select { thread, limit, peek };
            show_inbox(&cli.nats, cli.creds.as_deref(), &select, follow, json).await
        }
        Command::Watch { subject } => watch(&cli.nats, cli.creds.as_deref(), subject).await,
        Command::Init => {
            let (_, js) = bus::connect(&cli.nats, cli.creds.as_deref(), &PersonaId::human(), "ants-cli").await?;
            bus::ensure_stream(&js).await?;
            println!("stream {} ready", ants_proto::STREAM);
            Ok(())
        }
        Command::Daemon(args) => {
            let personas = serve::personas(&args, &ants_daemon::config::default_path()?)?;
            serve::run(serve::configs(personas, &cli.nats, cli.creds)?).await
        }
        Command::Harness(HarnessCommand::Check { harness }) => {
            println!("{harness}: {}", harness::check(harness).await?);
            Ok(())
        }
        Command::Harness(HarnessCommand::Prompt { harness, words }) => {
            let launch = Launch::new(harness, std::env::current_dir()?, Vec::new(), PermissionPolicy::Reject);
            let reply = harness::prompt_once(&launch, &words.join(" ")).await?;
            println!("{reply}");
            Ok(())
        }
        Command::Auth(AuthCommand::Init { dir }) => {
            let dir = team_dir(dir)?;
            auth::init(&dir)?;
            println!(
                "team keys in {0}\ninclude {0}/{1} from the nats-server configuration",
                dir.display(),
                auth::SERVER_CONF
            );
            Ok(())
        }
        Command::Auth(AuthCommand::User { persona, dir, out }) => {
            let dir = team_dir(dir)?;
            let out = out.unwrap_or_else(|| dir.join(format!("{persona}.creds")));
            auth::issue(&dir, &persona, &out)?;
            println!("{}", out.display());
            Ok(())
        }
    }
}

fn team_dir(dir: Option<PathBuf>) -> Result<PathBuf> {
    match dir {
        Some(dir) => Ok(dir),
        None => auth::default_dir(),
    }
}

/// Split a leading `@persona` off the words.
fn recipient(mut words: Vec<String>) -> Result<(PersonaId, String)> {
    let to = match words.first().and_then(|w| w.strip_prefix('@')) {
        Some(name) => {
            let to = name.parse()?;
            words.remove(0);
            to
        }
        None => PersonaId::messenger(),
    };
    if words.is_empty() {
        bail!("nothing to send");
    }
    Ok((to, words.join(" ")))
}

async fn ask(url: &str, creds: Option<&Path>, thread: Option<ThreadId>, wait: bool, words: Vec<String>) -> Result<()> {
    let (to, text) = recipient(words)?;
    let envelope = match thread {
        Some(thread) => Envelope::on_thread(thread, PersonaId::human(), to, text),
        None => Envelope::new(PersonaId::human(), to, text),
    };
    let (client, js) = bus::connect(url, creds, &PersonaId::human(), "ants-cli").await?;
    // Subscribe first so a fast reply is not missed.
    let mut replies = client.subscribe(subject::inbox(&PersonaId::human())).await?;
    bus::publish(&js, &envelope).await?;
    println!("thread {}", envelope.thread_id);
    if !wait {
        return Ok(());
    }
    while let Some(msg) = replies.next().await {
        let Ok(reply) = serde_json::from_slice::<Envelope>(&msg.payload) else { continue };
        if reply.thread_id == envelope.thread_id {
            println!("{}: {}", reply.from, reply.text);
            // Seen here, so `ants inbox` need not show it again. The other replies stay unread.
            match inbox::mark_read(&js, &reply).await {
                Ok(true) => {}
                Ok(false) => tracing::debug!("reply already read"),
                Err(e) => tracing::warn!("could not mark the reply read: {e:#}"),
            }
            return Ok(());
        }
    }
    bail!("connection closed before a reply arrived")
}

async fn show_inbox(url: &str, creds: Option<&Path>, select: &inbox::Select, follow: bool, json: bool) -> Result<()> {
    let (_, js) = bus::connect(url, creds, &PersonaId::human(), "ants-inbox").await?;
    let mut inbox = inbox::Inbox::open(&js).await?;
    let show = |e: &Envelope| -> Result<()> {
        if json {
            println!("{}", serde_json::to_string(e)?);
        } else {
            println!("{}", inbox::render(e));
        }
        Ok(())
    };
    let result = async {
        let unread = inbox.unread(select).await?;
        if unread.is_empty() && !follow && !json {
            println!("no unread replies");
        }
        unread.iter().try_for_each(show)?;
        if follow {
            inbox.follow(select, show, async { _ = tokio::signal::ctrl_c().await }).await?;
        }
        anyhow::Ok(())
    }
    .await;
    // Hand back what was not taken even after an error, so it is not hidden until the ack wait runs out.
    let released = inbox.release().await;
    result.and(released)
}

async fn watch(url: &str, creds: Option<&Path>, filter: String) -> Result<()> {
    let (client, _) = bus::connect(url, creds, &PersonaId::human(), "ants-watch").await?;
    let mut sub = client.subscribe(filter).await?;
    while let Some(msg) = sub.next().await {
        match serde_json::from_slice::<Envelope>(&msg.payload) {
            Ok(e) => println!("[{}] {} -> {}: {}", e.thread_id, e.from, e.to, e.text),
            Err(_) => println!("{} {}", msg.subject, String::from_utf8_lossy(&msg.payload)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(s: &str) -> Vec<String> {
        s.split(' ').map(str::to_owned).collect()
    }

    #[test]
    fn leading_at_picks_the_recipient() {
        let (to, text) = recipient(words("@dev-b check the build")).unwrap();
        assert_eq!((to.as_str(), text.as_str()), ("dev-b", "check the build"));
    }

    #[test]
    fn messenger_is_the_default_recipient() {
        let (to, text) = recipient(words("who owns billing?")).unwrap();
        assert_eq!((to, text.as_str()), (PersonaId::messenger(), "who owns billing?"));
    }

    #[test]
    fn a_recipient_alone_is_an_error() {
        assert!(recipient(words("@dev-b")).is_err());
        assert!(recipient(words("@Bad.Name hi")).is_err());
    }
}
