//! `ants`: talk to agent teams over NATS from the terminal, and run the persona daemon.

mod auth;

use std::path::{Path, PathBuf};

use ants_daemon::{HarnessKind, PermissionPolicy, bus, harness};
use ants_proto::{Envelope, PersonaId, ThreadId, subject};
use anyhow::{Context as _, Result, bail};
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
        /// Wait for the first reply on the thread and print it.
        #[arg(long)]
        wait: bool,
        /// `@persona` (optional), then the message.
        #[arg(required = true, num_args = 1..)]
        words: Vec<String>,
    },
    /// Print ants traffic as it happens.
    Watch {
        /// Subject filter.
        #[arg(default_value = subject::ALL)]
        subject: String,
    },
    /// Create the ANTS stream on the server (the daemon also does this on start).
    Init,
    /// Serve one persona's inbox through an agent harness.
    Daemon {
        #[arg(long, env = "ANTS_PERSONA")]
        persona: PersonaId,
        /// Directory the harness works in (default: the current directory).
        #[arg(long)]
        workdir: Option<PathBuf>,
        /// The persona's own files, its bd workspace of to-dos and memories among them (default: ~/.ants/<persona>).
        #[arg(long)]
        home: Option<PathBuf>,
        #[arg(long, default_value_t = HarnessKind::Copilot)]
        harness: HarnessKind,
        /// Allow every tool request once instead of rejecting it. Only for trusted, sandboxed workdirs.
        #[arg(long)]
        allow_tools: bool,
    },
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
        Command::Watch { subject } => watch(&cli.nats, cli.creds.as_deref(), subject).await,
        Command::Init => {
            let (_, js) = bus::connect(&cli.nats, cli.creds.as_deref(), &PersonaId::human(), "ants-cli").await?;
            bus::ensure_stream(&js).await?;
            println!("stream {} ready", ants_proto::STREAM);
            Ok(())
        }
        Command::Daemon { persona, workdir, home, harness, allow_tools } => {
            let workdir = match workdir {
                Some(dir) => dir,
                None => std::env::current_dir()?,
            };
            let home = match home {
                Some(dir) => dir,
                None => ants_daemon::workspace::default_home(&persona)?,
            };
            ants_daemon::run(ants_daemon::Config {
                persona,
                nats_url: cli.nats,
                creds: cli.creds,
                workdir: workdir.canonicalize().with_context(|| format!("workdir {}", workdir.display()))?,
                home,
                harness,
                permissions: if allow_tools { PermissionPolicy::AllowOnce } else { PermissionPolicy::Reject },
            })
            .await
        }
        Command::Harness(HarnessCommand::Check { harness }) => {
            println!("{harness}: {}", harness::check(harness).await?);
            Ok(())
        }
        Command::Harness(HarnessCommand::Prompt { harness, words }) => {
            let reply = harness::prompt_once(
                harness,
                &std::env::current_dir()?,
                &[],
                &words.join(" "),
                PermissionPolicy::Reject,
            )
            .await?;
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
            return Ok(());
        }
    }
    bail!("connection closed before a reply arrived")
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
