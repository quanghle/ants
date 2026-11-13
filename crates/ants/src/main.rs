//! `ants`: talk to agent teams over NATS from the terminal, and run the persona daemon.

mod approve;
mod auth;
mod inbox;
mod roster;
mod serve;

use std::path::{Path, PathBuf};

use ants_daemon::{HarnessKind, Launch, PermissionPolicy, bus, dead, harness};
use ants_proto::{DeadLetter, Envelope, PersonaId, PlanPost, ThreadId, subject};
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
    /// List the personas being served: harness, machine, capabilities, last heartbeat, and whether it is recent.
    Roster {
        /// One JSON entry per line, with its state.
        #[arg(long)]
        json: bool,
    },
    /// Print ants traffic as it happens.
    Watch {
        /// Subject filter.
        #[arg(default_value = subject::ALL)]
        subject: String,
    },
    /// Create the ANTS stream and the roster and sessions buckets on the server, or add the stream subjects an older
    /// version lacks.
    Init,
    /// Tool requests personas escalate: list the pending ones, show one, or answer it.
    Approve {
        /// Request id, or a unique start of it. Without it, list the pending requests.
        id: Option<String>,
        /// Answer the request. Without it, show the request.
        #[arg(requires = "id")]
        verdict: Option<approve::Answer>,
        /// After listing, keep printing new requests until Ctrl-C.
        #[arg(long, short, conflicts_with = "id")]
        follow: bool,
        /// One JSON request per line.
        #[arg(long)]
        json: bool,
    },
    /// Messages personas gave up on: inspect, replay or drop them.
    #[command(subcommand)]
    Dead(DeadCommand),
    /// Serve persona inboxes through agent harnesses: those of the config file, or one described by flags.
    Daemon(serve::DaemonArgs),
    /// Bridge stdio to a daemon's MCP tools, for harnesses without HTTP MCP. The daemon configures this itself.
    Mcp {
        /// The daemon's MCP socket.
        #[arg(long)]
        socket: PathBuf,
        /// The session's token.
        #[arg(long, env = ants_daemon::mcp::TOKEN_ENV, hide_env_values = true)]
        token: String,
    },
    /// Talk to a harness directly, without NATS.
    #[command(subcommand)]
    Harness(HarnessCommand),
    /// Issue NATS credentials: one operator and account for the team, one user per persona.
    #[command(subcommand)]
    Auth(AuthCommand),
}

#[derive(Subcommand)]
enum DeadCommand {
    /// List dead letters, oldest first.
    List,
    /// Send a dead letter's message to its inbox again, as it was, and remove the dead letter.
    Replay { seq: u64 },
    /// Remove a dead letter without replaying it.
    Drop { seq: u64 },
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
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("ANTS_LOG").unwrap_or_else(|_| "info,rmcp=warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Ask { thread, wait, words } => ask(&cli.nats, cli.creds.as_deref(), thread, wait, words).await,
        Command::Inbox { peek, thread, limit, follow, json } => {
            let select = inbox::Select { thread, limit, peek };
            show_inbox(&cli.nats, cli.creds.as_deref(), &select, follow, json).await
        }
        Command::Roster { json } => roster::show(&cli.nats, cli.creds.as_deref(), json).await,
        Command::Watch { subject } => watch(&cli.nats, cli.creds.as_deref(), subject).await,
        Command::Init => {
            let (_, js) = bus::connect(&cli.nats, cli.creds.as_deref(), &PersonaId::human(), "ants-cli").await?;
            bus::init_stream(&js).await?;
            bus::ensure_roster(&js, ants_proto::ROSTER_TTL).await?;
            bus::ensure_sessions(&js).await?;
            println!(
                "stream {} and buckets {} and {} ready",
                ants_proto::STREAM,
                ants_proto::ROSTER_BUCKET,
                ants_proto::SESSIONS_BUCKET
            );
            Ok(())
        }
        Command::Approve { id, verdict, follow, json } => {
            let args = approve::Args { id, answer: verdict, follow, json };
            approve::run(&cli.nats, cli.creds.as_deref(), args).await
        }
        Command::Dead(command) => dead_letters(&cli.nats, cli.creds.as_deref(), command).await,
        Command::Daemon(args) => {
            let personas = serve::personas(&args, &ants_daemon::config::default_path()?)?;
            serve::run(serve::configs(personas, &cli.nats, cli.creds)?).await
        }
        Command::Mcp { socket, token } => ants_daemon::mcp::bridge_stdio(&socket, &token).await,
        Command::Harness(HarnessCommand::Check { harness }) => {
            println!("{harness}: {}", harness::check(harness).await?);
            Ok(())
        }
        Command::Harness(HarnessCommand::Prompt { harness, words }) => {
            let launch = Launch::new(harness, std::env::current_dir()?, Vec::new(), PermissionPolicy::reject_all());
            let reply = harness::prompt_once(&launch, &words.join(" ")).await?;
            println!("{reply}");
            Ok(())
        }
        Command::Auth(AuthCommand::Init { dir }) => {
            let dir = team_dir(dir)?;
            auth::init(&dir)?;
            println!(
                "team keys in {0}\ncopy {0}/{1} next to the nats-server configuration and include \"{1}\" from it (include paths are relative)",
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
    let mut replies = client.subscribe(subject::inbox_of(&PersonaId::human())).await?;
    bus::publish(&js, &envelope).await?;
    println!("thread {}", envelope.thread_id);
    if !wait {
        return Ok(());
    }
    while let Some(msg) = replies.next().await {
        let Ok(reply) = serde_json::from_slice::<Envelope>(&msg.payload) else { continue };
        if let Err(e) = reply.check_subject(&msg.subject) {
            tracing::warn!("ignoring a reply with a forged sender: {e}");
            continue;
        }
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

async fn dead_letters(url: &str, creds: Option<&Path>, command: DeadCommand) -> Result<()> {
    let (_, js) = bus::connect(url, creds, &PersonaId::human(), "ants-cli").await?;
    let stream = js.get_stream(ants_proto::STREAM).await.context("opening the ANTS stream")?;
    match command {
        DeadCommand::List => {
            let letters = dead::list(&stream).await?;
            if letters.is_empty() {
                println!("no dead letters");
            }
            for (seq, letter) in &letters {
                println!("{}", dead_row(*seq, letter));
            }
        }
        DeadCommand::Replay { seq } => {
            let envelope = dead::replay(&js, &stream, seq).await?;
            println!("replayed to {} on thread {}", envelope.to, envelope.thread_id);
        }
        DeadCommand::Drop { seq } => {
            dead::discard(&stream, seq).await?;
            println!("dropped dead letter {seq}");
        }
    }
    Ok(())
}

/// One line of `ants dead list`: sequence, persona, thread, sender, deliveries, time, error and the start of the text.
fn dead_row(seq: u64, letter: &DeadLetter) -> String {
    let time = letter.died_at.format("%Y-%m-%d %H:%M:%S");
    let (thread, from, text) = match &letter.envelope {
        Some(e) => (e.thread_id.as_str(), e.from.as_str(), e.text.as_str()),
        None => ("-", "-", letter.payload.as_deref().unwrap_or("")),
    };
    format!(
        "{seq}\t{}\t{thread}\t{from}\t{}x\t{time}\t{}\t{}",
        letter.persona,
        letter.deliveries,
        excerpt(&letter.error, 60),
        excerpt(text, 60)
    )
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

async fn watch(url: &str, creds: Option<&Path>, filter: String) -> Result<()> {
    let (client, _) = bus::connect(url, creds, &PersonaId::human(), "ants-watch").await?;
    let mut sub = client.subscribe(filter).await?;
    while let Some(msg) = sub.next().await {
        if let Ok(e) = serde_json::from_slice::<Envelope>(&msg.payload) {
            let forged = match e.check_subject(&msg.subject) {
                Ok(()) => String::new(),
                Err(_) => format!(" (forged: sent on {})", msg.subject),
            };
            println!("[{}] {} -> {}{forged}: {}", e.thread_id, e.from, e.to, e.text);
        } else if let Ok(post) = serde_json::from_slice::<PlanPost>(&msg.payload) {
            let forged = match post.check_subject(&msg.subject) {
                Ok(()) => String::new(),
                Err(_) => format!(" (forged: sent on {})", msg.subject),
            };
            println!("[{}] {} -> plan room{forged}: {}", post.thread_id, post.from, post.text);
        } else if let Ok(d) = serde_json::from_slice::<DeadLetter>(&msg.payload) {
            println!("dead letter of {} after {} deliveries: {}", d.persona, d.deliveries, d.error);
        } else {
            println!("{} {}", msg.subject, String::from_utf8_lossy(&msg.payload));
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
    fn dead_rows_show_the_message_or_the_bad_payload() {
        let mut ask = Envelope::new(PersonaId::human(), "dev-a".parse().unwrap(), "build it\nand test it");
        ask.thread_id = "t1".parse().unwrap();
        let letter = DeadLetter::of_envelope("dev-a".parse().unwrap(), 3, 5, "harness crashed", ask);
        let row = dead_row(12, &letter);
        assert!(row.starts_with("12\tdev-a\tt1\thuman\t5x\t"), "{row}");
        assert!(row.ends_with("\tharness crashed\tbuild it…"), "{row}");

        let letter = DeadLetter::of_payload("dev-a".parse().unwrap(), 4, 1, "not an envelope", b"{oops");
        assert!(dead_row(13, &letter).ends_with("\tnot an envelope\t{oops"));
    }

    #[test]
    fn a_recipient_alone_is_an_error() {
        assert!(recipient(words("@dev-b")).is_err());
        assert!(recipient(words("@Bad.Name hi")).is_err());
    }
}
