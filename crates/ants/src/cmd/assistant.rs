//! `ants assistant`: designate, withdraw, enable and disable the human's assistant, enroll it for sealed messages,
//! and sign the human's consent to its decisions ([`crate::people::assistant`]).

use ants_proto::UserId;
use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::completions;
use crate::connection::Connection;
use crate::people::assistant;

#[derive(Subcommand, Debug)]
pub(crate) enum AssistantCommand {
    /// Create the human's dedicated assistant ant and designate it, in one step: its credentials naming the human
    /// (`ants auth user ANT --assistant-of HUMAN`), an [[ant]] working in its own home with an assistant's whoami and
    /// no fetch or execute rules, and the grant; then reload the daemon. Needs the bus's account key; run again,
    /// it keeps what is in place and completes the rest.
    #[expect(rustdoc::broken_intra_doc_links, reason = "clap help text: brackets name config tables")]
    Enable(Box<assistant::EnableArgs>),
    /// Withdraw the human's assistant, ending any enrollment as a device, and offer to retire the dedicated one
    /// `enable` created: unlist it, revoke its credentials and remove their file, keeping its home.
    Disable(assistant::DisableArgs),
    /// Designate ANT as the human's assistant, in place of any other. Refused for an ant added from a template; a
    /// warning for one with a workdir other than its home or tools allowed without asking. ANT acts for the human only while its
    /// credentials also name the human (`ants auth user ANT --assistant-of HUMAN`).
    Set {
        #[arg(value_name = "ANT", add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
    },
    /// Withdraw the human's assistant, at once for every reader.
    Clear,
    /// Show the assistant of HUMAN, by default the CLI's human, if its grant verifies, and whether it is enrolled as a
    /// device of the human.
    Show {
        #[arg(value_name = "HUMAN")]
        human: Option<UserId>,
    },
    /// Enroll the human's assistant as one of its devices (`on`), so messages sealed for the human are sealed for it
    /// too and it seals those it sends for the human, or end that (`off`). Off unless enrolled; `clear` ends it too.
    #[command(subcommand)]
    Seal(SealCommand),
    /// Print the encryption key ANT, an ant of this machine's config file, is enrolled with, making it if it has none:
    /// for `ants assistant seal on --key` on the human's machine. Its seed waits beside its credentials, as `ANT.xk`.
    Key {
        #[arg(value_name = "ANT", add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
    },
    /// List the decisions the human's assistant made for the human on pending tool requests, held until the human
    /// confirms them; or answer the one on ACT (`ANT/ID`, as `ants tools` writes a request): confirm it, so it takes
    /// effect as the human's, or drop it with `--deny`. Signed by the human's device.
    Confirm {
        #[arg(value_name = "ACT")]
        act: Option<String>,
        /// Drop the decision instead.
        #[arg(long, requires = "act")]
        deny: bool,
    },
    /// Show the human's pre-allow rules, or add RULES: a decision of the assistant on a tool request a rule covers takes
    /// effect unconfirmed, unless it allows always. A rule is `ANT/RULE` or `*/RULE` for every ant, RULE an allow rule
    /// (`execute:cargo test*`, `read:/work/*`; paths absolute). Signed by the human's device, for the assistant now
    /// designated.
    Allow {
        #[arg(value_name = "RULE")]
        rules: Vec<String>,
        /// Remove RULES instead.
        #[arg(long, requires = "rules")]
        remove: bool,
        /// Remove every rule.
        #[arg(long, conflicts_with_all = ["rules", "remove"])]
        clear: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum SealCommand {
    /// Enroll the assistant, with the key of the assistant on this machine, made if it has none, unless `--key`.
    On {
        /// The encryption key `ants assistant key ANT` printed on the machine running the assistant.
        #[arg(long, value_name = "KEY")]
        key: Option<ants_proto::DeviceKey>,
    },
    /// End the enrollment: nothing is sealed for the assistant any more, and it opens nothing.
    Off,
}

/// `ants assistant`, connecting through `conn` for the subcommands that need it.
pub(crate) async fn run(conn: &Connection<'_>, command: AssistantCommand) -> Result<()> {
    let nats = || conn.nats();
    match command {
        AssistantCommand::Enable(args) => assistant::dedicated::enable(&nats()?, *args).await,
        AssistantCommand::Disable(args) => assistant::dedicated::disable(&nats()?, args).await,
        AssistantCommand::Set { ant } => assistant::set(&nats()?, &ant).await,
        AssistantCommand::Clear => assistant::clear(&nats()?).await,
        AssistantCommand::Show { human } => assistant::show(&nats()?, human).await,
        AssistantCommand::Seal(SealCommand::On { key }) => assistant::seal_on(&nats()?, key).await,
        AssistantCommand::Seal(SealCommand::Off) => assistant::seal_off(&nats()?).await,
        AssistantCommand::Key { ant } => assistant::key(&ant),
        AssistantCommand::Confirm { act, deny } => {
            Box::pin(assistant::consent::confirm(&nats()?, act.as_deref(), deny)).await
        }
        AssistantCommand::Allow { rules, remove, clear } => {
            assistant::consent::allow(&nats()?, rules, remove, clear).await
        }
    }
}
