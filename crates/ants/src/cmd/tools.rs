//! `ants tools`: the tool calls ants ask approval for, listed, shown and answered.

use std::path::{Path, PathBuf};

use ants_client::text::spelling;
use ants_daemon::{bus, config, permission};
use ants_proto::{ApprovalRequest, Envelope, Following, STREAM, UserId, Verdict};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::Utc;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::cmd::send;
use crate::completions;
use crate::connection::{Connection, Nats};
use crate::service::{Action, Outcome, Service, Services, failure, manual};
use crate::tools::{
    self, Update, answer_request, details, find, pending, qualified, row, state, suggestion, suggestion_text,
};

#[derive(Subcommand, Debug)]
pub(crate) enum ToolsCommand {
    /// List the pending tool requests: `<ant>/<id>`, thread, tool, files, options offered and time left.
    List {
        /// After listing, keep printing new requests until Ctrl-C.
        #[arg(long, short)]
        follow: bool,
        /// One JSON request per line.
        #[arg(long)]
        json: bool,
    },
    /// Show a tool request in full: its tool input, where it stands, and the commands answering it.
    Show {
        /// Request as `<ant>/<id>`, or a bare id when only one ant has it; a unique start of the id will do.
        #[arg(add = ArgValueCandidates::new(completions::tool_requests))]
        id: String,
        /// The request as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Allow the tool call of a pending request, once or with --always for the rest of the ant's session.
    Approve {
        /// Request as `<ant>/<id>`, or a bare id when only one ant has it; a unique start of the id will do.
        #[arg(add = ArgValueCandidates::new(completions::tool_requests))]
        id: String,
        /// Allow this tool for the rest of the session (allow-always), when the request offers it.
        #[arg(long)]
        always: bool,
        /// Also add the allow rule fitting the request exactly to the ant's allow list in the config file, then
        /// reload the daemon serving it. An ant added from a template takes its allow rules from the template:
        /// nothing is added to its entry.
        #[arg(long)]
        rule: bool,
        /// Config file listing the ant, for --rule (default: ~/.ants/config.toml).
        #[arg(long, env = "ANTS_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Reject the tool call of a pending request, with --note telling the ant what to do instead.
    Reject {
        /// Request as `<ant>/<id>`, or a bare id when only one ant has it; a unique start of the id will do.
        #[arg(add = ArgValueCandidates::new(completions::tool_requests))]
        id: String,
        /// Once the rejection is stored, send this to the ant on the request's thread as the human's message, which
        /// steers its running prompt and renews the thread's turn budget. Not sent when the rejection is refused.
        #[arg(long, value_name = "TEXT")]
        note: Option<String>,
    },
}

pub(crate) async fn run(conn: &Connection<'_>, command: ToolsCommand) -> Result<()> {
    let nats = || conn.nats();
    match command {
        ToolsCommand::List { follow, json } => list(&nats()?, crate::connection::Listing { follow, json }).await,
        ToolsCommand::Show { id, json } => show(&nats()?, &id, json).await,
        ToolsCommand::Approve { id, always, rule, config } => {
            let verdict = if always { Verdict::AllowAlways } else { Verdict::AllowOnce };
            let config = match (rule, config) {
                (false, _) => None,
                (true, Some(path)) => Some(path),
                (true, None) => Some(ants_daemon::config::default_path()?),
            };
            decide_request(&nats()?, &id, verdict, config.as_deref(), None).await
        }
        ToolsCommand::Reject { id, note } => decide_request(&nats()?, &id, Verdict::Reject, None, note).await,
    }
}

/// `ants tools list`: the pending requests, then with `follow` new ones as they come, until interrupted.
async fn list(nats: &Nats<'_>, listing: crate::connection::Listing) -> Result<()> {
    let crate::connection::Listing { follow, json } = listing;
    let (client, js) = nats.connect("ants-tools").await?;
    let print = |request: &ApprovalRequest| -> Result<()> {
        if json {
            println!("{}", serde_json::to_string(request)?);
        } else {
            println!("{}", row(request, Utc::now()));
        }
        Ok(())
    };
    if !follow {
        let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
        let listed = pending(&js, &stream, Utc::now()).await?;
        if listed.is_empty() && !json {
            println!("no pending tool requests");
        }
        return listed.iter().try_for_each(print);
    }
    let follow = Box::pin(tools::follow(&client, &js, |update| match update {
        Update::Pending(request) => print(request),
        Update::Held(..) | Update::Resolved(..) => Ok(()),
    }));
    // Ctrl-C ends the following cleanly, so the device's presence goes at once.
    let follow = async {
        tokio::select! {
            followed = follow => followed,
            _ = tokio::signal::ctrl_c() => Ok(()),
        }
    };
    let (me, device) = (&nats.me.id, nats.device);
    crate::people::presence::while_following(&js, nats.domain, me, device, Following::Approvals, follow).await
}

/// `ants tools show ID`: one request in full, with where it stands.
async fn show(nats: &Nats<'_>, id: &str, json: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-tools").await?;
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let (seq, request) = find(&stream, id).await?;
    if json {
        println!("{}", serde_json::to_string(&request)?);
    } else {
        let now = Utc::now();
        let state = state(&js, &stream, seq, &request, now).await?;
        println!("{}", details(&request, &state, now));
    }
    Ok(())
}

/// Send `note` on the thread of `request` to its ant as `me`'s message, as `ants ask --thread` does: as any human
/// message there, it steers the prompt running and renews the thread's turn budget. Returns the message, where it
/// was stored, and the warnings for an ant no daemon serves now.
async fn send_note(
    js: &jetstream::Context,
    domain: Option<&ants_proto::JsDomain>,
    me: &crate::people::humans::Me,
    request: &ApprovalRequest,
    note: String,
) -> Result<(Envelope, bus::Stored, Vec<String>)> {
    let envelope = send::ask_envelope(me, Some(request.thread_id.clone()), request.ant.clone(), note);
    let (warnings, _) = send::unserved(js, me, &envelope.to).await;
    let stored = send::deliver(js, domain, &envelope).await?;
    Ok((envelope, stored, warnings))
}

/// `ants tools approve ID [--always] [--rule]` and `ants tools reject ID [--note TEXT]`: answer one request with
/// `verdict`. With `rule`, the config file there, also add the allow rule fitting the request to its ant's allow list,
/// then reload the daemon serving the file; for an ant added from a template, whose entry may set no allow list, only
/// answer. With `note`, send it to the ant on the request's thread once the answer is stored ([`send_note`]); not when
/// the answer is refused.
pub(crate) async fn decide_request(
    nats: &Nats<'_>,
    id: &str,
    verdict: Verdict,
    rule: Option<&Path>,
    note: Option<String>,
) -> Result<()> {
    if note.as_deref().is_some_and(|n| n.trim().is_empty()) {
        bail!("the note is empty: give its text, or reject without --note");
    }
    let (_, js) = nats.connect("ants-tools").await?;
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let (seq, request) = find(&stream, id).await?;
    // An ant added from a template takes its allow rules from the template, and its entry may set no `allow`.
    let template = match rule {
        Some(path) => config::template_of(path, &request.ant)?,
        None => None,
    };
    // Checked before answering, so a rule that cannot be added leaves the request pending.
    let edit = match rule {
        Some(_) if template.is_some() => None,
        Some(path) => {
            let rule = permission::rule_for(&request.tool).with_context(|| {
                format!(
                    "no allow rule fits request {} exactly; answer without --rule, and widen the allow list of {} by \
                     hand if needed",
                    qualified(&request),
                    request.ant
                )
            })?;
            let text = config::with_allow_rule(path, &request.ant, &rule)?;
            Some((path, rule, text))
        }
        None => None,
    };
    let decision = match answer_request(&js, &stream, seq, &request, verdict, &nats.me.id, Utc::now()).await {
        Ok(decision) => decision,
        Err(e) if note.is_some() => bail!("{e:#}; the note was not sent"),
        Err(e) => return Err(e),
    };
    println!("answered request {}/{}: {}", decision.ant, decision.request_id, spelling(decision.verdict));
    let ant = &request.ant;
    if let Some(note) = note {
        let (envelope, stored, warnings) = send_note(&js, nats.domain, &nats.me, &request, note)
            .await
            .with_context(|| format!("request {} is rejected, but the note was not sent", qualified(&request)))?;
        warnings.iter().for_each(|w| tracing::warn!("{w}"));
        if stored == bus::Stored::Outbox {
            println!("{}", send::OUTBOX);
        }
        println!("sent the note to {ant} on thread {} (message {})", envelope.thread_id, envelope.id);
    }
    match edit {
        Some((path, rule, None)) => println!("{rule:?} is in the allow list of {ant} already in {}", path.display()),
        Some((path, rule, Some(text))) => {
            config::write(path, &text)?;
            println!("{rule:?} added to the allow list of {ant} in {}", path.display());
            reload_daemon(path, ant);
        }
        None if verdict == Verdict::Reject => {}
        None if template.is_some() => println!(
            "{ant} is added from template {}, whose allow rules it takes: no rule added to its entry; add one to the \
             template's allow or one of its allow_sets by hand",
            template.as_deref().unwrap_or_default()
        ),
        None => match suggestion(&stream, seq, &request).await {
            Ok(Some(s)) => println!("{}", suggestion_text(&request, &s)),
            Ok(None) => {}
            Err(e) => tracing::warn!(error = format!("{e:#}"), "counting earlier approvals"),
        },
    }
    Ok(())
}

/// Reload the daemon serving the config file at `path`, for `ant` to take its new allow list, or say how.
fn reload_daemon(path: &Path, ant: &UserId) {
    match Services::local().apply(Service::Daemon, Action::Reload, path) {
        Outcome::Done(command) => {
            println!("daemon reloaded ({command}): {ant} restarts with the rule once its prompts in flight finish");
        }
        Outcome::NotRunning => println!("the daemon service is not running; {ant} has the rule once started"),
        outcome => println!(
            "{}reload `ants daemon` with SIGHUP, or its service ({}), for {ant} to take the rule",
            failure(outcome),
            manual(Service::Daemon, Action::Reload)
        ),
    }
}
