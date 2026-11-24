//! The bus's account pushed to a server through its system account: [`push`] sends the JWTs of the accounts with
//! their revocations, and [`push_and_report`] prints what was applied where, and how to apply it otherwise, as `ants
//! auth push` does and every command that revokes credentials after it.

use std::path::Path;
use std::time::Duration;

use ants_daemon::bus::relayed;
use ants_proto::{DEVICES_BUCKET, JsDomain};
use anyhow::{Context as _, Result, bail};
use futures::StreamExt as _;

use super::revoke::apply;
use crate::auth;
use crate::people::devices;

/// The inbox prefix of [`push`], where the leaf nodes' system account uplinks may answer it.
pub const PUSH_INBOX: &str = "_INBOX.ants-push";

/// How long [`push`] waits for more servers to answer once the one it pushed to has.
const PUSH_SETTLE: Duration = Duration::from_secs(1);

const PUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// The servers that applied a pushed account.
#[derive(Debug)]
pub struct Pushed {
    /// The server pushed to.
    pub server: String,
    /// Other servers that answered, mostly leaf nodes connected to it through a system account uplink, each with the
    /// error it gave, if any.
    pub others: Vec<(String, Option<String>)>,
    /// Whether the server pushed to keeps the accounts pushed to it across restarts ([`keeps_accounts`]); none when
    /// it did not tell.
    pub kept: Option<bool>,
}

/// Send the JWTs of the team's store account, account and system account, with their revocations, to the server at
/// `url`, as a user of the system account signed with its key in `dir`; the server, and every leaf node connected to
/// it through a system account uplink, closes the connections of users revoked since. Fails unless the server applies
/// all three.
pub async fn push(url: &str, dir: &Path) -> Result<Pushed> {
    ants_daemon::bus::check_url(url)?;
    let accounts = apply(dir)?;
    let client = connect_system(url, dir).await?;
    let server = client.server_info().server_name;
    let mut others = Vec::new();
    for (account, jwt) in accounts {
        others.extend(update(&client, &server, &account, jwt).await?);
    }
    Ok(Pushed { kept: keeps_accounts(&client).await, server, others })
}

/// Send `jwt`, that of `account`, through `client`, connected to `server`; the other servers that answered, each with
/// the error it gave, if any. Fails unless `server` applies it.
async fn update(
    client: &async_nats::Client,
    server: &str,
    account: &str,
    jwt: String,
) -> Result<Vec<(String, Option<String>)>> {
    let inbox = client.new_inbox();
    let mut answers = relayed::subscribe(client, inbox.clone()).await?;
    let subject = format!("$SYS.REQ.ACCOUNT.{account}.CLAIMS.UPDATE");
    client.publish_with_reply(subject, inbox, jwt.into()).await.context("asking the server to update the account")?;
    client.flush().await?;
    let mut until = tokio::time::Instant::now() + PUSH_TIMEOUT;
    let (mut pushed, mut others) = (None, Vec::new());
    while let Ok(Some(message)) = tokio::time::timeout_at(until, answers.next()).await {
        let (name, outcome) = answer(&message.payload);
        if name == server && pushed.is_none() {
            pushed = Some(outcome);
            until = tokio::time::Instant::now() + PUSH_SETTLE;
        } else {
            others.push((name, outcome));
        }
    }
    match pushed {
        None => bail!("no answer from server {server}"),
        Some(Some(error)) => bail!("server {server}: {error}"),
        Some(None) => Ok(others),
    }
}

/// A connection to the server at `url` as a short-lived user of the bus's system account, signed with its key in
/// `dir`. A refusal leaves an [`async_nats::ConnectError`] in the error's chain.
pub async fn connect_system(url: &str, dir: &Path) -> Result<async_nats::Client> {
    ants_daemon::bus::check_url(url)?;
    let creds = auth::system_creds(dir)?;
    let connect = async_nats::ConnectOptions::with_credentials(&creds)?
        .name("ants-auth")
        .custom_inbox_prefix(PUSH_INBOX)
        .connection_timeout(PUSH_TIMEOUT)
        .connect(url);
    let client = tokio::time::timeout(PUSH_TIMEOUT, connect).await.context("no answer from the server")?;
    client.context("connecting to NATS")
}

/// Whether the server `client` is connected to, as a system account user, keeps the accounts pushed to it across
/// restarts: only a `full` resolver answers `$SYS.REQ.CLAIMS.LIST`, which no leaf node's system account uplink may
/// hear. The `MEMORY` resolver of `server.conf` has none answer it, so a restart brings back the account of the
/// server's `server.conf`. None without an answer of that server.
pub async fn keeps_accounts(client: &async_nats::Client) -> Option<bool> {
    let server = client.server_info().server_name;
    match relayed::request(client, LIST_REQUEST, "".into()).await {
        Ok(answer) => {
            let answer: serde_json::Value = serde_json::from_slice(&answer.payload).ok()?;
            (answer["server"]["name"] == server.as_str() && answer["data"].is_array()).then_some(true)
        }
        Err(e) if e.kind() == async_nats::RequestErrorKind::NoResponders => Some(false),
        Err(_) => None,
    }
}

const LIST_REQUEST: &str = "$SYS.REQ.CLAIMS.LIST";

/// The server answering an account update, and the error it gave, if any.
fn answer(payload: &[u8]) -> (String, Option<String>) {
    let Ok(answer) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return ("?".to_owned(), Some("unreadable answer".to_owned()));
    };
    let server = answer["server"]["name"].as_str().unwrap_or("?").to_owned();
    let error = match (answer["error"]["description"].as_str(), answer["data"]["message"].as_str()) {
        (Some(description), _) => Some(description.to_owned()),
        (None, Some("jwt updated")) => None,
        (None, Some(message)) => Some(format!("{message} (the account is not loaded there)")),
        (None, None) => Some(format!("unexpected answer {answer}")),
    };
    (server, error)
}

/// Push the bus's account to the server at `url` ([`push`]), saying how to apply it otherwise on failure;
/// once it is applied, record in the hub's directory of encryption keys, in JetStream domain `domain` if it has one,
/// the revoked keys of humans' credentials not recorded yet ([`devices::record_revocations`]).
pub(crate) async fn push_and_report(url: &str, domain: Option<&JsDomain>, dir: &Path) -> Result<()> {
    let pushed = match push(url, dir).await {
        Ok(pushed) => {
            println!(
                "account pushed to server {} ({url}); connections of revoked credentials are closed",
                pushed.server
            );
            let applied: Vec<&str> =
                pushed.others.iter().filter(|(_, error)| error.is_none()).map(|(name, _)| name.as_str()).collect();
            if !applied.is_empty() {
                println!("also applied by: {}", applied.join(", "));
            }
            for (name, error) in &pushed.others {
                if let Some(error) = error {
                    println!("not applied by {name}: {error}");
                }
            }
            let conf = dir.join(auth::SERVER_CONF);
            match pushed.kept {
                Some(true) => println!(
                    "server {} keeps it across restarts (full resolver): copy {} to it only for changes besides the \
                     account's (operator, system account, max_control_line)",
                    pushed.server,
                    conf.display()
                ),
                Some(false) => println!(
                    "server {0} keeps it in memory only: a restart brings back the account of its server.conf, so \
                     copy {1} to it and reload it (SIGHUP); for it to keep pushed accounts, add `resolver {{ type: \
                     full, dir: \"/ABSOLUTE/DIR\" }}` after its `include \"{2}\"` and restart it",
                    pushed.server,
                    conf.display(),
                    auth::SERVER_CONF
                ),
                None => println!(
                    "server {} did not tell whether it keeps pushed accounts across restarts (`ants doctor` on this \
                     machine checks it); without a full resolver, copy {} to it and reload it (SIGHUP)",
                    pushed.server,
                    conf.display()
                ),
            }
            println!(
                "leaf nodes not listed (offline, or with no system account uplink) did not apply it: run `ants auth \
                 push` again once they are connected, unless the hub keeps it (they then catch up within 10 s of \
                 reconnecting)"
            );
            Ok(())
        }
        Err(e) => Err(e.context(format!(
            "the account was not pushed to {url}; the revocations stay in {}: push them with `ants auth push \
             --nats URL` once the hub is in reach, or copy server.conf to the hub and reload it (SIGHUP)",
            dir.display()
        ))),
    };
    pushed?;
    match devices::record_revocations(url, domain, dir).await {
        Ok(recorded) => {
            if !recorded.is_empty() {
                println!(
                    "recorded in {DEVICES_BUCKET} as revoked, so no message is sealed for their devices any more: {}",
                    recorded.join(", ")
                );
            }
            Ok(())
        }
        Err(e) => Err(e.context(format!(
            "the account was pushed, but the revoked keys of humans' credentials are not all recorded in \
             {DEVICES_BUCKET}, so messages may still be sealed for their devices: run `ants auth push --nats URL` again"
        ))),
    }
}
