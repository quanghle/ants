//! The team's account pushed to a server, as `ants auth push` does and every command that revokes credentials after
//! it: what was applied where is printed, and how to apply it otherwise.

use std::path::Path;

use ants_proto::{DEVICES_BUCKET, JsDomain};
use anyhow::Result;

use crate::auth;
use crate::credentials::revoke;
use crate::people::devices;

/// Push the team's account to the server at `url` ([`revoke::push`]), saying how to apply it otherwise on failure;
/// once it is applied, record in the hub's directory of encryption keys, in JetStream domain `domain` if it has one,
/// the revoked keys of humans' credentials not recorded yet ([`devices::record_revocations`]).
pub(crate) async fn push_and_report(url: &str, domain: Option<&JsDomain>, dir: &Path) -> Result<()> {
    let pushed = match revoke::push(url, dir).await {
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
