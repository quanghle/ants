//! Channels: whether each channel's members, messenger and owner are ants of the bus or humans, none left over from
//! an ant retired or revoked, or a human gone.

use std::collections::BTreeSet;

use ants_proto::{CHANNELS_BUCKET, ChannelEntry, Humans, UserId};

use super::{Check, Connected, NETWORK_TIMEOUT, Setup};

const CHECK: &str = "channels";

/// The channels as the CLI's credentials read them ([`findings`]), with the ants of the bus and its humans.
pub(super) async fn check_channels(connected: Option<&Connected>, setup: &Setup) -> Vec<Check> {
    let Some(connected) = connected else { return vec![Check::skip(CHECK, "the hub is not reached")] };
    let domain = setup.js_domain.as_ref();
    let read = async {
        let kv = connected.js.get_key_value(CHANNELS_BUCKET).await?;
        let entries = crate::channel::list(&kv).await?;
        let names: BTreeSet<&UserId> = entries.iter().flat_map(named).collect();
        let names: Vec<&UserId> = names.into_iter().collect();
        let known = crate::teams::known_ants(&connected.js, domain, &names).await?;
        let humans = crate::people::humans::read(&connected.js).await?;
        anyhow::Ok((entries, known, humans))
    };
    match tokio::time::timeout(NETWORK_TIMEOUT * 2, read).await {
        Ok(Ok((entries, known, humans))) => findings(&entries, &known, &humans),
        Ok(Err(e)) => vec![Check::skip(CHECK, format!("no channels read: {e:#}"))],
        Err(_) => vec![Check::skip(CHECK, "no channels read within 10s")],
    }
}

/// The members, the messenger and the owner `entry` names.
fn named(entry: &ChannelEntry) -> impl Iterator<Item = &UserId> {
    entry.members.iter().chain(&entry.messenger).chain(&entry.owner)
}

/// What is left over in `entries`, one warning each: a member, messenger or owner neither an ant of `known` (one with
/// an identity record) nor one of `humans`, as an ant retired or revoked leaves behind; else one check telling the
/// channels are sound.
pub(super) fn findings(entries: &[ChannelEntry], known: &BTreeSet<UserId>, humans: &Humans) -> Vec<Check> {
    let gone = |user: &UserId| !known.contains(user) && !humans.contains(user);
    let mut checks = Vec::new();
    for entry in entries {
        let channel = &entry.channel;
        for member in entry.members.iter().filter(|m| gone(m)) {
            checks.push(Check::warn(
                CHECK,
                format!("#{channel} holds {member}, neither an ant of the bus nor a human"),
                vec![format!("ants channel remove {channel} {member}")],
            ));
        }
        if let Some(messenger) = entry.messenger.as_ref().filter(|m| gone(m)) {
            checks.push(Check::warn(
                CHECK,
                format!(
                    "the messenger of #{channel}, {messenger}, is not an ant of the bus: humans' posts mentioning \
                     nobody go untriaged"
                ),
                vec![format!("ants channel set {channel} --default-messenger")],
            ));
        }
        if let Some(owner) = entry.owner.as_ref().filter(|o| gone(o)) {
            checks.push(Check::warn(
                CHECK,
                format!("the owner of #{channel}, {owner}, is not an ant of the bus"),
                vec![format!("ants channel set {channel} --no-owner")],
            ));
        }
    }
    if checks.is_empty() {
        let detail = match entries.len() {
            0 => "no channels".to_owned(),
            1 => "1 channel, its members, messenger and owner ants of the bus or humans".to_owned(),
            n => format!("{n} channels, their members, messengers and owners ants of the bus or humans"),
        };
        checks.push(Check::ok(CHECK, detail));
    }
    checks
}
