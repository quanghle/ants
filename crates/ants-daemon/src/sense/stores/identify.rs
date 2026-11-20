//! The sense id of an ant's home settled against its identity record ([`take_named`], [`settle`]), and the growth
//! the record holds noted on the ant's card ([`note_growth`]).

use std::path::Path;

use ants_proto::{AntIdentity, SenseId, UserId};
use ants_sense::home::Home;
use ants_sense::identity::{self, Growth};
use ants_sense::store::{Items, StoreError};

use super::SenseStores;
use crate::profile::identity::Identity;

/// Have the home of `ant`, `home`, keep a sense id when it keeps none: the one its identity `record` names, or a new
/// one the record then names, unless another daemon named one first. Nothing when it keeps one, which [`settle`]
/// settles against the record once opened.
pub(super) async fn take_named(ant: &UserId, home: &Path, record: &Identity) -> Result<(), StoreError> {
    let unavailable = |e: anyhow::Error| StoreError::Unavailable(format!("naming the sense identity of {ant}: {e:#}"));
    if identity::read_id(home).map_err(|e| unavailable(e.into()))?.is_some() || Home::new(home).has_pending_writes() {
        return Ok(());
    }
    let minted = SenseId::new();
    let named = record.name_sense(&minted, None).await.map_err(unavailable)?;
    let sense = named.and_then(|record| record.sense).unwrap_or(minted);
    identity::keep_id(home, &sense).map_err(|e| unavailable(e.into()))
}

/// Settle the sense id of `items`, those of `ant` whose home is `home`, against its identity `record`, naming it there
/// when the record names none, the record then starting from the growth the card and the hub kept of an earlier one,
/// as after a revoke and re-add, and noting the record's on the card: whether it is settled, false when the record
/// cannot be read, or the card when the record names no sense id, so that a later call settles it; refused when the
/// record names another sense id, with how to settle that.
pub(super) async fn settle(
    stores: &SenseStores,
    ant: &UserId,
    home: &Path,
    record: &Identity,
    items: &dyn Items,
) -> Result<bool, StoreError> {
    let sense = items.sense().await;
    let unsettled = |e: &dyn std::fmt::Display| {
        tracing::warn!(%ant, error = format!("{e:#}"), "naming the sense identity in the ant's identity record");
        Ok(false)
    };
    let stored = match identity::card(items).await {
        Ok(card) => {
            let earlier = card.and_then(|card| card.growth).map(|growth| (growth.born, growth.prompts));
            match record.resume_sense(&sense, earlier).await {
                Ok(stored) => stored,
                Err(e) => return unsettled(&e),
            }
        }
        // Only a record naming no sense id yet starts from the card's growth.
        Err(e) => match record.read().await {
            Ok(Some(stored)) if stored.sense.is_some() => Some(stored),
            Ok(_) => {
                return if stores.state().warned_unreadable_cards.insert(sense) { unsettled(&e) } else { Ok(false) };
            }
            Err(e) => return unsettled(&e),
        },
    };
    match stored.as_ref().and_then(|stored| stored.sense.as_ref()) {
        Some(named) if *named != sense => Err(StoreError::Unavailable(format!(
            "the home of {ant} ({}) keeps sense identity {sense} and its identity record names {named}: `ants ant \
             sense-id {ant} --take home` on this machine keeps the home's, `--take record` the record's",
            home.display()
        ))),
        _ => {
            if let Some(stored) = &stored
                && let Err(e) = note_growth(ant, items, stored).await
            {
                tracing::warn!(%ant, error = format!("{e:#}"), "noting the ant's growth on its card");
            }
            Ok(true)
        }
    }
}

/// Note on the card of `ant` in `items` the birth and the prompt count its identity `record` holds, when the record
/// names their sense id, so a record made again for the ant starts from them ([`settle`]).
pub(in crate::sense) async fn note_growth(
    ant: &UserId,
    items: &dyn Items,
    record: &AntIdentity,
) -> Result<(), StoreError> {
    if record.sense.as_ref() != Some(&items.sense().await) {
        return Ok(());
    }
    identity::record_growth(items, ant, Growth { born: record.born, prompts: record.prompts }).await.map(drop)
}
