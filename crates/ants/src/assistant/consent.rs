//! `ants assistant confirm|allow`: the human of the CLI's credentials consents to what its assistant decided for it,
//! one act at a time ([`Confirmations`]) or ahead by rules ([`PreAllow`]), records its device signs and keeps under
//! its own name in [`HUMANS_BUCKET`]. The daemon that applies an act verifies them; see `ants_daemon::approvals`.

use ants_daemon::Allowlist;
use ants_proto::{
    ActAnswer, Confirmations, HUMANS_BUCKET, MAX_PRE_ALLOW, PreAllow, STREAM, confirmations_key, pre_allow_key,
    pre_allow_rule,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::kv;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{CONFLICTS, after, designated, device_creds, signing_creds};
use crate::{Nats, auth, humans, tools};

/// `ants assistant confirm [ACT] [--deny]`.
pub async fn confirm(nats: &Nats<'_>, act: Option<&str>, deny: bool) -> Result<()> {
    let creds = signing_creds(nats)?;
    let human = &nats.me.id;
    let (_, js) = nats.connect("ants-assistant").await?;
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let now = Utc::now();
    let held: Vec<_> = tools::held(&js, &stream, now)
        .await?
        .into_iter()
        .filter_map(|(request, acts)| {
            let acts: Vec<_> = acts.into_iter().filter(|a| a.human == *human).collect();
            (!acts.is_empty()).then_some((request, acts))
        })
        .collect();
    let Some(act) = act else {
        if held.is_empty() {
            println!("no decision of an assistant waits for {human} to confirm it");
        }
        for (request, acts) in &held {
            for a in acts {
                println!(
                    "{}  {} chose {} for {human}; {}: {}; expires in {}",
                    tools::qualified(request),
                    a.assistant,
                    tools::spelling(a.verdict),
                    request.tool.kind,
                    request.tool.title.lines().next().unwrap_or_default(),
                    tools::left(request.expires_at, now)
                );
            }
        }
        return Ok(());
    };
    let query = tools::Query::parse(act)?;
    let requests = held.iter().map(|(request, _)| request.clone()).collect();
    let request = tools::select(requests, &query)
        .with_context(|| format!("no decision of an assistant on {query} waits for {human} to confirm it"))?;
    let acts = held.into_iter().find(|(r, _)| r.id == request.id).map_or_default(|(_, acts)| acts);
    let Some(assistant) = designated(&js, nats.account.as_deref(), human).await else {
        bail!("{human} has no assistant: nothing an assistant decided takes effect");
    };
    let Some(held) = acts.into_iter().rev().find(|a| a.assistant == assistant) else {
        bail!("{assistant}, the assistant of {human}, decided nothing on {}", tools::qualified(&request));
    };
    let answer = if deny { ActAnswer::Deny } else { ActAnswer::Confirm };
    let (jwt, key, account) = device_creds(creds, human)?;
    let kv = humans::ensure(&js).await?;
    let record_key = confirmations_key(human);
    store(&kv, &record_key, "the confirmations", |before: Option<Confirmations>, now| {
        let before = before.filter(|c| c.verify(&record_key, &account, now).is_ok());
        let acts = Confirmations::answering(
            before.as_ref().map_or(&[][..], |c| &c.acts),
            &assistant,
            held.act(&request),
            answer,
            request.expires_at,
            now,
        );
        let issued_at = after(now, before.map(|c| c.issued_at));
        let mut record = Confirmations::new(human.clone(), acts, issued_at, jwt.clone());
        record.signature = auth::sign(&key, &record.signed())?;
        Ok(record)
    })
    .await?;
    let (verdict, id) = (tools::spelling(held.verdict), tools::qualified(&request));
    match answer {
        ActAnswer::Confirm => println!("confirmed: {assistant}'s {verdict} on request {id} takes effect for {human}"),
        ActAnswer::Deny => println!("denied: {assistant}'s {verdict} on request {id} is dropped"),
    }
    Ok(())
}

/// `ants assistant allow [RULE...] [--remove] [--clear]`.
pub async fn allow(nats: &Nats<'_>, rules: Vec<String>, remove: bool, clear: bool) -> Result<()> {
    let human = &nats.me.id;
    let rules: Vec<String> = rules.iter().map(|r| r.trim().to_owned()).collect();
    for rule in &rules {
        check_rule(rule)?;
    }
    let (_, js) = nats.connect("ants-assistant").await?;
    let account = nats.account.as_deref();
    let assistant = designated(&js, account, human).await;
    let record_key = pre_allow_key(human);
    if rules.is_empty() && !clear {
        let kv = js.get_key_value(HUMANS_BUCKET).await.ok();
        let stored = match &kv {
            Some(kv) => tools::record::<PreAllow>(kv, &record_key).await?,
            None => None,
        };
        let stored = stored.filter(|p| account.is_some_and(|a| p.verify(&record_key, a, Utc::now()).is_ok()));
        match stored.filter(|p| !p.rules.is_empty()) {
            Some(p) if Some(&p.assistant) == assistant.as_ref() => {
                println!("decisions of {} for {human} take effect unconfirmed on:", p.assistant);
                for rule in &p.rules {
                    println!("  {rule}");
                }
            }
            Some(p) => println!("{human} has rules for {}, no longer its assistant: none stand", p.assistant),
            None => println!("{human} has no pre-allow rules"),
        }
        return Ok(());
    }
    let creds = signing_creds(nats)?;
    let Some(assistant) = assistant else {
        bail!("{human} has no assistant: designate one with `ants assistant set ANT` first");
    };
    let (jwt, key, account) = device_creds(creds, human)?;
    let kv = humans::ensure(&js).await?;
    let stored = store(&kv, &record_key, "the pre-allow rules", |before: Option<PreAllow>, now| {
        let before = before.filter(|p| p.verify(&record_key, &account, now).is_ok());
        let issued_at = after(now, before.as_ref().map(|p| p.issued_at));
        let kept = before.filter(|p| p.assistant == assistant).map_or_default(|p| p.rules);
        let mut kept: Vec<String> = match (clear, remove) {
            (true, _) => Vec::new(),
            (false, true) => kept.into_iter().filter(|r| !rules.contains(r)).collect(),
            (false, false) => kept,
        };
        if !clear && !remove {
            for rule in &rules {
                if !kept.contains(rule) {
                    kept.push(rule.clone());
                }
            }
        }
        if kept.len() > MAX_PRE_ALLOW {
            bail!("{} rules, more than {MAX_PRE_ALLOW}", kept.len());
        }
        let mut record = PreAllow::new(human.clone(), assistant.clone(), kept, issued_at, jwt.clone());
        record.signature = auth::sign(&key, &record.signed())?;
        Ok(record)
    })
    .await?;
    if stored.rules.is_empty() {
        println!("every decision of {assistant} for {human} waits for {human} to confirm it");
    } else {
        println!("decisions of {assistant} for {human} take effect unconfirmed on:");
        for rule in &stored.rules {
            println!("  {rule}");
        }
    }
    Ok(())
}

/// Refuse a pre-allow rule [`pre_allow_rule`] or an ant's allowlist would not take.
fn check_rule(rule: &str) -> Result<()> {
    let Some((_, tool)) = pre_allow_rule(rule) else {
        bail!("pre-allow rule {rule:?}: expected ANT/RULE or */RULE");
    };
    Allowlist::parse(&[tool]).with_context(|| format!("pre-allow rule {rule:?}"))?;
    Ok(())
}

/// Store the record `build` makes from the one stored at `key` and the time, in its place, trying again while another
/// device of the human stores one meanwhile; `what` names it in errors. Returns the record stored.
async fn store<T: Serialize + DeserializeOwned>(
    kv: &kv::Store,
    key: &str,
    what: &str,
    mut build: impl FnMut(Option<T>, DateTime<Utc>) -> Result<T>,
) -> Result<T> {
    for _ in 0..CONFLICTS {
        let stored = kv.entry(key).await.with_context(|| format!("reading {what}"))?;
        let (revision, before) = match &stored {
            Some(entry) if entry.operation == kv::Operation::Put => {
                (Some(entry.revision), serde_json::from_slice::<T>(&entry.value).ok())
            }
            Some(entry) => (Some(entry.revision), None),
            None => (None, None),
        };
        let record = build(before, Utc::now())?;
        let value = serde_json::to_vec(&record)?.into();
        let stored = match revision {
            Some(revision) => kv.update(key, value, revision).await.map_err(|e| e.kind().to_string()),
            None => kv.create(key, value).await.map_err(|e| e.kind().to_string()),
        };
        match stored {
            Ok(_) => return Ok(record),
            Err(e) if e.contains("wrong last") || e.contains("already exists") => {}
            Err(e) => bail!("storing {what} in {HUMANS_BUCKET}: {e}"),
        }
    }
    bail!("storing {what}: another device kept storing them")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_allow_rules_name_an_ant_or_all_and_an_allow_rule() {
        for rule in ["*/execute:cargo test*", "dev-a/read:/work/*", "dev-a/*"] {
            check_rule(rule).unwrap();
        }
        for rule in ["execute:cargo test*", "*/", "dev-a/launch", "dev a/read"] {
            assert!(check_rule(rule).is_err(), "{rule} taken");
        }
    }
}
