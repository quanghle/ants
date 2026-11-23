//! `ants assistant confirm|allow`: the human of the CLI's credentials consents to what its assistant decided or
//! ordered for it, one act at a time ([`Confirmations`]) or ahead by rules ([`PreAllow`]), records its device signs and
//! keeps under its own name in [`HUMANS_BUCKET`]. The daemon that applies an act verifies them; see
//! `ants_daemon::approvals` and, for admin orders held by their machine's daemon, `ants_daemon::admin::held`.

use ants_daemon::Allowlist;
use ants_daemon::admin::held::held_orders;
use ants_daemon::bus::relayed::GuardedKv as _;
use ants_proto::{
    ASSISTANT_ORDER_KINDS, Act, ActAnswer, AdminOrder, ApprovalRequest, Confirmations, HUMANS_BUCKET, MACHINES_BUCKET,
    MAX_PRE_ALLOW, ORDER_HOLD, ORDER_RULE, PreAllow, STREAM, UserId, confirmations_key, pre_allow_key, pre_allow_rule,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use async_nats::jetstream::kv;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{CONFLICTS, after, designated, device_creds, signing_creds};
use crate::cas;
use crate::connection::Nats;
use crate::people::humans;
use crate::tools::HeldAct;
use crate::{auth, tools};

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
    let orders = orders_held_for(&js, human).await?;
    let Some(act) = act else {
        if held.is_empty() && orders.is_empty() {
            println!("no decision or order of an assistant waits for {human} to confirm it");
        }
        for order in &orders {
            println!(
                "{}  {} ordered {} for {human}; expires in {}",
                Act::Order { machine: order.machine.clone(), ant: order.ant.clone(), id: order.id }.qualified(),
                order.admin,
                order.op.describe(),
                tools::left(order.sent_at + ORDER_HOLD, now)
            );
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
    let answer = if deny { ActAnswer::Deny } else { ActAnswer::Confirm };
    if act.matches('/').count() == 2 {
        let order = select_order(orders, act)
            .with_context(|| format!("no order of an assistant {act} waits for {human} to confirm it"))?;
        println!("{}", answer_order(&js, creds, nats.account.as_deref(), human, &order, answer).await?);
        return Ok(());
    }
    let query = tools::Query::parse(act)?;
    let requests = held.iter().map(|(request, _)| request.clone()).collect();
    let request = tools::select(requests, &query)
        .with_context(|| format!("no decision of an assistant on {query} waits for {human} to confirm it"))?;
    let acts = held.into_iter().find(|(r, _)| r.id == request.id).map_or_default(|(_, acts)| acts);
    println!("{}", answer_decision(&js, creds, nats.account.as_deref(), human, &request, &acts, answer).await?);
    Ok(())
}

/// Record `human`'s `answer` to the decision on `request`, of those its assistants made for it (`held`), that the
/// assistant its grant names now made, signed with the device credentials `creds` of the team of `account`. What was
/// done, in a line.
pub(crate) async fn answer_decision(
    js: &jetstream::Context,
    creds: &std::path::Path,
    account: Option<&str>,
    human: &UserId,
    request: &ApprovalRequest,
    held: &[HeldAct],
    answer: ActAnswer,
) -> Result<String> {
    let Some(assistant) = designated(js, account, human).await else {
        bail!("{human} has no assistant: nothing an assistant decided takes effect");
    };
    let Some(held) = held.iter().rev().find(|a| a.assistant == assistant && a.human == *human) else {
        bail!("{assistant}, the assistant of {human}, decided nothing on {}", tools::qualified(request));
    };
    record(js, creds, human, &assistant, held.act(request), answer, request.expires_at).await?;
    let (verdict, id) = (tools::spelling(held.verdict), tools::qualified(request));
    Ok(match answer {
        ActAnswer::Confirm => format!("confirmed: {assistant}'s {verdict} on request {id} takes effect for {human}"),
        ActAnswer::Deny => format!("denied: {assistant}'s {verdict} on request {id} is dropped"),
    })
}

/// Record `human`'s `answer` to `act` its assistant `assistant` made, standing `until`, signed with the device
/// credentials `creds`.
async fn record(
    js: &jetstream::Context,
    creds: &std::path::Path,
    human: &UserId,
    assistant: &UserId,
    act: Act,
    answer: ActAnswer,
    until: DateTime<Utc>,
) -> Result<()> {
    let (jwt, key, account) = device_creds(creds, human)?;
    let kv = humans::ensure(js).await?;
    let record_key = confirmations_key(human);
    store(&kv, &record_key, "the confirmations", |before: Option<Confirmations>, now| {
        let before = before.filter(|c| c.verify(&record_key, &account, now).is_ok());
        let acts = Confirmations::answering(
            before.as_ref().map_or(&[][..], |c| &c.acts),
            assistant,
            act.clone(),
            answer,
            until,
            now,
        );
        let issued_at = after(now, before.map(|c| c.issued_at));
        let mut record = Confirmations::new(human.clone(), acts, issued_at, jwt.clone());
        record.signature = auth::sign(&key, &record.signed())?;
        Ok(record)
    })
    .await?;
    Ok(())
}

/// Record `human`'s `answer` to `order`, which a machine's daemon holds for it, signed with the device credentials
/// `creds` of the team of `account`: refused unless the assistant the grant names now sent it. What was done, in a line.
pub(crate) async fn answer_order(
    js: &jetstream::Context,
    creds: &std::path::Path,
    account: Option<&str>,
    human: &UserId,
    order: &AdminOrder,
    answer: ActAnswer,
) -> Result<String> {
    let act = Act::Order { machine: order.machine.clone(), ant: order.ant.clone(), id: order.id };
    let qualified = act.qualified();
    let Some(assistant) = designated(js, account, human).await else {
        bail!("{human} has no assistant: nothing an assistant ordered takes effect");
    };
    if order.admin != assistant {
        bail!("{} sent order {qualified}, not {assistant}, the assistant of {human}", order.admin);
    }
    record(js, creds, human, &assistant, act, answer, order.sent_at + ORDER_HOLD).await?;
    let op = order.op.describe();
    Ok(match answer {
        ActAnswer::Confirm => format!("confirmed: {assistant}'s order {qualified} ({op}) is carried out"),
        ActAnswer::Deny => format!("denied: {assistant}'s order {qualified} ({op}) is refused"),
    })
}

/// The admin orders the machines' daemons hold for `human` to confirm, oldest first.
pub(crate) async fn orders_held_for(js: &jetstream::Context, human: &UserId) -> Result<Vec<AdminOrder>> {
    let mut orders: Vec<AdminOrder> = crate::machines::latest(js, &format!("$KV.{MACHINES_BUCKET}.*.held"))
        .await?
        .into_iter()
        .flat_map(|(_, value)| held_orders(&value))
        .filter(|order| order.on_behalf_of.as_ref() == Some(human))
        .collect();
    orders.sort_by_key(|order| order.sent_at);
    Ok(orders)
}

/// The order of `orders` that `act`, `MACHINE/ANT/ID` with the id or a prefix of it, names alone.
fn select_order(orders: Vec<AdminOrder>, act: &str) -> Result<AdminOrder> {
    let mut parts = act.splitn(3, '/');
    let (Some(machine), Some(ant), Some(id)) = (parts.next(), parts.next(), parts.next()) else {
        bail!("expected MACHINE/ANT/ORDER-ID");
    };
    let mut found = orders
        .into_iter()
        .filter(|o| o.machine.as_str() == machine && o.ant.as_str() == ant && o.id.to_string().starts_with(id));
    match (found.next(), found.next()) {
        (Some(order), None) if !id.is_empty() => Ok(order),
        (Some(_), Some(_)) => bail!("{act} names more than one order"),
        _ => bail!("no such order"),
    }
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
        let kv =
            js.get_key_value(HUMANS_BUCKET).await.inspect_err(|e| tracing::debug!("{HUMANS_BUCKET} unread: {e}")).ok();
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

/// Refuse a pre-allow rule [`pre_allow_rule`] or an ant's allowlist would not take, unless it is an order rule
/// (`order:KIND` or `order:*`) naming a kind of admin order an assistant may send.
fn check_rule(rule: &str) -> Result<()> {
    let Some((_, tool)) = pre_allow_rule(rule) else {
        bail!("pre-allow rule {rule:?}: expected ANT/RULE or */RULE");
    };
    if let Some(kind) = tool.strip_prefix(ORDER_RULE) {
        if kind != "*" && !ASSISTANT_ORDER_KINDS.contains(&kind) {
            bail!(
                "pre-allow rule {rule:?}: expected {ORDER_RULE}* or {ORDER_RULE}KIND, KIND one of {}",
                ASSISTANT_ORDER_KINDS.join(", ")
            );
        }
        return Ok(());
    }
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
        let stored = kv.guarded_entry(key).await.with_context(|| format!("reading {what}"))?;
        let (revision, before) = match &stored {
            Some(entry) if entry.operation == kv::Operation::Put => {
                (Some(entry.revision), serde_json::from_slice::<T>(&entry.value).ok())
            }
            Some(entry) => (Some(entry.revision), None),
            None => (None, None),
        };
        let record = build(before, Utc::now())?;
        let value = serde_json::to_vec(&record)?;
        let stored = match revision {
            Some(revision) => cas::update(kv, key, value, revision).await.map_err(|e| e.kind().to_string()),
            None => cas::create(kv, key, value).await.map_err(|e| e.kind().to_string()),
        };
        match stored {
            Ok(true) => return Ok(record),
            Ok(false) => {}
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
        for rule in ["*/execute:cargo test*", "dev-a/read:/work/*", "dev-a/*", "dev-a/order:pause", "*/order:*"] {
            check_rule(rule).unwrap();
        }
        for rule in ["execute:cargo test*", "*/", "dev-a/launch", "dev a/read", "*/order:retire", "*/order:"] {
            assert!(check_rule(rule).is_err(), "{rule} taken");
        }
    }
}
