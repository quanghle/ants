//! The team's humans: the KV bucket [`HUMANS_BUCKET`], which only humans' credentials write and personas read, and the
//! human the CLI acts as ([`Me`]), which registers itself there when it connects ([`register`]).

use std::path::Path;

use ants_proto::{HUMANS_BUCKET, HumanEntry, Humans, PersonaId, Role, subject};
use anyhow::{Context as _, Result, anyhow};
use async_nats::jetstream::stream::DirectGetErrorKind;
use async_nats::jetstream::{self, kv, stream};
use data_encoding::BASE64URL_NOPAD;
use futures::TryStreamExt as _;
use serde_json::Value;

/// The humans bucket, created when missing and the credentials allow it (a human's). Entries never expire: a human
/// stays one until removed.
pub async fn ensure(js: &jetstream::Context) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(HUMANS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(kv::Config {
        bucket: HUMANS_BUCKET.to_owned(),
        description: "Humans of the team, written by humans".to_owned(),
        history: 1,
        storage: stream::StorageType::File,
        ..Default::default()
    })
    .await
    .context("creating the humans bucket (with credentials, `ants init` as a human creates it)")
}

/// Who the CLI acts as: the persona its credentials name, or `human` without credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Me {
    pub id: PersonaId,
    /// A human's credentials, or a persona's (a persona using the CLI).
    pub role: Role,
}

impl Me {
    /// The one the credentials file `creds` names, from the `name` of its user JWT; `human` without credentials.
    pub fn from_creds(creds: Option<&Path>) -> Result<Self> {
        let Some(path) = creds else {
            return Ok(Self { id: PersonaId::human(), role: Role::Human });
        };
        let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
        Self::from_creds_text(&text).with_context(|| format!("credentials {}", path.display()))
    }

    fn from_creds_text(text: &str) -> Result<Self> {
        let jwt = user_jwt(text).context("no user JWT")?;
        let payload = jwt.split('.').nth(1).context("a malformed user JWT")?;
        let payload = BASE64URL_NOPAD
            .decode(payload.trim_end_matches('=').as_bytes())
            .map_err(|e| anyhow!("a malformed user JWT: {e}"))?;
        let claims: Value = serde_json::from_slice(&payload).context("a malformed user JWT")?;
        let name = claims["name"].as_str().context("a user JWT without a name")?;
        let id: PersonaId = name.parse().with_context(|| format!("a user JWT naming {name:?}, no persona"))?;
        // Writing the humans bucket is the right only humans have.
        let humans = format!("$KV.{HUMANS_BUCKET}.>");
        let writes_humans =
            claims["nats"]["pub"]["allow"].as_array().is_some_and(|allow| allow.iter().any(|s| *s == *humans));
        let role = if writes_humans { Role::Human } else { Role::by_default(&id) };
        Ok(Self { id, role })
    }
}

/// The user JWT of a `.creds` file: the line after the `BEGIN NATS USER JWT` marker.
fn user_jwt(creds: &str) -> Option<&str> {
    let mut lines = creds.lines().map(str::trim);
    lines.find(|line| line.starts_with("---") && line.contains("BEGIN NATS USER JWT"))?;
    lines.find(|line| !line.is_empty()).filter(|line| !line.starts_with("---"))
}

/// Register `me` in the humans bucket, creating the bucket when missing, unless its entry is there already: one
/// direct get then. `human`, a human without an entry, and a persona are never registered. Returns whether it put an
/// entry.
pub async fn register(js: &jetstream::Context, me: &Me) -> Result<bool> {
    if me.id.is_human() || !me.role.is_human() {
        return Ok(false);
    }
    let registered = |payload: &[u8]| {
        serde_json::from_slice::<HumanEntry>(payload).is_ok_and(|e| e.check_key(me.id.as_str()).is_ok())
    };
    let bucket = js.get_stream_no_info(format!("KV_{HUMANS_BUCKET}")).await?;
    let found = match bucket.direct_get_last_for_subject(subject::human_entry(&me.id)).await {
        Ok(msg) => Some(registered(&msg.payload)),
        Err(e) if e.kind() == DirectGetErrorKind::NotFound => Some(false),
        // No bucket, or one without direct gets: the bucket tells.
        Err(e) if matches!(e.kind(), DirectGetErrorKind::ErrorResponse(..)) => None,
        Err(e) => return Err(e).context("reading the humans bucket"),
    };
    if found == Some(true) {
        return Ok(false);
    }
    let kv = ensure(js).await?;
    if found.is_none() && kv.get(me.id.as_str()).await?.is_some_and(|payload| registered(&payload)) {
        return Ok(false);
    }
    let entry = HumanEntry::new(me.id.clone());
    kv.put(entry.key(), serde_json::to_vec(&entry)?.into()).await.context("putting the entry")?;
    Ok(true)
}

/// The humans of the team: those the humans bucket lists, `human`, and `me` when a human. Without the bucket, or when
/// it cannot be read (with a warning), only `human` and `me`.
pub async fn load(js: &jetstream::Context, me: &Me) -> Humans {
    let mut humans = match read(js).await {
        Ok(humans) => humans,
        Err(e) => {
            tracing::warn!("reading {HUMANS_BUCKET}; only {} and human count as humans: {e:#}", me.id);
            Humans::default()
        }
    };
    if me.role.is_human() {
        let _ = humans.add(me.id.as_str(), HumanEntry::new(me.id.clone()));
    }
    humans
}

async fn read(js: &jetstream::Context) -> Result<Humans> {
    let Ok(kv) = js.get_key_value(HUMANS_BUCKET).await else { return Ok(Humans::default()) };
    let keys: Vec<String> = kv.keys().await.context("listing the humans")?.try_collect().await?;
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let Some(value) = kv.get(&key).await.with_context(|| format!("reading human {key}"))? else { continue };
        match serde_json::from_slice::<HumanEntry>(&value) {
            Ok(entry) => entries.push((key, entry)),
            Err(e) => tracing::warn!(key, error = %e, "skipping a malformed human entry"),
        }
    }
    Ok(Humans::from_entries(entries.iter().map(|(key, entry)| (key.as_str(), entry.clone()))))
}

/// [`register`], reporting the outcome rather than failing: the CLI works without the entry, though daemons then take
/// the human for an agent.
pub async fn register_or_warn(js: &jetstream::Context, me: &Me) {
    let id = &me.id;
    if !me.role.is_human() {
        tracing::warn!(
            "the credentials of {id} are a persona's: acting as the persona {id}, not registered in {HUMANS_BUCKET} \
             (`ants auth user {id} --human` issues a human's)"
        );
        return;
    }
    match register(js, me).await {
        Ok(true) => tracing::info!("registered {id} in {HUMANS_BUCKET}"),
        Ok(false) => {}
        Err(e) => tracing::warn!(
            "{id} is not in {HUMANS_BUCKET}, so daemons take its messages for an agent's; the next connection tries \
             again: {e:#}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use ants_daemon::bus;
    use serde_json::json;

    use super::*;
    use crate::auth::tests::Server;

    /// A `.creds` file whose user JWT carries `claims`, unsigned: only the claims matter here.
    fn creds_with(claims: Value) -> String {
        let part = |v: &Value| BASE64URL_NOPAD.encode(&serde_json::to_vec(v).unwrap());
        let jwt = format!("{}.{}.c2ln", part(&json!({ "typ": "JWT" })), part(&claims));
        format!("-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\nseed follows\n")
    }

    fn p(s: &str) -> PersonaId {
        s.parse().unwrap()
    }

    #[test]
    fn the_credentials_name_whom_the_cli_acts_as() {
        assert_eq!(Me::from_creds(None).unwrap(), Me { id: PersonaId::human(), role: Role::Human });
        let human = json!({ "pub": { "allow": ["ants.inbox.*.alice", "$KV.ants_humans.>"] } });
        let me = |claims| Me::from_creds_text(&creds_with(claims)).map_err(|e| format!("{e:#}"));
        assert_eq!(me(json!({ "name": "alice", "nats": human })), Ok(Me { id: p("alice"), role: Role::Human }));
        let persona = json!({ "pub": { "allow": ["ants.inbox.*.dev-a"] } });
        assert_eq!(me(json!({ "name": "dev-a", "nats": persona })), Ok(Me { id: p("dev-a"), role: Role::Persona }));
        assert_eq!(me(json!({ "name": "human" })), Ok(Me { id: PersonaId::human(), role: Role::Human }));
        assert!(me(json!({ "name": "Alice Smith" })).unwrap_err().starts_with("a user JWT naming \"Alice Smith\""));
        assert_eq!(me(json!({ "sub": "U123" })).unwrap_err(), "a user JWT without a name");
        assert_eq!(Me::from_creds_text("no JWT here").unwrap_err().to_string(), "no user JWT");
    }

    /// With real credentials: the human's first connection creates the bucket and registers it, later ones find it.
    #[tokio::test]
    async fn a_human_registers_itself_once_and_a_persona_never() {
        let Some(server) = Server::start() else { return };
        let (alice, dev_a) = (p("alice"), p("dev-a"));
        let connect =
            async |me: &Me, creds: &Path| bus::connect(&server.url, Some(creds), None, &me.id, "test").await.unwrap().1;
        let creds = server.creds_as(&alice, Role::Human);
        let me = Me::from_creds(Some(&creds)).unwrap();
        assert_eq!(me, Me { id: alice.clone(), role: Role::Human });
        let a_js = connect(&me, &creds).await;
        assert!(register(&a_js, &me).await.unwrap(), "the first connection registers alice");
        assert!(!register(&a_js, &me).await.unwrap(), "already registered");
        let kv = ensure(&a_js).await.unwrap();
        let entry: HumanEntry = serde_json::from_slice(&kv.get("alice").await.unwrap().unwrap()).unwrap();
        assert_eq!(entry.persona, alice);
        kv.delete("alice").await.unwrap();
        assert!(register(&a_js, &me).await.unwrap(), "a removed human registers again");

        let creds = server.creds_as(&dev_a, Role::Persona);
        let persona = Me::from_creds(Some(&creds)).unwrap();
        assert_eq!(persona.role, Role::Persona);
        assert!(!register(&connect(&persona, &creds).await, &persona).await.unwrap());
        let creds = server.creds_as(&PersonaId::human(), Role::Human);
        let human = Me::from_creds(Some(&creds)).unwrap();
        assert!(!register(&connect(&human, &creds).await, &human).await.unwrap(), "human needs no entry");
        let keys: Vec<String> = kv.keys().await.unwrap().try_collect().await.unwrap();
        assert_eq!(keys, ["alice"]);
    }
}
