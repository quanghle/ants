//! The team's humans: the KV bucket [`HUMANS_BUCKET`], which only humans' credentials write and ants read, and the
//! human the CLI acts as ([`Me`]), which registers itself there when it connects ([`register`]).

use ants_daemon::bus::relayed::GuardedKv as _;
use std::path::Path;

use ants_daemon::stream::reading::Reading;
use ants_proto::{DeviceId, HUMANS_BUCKET, HumanEntry, Humans, Role, UserId, subject};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::stream::DirectGetErrorKind;
use async_nats::jetstream::{self, kv};
use data_encoding::BASE64URL_NOPAD;
use futures::TryStreamExt as _;
use serde_json::Value;

/// The humans bucket, created as `ants init` does when missing and the credentials allow it (a human's). Entries
/// never expire: a human stays one until removed.
pub async fn ensure(js: &jetstream::Context) -> Result<kv::Store> {
    Box::pin(ants_daemon::hub::ensure_bucket(js, ants_daemon::hub::Object::Humans))
        .await
        .context("opening the humans bucket (with credentials, `ants init` as a human creates it)")
}

/// Who the CLI acts as: the user its credentials name, a human or an ant, or without credentials the human the config
/// file's `human` names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Me {
    pub id: UserId,
    /// A human's credentials, or an ant's (an ant using the CLI).
    pub role: Role,
    /// The human an ant's credentials make it the assistant of ([`ants_proto::AssistantOf`]).
    pub assistant_of: Option<UserId>,
}

impl Me {
    /// The one the credentials file `creds` names, from the `name` of its user JWT; without credentials, the human
    /// `human` (the config file's `human`, or `ANTS_HUMAN`), failing when that is unset too.
    pub fn from_creds(creds: Option<&Path>, human: Option<&UserId>) -> Result<Self> {
        let Some(path) = creds else {
            let Some(human) = human else {
                bail!(
                    "no credentials and no human to act as: set `human` in the config file (top level or [cli]), \
                     or ANTS_HUMAN, or give --creds"
                );
            };
            return Ok(Self { id: human.clone(), role: Role::Human, assistant_of: None });
        };
        let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
        Self::from_creds_text(&text).with_context(|| format!("credentials {}", path.display()))
    }

    /// Refuse the credentials of an ant for `what`, which the server allows only a human.
    pub fn must_be_human(&self, what: &str) -> Result<()> {
        if !self.role.is_human() {
            bail!("{what} needs a human's credentials; those of {} are an ant's", self.id);
        }
        Ok(())
    }

    fn from_creds_text(text: &str) -> Result<Self> {
        let claims = creds_claims(text)?;
        let name = claims["name"].as_str().context("a user JWT without a name")?;
        let id: UserId = name.parse().with_context(|| format!("a user JWT naming {name:?}, not a user id"))?;
        let role = if writes_humans(&claims) { Role::Human } else { Role::Ant };
        let tags = claims["nats"]["tags"].as_array().into_iter().flatten().filter_map(Value::as_str);
        let assistant_of = if role.is_human() { None } else { ants_proto::AssistantOf::from_tags(tags) };
        Ok(Self { id, role, assistant_of })
    }

    /// How the CLI reads messages back as this user ([`Reading::cli`]).
    pub fn reading(&self) -> Reading {
        Reading::cli(self.role, self.assistant_of.is_some())
    }
}

/// The device of a human the credentials file `creds` were issued for ([`DeviceId`]), which a human's credentials
/// carry; none for an ant's, and without credentials.
pub fn device(creds: Option<&Path>) -> Result<Option<DeviceId>> {
    let Some(path) = creds else { return Ok(None) };
    let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    device_of(&text).with_context(|| format!("credentials {}", path.display()))
}

fn device_of(text: &str) -> Result<Option<DeviceId>> {
    let claims = creds_claims(text)?;
    if !writes_humans(&claims) {
        return Ok(None);
    }
    let tags = claims["nats"]["tags"].as_array().into_iter().flatten().filter_map(Value::as_str);
    DeviceId::from_tags(tags).context("a human's credentials without a device id").map(Some)
}

/// Whether user JWT `claims` write the humans bucket, the right only humans have.
pub fn writes_humans(claims: &Value) -> bool {
    let publish = claims["nats"]["pub"]["allow"].as_array().into_iter().flatten().filter_map(Value::as_str);
    ants_proto::writes_human_entries(publish)
}

/// What the credentials let the CLI publish, so JetStream API requests they would have denied, which only time out,
/// are not made.
#[derive(Debug, Clone)]
pub struct Rights {
    /// The claims of the user JWT; None without credentials, which allow everything.
    claims: Option<Value>,
}

impl Rights {
    /// The rights of the credentials file `creds`.
    pub fn from_creds(creds: Option<&Path>) -> Result<Self> {
        let Some(path) = creds else { return Ok(Self { claims: None }) };
        let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
        let claims = creds_claims(&text).with_context(|| format!("credentials {}", path.display()))?;
        Ok(Self { claims: Some(claims) })
    }

    /// Whether they allow publishing `subject`: listed as it is, or no publish allow list at all.
    pub fn may_publish(&self, subject: &str) -> bool {
        match &self.claims {
            Some(claims) if claims["nats"]["pub"]["allow"].is_array() => may_publish(claims, subject),
            _ => true,
        }
    }
}

/// The claims of the user JWT in a `.creds` file's text.
pub fn creds_claims(text: &str) -> Result<Value> {
    let jwt = crate::auth::creds_jwt(text).context("no user JWT")?;
    let payload = jwt.split('.').nth(1).context("a malformed user JWT")?;
    let payload = BASE64URL_NOPAD
        .decode(payload.trim_end_matches('=').as_bytes())
        .map_err(|e| anyhow!("a malformed user JWT: {e}"))?;
    serde_json::from_slice(&payload).context("a malformed user JWT")
}

/// Whether user JWT `claims` allow publishing `subject`, as listed.
pub fn may_publish(claims: &Value, subject: &str) -> bool {
    claims["nats"]["pub"]["allow"].as_array().is_some_and(|allow| allow.iter().any(|s| s == subject))
}

/// Register `me` in the humans bucket, creating the bucket when missing, unless its entry is there already: one
/// direct get then. An ant is never registered. Returns whether it put an entry.
pub async fn register(js: &jetstream::Context, me: &Me) -> Result<bool> {
    if !me.role.is_human() {
        return Ok(false);
    }
    let registered = |payload: &[u8]| {
        serde_json::from_slice::<HumanEntry>(payload).is_ok_and(|e| e.check_key(me.id.as_str()).is_ok())
    };
    let bucket = js.get_stream_no_info(subject::kv_stream(HUMANS_BUCKET)).await?;
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
    if found.is_none() && kv.guarded_get(me.id.as_str()).await?.is_some_and(|payload| registered(&payload)) {
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

pub(crate) async fn read(js: &jetstream::Context) -> Result<Humans> {
    let Ok(kv) = js.get_key_value(HUMANS_BUCKET).await else { return Ok(Humans::default()) };
    let keys: Vec<String> = kv.keys().await.context("listing the humans")?.try_collect().await?;
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys.into_iter().filter(|key| !key.contains('.')) {
        let Some(value) = kv.guarded_get(&key).await.with_context(|| format!("reading human {key}"))? else { continue };
        match serde_json::from_slice::<HumanEntry>(&value) {
            Ok(entry) => entries.push((key, entry)),
            Err(e) => tracing::warn!(key, error = %e, "skipping a malformed human entry"),
        }
    }
    Ok(Humans::from_entries(entries.iter().map(|(key, entry)| (key.as_str(), entry.clone()))))
}

/// [`register`], reporting the outcome rather than failing: the CLI works without the entry, though daemons then take
/// the human for an ant.
pub async fn register_or_warn(js: &jetstream::Context, me: &Me) {
    let id = &me.id;
    if !me.role.is_human() {
        tracing::warn!(
            "the credentials of {id} are an ant's: acting as the ant {id}, not registered in {HUMANS_BUCKET} \
             (`ants auth user {id} --human` issues a human's)"
        );
        return;
    }
    match register(js, me).await {
        Ok(true) => tracing::info!("registered {id} in {HUMANS_BUCKET}"),
        Ok(false) => {}
        Err(e) => tracing::warn!(
            "{id} is not in {HUMANS_BUCKET}, so daemons take its messages for an ant's; the next connection tries \
             again: {e:#}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use ants_daemon::bus;
    use ants_test_support::user as p;
    use serde_json::json;

    use super::*;
    use crate::test_support::Server;

    /// A `.creds` file whose user JWT carries `claims`, unsigned: only the claims matter here.
    fn creds_with(claims: Value) -> String {
        let part = |v: &Value| BASE64URL_NOPAD.encode(&serde_json::to_vec(v).unwrap());
        let jwt = format!("{}.{}.c2ln", part(&json!({ "typ": "JWT" })), part(&claims));
        format!("-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\nseed follows\n")
    }

    #[test]
    fn the_credentials_name_whom_the_cli_acts_as() {
        let err = Me::from_creds(None, None).unwrap_err().to_string();
        assert!(err.contains("no credentials and no human to act as"), "{err}");
        assert_eq!(
            Me::from_creds(None, Some(&p("alice"))).unwrap(),
            Me { id: p("alice"), role: Role::Human, assistant_of: None }
        );
        let human = json!({ "pub": { "allow": ["ants.inbox.*.alice", "$KV.ants_humans.*"] } });
        let me = |claims| Me::from_creds_text(&creds_with(claims)).map_err(|e| format!("{e:#}"));
        assert_eq!(
            me(json!({ "name": "alice", "nats": human })),
            Ok(Me { id: p("alice"), role: Role::Human, assistant_of: None })
        );
        let ant = json!({ "pub": { "allow": ["ants.inbox.*.dev-a"] } });
        assert_eq!(
            me(json!({ "name": "dev-a", "nats": ant })),
            Ok(Me { id: p("dev-a"), role: Role::Ant, assistant_of: None })
        );
        assert_eq!(
            me(json!({ "name": "human" })),
            Ok(Me { id: ants_test_support::human(), role: Role::Ant, assistant_of: None }),
            "no name makes a human"
        );
        assert!(me(json!({ "name": "Alice Smith" })).unwrap_err().starts_with("a user JWT naming \"Alice Smith\""));
        assert_eq!(me(json!({ "sub": "U123" })).unwrap_err(), "a user JWT without a name");
        assert_eq!(Me::from_creds_text("no JWT here").unwrap_err().to_string(), "no user JWT");
    }

    #[test]
    fn a_human_s_credentials_tell_the_device() {
        let device = DeviceId::generate();
        let human = json!({ "allow": ["$KV.ants_humans.*"] });
        let claims = json!({ "name": "alice", "nats": { "tags": ["team", device.tag()], "pub": human } });
        assert_eq!(device_of(&creds_with(claims)).unwrap(), Some(device));
        let without = json!({ "name": "alice", "nats": { "pub": human } });
        assert!(device_of(&creds_with(without)).is_err(), "a human's credentials name their device");
        let ant = json!({ "name": "dev-a", "nats": { "pub": { "allow": ["ants.inbox.*.dev-a"] } } });
        assert_eq!(device_of(&creds_with(ant)).unwrap(), None);
        assert_eq!(super::device(None).unwrap(), None);
    }

    /// With real credentials: the human's first connection creates the bucket and registers it, later ones find it.
    #[tokio::test]
    async fn a_human_registers_itself_once_and_an_ant_never() {
        let Some(server) = Server::start() else { return };
        let (alice, dev_a) = (p("alice"), p("dev-a"));
        let connect =
            async |me: &Me, creds: &Path| bus::connect(&server.url, Some(creds), None, &me.id, "test").await.unwrap().1;
        let creds = server.creds_as(&alice, Role::Human);
        let me = Me::from_creds(Some(&creds), None).unwrap();
        assert_eq!(me, Me { id: alice.clone(), role: Role::Human, assistant_of: None });
        let a_js = connect(&me, &creds).await;
        assert!(register(&a_js, &me).await.unwrap(), "the first connection registers alice");
        assert!(!register(&a_js, &me).await.unwrap(), "already registered");
        let kv = ensure(&a_js).await.unwrap();
        let entry: HumanEntry = serde_json::from_slice(&kv.get("alice").await.unwrap().unwrap()).unwrap();
        assert_eq!(entry.human, alice);
        kv.delete("alice").await.unwrap();
        assert!(register(&a_js, &me).await.unwrap(), "a removed human registers again");

        let creds = server.creds_as(&dev_a, Role::Ant);
        let ant = Me::from_creds(Some(&creds), None).unwrap();
        assert_eq!(ant.role, Role::Ant);
        assert!(!register(&connect(&ant, &creds).await, &ant).await.unwrap());
        let creds = server.creds_as(&ants_test_support::human(), Role::Human);
        let human = Me::from_creds(Some(&creds), None).unwrap();
        assert!(register(&connect(&human, &creds).await, &human).await.unwrap(), "no name is a human without an entry");
        let mut keys: Vec<String> = kv.keys().await.unwrap().try_collect().await.unwrap();
        keys.sort();
        assert_eq!(keys, ["alice", "human"]);
    }
}
