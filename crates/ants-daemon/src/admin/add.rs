//! [`AdminOp::Add`]: add the order's ant to the config file the daemon serves, which starts it on the reload. The
//! daemon generates the ant's key, has `ants signer` sign it on the order ([`super::ask`]), writes the credentials
//! beside its host credentials and appends the ant's `[[ant]]` table, with the order's settings, its `name`, `creds`
//! and, unless the settings give one, a `workdir` of the ant's home, created. A comment above the table names the
//! order, so that the order taken again after the table was written, its outcome not stored, is applied without adding
//! the ant twice; the key waits in `<ant>.nk` beside the credentials until they are written, so that the signer is
//! asked about the same key again. On a machine running a leaf node ([`LeafNode`]), the uplink credentials sign the
//! request too, and the signer renews them to carry the ant, which the daemon has the leaf node use before listing the
//! ant. An order adding the assistant of a human (`assistant_of`, as `ants assistant enable --on` sends) has the
//! credentials checked as that human's assistant's, and, where the hub keeps shared settings ([`crate::shared`]), the
//! order's shared settings written as the ant's record in place of any an earlier ant of the name left, so that the
//! assistant runs with the role and tool rules it was added with.

use std::path::{Path, PathBuf};

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::{AdminOrder, AdminOutcome, EnrollReply, Machine, SharedSettings, UserId, is_shared_field};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream;
use nkeys::KeyPair;
use serde_json::{Map, Value, json};
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table};

use super::ask::{SIGNER_WAIT, Uplink, ask};
use super::{ConfigFile, LeafNode, NO_CONFIG, Orders, applied, refused};
use crate::config::{self, edit::toml_value};
use crate::creds;
use crate::direct_kv::DirectKv;
use crate::shared::{self, Found, Roles};

/// Settings an order does not give: what the daemon sets itself, or what an ant added from a template has.
pub(super) const NOT_GIVEN: [&str; 4] = ["creds", "template", "added_by", "builtin"];

pub(super) async fn apply(
    js: &jetstream::Context,
    identities: &DirectKv,
    orders: &Orders,
    order: &AdminOrder,
    settings: &Map<String, Value>,
    assistant_of: Option<&UserId>,
) -> AdminOutcome {
    let Some(config) = &orders.config else { return refused(order, NO_CONFIG) };
    let Some(host_creds) = &orders.creds else {
        return refused(order, "the machine has no host credentials to ask `ants signer` with");
    };
    let assisting = assistant_of.map(|human| (human, identities));
    match add(js, orders, config, host_creds, order, settings, assisting).await {
        Ok(Added::Already) => applied(order, None),
        Ok(Added::Now { creds, key, uplink }) => {
            config.edited.notify_one();
            let mut result = json!({ "creds": creds, "key": key });
            if let Some(uplink) = uplink {
                result["uplink"] = Value::String(uplink);
            }
            applied(order, Some(result))
        }
        Err(e) => refused(order, &format!("{e:#}")),
    }
}

enum Added {
    /// By this order, taken before.
    Already,
    Now {
        creds: PathBuf,
        key: String,
        /// What came of renewing the uplink of the machine's leaf node to carry the ant.
        uplink: Option<String>,
    },
}

/// Add the order's ant, with `assisting` the human it is to assist and the bucket of shared settings.
async fn add(
    js: &jetstream::Context,
    orders: &Orders,
    config: &ConfigFile,
    host_creds: &Path,
    order: &AdminOrder,
    settings: &Map<String, Value>,
    assisting: Option<(&UserId, &DirectKv)>,
) -> Result<Added> {
    let (path, accounts) = (&config.path, &orders.accounts);
    let ant = &order.ant;
    let mark = mark(order);
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    if text.lines().any(|line| line.trim() == mark) {
        return Ok(Added::Already);
    }
    if ant.is_human() {
        bail!("{ant} is a human's name, not an ant's");
    }
    if let Some(key) = NOT_GIVEN.iter().find(|key| settings.contains_key(**key)) {
        bail!("the daemon sets {key} itself: an order does not give it");
    }
    if settings.get("name").is_some_and(|name| name.as_str() != Some(ant.as_str())) {
        bail!("the settings name another ant than {ant}");
    }
    let loaded = config::load(path)?;
    if loaded.ants.iter().any(|a| a.name == *ant) {
        bail!("config {} lists {ant} already", path.display());
    }
    let dir = host_creds.parent().context("the host credentials are in no directory")?;
    let creds_path = dir.join(format!("{ant}.creds"));
    let (base, home) = (path.parent().unwrap_or(Path::new(".")), config::home_dir()?);
    let workdir = match settings.get("workdir") {
        Some(_) => None,
        None => {
            let home_set = settings.get("home").map(|h| h.as_str().context("home is no string")).transpose()?;
            let ant_home = home_set
                .map_or_else(|| home.join(".ants").join(ant.as_str()), |h| config::resolve(Path::new(h), base, &home));
            Some(ant_home)
        }
    };
    let table = entry(ant, &creds_path, workdir.as_deref(), settings, &mark)?;
    // Before any key is asked for: the file must load with the entry, a workdir to create aside.
    let probe = entry(ant, &creds_path, workdir.as_ref().map(|_| base), settings, &mark)?;
    let probe = entry_text(&text, &probe);
    config::parse(&probe, &std::path::absolute(path)?, &home).context("the config with the ant added")?;
    let shared_fields: Map<String, Value> =
        settings.iter().filter(|(key, _)| is_shared_field(key)).map(|(k, v)| (k.clone(), v.clone())).collect();
    if assisting.is_some() {
        Roles::of(&shared_fields)?;
    }
    let leaf = leaf_uplink(orders.leaf.as_ref())?;
    let seed_path = dir.join(format!("{ant}.nk"));
    let key = key_for(&creds_path, &seed_path, ant)?;
    let public_key = key.public_key();
    let (jwt, renewal) = sign(js, host_creds, leaf, accounts, order, ant, &public_key).await?;
    if let Some((human, _)) = assisting {
        let now = u64::try_from(chrono::Utc::now().timestamp()).unwrap_or_default();
        ants_proto::verify_assistant_of(&jwt, accounts.iter().map(String::as_str), human, now)
            .with_context(|| format!("the signed credentials of {ant}, to assist {human}"))?;
    }
    let seed = key.seed().map_err(|e| anyhow!("the seed of {public_key}: {e}"))?;
    write_secret(&creds_path, &creds::text(&jwt, &seed))?;
    if let Some(workdir) = &workdir {
        std::fs::create_dir_all(workdir).with_context(|| format!("creating {}", workdir.display()))?;
    }
    // Before listing the ant, so its first connection finds the uplink carrying it.
    let uplink = match renewal {
        Some((leaf, machine, renewed)) => Some(renew(leaf, &machine, ant, renewed).await),
        None => None,
    };
    if let Some((_, identities)) = assisting
        && !matches!(shared::read(identities, ant).await?, Found::NoBucket)
    {
        let record = SharedSettings {
            ant: ant.clone(),
            fields: shared_fields,
            machine: orders.machine.clone(),
            by: Some(order.admin.clone()),
            updated_at: chrono::Utc::now(),
        };
        identities.put(record.key(), serde_json::to_vec(&record)?).await.context("writing the shared settings")?;
        config.shared.store(record);
    }
    config::append(path, &table)?;
    if let Err(e) = std::fs::remove_file(&seed_path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(path = %seed_path.display(), error = %e, "removing the key of an added ant");
    }
    Ok(Added::Now { creds: creds_path, key: public_key, uplink })
}

/// The machine's leaf node, if it runs one, with its uplink.
pub(super) fn leaf_uplink(leaf: Option<&LeafNode>) -> Result<Option<(&LeafNode, Uplink)>> {
    let Some(leaf) = leaf.filter(|leaf| leaf.conf.exists()) else { return Ok(None) };
    let (machine, creds) = (leaf.uplink)(&leaf.conf)
        .with_context(|| format!("the configuration of the machine's leaf node {}", leaf.conf.display()))?;
    Ok(Some((leaf, Uplink { machine, creds })))
}

/// The leaf node of the machine, its machine and its uplink credentials as the signer renewed them to carry an ant.
pub(super) type Renewal<'a> = (&'a LeafNode, Machine, String);

/// Have `ants signer` sign `public_key` for `ant`, the ant `order` adds or the clone it makes, with the host
/// credentials at `host_creds`, and renew the uplink of `leaf`, if given, to carry it: the ant's user JWT, checked, and
/// the renewal, checked, for [`renew`].
pub(super) async fn sign<'a>(
    js: &jetstream::Context,
    host_creds: &Path,
    leaf: Option<(&'a LeafNode, Uplink)>,
    accounts: &[String],
    order: &AdminOrder,
    ant: &UserId,
    public_key: &str,
) -> Result<(String, Option<Renewal<'a>>)> {
    let uplink = leaf.as_ref().map(|(_, uplink)| uplink);
    let (jwt, leaf_jwt) = match ask(js, host_creds, uplink, order, ant, public_key, SIGNER_WAIT).await? {
        EnrollReply::Approved { jwt, leaf_jwt, .. } => (jwt, leaf_jwt),
        EnrollReply::Rejected { reason, .. } => {
            bail!("`ants signer` refused the key: {}", reason.as_deref().unwrap_or("no reason given"))
        }
        EnrollReply::Retired { .. } => bail!("`ants signer` answered with a retirement"),
    };
    check_approval(&jwt, accounts, ant, public_key)?;
    let renewal = match (leaf, leaf_jwt) {
        (Some((leaf, uplink)), Some(renewed)) => {
            check_uplink(&renewed, accounts, &uplink)?;
            Some((leaf, uplink.machine, renewed))
        }
        (Some((_, uplink)), None) => {
            bail!("`ants signer` renewed no uplink credentials of the leaf node {} to carry {ant}", uplink.machine)
        }
        (None, _) => None,
    };
    Ok((jwt, renewal))
}

/// Have the leaf node `leaf` of `machine` use `renewed`, its uplink credentials renewed to carry `ant`: what came of
/// it, said in the order's outcome. The ant is added all the same, reaching only the ants of the machine until the
/// uplink carries it.
pub(super) async fn renew(leaf: &LeafNode, machine: &Machine, ant: &UserId, renewed: String) -> String {
    let (renew, conf) = (leaf.renew, leaf.conf.clone());
    match tokio::task::spawn_blocking(move || renew(&conf, &renewed)).await.map_err(anyhow::Error::from) {
        Ok(Ok(Some(said))) => format!("the uplink credentials of {machine} were renewed to carry {ant}: {said}"),
        Ok(Ok(None)) => format!(
            "the uplink credentials of {machine} changed meanwhile and were left as they are: until a human runs \
             `ants auth leaf {machine} --add-ant {ant}` where the account key is, {ant} reaches only the ants of this \
             machine"
        ),
        Ok(Err(e)) | Err(e) => format!(
            "renewing the uplink credentials of {machine} to carry {ant} failed: {e:#}; until a human runs `ants auth \
             leaf {machine} --add-ant {ant}` where the account key is, {ant} reaches only the ants of this machine"
        ),
    }
}

/// Check that `jwt`, the signer's renewal of the credentials of `uplink`, is a leaf node's of its machine for the same
/// key, which one of `accounts` signed.
fn check_uplink(jwt: &str, accounts: &[String], uplink: &Uplink) -> Result<()> {
    let (claims, _) = ants_proto::verified_jwt(jwt).context("the renewed uplink credentials")?;
    if !accounts.contains(&claims.iss) {
        bail!(
            "the renewed uplink credentials are issued by {}, not by the account of the host credentials",
            claims.iss
        );
    }
    let key = creds::load(&uplink.creds, crate::keystore::os())?.1.public_key();
    let name = format!("leaf-{}", uplink.machine);
    if claims.sub != key || claims.name != name {
        bail!("the renewed uplink credentials are of {} ({}), not of {name} ({key})", claims.name, claims.sub);
    }
    Ok(())
}

/// Check that `jwt`, the signer's approval, is the user JWT of `ant` for `public_key`, which one of `accounts` signed.
fn check_approval(jwt: &str, accounts: &[String], ant: &UserId, public_key: &str) -> Result<()> {
    let (claims, _) = ants_proto::verified_jwt(jwt).context("the signed credentials")?;
    if !accounts.contains(&claims.iss) {
        bail!("the signed credentials are issued by {}, not by the account of the host credentials", claims.iss);
    }
    if claims.sub != public_key || claims.name != ant.as_str() {
        bail!("the signed credentials are of {} ({}), not of {ant} ({public_key})", claims.name, claims.sub);
    }
    Ok(())
}

/// The comment above the table of the ant added by `order`.
fn mark(order: &AdminOrder) -> String {
    format!("# added by {}'s order {}", order.admin, order.id)
}

/// Whether `line` is the comment above the table of an ant added by an order ([`mark`]), or cloned by one
/// ([`super::clone`]).
pub(super) fn is_mark(line: &str) -> bool {
    let line = line.trim_end();
    line.strip_prefix("# added by ")
        .or_else(|| line.strip_prefix("# cloned from ").and_then(|rest| rest.split_once(" by ")).map(|(_, rest)| rest))
        .and_then(|rest| rest.rsplit_once("'s order "))
        .is_some_and(|(_, id)| id.parse::<ants_proto::Uuid>().is_ok())
}

/// The key to ask the signer about for `ant`: that of its credentials at `creds` if they are the ant's, else the one
/// waiting at `seed`, else a new one, kept there.
pub(super) fn key_for(creds: &Path, seed: &Path, ant: &UserId) -> Result<KeyPair> {
    if creds.exists() {
        let finding = creds::check(creds)?;
        if finding.name != ant.as_str() {
            bail!("{} are the credentials of {}, not of {ant}", creds.display(), finding.name);
        }
        return Ok(creds::load(creds, crate::keystore::os())?.1);
    }
    match std::fs::read_to_string(seed) {
        Ok(text) => KeyPair::from_seed(text.trim()).map_err(|e| anyhow!("the key in {}: {e}", seed.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let key = KeyPair::new_user();
            write_secret(seed, &key.seed().map_err(|e| anyhow!("a new key: {e}"))?)?;
            Ok(key)
        }
        Err(e) => Err(e).with_context(|| format!("reading {}", seed.display())),
    }
}

/// The `[[ant]]` table of `ant` with `settings`, its credentials `creds` and `workdir` if given, below `mark`.
pub(super) fn entry(
    ant: &UserId,
    creds: &Path,
    workdir: Option<&Path>,
    settings: &Map<String, Value>,
    mark: &str,
) -> Result<String> {
    let path = |p: &Path| p.to_str().map(str::to_owned).with_context(|| format!("{} is not UTF-8", p.display()));
    let mut table = Table::new();
    table.insert("name", toml_edit::value(ant.as_str()));
    table.insert("creds", toml_edit::value(path(creds)?));
    if let Some(workdir) = workdir {
        table.insert("workdir", toml_edit::value(path(workdir)?));
    }
    for (key, value) in settings.iter().filter(|(key, _)| *key != "name") {
        table.insert(key, Item::Value(toml_value(value).with_context(|| key.clone())?));
    }
    table.decor_mut().set_prefix(format!("{mark}\n"));
    let mut tables = ArrayOfTables::new();
    tables.push(table);
    let mut doc = DocumentMut::new();
    doc.insert("ant", Item::ArrayOfTables(tables));
    Ok(doc.to_string())
}

/// `text` with `entry` appended, as [`config::append`] does.
pub(super) fn entry_text(text: &str, entry: &str) -> String {
    let mut text = text.to_owned();
    if !text.is_empty() {
        if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push('\n');
    }
    text.push_str(entry);
    text
}

/// Write `contents` to `path`, readable by its owner only.
pub(super) fn write_secret(path: &Path, contents: &str) -> Result<()> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path).with_context(|| format!("writing {}", path.display()))?;
    #[cfg(unix)]
    file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    file.write_all(contents.as_bytes()).with_context(|| format!("writing {}", path.display()))
}
