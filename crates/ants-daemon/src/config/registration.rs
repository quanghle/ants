//! The `[registration]` table of the signer's config file: how `ants signer` registers a human's new device after a
//! GitHub sign-in (`ants login`), and whom it signs for at once. Reread per request, like the ant templates.
//!
//! ```toml
//! [registration]
//! client_id = "Ov23li..."             # the team's GitHub OAuth App, with the device flow enabled
//! orgs = ["acme"]                     # members of these organizations are signed at once
//! teams = ["acme/core"]               # ... and of these teams, as org/team
//! email_domains = ["acme.example"]    # ... and users with a verified email address in these domains
//! ttl = "30d"                         # how long the credentials of a registered device stay valid
//! web_ttl = "7d"                      # ... and those of a registered web device, a page of the web client
//! sweep = "6h"                        # how often memberships are checked again; "0": never
//! token_file = "~/.config/ants/github-token"   # the signer's GitHub token, mode 0600; or:
//! token_store = "github-token"        # its entry in the OS credential store (service "ants")
//! ```

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

/// How long the credentials of a registered device stay valid by default.
pub const DEFAULT_REGISTRATION_TTL: Duration = Duration::from_hours(30 * 24);

/// How long the credentials of a registered web device stay valid by default.
pub const DEFAULT_WEB_TTL: Duration = Duration::from_hours(7 * 24);

/// How often the signer checks again by default whether the GitHub users of registered devices still fall under the
/// policy.
pub const DEFAULT_SWEEP: Duration = Duration::from_hours(6);

/// Where GitHub signs users in, unless `github_url` says otherwise.
pub const GITHUB_URL: &str = "https://github.com";
/// Where GitHub's REST API is, unless `api_url` says otherwise.
pub const GITHUB_API_URL: &str = "https://api.github.com";

/// The `[registration]` table.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    /// The client id of the team's GitHub OAuth App, whose device flow `ants login` users go through.
    pub client_id: String,
    /// GitHub organizations whose members are signed at once, lowercase.
    #[serde(default)]
    pub orgs: Vec<String>,
    /// GitHub teams whose members are signed at once.
    #[serde(default)]
    pub teams: Vec<Team>,
    /// Domains of verified email addresses whose users are signed at once, lowercase.
    #[serde(default)]
    pub email_domains: Vec<String>,
    /// How long the credentials of a registered device stay valid.
    #[serde(default = "default_ttl", deserialize_with = "ants_proto::duration::deserialize")]
    pub ttl: Duration,
    /// How long the credentials of a registered web device ([`ants_proto::WEB_TAG`]) stay valid.
    #[serde(default = "default_web_ttl", deserialize_with = "ants_proto::duration::deserialize")]
    pub web_ttl: Duration,
    /// How often the signer checks again, with its token, whether the GitHub users of the devices an organization or
    /// team registered still fall under the policy, revoking the names of those who left; zero: never.
    #[serde(default = "default_sweep", deserialize_with = "ants_proto::duration::deserialize")]
    pub sweep: Duration,
    /// A file holding the signer's GitHub token, of mode 0600, used to check memberships and look users up.
    pub token_file: Option<PathBuf>,
    /// The entry of the OS credential store, under service `ants`, holding the signer's GitHub token.
    pub token_store: Option<String>,
    /// Where GitHub signs users in: GitHub's, or a GitHub Enterprise Server's.
    #[serde(default = "default_github_url")]
    pub github_url: String,
    /// Where GitHub's REST API is.
    #[serde(default = "default_api_url")]
    pub api_url: String,
}

fn default_ttl() -> Duration {
    DEFAULT_REGISTRATION_TTL
}

fn default_web_ttl() -> Duration {
    DEFAULT_WEB_TTL
}

fn default_sweep() -> Duration {
    DEFAULT_SWEEP
}

fn default_github_url() -> String {
    GITHUB_URL.to_owned()
}

fn default_api_url() -> String {
    GITHUB_API_URL.to_owned()
}

/// A GitHub team, `org/team` (the team's slug), lowercase.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Team {
    pub org: String,
    pub slug: String,
}

impl TryFrom<String> for Team {
    type Error = String;

    fn try_from(text: String) -> std::result::Result<Self, String> {
        let invalid = || format!("team {text:?} is not org/team");
        let (org, slug) = text.split_once('/').ok_or_else(invalid)?;
        if !is_login(org) || slug.is_empty() || !slug.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c)) {
            return Err(invalid());
        }
        Ok(Self { org: org.to_ascii_lowercase(), slug: slug.to_ascii_lowercase() })
    }
}

impl From<Team> for String {
    fn from(team: Team) -> Self {
        team.to_string()
    }
}

impl std::fmt::Display for Team {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.org, self.slug)
    }
}

/// Whether `name` may be a GitHub login: 1-39 letters, digits or single hyphens, not at either end.
pub fn is_login(name: &str) -> bool {
    (1..=39).contains(&name.len())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.contains("--")
}

/// What a GitHub user is, as the signer read it with the user's token, which it drops right after.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GithubUser {
    /// The numeric id, which never changes.
    pub id: u64,
    /// The login, which may change.
    pub login: String,
    /// Verified email addresses.
    pub emails: Vec<String>,
    /// The organizations the user is a member of, lowercase.
    pub orgs: Vec<String>,
    /// The teams the user is a member of.
    pub teams: Vec<Team>,
}

impl GithubUser {
    /// The account a name binds to: the user's id and login.
    pub fn account(&self) -> GithubAccount {
        GithubAccount { id: self.id, login: self.login.clone() }
    }
}

/// The GitHub user a human's name is bound to, recorded with each of its devices' credentials in `issued.jsonl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GithubAccount {
    /// The numeric id, which binds.
    pub id: u64,
    /// The login when the device was signed, for display.
    pub login: String,
}

/// Why a device was signed, recorded with its credentials in `issued.jsonl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "by", content = "of")]
pub enum Grant {
    /// The user is a member of this organization.
    Org(String),
    /// The user is a member of this team.
    Team(Team),
    /// The user has a verified email address in this domain.
    EmailDomain(String),
    /// A human approved the request: this one.
    Approval(String),
}

impl std::fmt::Display for Grant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Org(org) => write!(f, "org {org}"),
            Self::Team(team) => write!(f, "team {team}"),
            Self::EmailDomain(domain) => write!(f, "email domain {domain}"),
            Self::Approval(human) => write!(f, "approved by {human}"),
        }
    }
}

impl Registration {
    /// How long the credentials of a device registered now stay valid: a web device's, when `web`, or any other's.
    pub fn lifetime(&self, web: bool) -> Duration {
        if web { self.web_ttl } else { self.ttl }
    }

    /// Check the table, lowercasing what GitHub compares without case.
    fn check(&mut self) -> Result<()> {
        if self.client_id.trim().is_empty() {
            bail!("[registration] client_id is empty");
        }
        for org in &mut self.orgs {
            if !is_login(org) {
                bail!("[registration] orgs: {org:?} is no GitHub organization");
            }
            org.make_ascii_lowercase();
        }
        for domain in &mut self.email_domains {
            let valid = |c: char| c.is_ascii_alphanumeric() || c == '-' || c == '.';
            if domain.is_empty() || !domain.chars().all(valid) || domain.starts_with('.') || domain.ends_with('.') {
                bail!("[registration] email_domains: {domain:?} is no domain");
            }
            domain.make_ascii_lowercase();
        }
        for (key, ttl) in [("ttl", self.ttl), ("web_ttl", self.web_ttl)] {
            if ttl < Duration::from_mins(1) {
                bail!("[registration] {key} {ttl:?} is under a minute");
            }
        }
        if self.token_file.is_some() && self.token_store.is_some() {
            bail!("[registration] names both token_file and token_store: keep one");
        }
        for url in [&mut self.github_url, &mut self.api_url] {
            if !url.starts_with("https://") && !url.starts_with("http://") {
                bail!("[registration] {url:?} is no http(s) URL");
            }
            while url.ends_with('/') {
                url.pop();
            }
        }
        Ok(())
    }

    /// The signer's GitHub token, if the table says where it is kept: a file only its owner may read, or an entry of
    /// the OS credential store.
    pub fn token(&self) -> Result<Option<String>> {
        let token = if let Some(file) = &self.token_file {
            #[cfg(unix)]
            {
                let mode = std::os::unix::fs::PermissionsExt::mode(
                    &std::fs::metadata(file).with_context(|| format!("reading {}", file.display()))?.permissions(),
                );
                if mode & 0o077 != 0 {
                    bail!("{} is readable by others (mode {:o}): chmod 600 it", file.display(), mode & 0o777);
                }
            }
            std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?
        } else if let Some(entry) = &self.token_store {
            crate::secrets::keystore::os()
                .get(entry)?
                .with_context(|| format!("no entry {entry} in the OS credential store (service ants)"))?
        } else {
            return Ok(None);
        };
        let token = token.trim();
        if token.is_empty() {
            bail!("the signer's GitHub token is empty");
        }
        Ok(Some(token.to_owned()))
    }

    /// The first grant of the policy `user` falls under, if any: an organization, then a team, then an email domain.
    pub fn grant(&self, user: &GithubUser) -> Option<Grant> {
        let org = self.orgs.iter().find(|org| user.orgs.iter().any(|o| o.eq_ignore_ascii_case(org)));
        let team = || self.teams.iter().find(|team| user.teams.contains(team));
        let domain = || {
            let domains = user.emails.iter().filter_map(|e| e.rsplit_once('@')).map(|(_, d)| d.to_ascii_lowercase());
            domains.into_iter().find(|d| self.email_domains.contains(d))
        };
        org.map(|o| Grant::Org(o.clone()))
            .or_else(|| team().map(|t| Grant::Team(t.clone())))
            .or_else(|| domain().map(Grant::EmailDomain))
    }

    /// The organizations whose memberships matter to the policy: those named alone or by a team.
    pub fn policy_orgs(&self) -> Vec<&str> {
        let mut orgs: Vec<&str> = self.orgs.iter().map(String::as_str).collect();
        orgs.extend(self.teams.iter().map(|t| t.org.as_str()));
        orgs.sort_unstable();
        orgs.dedup();
        orgs
    }
}

/// The `[registration]` table of the config file at `path`, if it has one, for `ants signer`, which reads it afresh
/// for each request. A relative `token_file` is relative to the file's directory.
pub fn signer_registration(path: &Path) -> Result<Option<Registration>> {
    #[derive(Deserialize)]
    struct Table {
        registration: Option<Registration>,
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    let at = || format!("config {}", path.display());
    let table: Table = toml::from_str(&text).with_context(|| format!("parsing {}", at()))?;
    let Some(mut registration) = table.registration else { return Ok(None) };
    registration.check().with_context(at)?;
    if let Some(file) = &mut registration.token_file {
        let path = std::path::absolute(path)?;
        *file = super::resolve(file, path.parent().unwrap_or(Path::new(".")), &super::home_dir()?);
    }
    Ok(Some(registration))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(orgs: &[&str], teams: &[&str], emails: &[&str]) -> GithubUser {
        GithubUser {
            id: 1,
            login: "octocat".to_owned(),
            emails: emails.iter().map(|e| (*e).to_owned()).collect(),
            orgs: orgs.iter().map(|o| (*o).to_owned()).collect(),
            teams: teams.iter().map(|t| (*t).to_owned().try_into().unwrap()).collect(),
        }
    }

    #[test]
    fn the_signer_reads_the_registration_policy_and_grants_by_org_team_then_email_domain() {
        let dir = ants_test_support::scratch_dir("config-registration").canonicalize().unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, "[[ant_template]]\nname = \"x\"\n").unwrap();
        assert_eq!(signer_registration(&path).unwrap(), None);
        let table = "[registration]\nclient_id = \"Ov23li\"\norgs = [\"Acme\"]\nteams = [\"Other/Core\"]\n\
                     email_domains = [\"Acme.Example\"]\ntoken_file = \"github-token\"\napi_url = \"http://127.0.0.1:1/\"\n";
        std::fs::write(&path, table).unwrap();
        let registration = signer_registration(&path).unwrap().unwrap();
        let daemon = format!("harness = \"copilot\"\n{table}\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n");
        super::super::parse(&daemon, &path, &dir).expect("the daemon skips the table");
        assert_eq!((registration.ttl, registration.github_url.as_str()), (DEFAULT_REGISTRATION_TTL, GITHUB_URL));
        assert_eq!(registration.sweep, DEFAULT_SWEEP);
        assert_eq!(
            (registration.lifetime(false), registration.lifetime(true)),
            (Duration::from_hours(30 * 24), Duration::from_hours(7 * 24))
        );
        std::fs::write(&path, format!("{table}ttl = \"10d\"\nweb_ttl = \"2d\"\n")).unwrap();
        let set = signer_registration(&path).unwrap().unwrap();
        assert_eq!((set.lifetime(false), set.lifetime(true)), (Duration::from_hours(240), Duration::from_hours(48)));
        assert_eq!(
            (registration.api_url.as_str(), registration.token_file.as_deref()),
            ("http://127.0.0.1:1", Some(dir.join("github-token").as_path()))
        );
        assert_eq!(registration.policy_orgs(), ["acme", "other"]);

        let org = Grant::Org("acme".to_owned());
        assert_eq!(registration.grant(&user(&["acme"], &["other/core"], &["a@acme.example"])), Some(org));
        let team = Grant::Team("other/core".to_owned().try_into().unwrap());
        assert_eq!(registration.grant(&user(&["other"], &["other/core"], &[])), Some(team));
        let domain = Grant::EmailDomain("acme.example".to_owned());
        assert_eq!(registration.grant(&user(&[], &["other/ops"], &["x@ACME.example"])), Some(domain));
        assert_eq!(registration.grant(&user(&["other"], &["acme/core"], &["x@sub.acme.example"])), None);

        assert!(format!("{:#}", registration.token().unwrap_err()).contains("github-token"));
        std::fs::write(dir.join("github-token"), "gho_x\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let token = dir.join("github-token");
            std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(format!("{:#}", registration.token().unwrap_err()).contains("chmod 600"));
            std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(registration.token().unwrap().as_deref(), Some("gho_x"));

        for (bad, why) in [
            ("client_id = \"\"", "client_id is empty"),
            ("client_id = \"x\"\nteams = [\"core\"]", "is not org/team"),
            ("client_id = \"x\"\nttl = \"10s\"", "ttl 10s is under a minute"),
            ("client_id = \"x\"\nweb_ttl = \"10s\"", "web_ttl 10s is under a minute"),
            ("client_id = \"x\"\nsweep = \"6\"", "sweep"),
            ("client_id = \"x\"\ntoken_file = \"f\"\ntoken_store = \"s\"", "keep one"),
            ("client_id = \"x\"\nscope = \"all\"", "unknown field"),
        ] {
            std::fs::write(&path, format!("[registration]\n{bad}\n")).unwrap();
            let err = format!("{:#}", signer_registration(&path).unwrap_err());
            assert!(err.contains(why), "{err}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn grants_are_recorded_with_what_granted_them() {
        let grant = Grant::Team("acme/core".to_owned().try_into().unwrap());
        let json = serde_json::to_string(&grant).unwrap();
        assert_eq!(json, r#"{"by":"team","of":"acme/core"}"#);
        assert_eq!(serde_json::from_str::<Grant>(&json).unwrap(), grant);
        assert_eq!(Grant::Approval("alice".to_owned()).to_string(), "approved by alice");
    }
}
