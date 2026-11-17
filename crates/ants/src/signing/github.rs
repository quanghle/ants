//! The few GitHub calls of human registration, against GitHub's REST API or a GitHub Enterprise Server's, as the
//! signer's `[registration]` table names them ([`Registration`]).

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ants_daemon::config::registration::{GITHUB_API_URL, GITHUB_URL, GithubAccount, GithubUser, Registration};
use anyhow::{Context as _, Result, bail};
use hyper::http::{HeaderMap, Method, StatusCode, header};
use serde::Deserialize;
use serde::de::DeserializeOwned;

/// Longest answer read from GitHub.
const MAX_ANSWER: usize = 256 * 1024;

/// The scopes a registering user grants the signer: reading org and team memberships and verified email addresses.
const SCOPES: &str = "read:org user:email";

/// Longest wait for GitHub's rate limit to let requests through again.
const MAX_RATE_WAIT: Duration = Duration::from_hours(1);

/// Where GitHub is, and the signer's own token, if any.
#[derive(Debug, Clone)]
pub struct Github {
    pub github_url: String,
    pub api_url: String,
    token: Option<String>,
    /// What GitHub's latest API answer said of the rate limit.
    rate: Arc<Mutex<RateLimit>>,
}

/// The rate limit of the token as GitHub's latest API answer told it: requests left in the window, when the window
/// resets, and, after GitHub refused for it, when to ask again (seconds since the epoch).
#[derive(Debug, Clone, Copy, Default)]
struct RateLimit {
    remaining: Option<u64>,
    reset: Option<u64>,
    retry_at: Option<u64>,
}

/// A device flow begun ([RFC 8628](https://www.rfc-editor.org/rfc/rfc8628)): the user signs in at `verification_uri`
/// and enters `user_code` there, within `expires_in` seconds, while the signer polls with `device_code` every
/// `interval` seconds.
#[derive(Debug, Clone, Deserialize)]
pub struct DeviceCode {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub expires_in: u64,
    #[serde(default = "default_interval")]
    pub interval: u64,
}

fn default_interval() -> u64 {
    5
}

/// What a poll of a device flow got.
#[derive(Debug, PartialEq, Eq)]
enum Poll {
    Token(String),
    Pending,
    /// Poll less often: every this many seconds.
    SlowDown(u64),
}

#[derive(Deserialize)]
struct TokenAnswer {
    access_token: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
    interval: Option<u64>,
}

#[derive(Deserialize)]
struct Email {
    email: String,
    verified: bool,
}

#[derive(Deserialize)]
struct Membership {
    state: String,
}

impl Github {
    /// GitHub as `registration` names it, with the signer's token; github.com, with no token, without a table.
    pub fn new(registration: Option<&Registration>) -> Result<Self> {
        Ok(match registration {
            Some(r) => Self::at(&r.github_url, &r.api_url, r.token()?),
            None => Self::at(GITHUB_URL, GITHUB_API_URL, None),
        })
    }

    fn at(github_url: &str, api_url: &str, token: Option<String>) -> Self {
        let rate = Arc::default();
        Self { github_url: github_url.to_owned(), api_url: api_url.to_owned(), token, rate }
    }

    /// Whether the signer has a GitHub token of its own.
    pub fn has_token(&self) -> bool {
        self.token.is_some()
    }

    /// How long to wait before the next API request so that at least `reserve` requests of the window stay for
    /// others, or GitHub, having refused for its rate limit, takes requests again: none when one may go now. At most
    /// an hour.
    pub fn rate_wait(&self, reserve: u64) -> Option<Duration> {
        let rate = *self.rate.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = epoch();
        let until = rate.retry_at.filter(|at| *at > now).or_else(|| {
            rate.reset.filter(|reset| *reset > now && rate.remaining.is_some_and(|left| left <= reserve))
        })?;
        Some(Duration::from_secs(until - now + 1).min(MAX_RATE_WAIT))
    }

    /// Keep what the answer `status` with `headers` says of the rate limit.
    fn note_rate(&self, status: StatusCode, headers: &HeaderMap) {
        let number = |name: &str| headers.get(name)?.to_str().ok()?.trim().parse::<u64>().ok();
        let mut rate = self.rate.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        rate.remaining = number("x-ratelimit-remaining").or(rate.remaining);
        rate.reset = number("x-ratelimit-reset").or(rate.reset);
        rate.retry_at = match status {
            StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS => number("retry-after")
                .map(|secs| epoch() + secs)
                .or_else(|| rate.reset.filter(|_| rate.remaining == Some(0))),
            _ => None,
        };
    }

    /// `GET` `path` of the API with `token`, else the signer's: the status, and the answer when it is a success.
    async fn get<T: DeserializeOwned>(&self, path: &str, token: Option<&str>) -> Result<(StatusCode, Option<T>)> {
        let (status, body) = self.fetch_get(path, token).await?;
        if !status.is_success() {
            return Ok((status, None));
        }
        let answer = serde_json::from_slice(&body).with_context(|| format!("GitHub's answer for {path}"))?;
        Ok((status, Some(answer)))
    }

    async fn fetch_get(&self, path: &str, token: Option<&str>) -> Result<(StatusCode, hyper::body::Bytes)> {
        let mut headers = vec![
            (header::ACCEPT.as_str(), "application/vnd.github+json".to_owned()),
            ("x-github-api-version", "2022-11-28".to_owned()),
        ];
        if let Some(token) = token.or(self.token.as_deref()) {
            headers.push((header::AUTHORIZATION.as_str(), format!("Bearer {token}")));
        }
        let url = format!("{}{path}", self.api_url);
        let (status, answer_headers, body) =
            crate::http::fetch_with_headers(Method::GET, &url, &headers, None, MAX_ANSWER)
                .await
                .with_context(|| format!("asking GitHub for {path}"))?;
        self.note_rate(status, &answer_headers);
        Ok((status, body))
    }

    /// Begin a device flow of the OAuth App `client_id`.
    pub async fn device_code(&self, client_id: &str) -> Result<DeviceCode> {
        let form = form(&[("client_id", client_id), ("scope", SCOPES)]);
        let (status, body) = self.post("/login/device/code", form).await.context("starting the GitHub sign-in")?;
        if !status.is_success() {
            bail!("GitHub answered {status} to starting the sign-in{}", described(&body));
        }
        serde_json::from_slice(&body).context("GitHub's answer to starting the sign-in")
    }

    /// Poll the device flow `code` of `client_id` until the user signs in, every `interval` seconds or as GitHub asks:
    /// the user's token. Fails when the user refuses or the code expires.
    pub async fn signed_in(&self, client_id: &str, code: &DeviceCode) -> Result<String> {
        let mut interval = code.interval;
        loop {
            tokio::time::sleep(Duration::from_secs(interval)).await;
            match self.poll(client_id, &code.device_code).await? {
                Poll::Token(token) => return Ok(token),
                Poll::Pending => {}
                Poll::SlowDown(slower) => interval = slower,
            }
        }
    }

    async fn poll(&self, client_id: &str, device_code: &str) -> Result<Poll> {
        let grant = "urn:ietf:params:oauth:grant-type:device_code";
        let form = form(&[("client_id", client_id), ("device_code", device_code), ("grant_type", grant)]);
        let (status, body) =
            self.post("/login/oauth/access_token", form).await.context("waiting for the GitHub sign-in")?;
        let answer: TokenAnswer = serde_json::from_slice(&body)
            .with_context(|| format!("GitHub's answer {status} while waiting for the sign-in"))?;
        if let Some(token) = answer.access_token.filter(|t| !t.is_empty()) {
            return Ok(Poll::Token(token));
        }
        match answer.error.as_deref() {
            Some("authorization_pending") => Ok(Poll::Pending),
            Some("slow_down") => Ok(Poll::SlowDown(answer.interval.unwrap_or(5))),
            Some("expired_token") => bail!("the GitHub sign-in code expired"),
            Some("access_denied") => bail!("the GitHub sign-in was cancelled"),
            Some(error) => {
                bail!("GitHub refused the sign-in: {}", answer.error_description.as_deref().unwrap_or(error))
            }
            None => bail!("GitHub answered {status} with no token while waiting for the sign-in"),
        }
    }

    async fn post(&self, path: &str, form: String) -> Result<(StatusCode, hyper::body::Bytes)> {
        let headers = [(header::ACCEPT.as_str(), "application/json".to_owned())];
        let body = Some(("application/x-www-form-urlencoded", form.into_bytes()));
        crate::http::fetch(Method::POST, &format!("{}{path}", self.github_url), &headers, body, MAX_ANSWER).await
    }

    /// What the policy of `registration` needs to know of the user whose token is `token`: id, login, verified email
    /// addresses, and memberships of the policy's organizations and teams, read with the signer's token when it has
    /// one, else with the user's.
    pub async fn user(&self, token: &str, registration: &Registration) -> Result<GithubUser> {
        let account = match self.get::<GithubAccount>("/user", Some(token)).await? {
            (_, Some(account)) => account,
            (status, _) => bail!("GitHub answered {status} for the signed-in user"),
        };
        let login = &account.login;
        let emails = match self.get::<Vec<Email>>("/user/emails", Some(token)).await? {
            (_, Some(emails)) => emails.into_iter().filter(|e| e.verified).map(|e| e.email).collect(),
            (status, _) => bail!("GitHub answered {status} for the email addresses of {login}"),
        };
        let mut orgs = Vec::new();
        for org in &registration.orgs {
            let member = match self.token {
                Some(_) => self.status(&format!("/orgs/{org}/members/{login}")).await? == StatusCode::NO_CONTENT,
                None => self.active(&format!("/user/memberships/orgs/{org}"), Some(token)).await?,
            };
            if member {
                orgs.push(org.clone());
            }
        }
        let mut teams = Vec::new();
        for team in &registration.teams {
            let path = format!("/orgs/{}/teams/{}/memberships/{login}", team.org, team.slug);
            if self.active(&path, self.token.is_none().then_some(token)).await? {
                teams.push(team.clone());
            }
        }
        Ok(GithubUser { id: account.id, login: account.login, emails, orgs, teams })
    }

    /// Whether `path` names an active membership; not when GitHub knows none or hides it.
    async fn active(&self, path: &str, token: Option<&str>) -> Result<bool> {
        Ok(match self.get::<Membership>(path, token).await? {
            (_, Some(membership)) => membership.state == "active",
            (StatusCode::NOT_FOUND | StatusCode::FORBIDDEN | StatusCode::FOUND, None) => false,
            (status, None) => bail!("GitHub answered {status} for {path}"),
        })
    }

    /// The status of `GET` `path` with the signer's token.
    async fn status(&self, path: &str) -> Result<StatusCode> {
        let (status, _) = self.fetch_get(path, None).await?;
        Ok(status)
    }

    /// The GitHub user whose id is `id` as the policy of `registration` sees it now, with the signer's token: login,
    /// and memberships of the policy's organizations and teams (no email addresses, which only the user's token
    /// reads); none if GitHub has no such user any more. Fails without the signer's token, and when GitHub cannot tell.
    pub async fn member(&self, id: u64, registration: &Registration) -> Result<Option<GithubUser>> {
        if self.token.is_none() {
            bail!(
                "the signer has no GitHub token ([registration] token_file or token_store) to check memberships with"
            );
        }
        let account = match self.get::<GithubAccount>(&format!("/user/{id}"), None).await? {
            (_, Some(account)) => account,
            (StatusCode::NOT_FOUND, _) => return Ok(None),
            (status, _) => bail!("GitHub answered {status} for the user {id}"),
        };
        let login = &account.login;
        let mut orgs = Vec::new();
        for org in &registration.orgs {
            match self.status(&format!("/orgs/{org}/members/{login}")).await? {
                StatusCode::NO_CONTENT => orgs.push(org.clone()),
                StatusCode::NOT_FOUND => {}
                status => bail!("GitHub answered {status} for the membership of {login} in {org}"),
            }
        }
        let mut teams = Vec::new();
        for team in &registration.teams {
            let path = format!("/orgs/{}/teams/{}/memberships/{login}", team.org, team.slug);
            let active = match self.get::<Membership>(&path, None).await? {
                (_, Some(membership)) => membership.state == "active",
                (StatusCode::NOT_FOUND, None) => false,
                (status, None) => bail!("GitHub answered {status} for the membership of {login} in {team}"),
            };
            if active {
                teams.push(team.clone());
            }
        }
        Ok(Some(GithubUser { id: account.id, login: account.login, emails: Vec::new(), orgs, teams }))
    }

    /// Fail unless the signer's token sees every organization and team of the policy of `registration`, so that GitHub
    /// answering that a user is no member of one means that, not that the token cannot tell: a misspelt, renamed or
    /// hidden organization or team, or a token without access to it, would otherwise make every user a non-member.
    pub async fn sees_policy(&self, registration: &Registration) -> Result<()> {
        if self.token.is_none() {
            bail!("the signer has no GitHub token ([registration] token_file or token_store)");
        }
        for org in registration.policy_orgs() {
            let status = self.status(&format!("/orgs/{org}")).await?;
            if status != StatusCode::OK {
                bail!("GitHub answered {status} for the organization {org}, which the signer's token must see");
            }
        }
        for team in &registration.teams {
            let status = self.status(&format!("/orgs/{}/teams/{}", team.org, team.slug)).await?;
            if status != StatusCode::OK {
                bail!("GitHub answered {status} for the team {team}, which the signer's token must see");
            }
        }
        Ok(())
    }

    /// The GitHub user whose login is `login`.
    pub async fn user_by_login(&self, login: &str) -> Result<GithubAccount> {
        if !ants_daemon::config::registration::is_login(login) {
            bail!("{login:?} is no GitHub login");
        }
        match self.get::<GithubAccount>(&format!("/users/{login}"), None).await? {
            (_, Some(account)) => Ok(account),
            (StatusCode::NOT_FOUND, _) => bail!("GitHub has no user {login}"),
            (status, _) => bail!("GitHub answered {status} for the user {login}"),
        }
    }
}

/// Seconds since the epoch.
fn epoch() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// `pairs` as an `application/x-www-form-urlencoded` body.
fn form(pairs: &[(&str, &str)]) -> String {
    let encode = |s: &str| {
        s.bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => char::from(b).to_string(),
                b' ' => "+".to_owned(),
                b => format!("%{b:02X}"),
            })
            .collect::<String>()
    };
    pairs.iter().map(|(k, v)| format!("{}={}", encode(k), encode(v))).collect::<Vec<_>>().join("&")
}

/// GitHub's description of an error in `body`, if it has one, for a message.
fn described(body: &[u8]) -> String {
    let answer: Option<TokenAnswer> = serde_json::from_slice(body).ok();
    answer.and_then(|a| a.error_description.or(a.error)).map_or_default(|e| format!(": {e}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::test_support::github::fake;

    fn at(url: &str, token: Option<&str>) -> Github {
        Github::at(url, url, token.map(str::to_owned))
    }

    #[tokio::test]
    async fn users_are_looked_up_by_login_with_the_signers_token() {
        let url = fake(Arc::new(|what, auth, _| match (what, auth) {
            ("GET /users/octocat", Some("Bearer signer")) => (200, r#"{"id": 583231, "login": "octocat"}"#.into()),
            _ => (404, r#"{"message": "Not Found"}"#.into()),
        }))
        .await;
        let github = at(&url, Some("signer"));
        let account = github.user_by_login("octocat").await.unwrap();
        assert_eq!(account, GithubAccount { id: 583_231, login: "octocat".into() });
        let err = github.user_by_login("nobody").await.unwrap_err().to_string();
        assert!(err.contains("GitHub has no user nobody"), "{err}");
        let err = github.user_by_login("../x").await.unwrap_err().to_string();
        assert!(err.contains("no GitHub login"), "{err}");
    }
    #[tokio::test]
    async fn memberships_are_checked_again_by_user_id_with_the_signers_token() {
        let url = fake(Arc::new(|what, auth, _| match (what, auth) {
            (_, auth) if auth != Some("Bearer signer") => (401, "{}".into()),
            ("GET /user/7", _) => (200, r#"{"id":7,"login":"octocat"}"#.into()),
            ("GET /user/8", _) => (200, r#"{"id":8,"login":"hidden"}"#.into()),
            ("GET /orgs/acme/members/octocat", _) => (204, String::new()),
            ("GET /orgs/acme/members/hidden", _) => (302, String::new()),
            ("GET /orgs/acme/teams/core/memberships/octocat", _) => (200, r#"{"state":"active"}"#.into()),
            ("GET /orgs/acme" | "GET /orgs/acme/teams/core", _) => (200, "{}".into()),
            _ => (404, "{}".into()),
        }))
        .await;
        let registration: Registration =
            toml::from_str("client_id = \"Iv1.x\"\norgs = [\"acme\"]\nteams = [\"acme/core\", \"acme/ops\"]\n")
                .unwrap();
        let github = at(&url, Some("signer"));
        let user = github.member(7, &registration).await.unwrap().unwrap();
        assert_eq!((user.login.as_str(), user.orgs.as_slice()), ("octocat", ["acme".to_owned()].as_slice()));
        assert_eq!(user.teams.iter().map(|t| t.slug.as_str()).collect::<Vec<_>>(), ["core"]);
        assert_eq!(github.member(9, &registration).await.unwrap(), None);
        let err = github.member(8, &registration).await.unwrap_err().to_string();
        assert!(err.contains("302"), "{err}");
        let err = at(&url, None).member(7, &registration).await.unwrap_err().to_string();
        assert!(err.contains("no GitHub token"), "{err}");

        let err = github.sees_policy(&registration).await.unwrap_err().to_string();
        assert!(err.contains("404") && err.contains("acme/ops"), "{err}");
        let seen: Registration = toml::from_str("client_id = \"Iv1.x\"\nteams = [\"acme/core\"]\n").unwrap();
        github.sees_policy(&seen).await.unwrap();
        let err = at(&url, None).sees_policy(&seen).await.unwrap_err().to_string();
        assert!(err.contains("no GitHub token"), "{err}");
    }

    #[test]
    fn requests_wait_for_the_rate_limit_keeping_a_reserve() {
        let github = at("http://127.0.0.1:1", Some("signer"));
        let headers = |pairs: &[(&'static str, u64)]| {
            let mut headers = HeaderMap::new();
            for (name, value) in pairs {
                headers.insert(*name, value.to_string().parse().unwrap());
            }
            headers
        };
        let reset = epoch() + 60;
        assert_eq!(github.rate_wait(100), None, "no answer told of a limit");
        github.note_rate(StatusCode::OK, &headers(&[("x-ratelimit-remaining", 500), ("x-ratelimit-reset", reset)]));
        assert_eq!(github.rate_wait(100), None);
        github.note_rate(StatusCode::OK, &headers(&[("x-ratelimit-remaining", 100)]));
        let wait = github.rate_wait(100).unwrap();
        assert!(wait > Duration::from_secs(55) && wait <= Duration::from_secs(61), "{wait:?}");
        assert_eq!(github.rate_wait(10), None, "a smaller reserve goes on");

        github.note_rate(StatusCode::TOO_MANY_REQUESTS, &headers(&[("retry-after", 30)]));
        let wait = github.rate_wait(10).unwrap();
        assert!(wait > Duration::from_secs(25) && wait <= Duration::from_secs(31), "{wait:?}");
        github.note_rate(StatusCode::FORBIDDEN, &headers(&[("x-ratelimit-remaining", 0)]));
        assert!(github.rate_wait(0).unwrap() > Duration::from_secs(55), "exhausted until the window resets");
        let past = epoch() - 1;
        github.note_rate(StatusCode::OK, &headers(&[("x-ratelimit-remaining", 0), ("x-ratelimit-reset", past)]));
        assert_eq!(github.rate_wait(100), None, "the window has reset");
    }

    #[tokio::test]
    async fn a_device_flow_waits_slows_down_and_stops_when_refused() {
        let polls = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let seen = polls.clone();
        let url = fake(Arc::new(move |what, _, body| match what {
            "POST /login/device/code" => {
                assert!(body.contains("client_id=Iv1.x") && body.contains("scope=read%3Aorg+user%3Aemail"), "{body}");
                (200, r#"{"device_code":"dc","user_code":"AB-CD","verification_uri":"https://g/device","expires_in":900,"interval":0}"#.into())
            }
            "POST /login/oauth/access_token" => {
                let mut polls = polls.lock().unwrap();
                polls.push(body.to_owned());
                let code = body.split('&').find_map(|kv| kv.strip_prefix("device_code=")).unwrap();
                match (code, polls.len()) {
                    (_, 1) => (200, r#"{"error":"authorization_pending"}"#.into()),
                    (_, 2) => (200, r#"{"error":"slow_down","interval":0}"#.into()),
                    ("dc", _) => (200, r#"{"access_token":"tok","token_type":"bearer"}"#.into()),
                    _ => (200, r#"{"error":"access_denied"}"#.into()),
                }
            }
            _ => (404, "{}".into()),
        }))
        .await;
        let github = at(&url, None);
        let code = github.device_code("Iv1.x").await.unwrap();
        assert_eq!((code.user_code.as_str(), code.expires_in), ("AB-CD", 900));
        assert_eq!(github.signed_in("Iv1.x", &code).await.unwrap(), "tok");
        assert_eq!(seen.lock().unwrap().len(), 3);
        assert!(seen.lock().unwrap()[0].contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code"));
        let other = DeviceCode { device_code: "other".into(), ..code };
        let err = github.signed_in("Iv1.x", &other).await.unwrap_err().to_string();
        assert!(err.contains("cancelled"), "{err}");
    }

    #[tokio::test]
    async fn memberships_are_read_with_the_signers_token_when_it_has_one() {
        let url = fake(Arc::new(|what, auth, _| match (what, auth) {
            ("GET /user", Some("Bearer user")) => (200, r#"{"id":7,"login":"octocat"}"#.into()),
            ("GET /user/emails", Some("Bearer user")) => {
                (200, r#"[{"email":"o@acme.dev","verified":true},{"email":"o@x.dev","verified":false}]"#.into())
            }
            ("GET /orgs/acme/members/octocat", Some("Bearer signer")) => (204, String::new()),
            ("GET /orgs/other/members/octocat", Some("Bearer signer")) => (404, "{}".into()),
            ("GET /user/memberships/orgs/acme", Some("Bearer user")) => (200, r#"{"state":"active"}"#.into()),
            ("GET /user/memberships/orgs/other", Some("Bearer user")) => (200, r#"{"state":"pending"}"#.into()),
            ("GET /orgs/acme/teams/core/memberships/octocat", Some("Bearer signer" | "Bearer user")) => {
                (200, r#"{"state":"active"}"#.into())
            }
            _ => (404, "{}".into()),
        }))
        .await;
        let registration: Registration = toml::from_str(
            "client_id = \"Iv1.x\"\norgs = [\"acme\", \"other\"]\nteams = [\"acme/core\", \"acme/ops\"]\n",
        )
        .unwrap();
        for token in [Some("signer"), None] {
            let user = at(&url, token).user("user", &registration).await.unwrap();
            assert_eq!(
                (user.id, user.login.as_str(), user.emails.as_slice()),
                (7, "octocat", ["o@acme.dev".to_owned()].as_slice())
            );
            assert_eq!(user.orgs, ["acme"], "{token:?}");
            assert_eq!(user.teams.iter().map(|t| t.slug.as_str()).collect::<Vec<_>>(), ["core"], "{token:?}");
        }
    }
}
