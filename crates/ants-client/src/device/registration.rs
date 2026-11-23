//! A web device's registration, as `ants login` asks for one: a [`LoginRequest`] naming the device's NKey and no
//! encryption key, signed by that key, repeated every [`ants_proto::LOGIN_REPEAT`] on [`subject::login`] over the
//! team's bootstrap credentials until the signer settles it on [`subject::login_reply`], each answer sealed to the
//! request's one-off X25519 key ([`Registration`]).
//!
//! A sign-in page is shown only when it is the device login page of the GitHub the device trusts ([`device_page`]),
//! and a JWT is taken only when the account of the bootstrap credentials issued it to the device's key, for a web
//! device of the human asked for.

use ants_proto::{LoginAnswer, LoginReply, LoginRequest, UserId, subject};
use chrono::{DateTime, Utc};
use data_encoding::BASE64URL_NOPAD;
use nkeys::XKey;
use serde::{Deserialize, Serialize};

use super::{Bootstrap, Credentials, Error};
use crate::relayed;
use crate::transport::Message;

/// The GitHub a device trusts to sign in at when told of no other.
pub const GITHUB_URL: &str = "https://github.com";

/// The device login page of the GitHub at `github_url`, `https://HOST` with an optional port and trailing slash:
/// `https://HOST/login/device`, the host in lower case.
pub fn device_page(github_url: &str) -> Result<String, Error> {
    let host = github_url
        .get(.."https://".len())
        .filter(|scheme| scheme.eq_ignore_ascii_case("https://"))
        .map(|_| &github_url["https://".len()..])
        .map(|rest| rest.strip_suffix('/').unwrap_or(rest))
        .filter(|host| plain_host(host));
    match host {
        Some(host) => Ok(format!("https://{}/login/device", host.to_ascii_lowercase())),
        None => Err(Error::Invalid(format!(
            "{github_url}: give a GitHub's https://HOST, without a path, such as {GITHUB_URL}"
        ))),
    }
}

/// Whether `host` is a host name or address, with an optional port, and nothing else.
fn plain_host(host: &str) -> bool {
    !host.is_empty() && host.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':' | '[' | ']'))
}

/// What the signer said of a registration, short of settling it with a refusal ([`Error::Refused`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "step", rename_all = "kebab-case")]
pub enum Step {
    /// Sign in to GitHub at `page`, the trusted device login page, and enter `user_code` there before `expires_at`.
    Verify { page: String, user_code: String, expires_at: DateTime<Utc> },
    /// Signed in as GitHub user `login`, outside the registration policy: a human of the team decides.
    Waiting { login: String, reason: String },
    /// The device is registered: its credentials, the user JWT `jwt`.
    Signed { jwt: String, credentials: Credentials },
}

/// A registration asked for: the request, signed once [`Registration::sign`] has the device key's signature, and what
/// opens and checks the answers.
pub struct Registration {
    request: LoginRequest,
    reply_key: XKey,
    account: String,
    device_page: String,
}

impl Registration {
    /// A registration of the web device of the human `name` whose NKey is `public_key`, labelled `label`, asked for
    /// with `bootstrap`, trusting the GitHub at `github_url` to sign in at.
    pub fn new(
        name: UserId,
        public_key: String,
        label: Option<String>,
        bootstrap: &Bootstrap,
        github_url: &str,
    ) -> Result<Self, Error> {
        let mut seed = [0; 32];
        getrandom::getrandom(&mut seed).map_err(|e| Error::Invalid(format!("no randomness for a one-off key: {e}")))?;
        let reply_key = XKey::new_from_raw(seed);
        let mut request = LoginRequest::new(name, public_key, reply_key.public_key());
        request.web = true;
        request.label = label.filter(|label| !label.trim().is_empty());
        Ok(Self { request, reply_key, account: bootstrap.account().to_owned(), device_page: device_page(github_url)? })
    }

    /// What the device's key signs.
    pub fn signed(&self) -> Vec<u8> {
        self.request.signed()
    }

    /// Complete the request with `signature`, of [`Registration::signed`] by the device's key, base64url without
    /// padding.
    pub fn sign(&mut self, signature: String) {
        self.request.signature = signature;
    }

    /// The request, to publish every [`ants_proto::LOGIN_REPEAT`] until an answer settles it or it expires.
    pub fn request(&self) -> Message {
        Message::new(self.request.subject(), serde_json::to_vec(&self.request).unwrap_or_default())
    }

    /// Where the answers come.
    pub fn replies(&self) -> String {
        subject::login_reply(&self.request.name)
    }

    /// When the request stops being answered.
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.request.expires_at
    }

    /// The trusted device login page.
    pub fn device_page(&self) -> &str {
        &self.device_page
    }

    /// What `message`, heard on [`Registration::replies`], tells: none for an answer to another request, one a stream
    /// relayed, one that does not open or a JWT that is not the device's; an error for a refusal or a sign-in page
    /// other than the trusted one.
    pub fn answer(&self, message: &Message) -> Option<Result<Step, Error>> {
        if relayed::heard_relayed(message.reply.as_deref(), message.header_names()) {
            return None;
        }
        match self.open(&message.payload)? {
            LoginAnswer::Verify { url, user_code, expires_at } => {
                Some(if url.eq_ignore_ascii_case(&self.device_page) {
                    Ok(Step::Verify { page: self.device_page.clone(), user_code, expires_at })
                } else {
                    Err(Error::NotShown { trusted: self.device_page.clone() })
                })
            }
            LoginAnswer::Waiting { login, reason } => Some(Ok(Step::Waiting { login, reason })),
            LoginAnswer::Refused { reason } => Some(Err(Error::Refused(reason))),
            LoginAnswer::Signed { jwt } => {
                let credentials = self.check_signed(&jwt).ok()?;
                Some(Ok(Step::Signed { jwt, credentials }))
            }
        }
    }

    /// The answer `payload` seals for this request; none for one to another, or one that does not open.
    fn open(&self, payload: &[u8]) -> Option<LoginAnswer> {
        let reply: LoginReply = serde_json::from_slice(payload).ok()?;
        if reply.name != self.request.name || !reply.answers(&self.request.public_key) {
            return None;
        }
        let sender = XKey::from_public_key(&reply.sender).ok()?;
        let sealed = BASE64URL_NOPAD.decode(reply.sealed.as_bytes()).ok()?;
        serde_json::from_slice(&self.reply_key.open(&sealed, &sender).ok()?).ok()
    }

    /// The credentials of `jwt`, checked to be issued by the account of the bootstrap credentials to the request's key,
    /// for its name.
    fn check_signed(&self, jwt: &str) -> Result<Credentials, Error> {
        let credentials = Credentials::read(jwt, &self.request.public_key, Some(&self.account))?;
        if credentials.human != self.request.name {
            return Err(Error::Credentials(format!(
                "credentials of {}, not of {}",
                credentials.human, self.request.name
            )));
        }
        Ok(credentials)
    }
}
