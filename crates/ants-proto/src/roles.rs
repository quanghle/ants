//! Roles a user JWT carries as tags, which only the team's account key grants: a human who administers ants
//! ([`AdminRole`]), and an ant that is a human's assistant ([`AssistantOf`]). A reader honours a role only from a user
//! JWT it verifies itself ([`verify_user_jwt`]): signed by the team's account, unexpired, naming the user, and for an
//! act, with the act signed by the JWT's key ([`verify_admin`], [`verify_assistant_of`]). No I/O here: whether the
//! JWT's key is revoked the caller checks, against the revocation records in [`DEVICES_BUCKET`] or the account's.

use data_encoding::BASE64URL_NOPAD;
use nkeys::{KeyPair, KeyPairType};
use serde_json::Value;

use crate::UserId;
#[cfg(doc)]
use crate::{DEVICES_BUCKET, HUMANS_BUCKET, Permissions};

/// The user JWT tag of a human who may administer every ant of the team ([`AdminRole`]).
pub const ADMIN_TAG: &str = "ants-admin";

/// The role of a human whose user JWT carries [`ADMIN_TAG`], put there by `ants auth user NAME --human --admin`: it may
/// order any ant's daemon to change the ant. Withdrawn by revoking the credentials carrying it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminRole;

impl AdminRole {
    pub const TAG: &str = ADMIN_TAG;

    /// Whether a user JWT's `tags` carry [`ADMIN_TAG`], in either case.
    pub fn from_tags<'a>(tags: impl IntoIterator<Item = &'a str>) -> bool {
        is_admin(tags)
    }
}

/// Whether a user JWT's `tags` carry [`ADMIN_TAG`], in either case.
pub fn is_admin<'a>(tags: impl IntoIterator<Item = &'a str>) -> bool {
    tags.into_iter().any(|tag| tag.eq_ignore_ascii_case(ADMIN_TAG))
}

/// The role of an ant that is a human's assistant, carried in its user JWT as the tag [`AssistantOf::tag`] by `ants
/// auth user ANT --assistant-of HUMAN`, with the permissions [`Permissions::for_assistant`]. An ant's config cannot
/// make it one: only the JWT, which only the account key signs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssistantOf;

impl AssistantOf {
    /// Prefix of the user JWT tag naming the human an ant assists.
    pub const TAG: &str = "ants-assistant-of:";

    /// The user JWT tag of the assistant of `human`.
    pub fn tag(human: &UserId) -> String {
        format!("{}{human}", Self::TAG)
    }

    /// The human among a user JWT's `tags` whose assistant it is, if any, in either case: tools that change the case of
    /// a JWT's tags lose nothing of a user id, which is lowercase.
    pub fn from_tags<'a>(tags: impl IntoIterator<Item = &'a str>) -> Option<UserId> {
        tags.into_iter().find_map(|tag| {
            let (prefix, human) = tag.split_at_checked(Self::TAG.len())?;
            prefix.eq_ignore_ascii_case(Self::TAG).then(|| human.to_ascii_lowercase().parse().ok())?
        })
    }
}

/// Why a reader refuses a user JWT, or an act signed under one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JwtError {
    #[error("not a JWT")]
    Malformed,
    #[error("JWT without {0}")]
    Missing(&'static str),
    #[error("JWT issuer {issuer}: {reason}")]
    InvalidIssuer { issuer: String, reason: String },
    #[error("JWT not signed by its issuer {0}")]
    NotSignedByIssuer(String),
    #[error("signed by {0}, not by the team's account")]
    ForeignAccount(String),
    #[error("not a user JWT")]
    NotUser,
    #[error("{0} is not the public key of a user (U...)")]
    NotUserKey(String),
    #[error("a user JWT named {0:?}, which is no user id")]
    InvalidName(String),
    #[error("expired at {0}")]
    Expired(u64),
    #[error("{0} is no admin: its user JWT carries no {ADMIN_TAG} tag")]
    NotAdmin(UserId),
    #[error("{user} is not the assistant of {human}: its user JWT carries no {}{human} tag", AssistantOf::TAG)]
    NotAssistant { user: UserId, human: UserId },
    #[error("not signed by {0}")]
    BadSignature(String),
}

/// The claims of a JWT whose issuer signed it ([`verified_jwt`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwtClaims {
    /// The subject's public key.
    pub sub: String,
    /// The public key that signed it.
    pub iss: String,
    pub name: String,
    /// When issued, in seconds since the epoch.
    pub iat: Option<u64>,
    /// Expiry, in seconds since the epoch.
    pub exp: Option<u64>,
}

/// Decode a NATS JWT and check that its issuer signed it: its claims, and all of them as JSON.
pub fn verified_jwt(jwt: &str) -> Result<(JwtClaims, Value), JwtError> {
    let jwt = jwt.trim();
    let (signed, signature) = jwt.rsplit_once('.').ok_or(JwtError::Malformed)?;
    let (_, body) = signed.split_once('.').ok_or(JwtError::Malformed)?;
    let decode = |part: &str| BASE64URL_NOPAD.decode(part.as_bytes()).map_err(|_| JwtError::Malformed);
    let claims: Value = serde_json::from_slice(&decode(body)?).map_err(|_| JwtError::Malformed)?;
    let text = |field: &'static str| claims[field].as_str().map(str::to_owned).ok_or(JwtError::Missing(field));
    let iss = text("iss")?;
    let issuer = KeyPair::from_public_key(&iss)
        .map_err(|e| JwtError::InvalidIssuer { issuer: iss.clone(), reason: e.to_string() })?;
    issuer.verify(signed.as_bytes(), &decode(signature)?).map_err(|_| JwtError::NotSignedByIssuer(iss.clone()))?;
    let verified = JwtClaims {
        sub: text("sub")?,
        iss,
        name: text("name")?,
        iat: claims["iat"].as_u64(),
        exp: claims["exp"].as_u64(),
    };
    Ok((verified, claims))
}

/// A user whose user JWT a reader verified ([`verify_user_jwt`]), with the roles it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedUser {
    /// The name the JWT carries.
    pub id: UserId,
    /// The user's public NKey, `U...`, which signs its acts.
    pub public_key: String,
    /// When the JWT was issued, in seconds since the epoch, for a revocation's time to compare with.
    pub issued_at: Option<u64>,
    /// When it expires, in seconds since the epoch; none when it never does.
    pub expires_at: Option<u64>,
    pub tags: Vec<String>,
}

impl VerifiedUser {
    /// Whether the JWT carries [`ADMIN_TAG`].
    pub fn is_admin(&self) -> bool {
        is_admin(self.tags.iter().map(String::as_str))
    }

    /// The human the JWT says the user assists ([`AssistantOf`]), if any.
    pub fn assistant_of(&self) -> Option<UserId> {
        AssistantOf::from_tags(self.tags.iter().map(String::as_str))
    }

    /// Check that `signature`, base64url without padding, is the signature of `payload` by the user's key.
    pub fn check_signature(&self, payload: &[u8], signature: &str) -> Result<(), JwtError> {
        let bad = || JwtError::BadSignature(self.public_key.clone());
        let key = KeyPair::from_public_key(&self.public_key).map_err(|_| bad())?;
        let signature = BASE64URL_NOPAD.decode(signature.as_bytes()).map_err(|_| bad())?;
        key.verify(payload, &signature).map_err(|_| bad())
    }
}

/// Verify `jwt` as the user JWT of a member of the team: signed by one of `accounts`, the team account's public keys
/// (`A...`), a user JWT for a user NKey, named for a valid user id, and unexpired at `now` (seconds since the epoch).
/// Whether its key is revoked the caller checks.
pub fn verify_user_jwt<'a>(
    jwt: &str,
    accounts: impl IntoIterator<Item = &'a str>,
    now: u64,
) -> Result<VerifiedUser, JwtError> {
    let (claims, body) = verified_jwt(jwt)?;
    if !accounts.into_iter().any(|account| account == claims.iss) {
        return Err(JwtError::ForeignAccount(claims.iss));
    }
    if body["nats"]["type"] != "user" {
        return Err(JwtError::NotUser);
    }
    match KeyPair::from_public_key(&claims.sub) {
        Ok(key) if key.key_pair_type() == KeyPairType::User => {}
        _ => return Err(JwtError::NotUserKey(claims.sub)),
    }
    let id = claims.name.parse().map_err(|_| JwtError::InvalidName(claims.name.clone()))?;
    if let Some(exp) = claims.exp.filter(|exp| *exp <= now) {
        return Err(JwtError::Expired(exp));
    }
    let tags = body["nats"]["tags"].as_array().into_iter().flatten().filter_map(Value::as_str);
    Ok(VerifiedUser {
        id,
        public_key: claims.sub,
        issued_at: claims.iat,
        expires_at: claims.exp,
        tags: tags.map(str::to_owned).collect(),
    })
}

/// Verify an act of an admin: `jwt` is the user JWT of a member of the team ([`verify_user_jwt`]) carrying
/// [`ADMIN_TAG`], and `signature` (base64url without padding) the signature of `payload`, such as an admin order's
/// bytes, by its key. Returns the admin's verified user, whose key the caller checks is not revoked.
pub fn verify_admin<'a>(
    jwt: &str,
    accounts: impl IntoIterator<Item = &'a str>,
    payload: &[u8],
    signature: &str,
    now: u64,
) -> Result<VerifiedUser, JwtError> {
    let user = verify_user_jwt(jwt, accounts, now)?;
    if !user.is_admin() {
        return Err(JwtError::NotAdmin(user.id));
    }
    user.check_signature(payload, signature)?;
    Ok(user)
}

/// Verify that `jwt` is the user JWT of a member of the team ([`verify_user_jwt`]) that the account made the
/// assistant of `human` ([`AssistantOf`]). Whether the human's grant stands ([`HUMANS_BUCKET`]), and that an act was
/// signed by the assistant's key ([`VerifiedUser::check_signature`]), the caller checks.
pub fn verify_assistant_of<'a>(
    jwt: &str,
    accounts: impl IntoIterator<Item = &'a str>,
    human: &UserId,
    now: u64,
) -> Result<VerifiedUser, JwtError> {
    let user = verify_user_jwt(jwt, accounts, now)?;
    if user.assistant_of().as_ref() != Some(human) {
        return Err(JwtError::NotAssistant { user: user.id, human: human.clone() });
    }
    Ok(user)
}

#[cfg(test)]
pub(crate) mod tests;
