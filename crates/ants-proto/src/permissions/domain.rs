//! Permissions in the JetStream domain forms of their API and KV subjects: for clients of a hub in a domain
//! ([`Permissions::with_js_domain`]), and for a leaf node's uplink to one ([`Permissions::with_uplink_js_domain`]).

use super::{Permissions, covers};
use crate::JsDomain;

impl Permissions {
    /// The same permissions, also for clients that use the JetStream API of `domain` (`$JS.<domain>.API.…`), as every
    /// ants process does when the hub has a domain. KV puts and deletes then go through that prefix too
    /// (`$JS.<domain>.API.$KV.…`). Acks, flow control and ants subjects are the same in both. A domain form already
    /// allowed by a wildcard (`$JS.*.API.…`) is not listed again, saving room in the JWT.
    ///
    /// Each domain form is that of a plain right, as listed, never a wildcard over several: the store account maps a
    /// domain form to the plain one past the service import of the team's account ([`crate::Permissions`]), where no
    /// permissions of the user are checked any more. Without a domain the permissions are unchanged.
    #[must_use]
    pub fn with_js_domain(mut self, domain: Option<&JsDomain>) -> Self {
        let Some(domain) = domain else { return self };
        let api = format!("$JS.{domain}.API.");
        let in_domain: Vec<String> = self
            .publish
            .iter()
            .filter_map(|s| match s.strip_prefix("$JS.API.") {
                Some(rest) => Some(format!("{api}{rest}")),
                None if s.starts_with("$KV.") => Some(format!("{api}{s}")),
                None => None,
            })
            .filter(|s| !self.publish.iter().any(|have| covers(have, s)))
            .collect();
        self.publish.extend(in_domain);
        self
    }

    /// The same permissions, for a leaf node's uplink to a hub in JetStream domain `domain`: the JetStream API and KV
    /// only in their domain form (`$JS.<domain>.API.…`, `$JS.<domain>.API.$KV.…`), the plain ones dropped. Their
    /// domains differ (the leaf's own JetStream is in the machine's), so nats-server denies `$JS.API.>`, `$KV.>` and
    /// `$OBJ.>` on the leaf connection both ways, and checks a message from the leaf against the uplink's permissions
    /// before mapping the domain prefix only: the plain forms never cross the uplink, and listing them would only
    /// take room in its JWT ([`Self::for_leaf`]). Without a domain the permissions are unchanged.
    #[must_use]
    pub fn with_uplink_js_domain(mut self, domain: Option<&JsDomain>) -> Self {
        let Some(domain) = domain else { return self };
        let api = format!("$JS.{domain}.API.");
        for s in &mut self.publish {
            if let Some(rest) = s.strip_prefix("$JS.API.") {
                *s = format!("{api}{rest}");
            } else if s.starts_with("$KV.") {
                *s = format!("{api}{s}");
            }
        }
        let mut seen = std::collections::HashSet::new();
        self.publish.retain(|s| seen.insert(s.clone()));
        self
    }
}

/// The JetStream domain the publish allowances `publish` of a user JWT name, as [`Permissions::with_js_domain`] adds
/// them; none for credentials of a hub without one.
pub fn js_domain_of<'a>(publish: impl IntoIterator<Item = &'a str>) -> Option<JsDomain> {
    publish.into_iter().find_map(|s| {
        let (domain, rest) = s.strip_prefix("$JS.")?.split_once('.')?;
        if domain == "API" || domain == "*" || !rest.starts_with("API.") {
            return None;
        }
        domain.parse().ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Role, p};

    #[test]
    fn the_domain_of_credentials_is_the_one_their_rights_name() {
        let hub: JsDomain = "hub".parse().unwrap();
        for plain in [Permissions::for_user(&p("alice"), Role::Human), Permissions::for_web(&p("alice"))] {
            assert_eq!(js_domain_of(plain.publish.iter().map(String::as_str)), None);
            let in_domain = plain.with_js_domain(Some(&hub));
            assert_eq!(js_domain_of(in_domain.publish.iter().map(String::as_str)), Some(hub.clone()));
        }
        assert_eq!(js_domain_of(["$JS.*.API.>", "$JS.ACK.>"]), None);
    }
}
