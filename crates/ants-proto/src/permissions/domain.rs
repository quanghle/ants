//! Permissions in the JetStream domain forms of their API and KV subjects: for clients of a hub in a domain
//! ([`Permissions::with_js_domain`]), and for a leaf node's uplink to one ([`Permissions::with_uplink_js_domain`]).

use super::{Permissions, covers};
use crate::{JsDomain, Role, UserId};

/// Families of JetStream API rights naming no user, by the pattern of their plain form after `$JS.API.`: looking up,
/// creating and updating streams, and creating the ordered consumers of a stream, whose name and filter its subject
/// leaves out. No credentials hold a right of these a human lacks, so a human's take each family's domain forms as one
/// wildcard ([`Permissions::with_js_domain`]), but for host credentials creating [`crate::SUGAR_STREAM`].
const DOMAIN_FAMILIES: [&str; 4] = ["STREAM.INFO.*", "STREAM.CREATE.*", "STREAM.UPDATE.*", "CONSUMER.CREATE.*"];

impl Permissions {
    /// The same permissions, also for clients that use the JetStream API of `domain` (`$JS.<domain>.API.…`), as every
    /// ants process does when the hub has a domain. KV puts and deletes then go through that prefix too
    /// (`$JS.<domain>.API.$KV.…`). The plain subjects stay allowed: the hub maps the domain prefix onto them and checks
    /// the mapped subject against the permissions as well. Acks, flow control and ants subjects are the same in both.
    /// A domain form already allowed by a wildcard (`$JS.*.API.…`) is not listed again, saving room in the JWT.
    ///
    /// Credentials holding every right of a family of [`DOMAIN_FAMILIES`] that any credentials hold, as a human's do,
    /// take the domain forms of the family as one wildcard (`$JS.<domain>.API.STREAM.CREATE.*`), keeping a human's JWT
    /// well within `max_control_line`. The wildcard grants nothing more: the hub checks the mapped plain subject against
    /// the plain rights, and a leaf node's uplink, which a request through a leaf crosses unmapped, holds no right of
    /// the family that such credentials lack ([`Self::for_leaf`]), but its host's creating [`crate::SUGAR_STREAM`]: a
    /// human on a leaf node may create that stream, yet a host writes in it only once its own create, with the one
    /// config hosts create it with, succeeded, which nats-server refuses for a stream of another config. Without a
    /// domain the permissions are unchanged.
    #[must_use]
    pub fn with_js_domain(mut self, domain: Option<&JsDomain>) -> Self {
        let Some(domain) = domain else { return self };
        let api = format!("$JS.{domain}.API.");
        let most = Self::for_user(&UserId::human(), Role::Human).publish;
        let whole: Vec<String> = DOMAIN_FAMILIES
            .iter()
            .map(|family| format!("$JS.API.{family}"))
            .filter(|family| {
                let mut held = most.iter().filter(|s| covers(family, s)).peekable();
                held.peek().is_some() && held.all(|s| self.publish.contains(s))
            })
            .collect();
        let mut in_domain: Vec<String> = whole.iter().map(|family| family.replacen("$JS.API.", &api, 1)).collect();
        in_domain.extend(
            self.publish
                .iter()
                .filter(|s| !whole.iter().any(|family| covers(family, s)))
                .filter_map(|s| match s.strip_prefix("$JS.API.") {
                    Some(rest) => Some(format!("{api}{rest}")),
                    None if s.starts_with("$KV.") => Some(format!("{api}{s}")),
                    None => None,
                })
                .filter(|s| !self.publish.iter().any(|have| covers(have, s))),
        );
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
