//! The initialize handshake: protocol versions and [`Capabilities`] offered by ants ([`InitializeParams`]), those the
//! provider answers with ([`InitializeResult`]), and what both then use ([`negotiate`]).

use std::fmt;

use serde::{Deserialize, Serialize};

use super::{PROTOCOL_VERSION, RpcError, valid_provider_name};

/// A program's name and version: ants' or the provider's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Info {
    pub name: String,
    pub version: String,
}

/// What a side of the protocol serves or uses. A capability missing from a document reads as not declared, and one
/// unknown is ignored.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Capabilities {
    /// [`super::method::TOOLS_LIST`] and [`super::method::TOOLS_CALL`]: tools served to the ant's sessions.
    pub tools: bool,
    /// [`super::method::BRIEF`]: the ant's brief, put in its prompts.
    pub context: bool,
    /// [`super::method::EVENT`]: the ant's lifecycle events.
    pub events: bool,
    /// [`super::method::WAKE`]: the provider waking the ant with a message.
    pub wake: bool,
    /// [`super::method::MEMORIES`]: the view of the ant's memories `ants ant memories` shows.
    pub memories: bool,
    /// [`super::method::EXPORT`] and [`super::method::IMPORT`]: the ant's data in `ants ant export` and `import`.
    pub export: bool,
}

impl Capabilities {
    pub const NONE: Self =
        Self { tools: false, context: false, events: false, wake: false, memories: false, export: false };
    pub const ALL: Self = Self { tools: true, context: true, events: true, wake: true, memories: true, export: true };

    /// The capabilities declared by both `self` and `other`.
    #[must_use]
    pub fn both(self, other: Self) -> Self {
        Self {
            tools: self.tools && other.tools,
            context: self.context && other.context,
            events: self.events && other.events,
            wake: self.wake && other.wake,
            memories: self.memories && other.memories,
            export: self.export && other.export,
        }
    }

    pub fn has(self, capability: Capability) -> bool {
        match capability {
            Capability::Tools => self.tools,
            Capability::Context => self.context,
            Capability::Events => self.events,
            Capability::Wake => self.wake,
            Capability::Memories => self.memories,
            Capability::Export => self.export,
        }
    }
}

/// One of the [`Capabilities`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    Tools,
    Context,
    Events,
    Wake,
    Memories,
    Export,
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Tools => "tools",
            Self::Context => "context",
            Self::Events => "events",
            Self::Wake => "wake",
            Self::Memories => "memories",
            Self::Export => "export",
        })
    }
}

/// The params of [`super::method::INITIALIZE`], ants' first request to a provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitializeParams {
    /// The protocol versions ants speaks.
    pub protocol_versions: Vec<u32>,
    /// What ants uses of a provider, and serves of it ([`Capabilities::wake`]).
    pub capabilities: Capabilities,
    pub client: Info,
}

impl InitializeParams {
    /// The params of an ants client speaking this build's [`PROTOCOL_VERSION`] and using `capabilities`.
    pub fn new(client: Info, capabilities: Capabilities) -> Self {
        Self { protocol_versions: vec![PROTOCOL_VERSION], capabilities, client }
    }

    /// The answer of `provider`, which speaks the protocol versions `supported` and declares `capabilities`: the
    /// highest version both speak, else an [`RpcError::UNSUPPORTED_VERSION`] error.
    pub fn accept(
        &self,
        supported: &[u32],
        provider: Info,
        capabilities: Capabilities,
    ) -> Result<InitializeResult, RpcError> {
        let protocol_version = choose_version(&self.protocol_versions, supported)
            .ok_or_else(|| RpcError::unsupported_version(&self.protocol_versions, supported))?;
        Ok(InitializeResult { protocol_version, provider, capabilities })
    }
}

/// The result of [`super::method::INITIALIZE`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitializeResult {
    /// The version chosen, one of [`InitializeParams::protocol_versions`].
    pub protocol_version: u32,
    /// The provider: its name prefixes the names its tools are served under ([`super::served_name`]).
    pub provider: Info,
    /// What the provider serves, and uses of ants ([`Capabilities::wake`]).
    pub capabilities: Capabilities,
}

/// The highest protocol version both in `offered` and in `supported`.
pub fn choose_version(offered: &[u32], supported: &[u32]) -> Option<u32> {
    offered.iter().filter(|v| supported.contains(v)).max().copied()
}

/// What ants and a provider agreed on at initialize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agreed {
    pub version: u32,
    pub provider: Info,
    /// The capabilities both declared: the only ones either side uses.
    pub capabilities: Capabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NegotiationError {
    #[error("the provider refused initialize: {0}")]
    Refused(RpcError),
    #[error("the provider chose protocol version {chosen}, not one of those offered, {offered:?}")]
    NoCommonVersion { offered: Vec<u32>, chosen: u32 },
    #[error("invalid provider name {0:?}: use 1-32 of a-z, 0-9 and '-', starting with a letter")]
    InvalidName(String),
}

/// What ants, having sent `params`, agrees on with a provider that answered `result`: the version it chose, when one
/// of those offered, and the capabilities both declared.
pub fn negotiate(params: &InitializeParams, result: InitializeResult) -> Result<Agreed, NegotiationError> {
    if !params.protocol_versions.contains(&result.protocol_version) {
        return Err(NegotiationError::NoCommonVersion {
            offered: params.protocol_versions.clone(),
            chosen: result.protocol_version,
        });
    }
    if !valid_provider_name(&result.provider.name) {
        return Err(NegotiationError::InvalidName(result.provider.name));
    }
    let capabilities = params.capabilities.both(result.capabilities);
    Ok(Agreed { version: result.protocol_version, provider: result.provider, capabilities })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(name: &str) -> Info {
        Info { name: name.to_owned(), version: "1.0.0".to_owned() }
    }

    fn params(versions: &[u32], capabilities: Capabilities) -> InitializeParams {
        InitializeParams { protocol_versions: versions.to_vec(), capabilities, client: info("ants") }
    }

    #[test]
    fn the_highest_common_version_is_chosen() {
        assert_eq!(choose_version(&[1, 2, 3], &[2, 3, 4]), Some(3));
        assert_eq!(choose_version(&[3, 1], &[1]), Some(1));
        assert_eq!(choose_version(&[1], &[2]), None);
        assert_eq!(choose_version(&[], &[1]), None);
    }

    #[test]
    fn only_capabilities_both_sides_declare_are_agreed() {
        let ants = Capabilities { tools: true, context: true, wake: true, export: true, ..Capabilities::NONE };
        let provider = Capabilities { tools: true, events: true, wake: true, memories: true, ..Capabilities::NONE };
        let offered = params(&[1], ants);
        let result = offered.accept(&[1], info("sense"), provider).unwrap();
        let agreed = negotiate(&offered, result).unwrap();
        assert_eq!(agreed.version, 1);
        assert_eq!(agreed.provider.name, "sense");
        assert_eq!(agreed.capabilities, Capabilities { tools: true, wake: true, ..Capabilities::NONE });
        assert!(agreed.capabilities.has(Capability::Wake) && !agreed.capabilities.has(Capability::Events));
        assert_eq!(Capabilities::ALL.both(ants), ants);
    }

    #[test]
    fn no_common_version_fails_on_either_side() {
        let offered = params(&[1], Capabilities::ALL);
        let refused = offered.accept(&[2, 3], info("sense"), Capabilities::ALL).unwrap_err();
        assert_eq!(refused.code, RpcError::UNSUPPORTED_VERSION);
        assert_eq!(refused.data, Some(serde_json::json!({"supported": [2, 3]})));

        let chosen_anyway =
            InitializeResult { protocol_version: 2, provider: info("sense"), capabilities: Capabilities::ALL };
        assert_eq!(
            negotiate(&offered, chosen_anyway),
            Err(NegotiationError::NoCommonVersion { offered: vec![1], chosen: 2 })
        );
    }

    #[test]
    fn a_provider_name_must_prefix_tool_names() {
        let offered = params(&[1], Capabilities::ALL);
        let result = offered.accept(&[1], info("My Provider"), Capabilities::ALL).unwrap();
        assert_eq!(negotiate(&offered, result), Err(NegotiationError::InvalidName("My Provider".to_owned())));
    }

    #[test]
    fn missing_capabilities_read_as_undeclared() {
        let read: Capabilities = serde_json::from_str(r#"{"tools": true, "telepathy": true}"#).unwrap();
        assert_eq!(read, Capabilities { tools: true, ..Capabilities::NONE });
    }
}
