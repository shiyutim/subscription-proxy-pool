use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use crate::{Error, Result};

/// Proxy transports supported directly by the HTTP client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProxyKind {
    /// HTTP forward proxy (including HTTPS targets via CONNECT).
    Http,
    /// TLS connection to the forward proxy itself.
    Https,
    /// SOCKS5 with local target DNS resolution.
    Socks5,
    /// SOCKS5 with target DNS resolution by the proxy.
    Socks5h,
}

impl ProxyKind {
    fn scheme(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
            Self::Socks5 => "socks5",
            Self::Socks5h => "socks5h",
        }
    }
}

/// A validated proxy endpoint. Debug output omits credentials and display names.
///
/// Serialization includes credentials so caches can restore functioning clients.
/// Treat serialized nodes and [`Self::url`] as secrets.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyNode {
    name: String,
    endpoint: String,
}

impl ProxyNode {
    /// Parse an HTTP(S) or SOCKS5(H) URL. URI fragments become the display name.
    pub fn from_url(value: &str) -> Result<Self> {
        let mut url = Url::parse(value).map_err(|_| Error::Config("invalid proxy URL"))?;
        Self::validate_url(&url)?;
        let name = url.fragment().unwrap_or("proxy").to_owned();
        url.set_fragment(None);
        Ok(Self {
            name,
            endpoint: url.into(),
        })
    }

    fn validate_url(url: &Url) -> Result<()> {
        if !matches!(url.scheme(), "http" | "https" | "socks5" | "socks5h") {
            return Err(Error::Config("unsupported proxy protocol"));
        }
        if url.host_str().is_none_or(str::is_empty) || url.port_or_known_default().unwrap_or(0) == 0
        {
            return Err(Error::Config("proxy host and a nonzero port are required"));
        }
        if !matches!(url.path(), "" | "/") || url.query().is_some() {
            return Err(Error::Config("proxy URLs cannot contain a path or query"));
        }
        Ok(())
    }

    /// Validate a node restored using Serde.
    pub fn validate(&self) -> Result<()> {
        let url = Url::parse(&self.endpoint).map_err(|_| Error::Config("invalid proxy URL"))?;
        Self::validate_url(&url)?;
        if url.fragment().is_some() {
            return Err(Error::Config("stored proxy URL cannot contain a fragment"));
        }
        Ok(())
    }

    /// Override the human-readable name; it has no effect on node identity.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Human-readable name supplied by the subscription.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Stable endpoint identity, including credentials but excluding its name.
    pub fn id(&self) -> String {
        format!("{:x}", Sha256::digest(self.endpoint.as_bytes()))
    }

    /// Full endpoint URL, including any credentials. Do not log it.
    pub fn url(&self) -> &str {
        &self.endpoint
    }

    /// The node's transport protocol.
    pub fn kind(&self) -> ProxyKind {
        match self.endpoint.split(':').next() {
            Some("https") => ProxyKind::Https,
            Some("socks5") => ProxyKind::Socks5,
            Some("socks5h") => ProxyKind::Socks5h,
            _ => ProxyKind::Http,
        }
    }
}

impl fmt::Debug for ProxyNode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxyNode")
            .field("id", &self.id())
            .field("protocol", &self.kind().scheme())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_ignores_name_but_includes_credentials() {
        let first = ProxyNode::from_url("http://user:secret@localhost:8080#one").unwrap();
        let renamed = first.clone().with_name("two");
        assert_eq!(first.id(), renamed.id());
        assert_ne!(
            first.id(),
            ProxyNode::from_url("http://user:changed@localhost:8080")
                .unwrap()
                .id()
        );
        assert!(!format!("{first:?}").contains("secret"));
    }

    #[test]
    fn validates_protocol_ports_and_endpoint_shape() {
        for value in [
            "ss://host:10",
            "socks5://host",
            "http://host:0",
            "http://host/path",
            "http://host?key=secret",
        ] {
            assert!(ProxyNode::from_url(value).is_err(), "{value}");
        }
        for value in [
            "http://localhost",
            "https://localhost",
            "socks5h://[::1]:1080",
        ] {
            assert!(ProxyNode::from_url(value).is_ok(), "{value}");
        }
    }
}
