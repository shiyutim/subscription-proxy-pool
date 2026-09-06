/// Errors do not include subscription contents or credential-bearing URLs.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid configuration or proxy endpoint.
    #[error("invalid configuration: {0}")]
    Config(&'static str),
    /// Subscription retrieval or parsing failed.
    #[error("subscription error: {0}")]
    Subscription(&'static str),
    /// Cache validation or asynchronous cache work failed.
    #[error("cache error: {0}")]
    Cache(&'static str),
    /// A network failure with its URL removed.
    #[error("network error: {0}")]
    Transport(#[source] reqwest::Error),
    /// Local file access failed.
    #[error("file access error: {0}")]
    Io(#[from] std::io::Error),
    /// No node is currently eligible; requests are never silently sent directly.
    #[error("no eligible proxy is available")]
    NoProxyAvailable,
    /// Healthy nodes exist, but all have reached their configured concurrency limit.
    #[error("all eligible proxies are at their in-flight request limit")]
    PoolSaturated,
    /// A configured subscription pool could not load any supported nodes.
    #[error("could not initialize subscriptions: {0:?}")]
    Initialization(Vec<crate::SourceReport>),
}

/// Result type used by this crate.
pub type Result<T> = std::result::Result<T, Error>;

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Self::Transport(error.without_url())
    }
}
