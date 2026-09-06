/// Result for an individual subscription source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceOutcome {
    /// A new successful HTTP response replaced the source's nodes.
    Updated,
    /// HTTP 304 confirmed the existing nodes; no body download was needed.
    NotModified,
    /// Startup used a fresh disk cache without contacting the source.
    FreshCache,
    /// A failed fetch fell back to a still-allowed disk cache.
    StaleCache,
    /// A failed fetch retained the source's running in-memory nodes.
    Retained,
    /// No source data could be loaded.
    Failed,
}

/// Per-source diagnostics. Keys are hashes; URLs and credentials are omitted.
#[derive(Clone, Debug)]
pub struct SourceReport {
    /// Stable hash returned by SubscriptionSource::key().
    pub source_key: String,
    /// How this source was resolved.
    pub outcome: SourceOutcome,
    /// Number of usable nodes in this source, before cross-source deduplication.
    pub nodes: usize,
    /// Sanitized remote error, if this update failed.
    pub error: Option<String>,
    /// Sanitized cache I/O error, if caching failed.
    pub cache_error: Option<String>,
}

/// Last completed refresh, available after build and background refresh too.
#[derive(Clone, Debug, Default)]
pub struct RefreshReport {
    /// Unique nodes in the resulting pool.
    pub nodes: usize,
    /// Sources with changed, successfully parsed content.
    pub updated_sources: usize,
    /// Sources confirmed by HTTP 304.
    pub not_modified_sources: usize,
    /// Sources restored from disk.
    pub cached_sources: usize,
    /// Failed remote sources, including those with successful fallback.
    pub failed_sources: usize,
    /// Unsupported, duplicate or invalid parsed nodes.
    pub skipped_nodes: usize,
    /// Cache reads that failed due to filesystem errors.
    pub cache_read_failures: usize,
    /// Cache writes that failed after successful remote validation.
    pub cache_write_failures: usize,
    /// Details for each configured subscription, in configuration order.
    pub sources: Vec<SourceReport>,
}
