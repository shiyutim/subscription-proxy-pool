use std::{
    collections::{HashMap, HashSet},
    error::Error as _,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::{StreamExt, stream};
use reqwest::{Client, Method, Request, Response, StatusCode};
use tokio::task::JoinHandle;

use crate::{
    CachePolicy, DEFAULT_MAX_SUBSCRIPTION_BYTES, Error, HealthPolicy, ProxyNode, RefreshReport,
    Result, RotationPolicy, SourceOutcome, SourceReport, SubscriptionSource, SubscriptionUpdate,
    SubscriptionValidators, cache::CacheStore, rotation::RotationState,
};

/// Build a reusable pool. Clone the resulting pool to share rotation counters,
/// health state, and per-node connection pools across tasks.
pub struct PoolBuilder {
    sources: Vec<SubscriptionSource>,
    nodes: Vec<ProxyNode>,
    cache: Option<CachePolicy>,
    rotation: RotationPolicy,
    health: HealthPolicy,
    subscription_client: Option<Client>,
    request_timeout: Duration,
    connect_timeout: Duration,
    subscription_timeout: Duration,
    refresh_interval: Duration,
    health_interval: Duration,
    max_subscription_bytes: usize,
    max_in_flight_per_proxy: Option<NonZeroUsize>,
    subscription_concurrency: NonZeroUsize,
}

impl Default for PoolBuilder {
    fn default() -> Self {
        Self {
            sources: vec![],
            nodes: vec![],
            cache: None,
            rotation: RotationPolicy::default(),
            health: HealthPolicy::default(),
            subscription_client: None,
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            subscription_timeout: Duration::from_secs(30),
            refresh_interval: Duration::from_secs(3600),
            health_interval: Duration::from_secs(60),
            max_subscription_bytes: DEFAULT_MAX_SUBSCRIPTION_BYTES,
            max_in_flight_per_proxy: None,
            subscription_concurrency: NonZeroUsize::new(4).unwrap(),
        }
    }
}

impl PoolBuilder {
    /// Start with per-request rotation, a 30s request timeout, and passive health feedback.
    pub fn new() -> Self {
        Self::default()
    }
    /// Add a subscription; repeated source URLs are deduplicated.
    pub fn subscription(mut self, source: SubscriptionSource) -> Self {
        self.sources.push(source);
        self
    }
    /// Add static nodes, optionally alongside subscriptions.
    pub fn nodes(mut self, nodes: impl IntoIterator<Item = ProxyNode>) -> Self {
        self.nodes.extend(nodes);
        self
    }
    /// Enable private, atomic on-disk subscription caching. Disabled by default.
    pub fn cache(mut self, policy: CachePolicy) -> Self {
        self.cache = Some(policy);
        self
    }
    /// Configure request-count and/or time-based rotation.
    pub fn rotation(mut self, policy: RotationPolicy) -> Self {
        self.rotation = policy;
        self
    }
    /// Configure health probes and failure cooldowns.
    pub fn health(mut self, policy: HealthPolicy) -> Self {
        self.health = policy;
        self
    }
    /// Override the subscription HTTP client, for example to use a bootstrap proxy.
    /// Its headers, redirects, and TLS policy are controlled by the caller.
    pub fn subscription_client(mut self, client: Client) -> Self {
        self.subscription_client = Some(client);
        self
    }
    /// Set the total timeout for each business request.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
    /// Set the connection timeout for each business client.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }
    /// Bound the entire subscription download, including custom clients.
    pub fn subscription_timeout(mut self, timeout: Duration) -> Self {
        self.subscription_timeout = timeout;
        self
    }
    /// Background remote refresh frequency, independent of the disk cache TTL.
    pub fn refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }
    /// Background health check frequency.
    pub fn health_interval(mut self, interval: Duration) -> Self {
        self.health_interval = interval;
        self
    }
    /// Maximum response bytes accepted per subscription.
    pub fn max_subscription_bytes(mut self, bytes: usize) -> Self {
        self.max_subscription_bytes = bytes;
        self
    }

    /// Bound business attempts per node. Saturated nodes are skipped; if all
    /// healthy nodes are busy, acquisition returns [`Error::PoolSaturated`].
    /// Automatic execution releases its slot at response headers; a manual
    /// lease can be held until its response body has been consumed.
    pub fn max_in_flight_per_proxy(mut self, limit: NonZeroUsize) -> Self {
        self.max_in_flight_per_proxy = Some(limit);
        self
    }

    /// Bound simultaneous subscription requests, preserving source order.
    pub fn subscription_concurrency(mut self, limit: NonZeroUsize) -> Self {
        self.subscription_concurrency = limit;
        self
    }

    /// Fetch or restore subscriptions, build clients, and optionally probe nodes.
    /// Returns an error for an empty pool or if initial checks find no healthy node.
    pub async fn build(mut self) -> Result<ProxyPool> {
        self.rotation.validate()?;
        self.health.validate()?;
        for duration in [
            self.request_timeout,
            self.connect_timeout,
            self.subscription_timeout,
            self.refresh_interval,
            self.health_interval,
            self.health.timeout,
            self.health.cooldown,
        ] {
            if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
                return Err(Error::Config(
                    "timeouts and intervals must be positive and representable",
                ));
            }
        }
        if self.max_subscription_bytes == 0 {
            return Err(Error::Config("subscription size limit must be positive"));
        }
        if let Some(cache) = &self.cache {
            cache.validate()?;
        }
        for node in &self.nodes {
            node.validate()?;
        }
        let mut seen = HashSet::new();
        self.sources.retain(|source| seen.insert(source.key()));
        let subscription_client = match self.subscription_client {
            Some(client) => client,
            None => Client::builder()
                .no_proxy()
                .timeout(self.subscription_timeout)
                .redirect(reqwest::redirect::Policy::limited(5))
                .build()?,
        };
        let pool = ProxyPool {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                sources: self.sources,
                static_nodes: self.nodes,
                cache: self.cache.map(CacheStore::new),
                rotation: self.rotation,
                health: self.health,
                subscription_client,
                request_timeout: self.request_timeout,
                connect_timeout: self.connect_timeout,
                subscription_timeout: self.subscription_timeout,
                refresh_interval: self.refresh_interval,
                health_interval: self.health_interval,
                max_subscription_bytes: self.max_subscription_bytes,
                max_in_flight_per_proxy: self.max_in_flight_per_proxy,
                subscription_concurrency: self.subscription_concurrency,
                maintenance: Mutex::new(Weak::new()),
                refresh_lock: tokio::sync::Mutex::new(()),
                probe_lock: tokio::sync::Mutex::new(()),
            }),
        };
        pool.refresh_inner(true).await?;
        if pool.inner.health.check_on_build {
            pool.check_health().await;
            // An expired cooldown permits a recovery attempt but is not proof
            // that any initial probe actually succeeded.
            let any_healthy = pool
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .entries
                .iter()
                .any(|entry| !entry.pending_probe && entry.cooldown_until.is_none());
            if !any_healthy {
                return Err(Error::NoProxyAvailable);
            }
        }
        Ok(pool)
    }
}

struct Inner {
    state: Mutex<State>,
    sources: Vec<SubscriptionSource>,
    static_nodes: Vec<ProxyNode>,
    cache: Option<CacheStore>,
    rotation: RotationPolicy,
    health: HealthPolicy,
    subscription_client: Client,
    request_timeout: Duration,
    connect_timeout: Duration,
    subscription_timeout: Duration,
    refresh_interval: Duration,
    health_interval: Duration,
    max_subscription_bytes: usize,
    max_in_flight_per_proxy: Option<NonZeroUsize>,
    subscription_concurrency: NonZeroUsize,
    refresh_lock: tokio::sync::Mutex<()>,
    probe_lock: tokio::sync::Mutex<()>,
    maintenance: Mutex<Weak<MaintenanceGroup>>,
}

#[derive(Default)]
struct State {
    entries: Vec<Entry>,
    by_source: HashMap<String, SourceState>,
    rotation: RotationState,
    session_allocator: RotationState,
    index: HashMap<String, usize>,
    candidates: Option<Arc<[String]>>,
    candidates_expire: Option<Instant>,
    refresh_revision: u64,
    last_refresh: Option<RefreshReport>,
}

#[derive(Clone)]
struct SourceState {
    nodes: Vec<ProxyNode>,
    validators: SubscriptionValidators,
}

struct Entry {
    id: String,
    node: ProxyNode,
    client: Client,
    failures: u32,
    cooldown_until: Option<Instant>,
    recovery_token: Option<Arc<()>>,
    pending_probe: bool,
    revision: u64,
    generation: Arc<()>,
    health_epoch: u64,
    cooldown_round: u32,
    in_flight: usize,
}

impl Entry {
    fn eligible(&self, now: Instant) -> bool {
        !self.pending_probe
            && self.recovery_token.is_none()
            && self.cooldown_until.is_none_or(|until| now >= until)
    }

    fn available(&self, now: Instant, limit: Option<NonZeroUsize>) -> bool {
        self.eligible(now) && limit.is_none_or(|limit| self.in_flight < limit.get())
    }

    fn release_recovery(&mut self, token: Option<&Arc<()>>) {
        if let Some(token) = token
            && self
                .recovery_token
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, token))
        {
            self.recovery_token = None;
        }
    }
}

/// A pool whose clones share clients, counters, cached nodes, and health state.
#[derive(Clone)]
pub struct ProxyPool {
    inner: Arc<Inner>,
}

/// A credential-free summary of pool availability.
#[derive(Clone, Copy, Debug, Default)]
pub struct PoolStats {
    /// Total unique nodes.
    pub total: usize,
    /// Nodes that can accept a request now, including one recovery attempt.
    pub eligible: usize,
    /// Nodes in cooldown or with a recovery attempt in progress.
    pub unavailable: usize,
    /// Current business request reservations; probes are excluded.
    pub in_flight: usize,
    /// Healthy nodes whose request capacity is fully occupied.
    pub saturated: usize,
}

/// An explicit session with its own rotation policy and counters. Session clones
/// share that session, while new sessions are independent. All sessions share
/// node health, per-node capacity, and HTTP connection pools with their parent.
#[derive(Clone)]
pub struct ProxySession {
    pool: ProxyPool,
    rotation: Arc<Mutex<RotationState>>,
    policy: RotationPolicy,
}

impl ProxySession {
    /// Reserve an attempt using this session's rotation sequence.
    pub fn acquire(&self) -> Result<ProxyLease> {
        self.pool.acquire_for(&HashSet::new(), Some(self))
    }
    /// Rotate this session on its next acquisition without changing other sessions.
    pub fn rotate(&self) {
        self.rotation
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .force_rotate();
    }
    /// Execute once, with the same no-replay behavior as [`ProxyPool::execute`].
    pub async fn execute(&self, request: Request) -> Result<Response> {
        self.pool.execute_lease(request, self.acquire()?).await
    }
    /// Explicit GET/HEAD/OPTIONS failover within this session.
    pub async fn execute_with_failover(
        &self,
        request: Request,
        max_attempts: NonZeroUsize,
    ) -> Result<Response> {
        self.pool
            .failover_for(request, max_attempts, Some(self))
            .await
    }
}

impl ProxyPool {
    /// Configure a new shared pool.
    pub fn builder() -> PoolBuilder {
        PoolBuilder::new()
    }

    /// Create an independently rotating session. New sessions receive initial
    /// nodes in round-robin order; no global session map or lifetime limit is needed.
    pub fn session(&self, policy: RotationPolicy) -> Result<ProxySession> {
        policy.validate()?;
        Ok(ProxySession {
            pool: self.clone(),
            rotation: Arc::new(Mutex::new(RotationState::default())),
            policy,
        })
    }

    /// Inspect the most recent startup, manual, or background refresh.
    pub fn last_refresh_report(&self) -> Option<RefreshReport> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .last_refresh
            .clone()
    }

    /// Inspect node counts without exposing subscription URLs or credentials.
    pub fn stats(&self) -> PoolStats {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let total = state.entries.len();
        let now = Instant::now();
        let eligible = state
            .entries
            .iter()
            .filter(|entry| entry.available(now, self.inner.max_in_flight_per_proxy))
            .count();
        PoolStats {
            total,
            eligible,
            unavailable: total - eligible,
            in_flight: state.entries.iter().map(|entry| entry.in_flight).sum(),
            saturated: state
                .entries
                .iter()
                .filter(|entry| {
                    entry.eligible(now) && !entry.available(now, self.inner.max_in_flight_per_proxy)
                })
                .count(),
        }
    }

    /// Snapshot configured nodes. Node Debug is redacted; serialization is not.
    pub fn nodes(&self) -> Vec<ProxyNode> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .iter()
            .map(|entry| entry.node.clone())
            .collect()
    }

    /// Force rotation on the next acquisition, preserving round-robin position.
    pub fn rotate(&self) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .rotation
            .force_rotate();
    }

    /// Reserve exactly one request attempt. The reservation consumes one quota
    /// even if later cancelled; network work does not hold the selection mutex.
    pub fn acquire(&self) -> Result<ProxyLease> {
        self.acquire_for(&HashSet::new(), None)
    }

    fn acquire_for(
        &self,
        excluded: &HashSet<String>,
        session: Option<&ProxySession>,
    ) -> Result<ProxyLease> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let now = Instant::now();
        if state.candidates.is_none() || state.candidates_expire.is_some_and(|until| now >= until) {
            state.candidates = Some(
                state
                    .entries
                    .iter()
                    .filter(|entry| entry.available(now, self.inner.max_in_flight_per_proxy))
                    .map(|entry| entry.id.clone())
                    .collect(),
            );
            state.candidates_expire = state
                .entries
                .iter()
                .filter_map(|entry| entry.cooldown_until.filter(|until| *until > now))
                .min();
        }
        let all = state.candidates.as_ref().unwrap().clone();
        let candidates = if excluded.is_empty() {
            all
        } else {
            all.iter()
                .filter(|id| !excluded.contains(*id))
                .cloned()
                .collect::<Arc<[String]>>()
        };
        if candidates.is_empty() {
            return Err(
                if state.entries.iter().any(|entry| {
                    !excluded.contains(&entry.id)
                        && entry.eligible(now)
                        && !entry.available(now, self.inner.max_in_flight_per_proxy)
                }) {
                    Error::PoolSaturated
                } else {
                    Error::NoProxyAvailable
                },
            );
        }
        let id = if let Some(session) = session {
            let mut rotation = session
                .rotation
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if !rotation.has_current() {
                let seed = state
                    .session_allocator
                    .select_shared(&candidates, &RotationPolicy::default(), now)
                    .ok_or(Error::NoProxyAvailable)?;
                rotation.seed(&seed, now);
            }
            rotation.select_shared(&candidates, &session.policy, now)
        } else {
            state
                .rotation
                .select_shared(&candidates, &self.inner.rotation, now)
        }
        .ok_or(Error::NoProxyAvailable)?;
        let index = *state.index.get(&id).ok_or(Error::NoProxyAvailable)?;
        let entry = &mut state.entries[index];
        entry.in_flight += 1;
        if entry.cooldown_until.is_some() {
            entry.recovery_token = Some(Arc::new(()));
        }
        let lease = ProxyLease {
            node: entry.node.clone(),
            client: entry.client.clone(),
            id,
            owner: Arc::downgrade(&self.inner),
            recovery_token: entry.recovery_token.clone(),
            generation: entry.generation.clone(),
            health_epoch: entry.health_epoch,
            session_rotation: session.map(|session| Arc::downgrade(&session.rotation)),
            completed: false,
        };
        if !entry.available(now, self.inner.max_in_flight_per_proxy) {
            state.candidates = None;
        }
        Ok(lease)
    }

    /// Send one request without automatic replay. Reports connection errors,
    /// timeouts, transport disconnects, and HTTP 407 as failures. Local request
    /// and body-stream errors do not count against the proxy's health.
    /// Other HTTP statuses count as connectivity success.
    /// Success is measured when response headers arrive, not when its body is read.
    pub async fn execute(&self, request: Request) -> Result<Response> {
        self.execute_lease(request, self.acquire()?).await
    }

    async fn execute_lease(&self, request: Request, mut lease: ProxyLease) -> Result<Response> {
        match lease.client.execute(request).await {
            Ok(response) => {
                if response.status() == StatusCode::PROXY_AUTHENTICATION_REQUIRED {
                    lease.report_failure();
                } else {
                    lease.report_success();
                }
                Ok(response)
            }
            Err(error) => {
                if is_passive_failure(&error) {
                    lease.report_failure();
                }
                Err(error.into())
            }
        }
    }

    /// Opt-in failover for GET, HEAD, and OPTIONS only. Each attempt uses a
    /// distinct eligible node and consumes quota. Only connection/timeouts retry;
    /// HTTP responses and uncloneable bodies are never automatically replayed.
    pub async fn execute_with_failover(
        &self,
        request: Request,
        max_attempts: NonZeroUsize,
    ) -> Result<Response> {
        self.failover_for(request, max_attempts, None).await
    }

    async fn failover_for(
        &self,
        request: Request,
        max_attempts: NonZeroUsize,
        session: Option<&ProxySession>,
    ) -> Result<Response> {
        if !matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        ) {
            return Err(Error::Config(
                "automatic failover only supports GET, HEAD, and OPTIONS",
            ));
        }
        if request.try_clone().is_none() {
            return Err(Error::Config("request body cannot be replayed"));
        }
        let mut excluded = HashSet::new();
        let mut last_error = None;
        for _ in 0..max_attempts.get() {
            let lease = match self.acquire_for(&excluded, session) {
                Ok(lease) => lease,
                Err(error) => return Err(last_error.unwrap_or(error)),
            };
            excluded.insert(lease.id.clone());
            let attempt = request
                .try_clone()
                .ok_or(Error::Config("request body cannot be replayed"))?;
            match self.execute_lease(attempt, lease).await {
                Err(Error::Transport(error)) if error.is_connect() || error.is_timeout() => {
                    last_error = Some(Error::Transport(error));
                }
                result => return result,
            }
        }
        Err(last_error.unwrap_or(Error::NoProxyAvailable))
    }

    /// Fetch remote sources now regardless of disk TTL. Failed or empty updates
    /// retain previous nodes. Unchanged endpoints retain clients and health state.
    pub async fn refresh(&self) -> Result<RefreshReport> {
        let report = self.refresh_inner(false).await?;
        if self.inner.health.check_on_build {
            self.check_health().await;
        }
        Ok(report)
    }

    async fn refresh_inner(&self, initial: bool) -> Result<RefreshReport> {
        let observed = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .refresh_revision;
        let _guard = self.inner.refresh_lock.lock().await;
        let mut by_source = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if !initial
                && state.refresh_revision != observed
                && let Some(report) = &state.last_refresh
            {
                return Ok(report.clone());
            }
            state.by_source.clone()
        };
        let loaded = stream::iter(self.inner.sources.clone())
            .map(|source| {
                let pool = self.clone();
                let previous = by_source.get(&source.key()).cloned();
                async move { pool.load_source(&source, initial, previous).await }
            })
            .buffered(self.inner.subscription_concurrency.get())
            .collect::<Vec<_>>()
            .await;
        let mut report = RefreshReport::default();
        for (source, result) in self.inner.sources.iter().zip(loaded) {
            report.updated_sources += usize::from(result.report.outcome == SourceOutcome::Updated);
            report.not_modified_sources +=
                usize::from(result.report.outcome == SourceOutcome::NotModified);
            report.cached_sources += usize::from(matches!(
                result.report.outcome,
                SourceOutcome::FreshCache | SourceOutcome::StaleCache
            ));
            report.failed_sources += usize::from(result.report.error.is_some());
            report.cache_read_failures += usize::from(result.cache_read_failed);
            report.cache_write_failures += usize::from(result.cache_write_failed);
            report.skipped_nodes += result.skipped;
            if let Some(data) = result.data {
                by_source.insert(source.key(), data);
            }
            report.sources.push(result.report);
        }
        let mut nodes = self.inner.static_nodes.clone();
        for source in &self.inner.sources {
            if let Some(data) = by_source.get(&source.key()) {
                nodes.extend(data.nodes.iter().cloned());
            }
        }
        let mut seen = HashSet::new();
        nodes.retain(|node| seen.insert(node.id()));
        if nodes.is_empty() {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.last_refresh = Some(report.clone());
            state.refresh_revision = state.refresh_revision.wrapping_add(1);
            return Err(if report.sources.is_empty() {
                Error::NoProxyAvailable
            } else {
                Error::Initialization(report.sources)
            });
        }
        // TLS/client construction is outside the request-selection lock.
        let existing: HashSet<_> = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .iter()
            .map(|entry| entry.id.clone())
            .collect();
        let mut new_clients = HashMap::new();
        for node in &nodes {
            if !existing.contains(&node.id()) {
                node.validate()?;
                let client = Client::builder()
                    .no_proxy()
                    .proxy(reqwest::Proxy::all(node.url())?)
                    .timeout(self.inner.request_timeout)
                    .connect_timeout(self.inner.connect_timeout)
                    .redirect(reqwest::redirect::Policy::limited(5))
                    .build()?;
                new_clients.insert(node.id(), client);
            }
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut old: HashMap<_, _> = std::mem::take(&mut state.entries)
            .into_iter()
            .map(|entry| (entry.id.clone(), entry))
            .collect();
        for node in nodes {
            let id = node.id();
            if let Some(mut entry) = old.remove(&id) {
                entry.node = node;
                state.entries.push(entry);
            } else if let Some(client) = new_clients.remove(&id) {
                state.entries.push(Entry {
                    id,
                    node,
                    client,
                    failures: 0,
                    cooldown_until: None,
                    recovery_token: None,
                    pending_probe: self.inner.health.check_on_build,
                    revision: 0,
                    generation: Arc::new(()),
                    health_epoch: 0,
                    cooldown_round: 0,
                    in_flight: 0,
                });
            }
        }
        state.index = state
            .entries
            .iter()
            .enumerate()
            .map(|(index, entry)| (entry.id.clone(), index))
            .collect();
        state.candidates = None;
        state.candidates_expire = None;
        state.by_source = by_source;
        report.nodes = state.entries.len();
        state.last_refresh = Some(report.clone());
        state.refresh_revision = state.refresh_revision.wrapping_add(1);
        Ok(report)
    }

    async fn load_source(
        &self,
        source: &SubscriptionSource,
        initial: bool,
        previous: Option<SourceState>,
    ) -> LoadedSource {
        let key = source.key();
        let mut cache_error = None;
        let cached = if let Some(cache) = &self.inner.cache {
            match cache.load(&key).await {
                Ok(cached) => cached,
                Err(error) => {
                    cache_error = Some(error.to_string());
                    None
                }
            }
        } else {
            None
        };
        let cache_read_failed = cache_error.is_some();
        if initial
            && let Some(cached) = &cached
            && cached.fresh
        {
            return LoadedSource {
                data: Some(SourceState {
                    nodes: cached.nodes.clone(),
                    validators: cached.validators.clone(),
                }),
                report: SourceReport {
                    source_key: key,
                    outcome: SourceOutcome::FreshCache,
                    nodes: cached.nodes.len(),
                    error: None,
                    cache_error,
                },
                skipped: 0,
                cache_read_failed,
                cache_write_failed: false,
            };
        }
        let had_memory = previous.is_some();
        let previous = previous.or_else(|| {
            cached.map(|cached| SourceState {
                nodes: cached.nodes,
                validators: cached.validators,
            })
        });
        let result = tokio::time::timeout(
            self.inner.subscription_timeout,
            source.fetch_update(
                &self.inner.subscription_client,
                self.inner.max_subscription_bytes,
                previous.as_ref().map(|data| &data.validators),
            ),
        )
        .await;
        let result = match result {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(_) => Err("subscription download timed out".to_owned()),
        };
        let (data, outcome, skipped, error) = match result {
            Ok(SubscriptionUpdate::Modified { report, validators }) => (
                Some(SourceState {
                    nodes: report.nodes,
                    validators,
                }),
                SourceOutcome::Updated,
                report.skipped,
                None,
            ),
            Ok(SubscriptionUpdate::NotModified) if previous.is_some() => {
                (previous, SourceOutcome::NotModified, 0, None)
            }
            other => {
                let error = match other {
                    Err(error) => error,
                    _ => "subscription returned 304 without stored nodes".to_owned(),
                };
                let outcome = if had_memory {
                    SourceOutcome::Retained
                } else if previous.is_some() {
                    SourceOutcome::StaleCache
                } else {
                    SourceOutcome::Failed
                };
                (previous, outcome, 0, Some(error))
            }
        };
        let mut cache_write_failed = false;
        if matches!(outcome, SourceOutcome::Updated | SourceOutcome::NotModified)
            && let (Some(cache), Some(data)) = (&self.inner.cache, &data)
            && let Err(error) = cache
                .save_with_validators(&key, &data.nodes, &data.validators)
                .await
        {
            cache_write_failed = true;
            cache_error = Some(error.to_string());
        }
        let report = SourceReport {
            source_key: key,
            outcome,
            nodes: data.as_ref().map_or(0, |data| data.nodes.len()),
            error,
            cache_error,
        };
        LoadedSource {
            data,
            report,
            skipped,
            cache_read_failed,
            cache_write_failed,
        }
    }

    /// Probe all nodes with bounded concurrency. Probes never consume rotation
    /// quota. Open cooldowns are respected; stale probe outcomes cannot overwrite
    /// a newer business-request result.
    pub async fn check_health(&self) -> PoolStats {
        let Some(check_url) = self.inner.health.check_url.clone() else {
            return self.stats();
        };
        let _guard = self.inner.probe_lock.lock().await;
        let probes: Vec<_> = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let now = Instant::now();
            let probes = state
                .entries
                .iter_mut()
                .filter_map(|entry| {
                    if entry.recovery_token.is_some()
                        || (!entry.pending_probe && !entry.eligible(now))
                    {
                        return None;
                    }
                    if entry.cooldown_until.is_some() || entry.pending_probe {
                        entry.recovery_token = Some(Arc::new(()));
                    }
                    Some((
                        ProbeLease {
                            id: entry.id.clone(),
                            revision: entry.revision,
                            generation: entry.generation.clone(),
                            health_epoch: entry.health_epoch,
                            recovery_token: entry.recovery_token.clone(),
                            owner: Arc::downgrade(&self.inner),
                            completed: false,
                        },
                        entry.client.clone(),
                    ))
                })
                .collect();
            state.candidates = None;
            probes
        };
        let timeout = self.inner.health.timeout;
        stream::iter(probes)
            .map(|(mut lease, client)| {
                let check_url = check_url.clone();
                async move {
                    // Guards exist for queued as well as in-flight probes, so
                    // cancellation also releases slots not yet polled by the stream.
                    let result = client.get(check_url).timeout(timeout).send().await;
                    let healthy = result.is_ok_and(|response| {
                        response.status().is_success() || response.status().is_redirection()
                    });
                    lease.complete(healthy);
                }
            })
            .buffer_unordered(self.inner.health.concurrency.get())
            .collect::<Vec<_>>()
            .await;
        self.stats()
    }

    /// Start maintenance if needed, sharing tasks with existing handles. The last
    /// handle's drop cancels maintenance. Build never implicitly starts tasks.
    pub fn spawn_maintenance(&self) -> MaintenanceTask {
        let mut active = self
            .inner
            .maintenance
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(group) = active.upgrade()
            && group.running.load(Ordering::Acquire)
        {
            return MaintenanceTask { group };
        }
        let mut tasks = Vec::new();
        for (period, refresh) in [
            (self.inner.refresh_interval, true),
            (self.inner.health_interval, false),
        ] {
            if (refresh && self.inner.sources.is_empty())
                || (!refresh && self.inner.health.check_url.is_none())
            {
                continue;
            }
            let pool = self.clone();
            tasks.push(tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if refresh {
                        // The source outcomes are retained in last_refresh_report.
                        // Active checks run on their own cadence below.
                        let _ = pool.refresh_inner(false).await;
                    } else {
                        pool.check_health().await;
                    }
                }
            }));
        }
        let group = Arc::new(MaintenanceGroup {
            tasks: tokio::sync::Mutex::new(tasks),
            running: AtomicBool::new(true),
        });
        *active = Arc::downgrade(&group);
        MaintenanceTask { group }
    }
}

fn is_passive_failure(error: &reqwest::Error) -> bool {
    let mut source = error.source();
    let mut disconnected = false;
    while let Some(cause) = source {
        if let Some(error) = cause.downcast_ref::<hyper::Error>() {
            // A caller-provided request body can fail with a network-shaped IO
            // error, including TimedOut. Exclude it before the timeout shortcut.
            if error.is_user() {
                return false;
            }
            disconnected |= error.is_incomplete_message() || error.is_closed();
        }
        if let Some(error) = cause.downcast_ref::<std::io::Error>() {
            disconnected |= matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::NotConnected
            );
        }
        source = cause.source();
    }
    error.is_connect() || error.is_timeout() || (error.is_request() && disconnected)
}

struct LoadedSource {
    data: Option<SourceState>,
    report: SourceReport,
    skipped: usize,
    cache_read_failed: bool,
    cache_write_failed: bool,
}

/// One request reservation and its reusable per-node HTTP client.
/// Report an outcome once if sending through the client manually. Dropping an
/// unreported lease releases a recovery slot without declaring success or failure.
pub struct ProxyLease {
    node: ProxyNode,
    client: Client,
    id: String,
    owner: Weak<Inner>,
    recovery_token: Option<Arc<()>>,
    generation: Arc<()>,
    health_epoch: u64,
    session_rotation: Option<Weak<Mutex<RotationState>>>,
    completed: bool,
}

impl ProxyLease {
    /// Selected node (credentials are hidden in Debug).
    pub fn node(&self) -> &ProxyNode {
        &self.node
    }
    /// Reused client. Use one request per lease to preserve accurate quota counts.
    pub fn client(&self) -> &Client {
        &self.client
    }
    /// Mark this attempt as successful, resetting consecutive failures.
    pub fn report_success(&mut self) {
        self.report(true);
    }
    /// Mark a transport/proxy failure and force the next request to reselect.
    pub fn report_failure(&mut self) {
        self.report(false);
    }
    fn report(&mut self, success: bool) {
        if !self.completed {
            let applied = self.owner.upgrade().is_some_and(|owner| {
                owner.finish(
                    &self.id,
                    Completion {
                        generation: &self.generation,
                        health_epoch: self.health_epoch,
                        expected_revision: None,
                        probe: false,
                        token: self.recovery_token.as_ref(),
                        outcome: Some(success),
                    },
                )
            });
            if applied
                && !success
                && let Some(rotation) = self.session_rotation.as_ref().and_then(Weak::upgrade)
            {
                rotation
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .invalidate(&self.id);
            }
            self.completed = true;
        }
    }
}

impl Drop for ProxyLease {
    fn drop(&mut self) {
        if !self.completed
            && let Some(owner) = self.owner.upgrade()
        {
            owner.finish(
                &self.id,
                Completion {
                    generation: &self.generation,
                    health_epoch: self.health_epoch,
                    expected_revision: None,
                    probe: false,
                    token: self.recovery_token.as_ref(),
                    outcome: None,
                },
            );
        }
    }
}

struct Completion<'a> {
    generation: &'a Arc<()>,
    health_epoch: u64,
    expected_revision: Option<u64>,
    probe: bool,
    token: Option<&'a Arc<()>>,
    outcome: Option<bool>,
}

impl Inner {
    // Returns whether this observation belongs to the current health epoch.
    // Capacity ownership and health observation ordering are independent.
    fn finish(&self, id: &str, completion: Completion<'_>) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(index) = state.index.get(id).copied() else {
            return false;
        };
        let entry = &mut state.entries[index];
        if !Arc::ptr_eq(&entry.generation, completion.generation) {
            return false;
        }
        let now = Instant::now();
        let before = entry.available(now, self.max_in_flight_per_proxy);
        let previous_deadline = entry.cooldown_until;
        if !completion.probe {
            entry.in_flight = entry.in_flight.saturating_sub(1);
        }
        entry.release_recovery(completion.token);
        let applicable = entry.health_epoch == completion.health_epoch
            && completion
                .expected_revision
                .is_none_or(|revision| entry.revision == revision);
        let mut failed = false;
        if applicable && let Some(success) = completion.outcome {
            entry.revision = entry.revision.wrapping_add(1);
            entry.pending_probe = false;
            if success {
                entry.failures = 0;
                entry.cooldown_round = 0;
                entry.cooldown_until = None;
            } else {
                failed = true;
                entry.failures = entry.failures.saturating_add(1);
                if completion.probe
                    || entry.cooldown_until.is_some()
                    || entry.failures >= self.health.failure_threshold.get()
                {
                    entry.cooldown_round = entry.cooldown_round.saturating_add(1);
                    entry.cooldown_until =
                        now.checked_add(self.health.cooldown_for(entry.cooldown_round));
                    entry.health_epoch = entry.health_epoch.wrapping_add(1);
                }
            }
        }
        if before != entry.available(now, self.max_in_flight_per_proxy)
            || previous_deadline != entry.cooldown_until
        {
            state.candidates = None;
        }
        if failed {
            state.rotation.invalidate(id);
        }
        applicable
    }
}

struct ProbeLease {
    id: String,
    revision: u64,
    generation: Arc<()>,
    health_epoch: u64,
    recovery_token: Option<Arc<()>>,
    owner: Weak<Inner>,
    completed: bool,
}
impl ProbeLease {
    fn complete(&mut self, success: bool) {
        if let Some(owner) = self.owner.upgrade() {
            owner.finish(
                &self.id,
                Completion {
                    generation: &self.generation,
                    health_epoch: self.health_epoch,
                    expected_revision: Some(self.revision),
                    probe: true,
                    token: self.recovery_token.as_ref(),
                    outcome: Some(success),
                },
            );
        }
        self.completed = true;
    }
}
impl Drop for ProbeLease {
    fn drop(&mut self) {
        if !self.completed
            && let Some(owner) = self.owner.upgrade()
        {
            owner.finish(
                &self.id,
                Completion {
                    generation: &self.generation,
                    health_epoch: self.health_epoch,
                    expected_revision: Some(self.revision),
                    probe: true,
                    token: self.recovery_token.as_ref(),
                    outcome: None,
                },
            );
        }
    }
}

/// A shared maintenance handle. Dropping the last handle cancels its tasks;
/// [`Self::shutdown`] cancels and joins tasks for every handle in this group.
#[derive(Clone)]
#[must_use = "keep this handle alive for background maintenance to continue"]
pub struct MaintenanceTask {
    group: Arc<MaintenanceGroup>,
}
struct MaintenanceGroup {
    tasks: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
    running: AtomicBool,
}
impl MaintenanceTask {
    /// Cancel the shared maintenance loops and await their exit.
    pub async fn shutdown(self) {
        let mut tasks = self.group.tasks.lock().await;
        self.group.running.store(false, Ordering::Release);
        for task in tasks.iter() {
            task.abort();
        }
        // Keep each handle in the group until it has exited. If this shutdown
        // future is cancelled, another handle can still finish joining it.
        while let Some(task) = tasks.last_mut() {
            let _ = task.await;
            tasks.pop();
        }
    }
}
impl Drop for MaintenanceGroup {
    fn drop(&mut self) {
        for task in self.tasks.get_mut() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Cleanup(Arc<AtomicBool>);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn cancelled_shutdown_can_be_resumed_and_still_waits_for_cleanup() {
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let observed = cleaned_up.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _cleanup = Cleanup(observed);
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        let handle = MaintenanceTask {
            group: Arc::new(MaintenanceGroup {
                tasks: tokio::sync::Mutex::new(vec![task]),
                running: AtomicBool::new(true),
            }),
        };
        let mut cancelled = Box::pin(handle.clone().shutdown());
        assert!(futures_util::poll!(&mut cancelled).is_pending());
        drop(cancelled);
        // The aborted task has not run its cleanup on this single-thread runtime.
        assert!(!cleaned_up.load(Ordering::Acquire));
        handle.shutdown().await;
        assert!(cleaned_up.load(Ordering::Acquire));
    }
}
