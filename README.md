# subscription-proxy-pool

A Rust library for subscription-backed HTTP and SOCKS proxy pools. It fetches
node lists, caches subscriptions, reuses HTTP clients, and manages rotation and
node health. Requires Tokio and Rust 1.89 or newer.

Defaults suit a shared request pool: rotate on every request using round robin,
learn node health from request outcomes, and enable disk caching or active
probes only when configured. Sessions support workflows that need one proxy
across several requests while sharing the pool's connections and health state.

[crates.io](https://crates.io/crates/subscription-proxy-pool) · [API docs](https://docs.rs/subscription-proxy-pool) · [GitHub](https://github.com/shiyutim/subscription-proxy-pool) · [中文说明](README.zh-CN.md) · [Publishing](PUBLISHING.md)

## Quick start

Add the crate and Tokio to your dependencies:

```toml
[dependencies]
subscription-proxy-pool = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

For local development, use `subscription-proxy-pool = { path = "./subscription-proxy-pool" }`.

```rust,no_run
use subscription_proxy_pool::{
    CachePolicy, ProxyPool, SubscriptionSource,
    reqwest::{Method, Request},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = SubscriptionSource::new(&std::env::var("PROXY_SUBSCRIPTION_URL")?)?;
    let pool = ProxyPool::builder()
        .subscription(source)
        .cache(CachePolicy::new("./proxy-cache")) // Optional persistent cache.
        .build()
        .await?;

    let maintenance = pool.spawn_maintenance();
    let request = Request::new(Method::GET, "https://example.com/".parse()?);
    let response = pool.execute(request).await?;
    println!("status: {}", response.status());
    println!("pool: {:?}", pool.stats());
    println!("refresh: {:?}", pool.last_refresh_report());

    // Pool clones share rotation counters, node health, and connection pools.
    let _another_task_pool = pool.clone();
    maintenance.shutdown().await;
    Ok(())
}
```

## Supported subscriptions and transports

- Clash YAML with a top-level `proxies` array (JSON is also valid input).
- Newline-separated HTTP, HTTPS, SOCKS5, or SOCKS5H proxy URLs.
- Either input wrapped once in standard or URL-safe Base64, with optional padding.
- HTTP proxy authentication, TLS to HTTPS proxies, SOCKS5 authentication, and IPv6.
- Duplicate endpoints are removed in input order. Unsupported, malformed, and
  duplicate entries count toward `ParseReport::skipped`; an empty usable result
  is an error.

Clash `type: http` plus `tls: true` produces an HTTPS proxy. SOCKS5 and SOCKS5H
select local and remote target DNS resolution respectively. This crate directly
executes those four transports. SS, SSR, VMess, VLESS, Trojan, Hysteria, and
TLS-wrapped SOCKS nodes are skipped. Clash `proxy-providers` are not recursively
fetched. `skip-cert-verify` does not disable TLS validation.

`parse_subscription()` works without a pool and uses a default 4 MiB input and
decoded-document limit. `parse_subscription_with_options(content, &ParseOptions
{ max_bytes })` accepts another positive limit. Set the pool's corresponding
limit with `PoolBuilder::max_subscription_bytes()`. YAML nesting and alias
expansion remain subject to the parser's structural limits.

## Rotation and independent sessions

| Policy or strategy | Behavior |
| --- | --- |
| `RotationPolicy::default()` | Rotate every acquisition in round-robin order |
| `RotationPolicy::every(N)` | Keep a selection for N acquisitions; N+1 selects the next |
| `RotationPolicy::for_duration(duration)?` | Keep a selection for a positive duration, checked on acquisition |
| `RotationPolicy::sticky()` | Keep a selection until unavailable, removed, or manually rotated |
| Both `requests_per_proxy` and `max_age` set | Rotate when either limit is reached |
| `SelectionStrategy::RoundRobin` | Visit eligible nodes in their list order |
| `SelectionStrategy::Random` | Uniformly select another eligible node |
| `SelectionStrategy::ShuffledRoundRobin` | Visit each eligible node once per shuffled round; request/time limits still apply |
| `pool.rotate()` / `session.rotate()` | Replace that pool or session's selection on its next acquisition |

`pool.clone()` shares one rotation sequence. `pool.session(policy)?` creates a
separate sequence and policy; `session.clone()` shares that session's sequence.
Sessions share node health, connection pools, and per-node capacity. Initial
session selections are distributed in round-robin order across eligible nodes.
A session changes proxy when its current node fails or becomes unavailable,
including reaching a configured capacity limit; stickiness is not a guarantee
of an unchanged public IP.

```rust,no_run
use std::{num::NonZeroU64, time::Duration};
use subscription_proxy_pool::{ProxyPool, RotationPolicy, SelectionStrategy};

fn sessions(pool: &ProxyPool) -> subscription_proxy_pool::Result<()> {
    let login_flow = pool.session(RotationPolicy::sticky())?;
    let _same_flow = login_flow.clone();
    let _batches = pool.session(RotationPolicy::every(NonZeroU64::new(20).unwrap()))?;
    let _timed = pool.session(RotationPolicy::for_duration(Duration::from_secs(300))?)?;
    let _shuffled = pool.session(RotationPolicy {
        strategy: SelectionStrategy::ShuffledRoundRobin,
        ..RotationPolicy::default()
    })?;
    Ok(())
}
```

Selection and counter updates are serialized under short mutexes; network work
runs outside them. Existing requests can finish on an earlier node after
rotation. A replacement avoids the current node when another eligible node
exists, and a single healthy node remains usable. Shuffled rounds skip
unavailable nodes; new and recovered nodes join the next round. Subscription
refreshes preserve clients, health, and rotation state for unchanged endpoints.
Removed nodes can finish requests already in flight.

When eligibility is unchanged, selections reuse a shared node snapshot and
index: retaining a node, round robin, and random selection do not scan the
whole pool. Eligibility changes rebuild that index; shuffled rounds also
shuffle their remaining nodes once per round.

One acquisition means **one reserved request attempt**, even if it later fails
or is cancelled. An unsuccessful acquisition consumes no reservation. Probes,
subscription downloads, redirects, and reqwest's internal transport behavior do
not allocate extra reservations. Use `execute()` for automatic accounting; when
using `acquire()` and `ProxyLease::client()`, send one request per lease.

## Capacity, failures, and health

`max_in_flight_per_proxy(NonZeroUsize)` optionally limits simultaneous business
reservations on each node across the pool and its sessions. Busy nodes are
skipped. If all otherwise eligible nodes are full, `acquire()` returns
`Error::PoolSaturated`; it does not wait in an internal queue. `PoolStats`
reports in-flight reservations and saturated nodes. Cancellation and dropping a
lease release its capacity.

`execute()` holds its reservation until response headers arrive. Body reads
happen afterward, so this limit does not cap concurrent response streams. For
that behavior, acquire a manual lease, send through `lease.client()`, and keep
the lease alive until the body has been consumed. Report the outcome with
`report_success()` or `report_failure()`; dropping an unreported lease releases
its reservation without inventing a health result.

`execute()` sends once. `execute_with_failover(request, max_attempts)` explicitly
enables bounded retries for GET, HEAD, and OPTIONS with cloneable bodies. Each
retry uses a distinct eligible node; only connection errors and timeouts retry.
A pool never silently falls back to a direct connection.

Passive health feedback is the default: transport failures and HTTP 407 force
reselection, and two consecutive failures open a node's circuit. The initial
cooldown is 30 seconds. Failed recovery attempts double it up to
`max_cooldown` (5 minutes by default), with configurable `cooldown_jitter`
(default 20%, capped at the maximum delay). One recovery attempt is admitted
after cooldown; success resets the backoff. Cancellation releases the recovery
slot. Ordinary target 4xx/5xx responses do not by themselves mark a proxy broken.

Active probes require a caller-selected HTTP(S) endpoint:

```rust,no_run
use subscription_proxy_pool::{HealthPolicy, ProxyPool};

let builder = ProxyPool::builder()
    .health(HealthPolicy::active("https://your-service.example/health")?);
# Ok::<(), subscription_proxy_pool::Error>(())
```

`HealthPolicy::active(url)?` enables initial checks and configures later probes.
Each check uses the actual proxy client and accepts 2xx/3xx responses, with a
default concurrency of 16 and timeout of 5 seconds. With initial checks enabled,
a build with no healthy node fails, and new nodes are probed before normal use.
A failed node enters cooldown, then can receive one recovery attempt, including
a business request. Set `check_on_build = false` while retaining
`check_url` to allow untested nodes and run probes manually or through maintenance.
Without `check_url`, `check_health()` performs no network probes.

Automatic passive success is recorded at response headers. A manual lease lets
you report body-read failures or apply your own outcome policy after reading.

## Cache, refresh, and diagnostics

Caching is opt-in. `CachePolicy::new(directory)` keeps successful downloads or
HTTP revalidations fresh for 3 days and permits disk fallback for 7 additional
days. Set `max_stale = Duration::ZERO` to disable expired disk fallback. Failed
fetches never renew these deadlines. Corrupt, incompatible, oversized,
future-dated, and symbolic-link cache files are cache misses.

ETag and Last-Modified validators are retained with their nodes in memory and
on disk. Refreshes use conditional requests; HTTP 304 retains those nodes and
renews cache freshness without downloading and parsing the body again. Original
schema 1 cache files without validators remain readable.

Cache filenames use a SHA-256 hash of the normalized subscription URL. Writes
atomically replace files in the same directory; new Unix directories use
`0700`, files use `0600`, and existing directory permissions are preserved.
Subscription URLs are not saved, but proxy credentials are stored to restore
clients. Keep the directory private. Node and subscription Debug output is
redacted; `ProxyNode::url()` and Serde serialization expose credentials to callers.

Build can restore a fresh cache without fetching. `refresh()` contacts remote
sources regardless of disk TTL, with overlapping refreshes sharing one update.
Sources are fetched with configurable bounded concurrency and combined in
configuration order with duplicate nodes removed. A failed or empty update
retains that source's existing nodes; a stale disk fallback never replaces
newer in-memory data. Disk `max_stale` controls restoration into a new pool,
while a running pool retains nodes through subscription outages and uses health
feedback to decide whether they can carry traffic.

`refresh()` returns `RefreshReport`. `last_refresh_report()` also exposes the
latest startup or background result. Aggregate counters distinguish updates,
304 confirmations, cache restores, failed sources, skipped nodes, and cache
read/write failures. Each `SourceReport` identifies its source by hash and gives
its outcome, node count, and sanitized error details. A failed source can still
supply usable nodes through fallback.

No background tasks start during `build()`. `spawn_maintenance()` starts remote
refreshes (default hourly) and configured active probes (default every minute).
Both intervals are configurable. Repeated calls share the same maintenance
tasks. Keep at least one handle alive; dropping the last handle stops the tasks.
Calling `shutdown().await` on any handle stops the shared tasks and waits for
exit, affecting all handles for that maintenance run.

Subscription downloads are direct by default and never change environment
variables. `subscription_client()` can supply a bootstrap proxy or custom
headers, redirects, and TLS policy. Download time and bytes are bounded.
Business traffic always uses an explicitly selected proxy, regardless of proxy
environment variables.

## Validation

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo test --doc
cargo package --locked
```

Tests use local simulated proxies and subscriptions. See `examples/fetch.rs`
for an executable example. Release steps are in [PUBLISHING.md](PUBLISHING.md).

## License

MIT.
