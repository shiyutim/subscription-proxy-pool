# Changelog

## Unreleased

- Support atomic Windows cache replacement while readers hold the previous file open.
- Report cache paths with non-directory ancestors consistently across platforms.

## 0.1.0

- Parse Clash HTTP/SOCKS nodes, URI lists, and Base64 subscriptions with caller-configurable input limits and skipped-entry reporting.
- Use per-request round-robin rotation by default; provide request-count, duration, sticky, random, and shuffled-round policies.
- Add independent sessions that share node health and HTTP clients, share their counters when cloned, and distribute initial node selections.
- Bound per-node business reservations when configured, expose saturation, and release reservations on cancellation or lease drop.
- Keep passive health as the default; enable active probes only for a caller-selected URL.
- Count disconnects before response headers as passive failures while excluding caller-provided request body errors.
- Add exponential recovery cooldowns with capped jitter, one recovery attempt per node, and protection against obsolete health results.
- Preserve unchanged clients and node state across subscription refreshes; let removed nodes finish in-flight requests.
- Persist HTTP validators with private atomic disk caches; use conditional requests and renew freshness on HTTP 304 while retaining schema 1 cache compatibility.
- Reject cached HTTP validators that cannot be sent as request headers so subscription refreshes can recover.
- Coalesce overlapping refreshes, configure source concurrency, and expose per-source startup, manual, and background refresh diagnostics.
- Share maintenance tasks across handles; stop on the last handle's drop or an explicit shutdown.
- Expose single-attempt execution and explicit failover for replayable GET/HEAD/OPTIONS requests.
- Include local-network tests, executable examples, English/Chinese guides, and manual release validation instructions.
