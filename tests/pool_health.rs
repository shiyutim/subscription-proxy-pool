use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicUsize, Ordering},
    },
    time::Duration,
};

use subscription_proxy_pool::{Error, HealthPolicy, ProxyNode, ProxyPool, RotationPolicy};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};

const WAIT_LIMIT: Duration = Duration::from_secs(3);

struct ProxyState {
    status: AtomicU16,
    requests: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    unexpected_requests: AtomicUsize,
    gate: Option<Arc<Semaphore>>,
}

struct ActiveRequest(Arc<ProxyState>);

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Answers absolute-form HTTP proxy requests locally without forwarding them.
struct LocalProxy {
    address: std::net::SocketAddr,
    state: Arc<ProxyState>,
    task: JoinHandle<()>,
}

impl LocalProxy {
    async fn start(status: u16, blocked: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(ProxyState {
            status: AtomicU16::new(status),
            requests: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            unexpected_requests: AtomicUsize::new(0),
            gate: blocked.then(|| Arc::new(Semaphore::new(0))),
        });
        let server_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let state = Arc::clone(&server_state);
                connections.spawn(async move {
                    let mut request = Vec::new();
                    loop {
                        let mut buffer = [0; 1024];
                        let Ok(Ok(read)) = timeout(WAIT_LIMIT, socket.read(&mut buffer)).await
                        else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        request.extend_from_slice(&buffer[..read]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                        if request.len() > 8192 {
                            return;
                        }
                    }
                    if !request.starts_with(b"GET http://probe.invalid/health HTTP/1.1\r\n") {
                        state.unexpected_requests.fetch_add(1, Ordering::SeqCst);
                    }
                    let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
                    state.peak.fetch_max(active, Ordering::SeqCst);
                    let _active = ActiveRequest(Arc::clone(&state));
                    let status = state.status.load(Ordering::SeqCst);
                    state.requests.fetch_add(1, Ordering::SeqCst);
                    if let Some(gate) = &state.gate {
                        let Ok(permit) = gate.acquire().await else {
                            return;
                        };
                        permit.forget();
                    }
                    let response = format!(
                        "HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            address,
            state,
            task,
        }
    }

    fn node(&self, index: usize) -> ProxyNode {
        // Credentials distinguish endpoints while all requests reach one local listener.
        ProxyNode::from_url(&format!("http://node{index}@{}", self.address)).unwrap()
    }

    fn requests(&self) -> usize {
        self.state.requests.load(Ordering::SeqCst)
    }

    fn release(&self, requests: usize) {
        self.state.gate.as_ref().unwrap().add_permits(requests);
    }

    fn assert_only_local_probes(&self) {
        assert_eq!(self.state.unexpected_requests.load(Ordering::SeqCst), 0);
    }
}

impl Drop for LocalProxy {
    fn drop(&mut self) {
        // Dropping the accept task also drops its JoinSet and aborts open sockets.
        self.task.abort();
    }
}

fn health(check_on_build: bool) -> HealthPolicy {
    HealthPolicy {
        check_url: Some("http://probe.invalid/health".into()),
        timeout: Duration::from_secs(2),
        cooldown: Duration::from_secs(60),
        cooldown_jitter: 0.0,
        check_on_build,
        ..HealthPolicy::default()
    }
}

async fn wait_until(mut ready: impl FnMut() -> bool) {
    timeout(WAIT_LIMIT, async {
        while !ready() {
            sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("local test condition did not become ready");
}

#[tokio::test]
async fn initial_probes_admit_successes_and_quarantine_failures() {
    let healthy = LocalProxy::start(204, false).await;
    let failing = LocalProxy::start(503, false).await;
    let good_node = healthy.node(0);
    let pool = ProxyPool::builder()
        .nodes([good_node.clone(), failing.node(0)])
        .health(health(true))
        .build()
        .await
        .unwrap();

    let stats = pool.stats();
    assert_eq!((stats.total, stats.eligible, stats.unavailable), (2, 1, 1));
    assert_eq!(pool.acquire().unwrap().node().id(), good_node.id());
    assert_eq!(healthy.requests(), 1);
    assert_eq!(failing.requests(), 1);

    // Open cooldowns are respected by repeated active probes.
    pool.check_health().await;
    assert_eq!(healthy.requests(), 2);
    assert_eq!(failing.requests(), 1);
    healthy.assert_only_local_probes();
    failing.assert_only_local_probes();
}

#[tokio::test]
async fn initial_probes_reject_a_pool_without_any_working_proxy() {
    let proxy = LocalProxy::start(407, false).await;
    let result = ProxyPool::builder()
        .nodes([proxy.node(0)])
        .health(health(true))
        .build()
        .await;
    assert!(matches!(result, Err(Error::NoProxyAvailable)));
    assert_eq!(proxy.requests(), 1);
}

#[tokio::test]
async fn expired_cooldown_during_initial_checks_is_not_a_successful_health_check() {
    let first = LocalProxy::start(503, false).await;
    let second = LocalProxy::start(503, true).await;
    let mut policy = health(true);
    policy.concurrency = NonZeroUsize::new(1).unwrap();
    policy.cooldown = Duration::from_millis(10);
    let builder = ProxyPool::builder()
        .nodes([first.node(0), second.node(0)])
        .health(policy);
    let task = tokio::spawn(async move { builder.build().await });

    // Serial probing guarantees the first failure has opened cooldown before
    // the second probe starts. Keep the second pending beyond that cooldown.
    wait_until(|| second.requests() == 1).await;
    sleep(Duration::from_millis(30)).await;
    second.release(1);
    let result = timeout(WAIT_LIMIT, task).await.unwrap().unwrap();
    assert!(matches!(result, Err(Error::NoProxyAvailable)));
    assert_eq!(first.requests(), 1);
    assert_eq!(second.requests(), 1);
    first.assert_only_local_probes();
    second.assert_only_local_probes();
}

#[tokio::test]
async fn probes_obey_the_configured_concurrency_limit() {
    let proxy = LocalProxy::start(204, true).await;
    let mut policy = health(false);
    policy.concurrency = NonZeroUsize::new(2).unwrap();
    let pool = ProxyPool::builder()
        .nodes((0..6).map(|index| proxy.node(index)))
        .health(policy)
        .build()
        .await
        .unwrap();
    let probing_pool = pool.clone();
    let task = tokio::spawn(async move { probing_pool.check_health().await });

    wait_until(|| proxy.requests() == 2).await;
    sleep(Duration::from_millis(20)).await;
    assert_eq!(proxy.requests(), 2);
    assert_eq!(proxy.state.active.load(Ordering::SeqCst), 2);
    proxy.release(2);
    wait_until(|| proxy.requests() == 4).await;
    proxy.release(4);
    let stats = timeout(WAIT_LIMIT, task).await.unwrap().unwrap();
    assert_eq!(stats.eligible, 6);
    assert_eq!(proxy.requests(), 6);
    assert_eq!(proxy.state.peak.load(Ordering::SeqCst), 2);
    proxy.assert_only_local_probes();
}

#[tokio::test]
async fn health_checks_do_not_consume_or_reset_twenty_request_quota() {
    let proxy = LocalProxy::start(204, false).await;
    let first = proxy.node(0);
    let second = proxy.node(1);
    let pool = ProxyPool::builder()
        .nodes([first.clone(), second.clone()])
        .rotation(RotationPolicy::every(20.try_into().unwrap()))
        .health(health(true))
        .build()
        .await
        .unwrap();

    for _ in 0..19 {
        let mut lease = pool.acquire().unwrap();
        assert_eq!(lease.node().id(), first.id());
        lease.report_success();
    }
    for _ in 0..3 {
        assert_eq!(pool.check_health().await.eligible, 2);
    }
    let mut twentieth = pool.acquire().unwrap();
    assert_eq!(twentieth.node().id(), first.id());
    twentieth.report_success();
    assert_eq!(pool.acquire().unwrap().node().id(), second.id());
    assert_eq!(proxy.requests(), 8); // Initial probes plus three manual rounds.
}

#[tokio::test]
async fn probe_timeout_marks_the_node_unavailable() {
    let proxy = LocalProxy::start(204, true).await;
    let mut policy = health(false);
    policy.timeout = Duration::from_millis(40);
    let pool = ProxyPool::builder()
        .nodes([proxy.node(0)])
        .health(policy)
        .build()
        .await
        .unwrap();

    let stats = timeout(WAIT_LIMIT, pool.check_health()).await.unwrap();
    assert_eq!((stats.eligible, stats.unavailable), (0, 1));
    assert!(matches!(pool.acquire(), Err(Error::NoProxyAvailable)));
    assert_eq!(proxy.requests(), 1);
}

#[tokio::test]
async fn consecutive_failures_trigger_cooldown_and_recovery_has_one_slot() {
    let proxy = LocalProxy::start(204, false).await;
    let mut policy = health(false);
    policy.failure_threshold = NonZeroU32::new(2).unwrap();
    policy.cooldown = Duration::from_millis(40);
    let pool = ProxyPool::builder()
        .nodes([proxy.node(0)])
        .health(policy)
        .build()
        .await
        .unwrap();

    pool.acquire().unwrap().report_failure();
    assert_eq!(pool.stats().eligible, 1);
    pool.acquire().unwrap().report_success();
    pool.acquire().unwrap().report_failure();
    assert_eq!(
        pool.stats().eligible,
        1,
        "success resets consecutive failures"
    );
    pool.acquire().unwrap().report_failure();
    assert_eq!(pool.stats().eligible, 0);
    assert!(matches!(pool.acquire(), Err(Error::NoProxyAvailable)));

    wait_until(|| pool.stats().eligible == 1).await;
    let recovery = pool.acquire().unwrap();
    assert_eq!(pool.stats().eligible, 0);
    for _ in 0..8 {
        assert!(matches!(
            pool.clone().acquire(),
            Err(Error::NoProxyAvailable)
        ));
    }
    drop(recovery);
    assert_eq!(
        pool.stats().eligible,
        1,
        "cancellation releases the recovery slot"
    );
    let mut recovery = pool.acquire().unwrap();
    recovery.report_success();
    assert_eq!(pool.stats().eligible, 1);
    let first = pool.acquire().unwrap();
    let second = pool.acquire().unwrap();
    assert_eq!(first.node().id(), second.node().id());
    assert_eq!(
        proxy.requests(),
        0,
        "manual outcomes need no network request"
    );
}

#[tokio::test]
async fn late_failure_cannot_release_another_requests_recovery_slot() {
    let mut policy = health(false);
    policy.failure_threshold = NonZeroU32::new(1).unwrap();
    policy.cooldown = Duration::from_millis(20);
    let pool = ProxyPool::builder()
        .nodes([ProxyNode::from_url("http://127.0.0.1:12001").unwrap()])
        .health(policy)
        .build()
        .await
        .unwrap();

    // Both requests began while the node was healthy. Keep the first in flight
    // while the second fails, then acquire the sole recovery reservation.
    let mut old_request = pool.acquire().unwrap();
    pool.acquire().unwrap().report_failure();
    wait_until(|| pool.stats().eligible == 1).await;
    let recovery = pool.acquire().unwrap();
    assert_eq!(pool.stats().eligible, 0);

    old_request.report_failure();
    sleep(Duration::from_millis(40)).await;
    assert_eq!(
        pool.stats().eligible,
        0,
        "a late failure must not release another lease's recovery reservation"
    );
    assert!(matches!(pool.acquire(), Err(Error::NoProxyAvailable)));

    // Ignoring the old epoch must not prevent the reservation's true owner
    // from releasing its slot when its request is cancelled.
    drop(recovery);
    assert_eq!(pool.stats().eligible, 1);
    let mut recovery = pool.acquire().unwrap();
    recovery.report_success();
    assert_eq!(pool.stats().eligible, 1);
}

#[tokio::test]
async fn cancelling_probes_releases_running_and_queued_recovery_slots() {
    let proxy = LocalProxy::start(204, true).await;
    let mut policy = health(false);
    policy.failure_threshold = NonZeroU32::new(1).unwrap();
    policy.cooldown = Duration::from_millis(40);
    policy.concurrency = NonZeroUsize::new(1).unwrap();
    let pool = ProxyPool::builder()
        .nodes((0..4).map(|index| proxy.node(index)))
        .health(policy)
        .build()
        .await
        .unwrap();
    for _ in 0..4 {
        pool.acquire().unwrap().report_failure();
    }
    assert_eq!(pool.stats().eligible, 0);
    wait_until(|| pool.stats().eligible == 4).await;

    let probing_pool = pool.clone();
    let task = tokio::spawn(async move { probing_pool.check_health().await });
    wait_until(|| proxy.requests() == 1).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        pool.stats().eligible,
        4,
        "every probe reservation must be released"
    );
    let recoveries: Vec<_> = (0..4).map(|_| pool.acquire().unwrap()).collect();
    assert!(matches!(pool.acquire(), Err(Error::NoProxyAvailable)));
    drop(recoveries);
    assert_eq!(pool.stats().eligible, 4);
}

#[tokio::test]
async fn stale_probe_failure_cannot_overwrite_a_newer_request_success() {
    let proxy = LocalProxy::start(503, true).await;
    let pool = ProxyPool::builder()
        .nodes([proxy.node(0)])
        .health(health(false))
        .build()
        .await
        .unwrap();
    let probing_pool = pool.clone();
    let task = tokio::spawn(async move { probing_pool.check_health().await });
    wait_until(|| proxy.requests() == 1).await;
    pool.acquire().unwrap().report_success();
    proxy.release(1);
    let stats = timeout(WAIT_LIMIT, task).await.unwrap().unwrap();
    assert_eq!((stats.eligible, stats.unavailable), (1, 0));
}

#[tokio::test]
async fn maintenance_shutdown_stops_both_background_loops() {
    let proxy = LocalProxy::start(204, false).await;
    let pool = ProxyPool::builder()
        .nodes([proxy.node(0)])
        .health(health(true))
        .health_interval(Duration::from_millis(20))
        .refresh_interval(Duration::from_millis(25))
        .build()
        .await
        .unwrap();
    assert_eq!(proxy.requests(), 1);
    let maintenance = pool.spawn_maintenance();
    wait_until(|| proxy.requests() >= 4).await;
    maintenance.shutdown().await;
    // Drain any request bytes already accepted before the tasks were joined.
    sleep(Duration::from_millis(30)).await;
    let stopped_at = proxy.requests();
    sleep(Duration::from_millis(100)).await;
    assert_eq!(proxy.requests(), stopped_at);
    assert_eq!(pool.stats().eligible, 1);
    proxy.assert_only_local_probes();
}
