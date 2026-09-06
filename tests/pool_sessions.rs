use std::{
    collections::HashMap,
    num::{NonZeroU32, NonZeroUsize},
    time::{Duration, Instant},
};

use subscription_proxy_pool::{
    Error, HealthPolicy, PoolBuilder, ProxyLease, ProxyNode, ProxyPool, RotationPolicy,
};

// These tests only reserve leases and report outcomes; no network traffic or
// active health checks are needed to exercise the public scheduling contract.
fn builder(count: usize) -> PoolBuilder {
    ProxyPool::builder().nodes((0..count).map(|index| {
        ProxyNode::from_url(&format!("http://127.0.0.1:{}", 22_001 + index))
            .unwrap()
            .with_name(format!("node-{index}"))
    }))
}

fn one() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}

fn quarantine_policy(cooldown: Duration) -> HealthPolicy {
    HealthPolicy {
        failure_threshold: NonZeroU32::new(1).unwrap(),
        cooldown,
        cooldown_jitter: 0.0,
        ..HealthPolicy::default()
    }
}

async fn wait_for_recovery(pool: &ProxyPool) -> ProxyLease {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match pool.acquire() {
                Ok(lease) => return lease,
                Err(Error::NoProxyAvailable) => {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                _ => panic!("a recovering node should be unavailable or admit one lease"),
            }
        }
    })
    .await
    .expect("an expired recovery delay must make the node acquirable")
}

#[tokio::test]
async fn default_pool_rotates_each_request_and_clones_share_the_sequence() {
    let pool = builder(3).build().await.unwrap();
    let cloned = pool.clone();
    for expected in ["node-0", "node-1", "node-2", "node-0", "node-1", "node-2"] {
        assert_eq!(cloned.acquire().unwrap().node().name(), expected);
    }
    assert_eq!(pool.acquire().unwrap().node().name(), "node-0");
    assert_eq!(cloned.acquire().unwrap().node().name(), "node-1");
    assert_eq!(pool.stats().in_flight, 0);
}

#[tokio::test]
async fn independent_sticky_sessions_distribute_first_acquisitions_evenly() {
    let pool = builder(3).build().await.unwrap();
    let sessions: Vec<_> = (0..12)
        .map(|_| pool.session(RotationPolicy::sticky()).unwrap())
        .collect();
    let mut distribution = HashMap::new();
    for (index, session) in sessions.iter().enumerate() {
        let expected = format!("node-{}", index % 3);
        let actual = session.acquire().unwrap().node().name().to_owned();
        assert_eq!(actual, expected);
        *distribution.entry(actual).or_insert(0) += 1;
        for _ in 0..4 {
            assert_eq!(session.acquire().unwrap().node().name(), expected);
        }
    }
    for name in ["node-0", "node-1", "node-2"] {
        assert_eq!(distribution[name], 4);
    }
}

#[tokio::test]
async fn session_clones_share_their_budget_and_new_sessions_keep_their_own() {
    let pool = builder(3).build().await.unwrap();
    let counted = pool
        .session(RotationPolicy::every(2.try_into().unwrap()))
        .unwrap();
    let cloned = counted.clone();
    let independent = pool.session(RotationPolicy::sticky()).unwrap();

    assert_eq!(counted.acquire().unwrap().node().name(), "node-0");
    assert_eq!(independent.acquire().unwrap().node().name(), "node-1");
    assert_eq!(cloned.acquire().unwrap().node().name(), "node-0");
    assert_eq!(counted.acquire().unwrap().node().name(), "node-1");
    assert_eq!(cloned.acquire().unwrap().node().name(), "node-1");
    assert_eq!(counted.acquire().unwrap().node().name(), "node-2");
    cloned.rotate();
    assert_eq!(counted.acquire().unwrap().node().name(), "node-0");
    assert_eq!(independent.acquire().unwrap().node().name(), "node-1");
}

#[tokio::test]
async fn global_and_session_manual_rotation_do_not_change_each_others_position() {
    let pool = builder(3).build().await.unwrap();
    let session = pool.session(RotationPolicy::sticky()).unwrap();
    assert_eq!(session.acquire().unwrap().node().name(), "node-0");
    assert_eq!(pool.acquire().unwrap().node().name(), "node-0");

    session.rotate();
    assert_eq!(session.acquire().unwrap().node().name(), "node-1");
    assert_eq!(pool.acquire().unwrap().node().name(), "node-1");
    pool.rotate();
    assert_eq!(pool.acquire().unwrap().node().name(), "node-2");
    assert_eq!(session.acquire().unwrap().node().name(), "node-1");

    for _ in 0..5 {
        pool.acquire().unwrap();
    }
    assert_eq!(session.acquire().unwrap().node().name(), "node-1");
}

#[tokio::test]
async fn a_failed_node_is_excluded_from_every_session_and_the_global_pool() {
    let pool = builder(2)
        .health(quarantine_policy(Duration::from_secs(60)))
        .build()
        .await
        .unwrap();
    let first = pool.session(RotationPolicy::sticky()).unwrap();
    let other = pool.session(RotationPolicy::sticky()).unwrap();
    let same_node = pool.session(RotationPolicy::sticky()).unwrap();
    assert_eq!(first.acquire().unwrap().node().name(), "node-0");
    assert_eq!(other.acquire().unwrap().node().name(), "node-1");
    assert_eq!(same_node.acquire().unwrap().node().name(), "node-0");
    first.acquire().unwrap().report_failure();

    assert_eq!(pool.stats().eligible, 1);
    assert_eq!(same_node.acquire().unwrap().node().name(), "node-1");
    assert_eq!(first.acquire().unwrap().node().name(), "node-1");
    assert_eq!(other.acquire().unwrap().node().name(), "node-1");
    assert_eq!(pool.acquire().unwrap().node().name(), "node-1");
}

#[tokio::test]
async fn capacity_skips_a_full_sticky_node_and_reports_when_every_node_is_busy() {
    let pool = builder(2)
        .rotation(RotationPolicy::sticky())
        .max_in_flight_per_proxy(one())
        .build()
        .await
        .unwrap();
    let first = pool.acquire().unwrap();
    assert_eq!(first.node().name(), "node-0");
    let second = pool.acquire().unwrap();
    assert_eq!(second.node().name(), "node-1");
    assert!(matches!(pool.acquire(), Err(Error::PoolSaturated)));
    let stats = pool.stats();
    assert_eq!(
        (stats.in_flight, stats.saturated, stats.eligible),
        (2, 2, 0)
    );

    drop(first);
    let released = pool.acquire().unwrap();
    assert_eq!(released.node().name(), "node-0");
    assert!(matches!(pool.acquire(), Err(Error::PoolSaturated)));
    drop(second);
    drop(released);
    let stats = pool.stats();
    assert_eq!(
        (stats.in_flight, stats.saturated, stats.eligible),
        (0, 0, 2)
    );
}

#[tokio::test]
async fn capacity_is_shared_by_global_leases_sessions_and_pool_clones() {
    let pool = builder(2)
        .max_in_flight_per_proxy(one())
        .build()
        .await
        .unwrap();
    let first = pool.session(RotationPolicy::sticky()).unwrap();
    let second = pool.session(RotationPolicy::sticky()).unwrap();
    let first_lease = first.acquire().unwrap();
    let second_lease = second.acquire().unwrap();
    assert_ne!(first_lease.node().id(), second_lease.node().id());
    assert!(matches!(pool.clone().acquire(), Err(Error::PoolSaturated)));
    assert!(matches!(second.acquire(), Err(Error::PoolSaturated)));

    drop(first_lease);
    let moved = second.clone().acquire().unwrap();
    assert_ne!(moved.node().id(), second_lease.node().id());
    assert_eq!(pool.stats().in_flight, 2);
}

#[tokio::test]
async fn report_and_drop_release_capacity_exactly_once_for_each_outcome() {
    for successful in [true, false] {
        let pool = builder(1)
            .max_in_flight_per_proxy(2.try_into().unwrap())
            .build()
            .await
            .unwrap();
        let mut completed = pool.acquire().unwrap();
        let still_pending = pool.acquire().unwrap();
        assert!(matches!(pool.acquire(), Err(Error::PoolSaturated)));
        if successful {
            completed.report_success();
        } else {
            completed.report_failure();
        }
        assert_eq!(pool.stats().in_flight, 1);
        // A repeated report cannot spend another slot or change the first result.
        completed.report_failure();
        completed.report_success();
        assert_eq!(pool.stats().in_flight, 1);
        let replacement = pool.acquire().unwrap();
        assert_eq!(pool.stats().in_flight, 2);
        drop(completed);
        assert_eq!(pool.stats().in_flight, 2);
        assert!(matches!(pool.acquire(), Err(Error::PoolSaturated)));
        drop(still_pending);
        assert_eq!(pool.stats().in_flight, 1);
        drop(replacement);
        assert_eq!(pool.stats().in_flight, 0);
        assert_eq!(pool.stats().eligible, 1);
    }
}

#[tokio::test]
async fn an_old_success_cannot_clear_a_cooldown_opened_by_a_newer_failure() {
    let pool = builder(1)
        .health(quarantine_policy(Duration::from_secs(60)))
        .build()
        .await
        .unwrap();
    let mut old_request = pool.acquire().unwrap();
    pool.acquire().unwrap().report_failure();
    assert_eq!(pool.stats().eligible, 0);
    old_request.report_success();
    assert_eq!(pool.stats().in_flight, 0);
    assert_eq!(pool.stats().eligible, 0);
    assert!(matches!(pool.acquire(), Err(Error::NoProxyAvailable)));
}

#[tokio::test]
async fn an_old_failure_cannot_requarantine_a_successfully_recovered_node() {
    let pool = builder(1)
        .health(quarantine_policy(Duration::from_millis(20)))
        .build()
        .await
        .unwrap();
    let mut old_request = pool.acquire().unwrap();
    pool.acquire().unwrap().report_failure();
    tokio::time::timeout(Duration::from_secs(3), async {
        while pool.stats().eligible == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the recovery delay should expire");
    pool.acquire().unwrap().report_success();
    assert_eq!(pool.stats().eligible, 1);
    old_request.report_failure();
    assert_eq!(pool.stats().in_flight, 0);
    assert_eq!(pool.stats().eligible, 1);
    assert!(pool.acquire().is_ok());
}

#[tokio::test]
async fn repeated_recovery_failures_refresh_cached_availability_and_back_off() {
    let initial_delay = Duration::from_millis(20);
    let pool = builder(1)
        .health(quarantine_policy(initial_delay))
        .build()
        .await
        .unwrap();
    pool.acquire().unwrap().report_failure();
    let mut recovery = wait_for_recovery(&pool).await;
    for multiplier in [2, 4] {
        // A concurrent caller observes an empty candidate snapshot while the
        // recovery reservation is held and the previous cooldown has expired.
        assert!(matches!(pool.acquire(), Err(Error::NoProxyAvailable)));
        let failed_at = Instant::now();
        recovery.report_failure();
        recovery = wait_for_recovery(&pool).await;
        assert!(
            failed_at.elapsed() >= initial_delay * multiplier,
            "each failed recovery should double its cooldown"
        );
        assert_eq!(pool.stats().in_flight, 1);
        assert_eq!(pool.stats().eligible, 0);
    }
    recovery.report_success();
    assert_eq!(pool.stats().in_flight, 0);
    assert_eq!(pool.stats().eligible, 1);
    assert!(pool.acquire().is_ok());
}
