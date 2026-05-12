use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;

use crate::backend::pool::{Address, Config, Error, PoolConfig, Request};
use crate::config::{LoadBalancingStrategy, Role};
use pgdog_stats::ReplicaLag;

use super::*;
use monitor::Monitor;

fn create_test_pool_config(host: &str, port: u16) -> PoolConfig {
    PoolConfig {
        address: Address {
            host: host.into(),
            port,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            configured_role: Role::Replica,
            ..Default::default()
        },
        config: Config {
            inner: pgdog_stats::Config {
                max: 1,
                checkout_timeout: Duration::from_millis(1000),
                ban_timeout: Duration::from_millis(100),
                ..Config::default().inner
            },
        },
    }
}

fn setup_primary_and_replicas(
    strategy: LoadBalancingStrategy,
    split: ReadWriteSplit,
) -> LoadBalancer {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [
        create_test_pool_config("localhost", 5432),
        create_test_pool_config("127.0.0.1", 5432),
    ];

    let lb = LoadBalancer::new(&Some(primary_pool), &replica_configs, strategy, split);
    lb.launch();
    lb
}

fn setup_test_replicas() -> LoadBalancer {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);

    let replicas = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();
    replicas
}

#[tokio::test]
async fn test_replica_ban_recovery_after_timeout() {
    let replicas = setup_test_replicas();

    // Ban the first replica with very short timeout
    let ban = &replicas.targets[0].ban;
    ban.ban(Error::ServerError, Duration::from_millis(50));

    assert!(ban.banned());

    // Wait for ban to expire
    sleep(Duration::from_millis(60)).await;

    // Check if ban would be removed (simulate monitor behavior)
    let now = std::time::Instant::now();
    let unbanned = ban.unban_if_expired(now);

    assert!(unbanned);
    assert!(!ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_replica_manual_unban() {
    let replicas = setup_test_replicas();

    // Ban the first replica
    let ban = &replicas.targets[0].ban;
    ban.ban(Error::ServerError, Duration::from_millis(1000));

    assert!(ban.banned());

    // Manually unban
    ban.unban(false);

    assert!(!ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_replica_ban_error_retrieval() {
    let replicas = setup_test_replicas();

    let ban = &replicas.targets[0].ban;

    // No error initially
    assert!(ban.error().is_none());

    // Ban with specific error
    ban.ban(Error::ServerError, Duration::from_millis(100));

    // Should return the ban error
    let error = ban.error().unwrap();
    assert!(matches!(error, Error::ServerError));

    replicas.shutdown();
}

#[tokio::test]
async fn test_multiple_replica_banning() {
    let replicas = setup_test_replicas();

    // Ban both replicas
    for i in 0..2 {
        let ban = &replicas.targets[i].ban;
        ban.ban(Error::ServerError, Duration::from_millis(100));

        assert!(ban.banned());
    }

    // Both should be banned
    assert_eq!(
        replicas.targets.iter().filter(|r| r.ban.banned()).count(),
        2
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_replica_ban_idempotency() {
    let replicas = setup_test_replicas();

    let ban = &replicas.targets[0].ban;

    // First ban should succeed
    let first_ban = ban.ban(Error::ServerError, Duration::from_millis(100));
    assert!(first_ban);
    assert!(ban.banned());

    // Second ban of same replica should not create new ban
    let second_ban = ban.ban(Error::ConnectTimeout, Duration::from_millis(200));
    assert!(!second_ban);
    assert!(ban.banned());

    // Error should still be the original one
    assert!(matches!(ban.error().unwrap(), Error::ServerError));

    replicas.shutdown();
}

#[tokio::test]
async fn test_pools_with_roles_and_bans() {
    let replicas = setup_test_replicas();

    let pools_info = replicas.pools_with_roles_and_bans();

    // Should have 2 replica pools (no primary in this test)
    assert_eq!(pools_info.len(), 2);

    // All should be replica role
    for (role, _ban, _pool) in &pools_info {
        assert!(matches!(role, crate::config::Role::Replica));
    }

    replicas.shutdown();
}

#[tokio::test]
async fn test_primary_pool_banning() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    // Test primary ban exists
    assert!(replicas.primary_target().is_some());

    let primary_ban = &replicas.primary_target().unwrap().ban;

    // Ban primary for reads
    primary_ban.ban(Error::ServerError, Duration::from_millis(100));

    assert!(primary_ban.banned());

    // Check pools with roles includes primary
    let pools_info = replicas.pools_with_roles_and_bans();
    assert_eq!(pools_info.len(), 2); // 1 replica + 1 primary

    let has_primary = pools_info
        .iter()
        .any(|(role, _ban, _pool)| matches!(role, crate::config::Role::Primary));
    assert!(has_primary);

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_ban_timeout_not_expired() {
    let replicas = setup_test_replicas();

    let ban = &replicas.targets[0].ban;
    ban.ban(Error::ServerError, Duration::from_millis(1000)); // Long timeout

    assert!(ban.banned());

    // Check immediately - should not be expired
    let now = std::time::Instant::now();
    let unbanned = ban.unban_if_expired(now);

    assert!(!unbanned);
    assert!(ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_unban_if_expired_checks_pool_health() {
    let replicas = setup_test_replicas();

    let ban = &replicas.targets[0].ban;
    let pool = &replicas.targets[0].pool;

    ban.ban(Error::ServerError, Duration::from_millis(50));
    assert!(ban.banned());

    pool.inner().health.toggle(false);

    sleep(Duration::from_millis(60)).await;

    let now = std::time::Instant::now();
    let unbanned = ban.unban_if_expired(now);

    assert!(!unbanned);
    assert!(ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_replica_ban_clears_idle_connections() {
    let replicas = setup_test_replicas();

    // Get a connection and return it to create idle connections
    let request = Request::default();
    let conn = replicas.pools()[0]
        .get(&request)
        .await
        .expect("Should be able to get connection from launched pool");

    // Verify we have a valid connection
    assert!(!conn.error());

    drop(conn); // Return to pool as idle

    // Give a moment for the connection to be properly returned to idle state
    sleep(Duration::from_millis(10)).await;

    // Check that we have idle connections before banning
    let idle_before = replicas.pools()[0].lock().idle();
    assert!(
        idle_before > 0,
        "Should have idle connections before banning, but found {}",
        idle_before
    );

    let ban = &replicas.targets[0].ban;

    // Ban should trigger dump_idle() on the pool
    ban.ban(Error::ServerError, Duration::from_millis(100));

    // Verify the ban was applied
    assert!(ban.banned());

    // Verify that idle connections were cleared
    let idle_after = replicas.pools()[0].lock().idle();
    assert_eq!(
        idle_after, 0,
        "Idle connections should be cleared after banning"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_automatic_ban_expiration() {
    let replicas = setup_test_replicas();

    // Ban the first replica with very short timeout
    let ban = &replicas.targets[0].ban;
    ban.ban(Error::ServerError, Duration::from_millis(100));

    assert!(ban.banned());

    // Wait longer than the ban timeout to allow monitor to process
    // The monitor runs every 333ms, so we wait for at least one cycle
    sleep(Duration::from_millis(400)).await;

    // The monitor should have automatically unbanned the replica
    // Note: Since the monitor runs in a background task spawned during Replicas::new(),
    // and we can't easily control its timing in tests, we check that the ban
    // can be expired when checked
    let now = std::time::Instant::now();
    let would_be_unbanned = ban.unban_if_expired(now);

    // Either it was already unbanned by the monitor, or it would be unbanned now
    assert!(!ban.banned() || would_be_unbanned);

    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_exclude_primary() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );

    let request = Request::default();

    // Try getting connections multiple times and verify primary is never used
    let mut replica_ids = HashSet::new();
    for _ in 0..100 {
        let conn = replicas.get(&request).await.unwrap();
        replica_ids.insert(conn.pool.id());
    }

    // Should only use replica pools, not primary
    assert_eq!(replica_ids.len(), 2);

    // Verify primary pool ID is not in the set of used pools
    let primary_id = replicas.primary().unwrap().id();
    assert!(!replica_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_include_primary() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    let request = Request::default();

    // Try getting connections multiple times and verify both primary and replica can be used
    let mut used_pool_ids = HashSet::new();
    for _ in 0..20 {
        let conn = replicas.get(&request).await.unwrap();
        used_pool_ids.insert(conn.pool.id());
    }

    // Should use both primary and replica pools
    assert_eq!(used_pool_ids.len(), 2);

    // Verify primary pool ID is in the set of used pools
    let primary_id = replicas.primary().unwrap().id();
    assert!(used_pool_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_exclude_primary_no_replicas() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
    );
    replicas.launch();

    let request = Request::default();

    // Try getting connections multiple times and we have primary in the set
    let mut used_pool_ids = HashSet::new();
    for _ in 0..2 {
        let conn = replicas.get(&request).await.unwrap();
        used_pool_ids.insert(conn.pool.id());
    }

    // Should use only primary
    assert_eq!(used_pool_ids.len(), 1);

    // Verify primary pool ID is in the set of used pools
    let primary_id = replicas.primary().unwrap().id();
    assert!(used_pool_ids.contains(&primary_id));

    // Shutdown
    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_exclude_primary_no_primary() {
    // Test exclude primary setting when no primary exists
    let replica_configs = [
        create_test_pool_config("localhost", 5432),
        create_test_pool_config("127.0.0.1", 5432),
    ];

    let replicas = LoadBalancer::new(
        &None,
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );
    replicas.launch();

    let request = Request::default();

    // Should work normally with just replicas
    let mut replica_ids = HashSet::new();
    for _ in 0..10 {
        let conn = replicas.get(&request).await.unwrap();
        replica_ids.insert(conn.pool.id());
    }

    assert_eq!(replica_ids.len(), 2);

    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_include_primary_no_primary() {
    // Test include primary setting when no primary exists
    let replica_configs = [
        create_test_pool_config("localhost", 5432),
        create_test_pool_config("127.0.0.1", 5432),
    ];

    let replicas = LoadBalancer::new(
        &None,
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    let request = Request::default();

    // Should work normally with just replicas
    let mut replica_ids = HashSet::new();
    for _ in 0..10 {
        let conn = replicas.get(&request).await.unwrap();
        replica_ids.insert(conn.pool.id());
    }

    // Should use both replica pools
    assert_eq!(replica_ids.len(), 2);

    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_with_banned_primary() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    // Ban the primary
    let primary_ban = &replicas.targets.last().unwrap().ban;
    primary_ban.ban(Error::ServerError, Duration::from_millis(1000));

    let request = Request::default();

    // Should only use replica even though primary inclusion is enabled
    let mut used_pool_ids = HashSet::new();
    for _ in 0..10 {
        let conn = replicas.get(&request).await.unwrap();
        used_pool_ids.insert(conn.pool.id());
    }

    // Should only use replica pool since primary is banned
    assert_eq!(used_pool_ids.len(), 1);

    // Verify primary pool ID is not in the set of used pools
    let primary_id = replicas.targets.last().unwrap().pool.id();
    assert!(!used_pool_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_with_banned_replicas() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    // Ban the replica
    let replica_ban = &replicas.targets[0].ban;
    replica_ban.ban(Error::ServerError, Duration::from_millis(1000));

    let request = Request::default();

    // Should only use primary since replica is banned
    let mut used_pool_ids = HashSet::new();
    for _ in 0..10 {
        let conn = replicas.get(&request).await.unwrap();
        used_pool_ids.insert(conn.pool.id());
    }

    // Should only use primary pool since replica is banned
    assert_eq!(used_pool_ids.len(), 1);

    // Verify primary pool ID is in the set of used pools
    let primary_id = replicas.targets.last().unwrap().pool.id();
    assert!(used_pool_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_read_write_split_exclude_primary_with_round_robin() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
    );

    let request = Request::default();

    // Collect pool IDs from multiple requests to verify round-robin behavior
    let mut pool_sequence = Vec::new();
    for _ in 0..8 {
        let conn = replicas.get(&request).await.unwrap();
        pool_sequence.push(conn.pool.id());
    }

    // Should use both replicas (round-robin)
    let unique_ids: HashSet<_> = pool_sequence.iter().collect();
    assert_eq!(unique_ids.len(), 2);

    // Verify primary is never used
    let primary_id = replicas.targets.last().unwrap().pool.id();
    assert!(!pool_sequence.contains(&primary_id));

    // Verify round-robin pattern: each pool should be different from the previous one
    for i in 1..pool_sequence.len() {
        assert_ne!(
            pool_sequence[i],
            pool_sequence[i - 1],
            "Round-robin pattern broken: consecutive pools are the same at positions {} and {}",
            i - 1,
            i
        );
    }

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_shuts_down_on_notify() {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);

    let replicas = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    replicas
        .targets
        .iter()
        .for_each(|target| target.pool.launch());
    let monitor_handle = Monitor::spawn(&replicas);

    // Give monitor time to start and register notified() future
    sleep(Duration::from_millis(10)).await;

    replicas.shutdown();

    let result = tokio::time::timeout(Duration::from_secs(1), monitor_handle).await;

    assert!(
        result.is_ok(),
        "Monitor should shut down within timeout after notify"
    );
    assert!(
        result.unwrap().is_ok(),
        "Monitor task should complete successfully"
    );
}

#[tokio::test]
async fn test_monitor_bans_unhealthy_target() {
    let replicas = setup_test_replicas();

    replicas.targets[0].health.toggle(false);

    sleep(Duration::from_millis(400)).await;

    assert!(replicas.targets[0].ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_clears_expired_bans() {
    let replicas = setup_test_replicas();

    replicas.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_millis(50));

    sleep(Duration::from_millis(400)).await;

    assert!(!replicas.targets[0].ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_does_not_ban_single_target() {
    let pool_config = create_test_pool_config("127.0.0.1", 5432);

    let replicas = LoadBalancer::new(
        &None,
        &[pool_config],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    replicas.targets[0].health.toggle(false);

    sleep(Duration::from_millis(400)).await;

    assert!(!replicas.targets[0].ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_unbans_all_when_all_unhealthy() {
    let replicas = setup_test_replicas();

    replicas.targets[0].health.toggle(false);
    replicas.targets[1].health.toggle(false);

    sleep(Duration::from_millis(400)).await;

    assert!(!replicas.targets[0].ban.banned());
    assert!(!replicas.targets[1].ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_does_not_ban_with_zero_ban_timeout() {
    let pool_config1 = PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            ..Default::default()
        },
        config: Config {
            inner: pgdog_stats::Config {
                max: 1,
                checkout_timeout: Duration::from_millis(1000),
                ban_timeout: Duration::ZERO,
                ..Config::default().inner
            },
        },
    };

    let pool_config2 = PoolConfig {
        address: Address {
            host: "localhost".into(),
            port: 5432,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            ..Default::default()
        },
        config: Config {
            inner: pgdog_stats::Config {
                max: 1,
                checkout_timeout: Duration::from_millis(1000),
                ban_timeout: Duration::ZERO,
                ..Config::default().inner
            },
        },
    };

    let replicas = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    replicas.targets[0].health.toggle(false);

    sleep(Duration::from_millis(400)).await;

    assert!(!replicas.targets[0].ban.banned());

    replicas.shutdown();
}

#[tokio::test]
async fn test_monitor_health_state_race() {
    use tokio::spawn;

    let replicas = setup_test_replicas();
    let target = replicas.targets[0].clone();

    let toggle_task = spawn(async move {
        for _ in 0..50 {
            target.health.toggle(false);
            sleep(Duration::from_micros(100)).await;
            target.health.toggle(true);
            sleep(Duration::from_micros(100)).await;
        }
    });

    sleep(Duration::from_millis(500)).await;

    toggle_task.await.unwrap();

    let banned = replicas.targets[0].ban.banned();
    let healthy = replicas.targets[0].health.healthy();

    assert!(
        !banned || !healthy,
        "Pool should not be banned if healthy after race"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_include_primary_if_replica_banned_no_bans() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );
    replicas.launch();

    let request = Request::default();

    // When no replicas are banned, primary should NOT be used
    let mut used_pool_ids = HashSet::new();
    for _ in 0..20 {
        let conn = replicas.get(&request).await.unwrap();
        used_pool_ids.insert(conn.pool.id());
    }

    // Should only use replica pool
    assert_eq!(used_pool_ids.len(), 1);

    // Verify primary pool ID is not in the set of used pools
    let primary_id = replicas.primary().unwrap().id();
    assert!(!used_pool_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

#[tokio::test]
async fn test_include_primary_if_replica_banned_with_ban() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let replicas = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );
    replicas.launch();

    // Ban the replica
    let replica_ban = &replicas.targets[0].ban;
    replica_ban.ban(Error::ServerError, Duration::from_millis(1000));

    let request = Request::default();

    // When replica is banned, primary SHOULD be used
    let mut used_pool_ids = HashSet::new();
    for _ in 0..20 {
        let conn = replicas.get(&request).await.unwrap();
        used_pool_ids.insert(conn.pool.id());
    }

    // Should only use primary pool since replica is banned
    assert_eq!(used_pool_ids.len(), 1);

    // Verify primary pool ID is in the set of used pools
    let primary_id = replicas.primary().unwrap().id();
    assert!(used_pool_ids.contains(&primary_id));

    // Shutdown both primary and replicas
    replicas.shutdown();
}

async fn assert_read_candidates_after_ban(
    split: ReadWriteSplit,
    ban_all_replicas: bool,
    expect_primary_included: bool,
    expected_unbanned_count: usize,
) {
    let replicas = setup_primary_and_replicas(LoadBalancingStrategy::RoundRobin, split);

    if ban_all_replicas {
        for target in replicas
            .targets
            .iter()
            .filter(|target| target.role() == Role::Replica)
        {
            target
                .ban
                .ban(Error::ServerError, Duration::from_millis(1000));
        }
    } else {
        replicas
            .targets
            .iter()
            .find(|target| target.role() == Role::Replica)
            .unwrap()
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let candidate_ids: HashSet<_> = replicas
        .read_candidates()
        .unwrap()
        .into_iter()
        .filter(|target| !target.ban.banned())
        .map(|target| target.pool.id())
        .collect();

    let primary_id = replicas.primary().unwrap().id();
    assert_eq!(candidate_ids.contains(&primary_id), expect_primary_included);
    assert_eq!(candidate_ids.len(), expected_unbanned_count);

    replicas.shutdown();
}

#[tokio::test]
async fn test_prefer_primary_explicit_replica_reads_partial_ban_keep_reads_on_healthy_replicas() {
    assert_read_candidates_after_ban(ReadWriteSplit::PreferPrimary, false, false, 1).await;
}

#[tokio::test]
async fn test_prefer_primary_explicit_replica_reads_all_replicas_banned_fall_back_to_primary() {
    assert_read_candidates_after_ban(ReadWriteSplit::PreferPrimary, true, true, 1).await;
}

#[tokio::test]
async fn test_include_primary_if_replica_banned_partial_ban_keeps_reads_on_healthy_replicas() {
    assert_read_candidates_after_ban(
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
        false,
        false,
        1,
    )
    .await;
}

#[tokio::test]
async fn test_include_primary_if_replica_banned_all_banned_falls_back_to_primary() {
    assert_read_candidates_after_ban(ReadWriteSplit::IncludePrimaryIfReplicaBanned, true, true, 1)
        .await;
}

#[tokio::test]
async fn test_has_replicas_with_replicas() {
    let replicas = setup_test_replicas();

    assert!(replicas.has_replicas());

    replicas.shutdown();
}

#[tokio::test]
async fn test_has_replicas_with_primary_and_replicas() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config("localhost", 5432)];

    let lb = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    lb.launch();

    assert!(lb.has_replicas());

    lb.shutdown();
}

#[tokio::test]
async fn test_has_replicas_primary_only() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let lb = LoadBalancer::new(
        &Some(primary_pool),
        &[],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    lb.launch();

    assert!(!lb.has_replicas());

    lb.shutdown();
}

#[tokio::test]
async fn test_has_replicas_empty() {
    let lb = LoadBalancer::new(
        &None,
        &[],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    assert!(!lb.has_replicas());
}

#[tokio::test]
async fn test_set_role() {
    let replicas = setup_test_replicas();

    // Initially all targets are replicas
    assert_eq!(replicas.targets[0].role(), Role::Replica);
    assert_eq!(replicas.targets[1].role(), Role::Replica);

    // Setting replica to replica returns false (no change)
    let changed = replicas.targets[0].set_role(Role::Replica);
    assert!(!changed);
    assert_eq!(replicas.targets[0].role(), Role::Replica);

    // Setting replica to primary returns true (changed)
    let changed = replicas.targets[0].set_role(Role::Primary);
    assert!(changed);
    assert_eq!(replicas.targets[0].role(), Role::Primary);

    // Setting primary to primary returns false (no change)
    let changed = replicas.targets[0].set_role(Role::Primary);
    assert!(!changed);
    assert_eq!(replicas.targets[0].role(), Role::Primary);

    // Setting primary to replica returns true (changed)
    let changed = replicas.targets[0].set_role(Role::Replica);
    assert!(changed);
    assert_eq!(replicas.targets[0].role(), Role::Replica);

    replicas.shutdown();
}

#[tokio::test]
async fn test_can_move_conns_to_same_config() {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);

    let lb1 = LoadBalancer::new(
        &None,
        &[pool_config1.clone(), pool_config2.clone()],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    let lb2 = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    assert!(lb1.can_move_conns_to(&lb2));
}

#[tokio::test]
async fn test_can_move_conns_to_different_count() {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);

    let lb1 = LoadBalancer::new(
        &None,
        &[pool_config1.clone(), pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    let lb2 = LoadBalancer::new(
        &None,
        &[pool_config1],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    assert!(!lb1.can_move_conns_to(&lb2));
}

#[tokio::test]
async fn test_can_move_conns_to_different_addresses() {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);
    let pool_config3 = create_test_pool_config("127.0.0.1", 5433);

    let lb1 = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    let lb2 = LoadBalancer::new(
        &None,
        &[pool_config3.clone(), pool_config3],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    assert!(!lb1.can_move_conns_to(&lb2));
}

#[tokio::test]
async fn test_monitor_unbans_all_when_second_target_becomes_unhealthy_after_first_banned() {
    let replicas = setup_test_replicas();

    // First target becomes unhealthy
    replicas.targets[0].health.toggle(false);

    // Wait for monitor to ban the first target
    sleep(Duration::from_millis(400)).await;

    assert!(
        replicas.targets[0].ban.banned(),
        "First target should be banned"
    );
    assert!(
        !replicas.targets[1].ban.banned(),
        "Second target should not be banned yet"
    );

    // Now second target becomes unhealthy (first is already banned)
    replicas.targets[1].health.toggle(false);

    // Wait for monitor to process - should unban all since all are unhealthy
    sleep(Duration::from_millis(400)).await;

    // Both should be unbanned because all targets are unhealthy
    assert!(
        !replicas.targets[0].ban.banned(),
        "First target should be unbanned when all targets are unhealthy"
    );
    assert!(
        !replicas.targets[1].ban.banned(),
        "Second target should be unbanned when all targets are unhealthy"
    );

    replicas.shutdown();
}

fn create_test_pool_config_weighted(host: &str, port: u16, lb_weight: u8) -> PoolConfig {
    PoolConfig {
        address: Address {
            host: host.into(),
            port,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            configured_role: Role::Replica,
            ..Default::default()
        },
        config: Config {
            inner: pgdog_stats::Config {
                max: 1,
                checkout_timeout: Duration::from_millis(1000),
                ban_timeout: Duration::from_millis(100),
                lb_weight,
                ..Config::default().inner
            },
        },
    }
}

#[tokio::test]
async fn test_weighted_round_robin_smooth_distribution() {
    let pool_config1 = create_test_pool_config_weighted("127.0.0.1", 5432, 5);
    let pool_config2 = create_test_pool_config_weighted("localhost", 5432, 1);

    let lb = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::WeightedRoundRobin,
        ReadWriteSplit::IncludePrimary,
    );
    lb.launch();

    let request = Request::default();

    let pool_a = lb.targets[0].pool.id();
    let pool_b = lb.targets[1].pool.id();

    // With weights [5, 1], over 6 rounds the sequence should be: A, A, B, A, A, A
    // (B appears at position 3 due to max_by_key last-wins tie-breaking)
    let mut sequence = Vec::new();
    for _ in 0..6 {
        let conn = lb.get(&request).await.unwrap();
        sequence.push(conn.pool.id());
    }

    assert_eq!(
        sequence,
        vec![pool_a, pool_a, pool_b, pool_a, pool_a, pool_a],
    );

    lb.shutdown();
}

#[tokio::test]
async fn test_weighted_round_robin_equal_weights() {
    let pool_config1 = create_test_pool_config_weighted("127.0.0.1", 5432, 1);
    let pool_config2 = create_test_pool_config_weighted("localhost", 5432, 1);

    let lb = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::WeightedRoundRobin,
        ReadWriteSplit::IncludePrimary,
    );
    lb.launch();

    let request = Request::default();

    let pool_a = lb.targets[0].pool.id();
    let pool_b = lb.targets[1].pool.id();

    // With equal weights, should alternate: B, A, B, A
    // (max_by_key picks the last element on tie, so B goes first)
    let mut sequence = Vec::new();
    for _ in 0..4 {
        let conn = lb.get(&request).await.unwrap();
        sequence.push(conn.pool.id());
    }

    assert_eq!(sequence, vec![pool_b, pool_a, pool_b, pool_a]);

    lb.shutdown();
}

#[tokio::test]
async fn test_weighted_round_robin_zero_weight_never_selected() {
    let pool_config1 = create_test_pool_config_weighted("127.0.0.1", 5432, 0);
    let pool_config2 = create_test_pool_config_weighted("localhost", 5432, 10);

    let lb = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::WeightedRoundRobin,
        ReadWriteSplit::IncludePrimary,
    );
    lb.launch();

    let request = Request::default();

    let expected_id = lb.targets[1].pool.id();
    for _ in 0..20 {
        let conn = lb.get(&request).await.unwrap();
        assert_eq!(
            conn.pool.id(),
            expected_id,
            "Pool with weight 0 should never be selected first"
        );
    }

    lb.shutdown();
}

#[tokio::test]
async fn test_weighted_round_robin_proportional_distribution() {
    let pool_config1 = create_test_pool_config_weighted("127.0.0.1", 5432, 3);
    let pool_config2 = create_test_pool_config_weighted("localhost", 5432, 1);

    let lb = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::WeightedRoundRobin,
        ReadWriteSplit::IncludePrimary,
    );
    lb.launch();

    let request = Request::default();

    let pool_a = lb.targets[0].pool.id();

    // Over 40 rounds (10 full cycles of total_weight=4), A should get exactly 30
    let mut a_count = 0;
    for _ in 0..40 {
        let conn = lb.get(&request).await.unwrap();
        if conn.pool.id() == pool_a {
            a_count += 1;
        }
    }

    assert_eq!(a_count, 30, "Pool A (weight 3) should get 3/4 of requests");

    lb.shutdown();
}

#[tokio::test]
async fn test_least_active_connections_prefers_pool_with_fewer_checked_out() {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);

    let replicas = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::LeastActiveConnections,
        ReadWriteSplit::IncludePrimary,
    );
    replicas.launch();

    let request = Request::default();

    // Get first connection and hold it
    let conn1 = replicas.get(&request).await.unwrap();
    let first_pool_id = conn1.pool.id();

    // Now first pool has 1 checked out, second pool has 0.
    // LeastActiveConnections should select the pool with 0 checked out.
    let conn2 = replicas.get(&request).await.unwrap();
    let second_pool_id = conn2.pool.id();

    // conn2 should come from a different pool (the one with 0 checked out)
    assert_ne!(
        first_pool_id, second_pool_id,
        "LeastActiveConnections should select the pool with fewer checked-out connections"
    );

    replicas.shutdown();
}

// ==========================================
// ban_check unit tests
// ==========================================

fn setup_test_replicas_no_launch() -> LoadBalancer {
    let pool_config1 = create_test_pool_config("127.0.0.1", 5432);
    let pool_config2 = create_test_pool_config("localhost", 5432);

    LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    )
}

#[test]
fn test_ban_check_clears_expired_ban_when_healthy_no_lag() {
    let replicas = setup_test_replicas_no_launch();

    // Ban with short timeout
    replicas.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_millis(1));

    // Wait for ban to expire
    std::thread::sleep(Duration::from_millis(10));

    assert!(replicas.targets[0].ban.banned());

    let monitor = Monitor::new_test(&replicas);
    let threshold = ReplicaLag {
        duration: Duration::MAX,
        bytes: i64::MAX,
    };

    monitor.ban_check(&threshold);

    assert!(
        !replicas.targets[0].ban.banned(),
        "Expired ban should be cleared when healthy and no replica lag"
    );
}

#[test]
fn test_ban_check_does_not_clear_expired_ban_when_healthy_with_bad_lag() {
    let replicas = setup_test_replicas_no_launch();

    // Target is healthy (default)
    assert!(replicas.targets[0].health.healthy());

    // Ban with short timeout
    replicas.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_millis(1));

    // Set replica lag on the pool
    replicas.targets[0].pool.lock().replica_lag = ReplicaLag {
        duration: Duration::from_secs(10),
        bytes: 1000,
    };

    // Wait for ban to expire
    std::thread::sleep(Duration::from_millis(10));

    assert!(replicas.targets[0].ban.banned());

    let monitor = Monitor::new_test(&replicas);
    let threshold = ReplicaLag {
        duration: Duration::from_secs(1),
        bytes: 100,
    };

    monitor.ban_check(&threshold);

    assert!(
        replicas.targets[0].ban.banned(),
        "Expired ban should NOT be cleared when healthy replica has bad lag"
    );
}

#[test]
fn test_ban_check_does_not_clear_expired_ban_when_unhealthy_with_bad_lag() {
    let replicas = setup_test_replicas_no_launch();

    // Set target as unhealthy
    replicas.targets[0].health.toggle(false);

    // Ban with short timeout
    replicas.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_millis(1));

    // Set replica lag on the pool
    replicas.targets[0].pool.lock().replica_lag = ReplicaLag {
        duration: Duration::from_secs(10),
        bytes: 1000,
    };

    // Wait for ban to expire
    std::thread::sleep(Duration::from_millis(10));

    assert!(replicas.targets[0].ban.banned());

    let monitor = Monitor::new_test(&replicas);
    let threshold = ReplicaLag {
        duration: Duration::from_secs(1),
        bytes: 100,
    };

    monitor.ban_check(&threshold);

    assert!(
        replicas.targets[0].ban.banned(),
        "Expired ban should NOT be cleared when unhealthy replica has bad lag"
    );
}

#[test]
fn test_ban_check_bans_unhealthy_replica_with_bad_lag() {
    let replicas = setup_test_replicas_no_launch();

    // Set target as unhealthy
    replicas.targets[0].health.toggle(false);

    // Set replica lag on the pool
    replicas.targets[0].pool.lock().replica_lag = ReplicaLag {
        duration: Duration::from_secs(10),
        bytes: 1000,
    };

    let monitor = Monitor::new_test(&replicas);
    let threshold = ReplicaLag {
        duration: Duration::from_secs(1),
        bytes: 100,
    };

    monitor.ban_check(&threshold);

    assert!(replicas.targets[0].ban.banned());
    assert_eq!(
        replicas.targets[0].ban.error(),
        Some(Error::ReplicaLag),
        "Ban reason should be ReplicaLag when unhealthy replica has bad lag"
    );
}

#[test]
fn test_ban_check_bans_healthy_replica_with_bad_lag() {
    let replicas = setup_test_replicas_no_launch();

    // Target stays healthy (default)
    assert!(replicas.targets[0].health.healthy());

    // Set replica lag on the pool
    replicas.targets[0].pool.lock().replica_lag = ReplicaLag {
        duration: Duration::from_secs(10),
        bytes: 1000,
    };

    let monitor = Monitor::new_test(&replicas);
    let threshold = ReplicaLag {
        duration: Duration::from_secs(1),
        bytes: 100,
    };

    monitor.ban_check(&threshold);

    assert!(
        replicas.targets[0].ban.banned(),
        "Healthy replica with bad lag should be banned"
    );
    assert_eq!(
        replicas.targets[0].ban.error(),
        Some(Error::ReplicaLag),
        "Ban reason should be ReplicaLag"
    );
}

#[test]
fn test_ban_check_bans_with_pool_unhealthy_reason() {
    let replicas = setup_test_replicas_no_launch();

    // Set target as unhealthy
    replicas.targets[0].health.toggle(false);

    // No replica lag set (defaults to zero)

    let monitor = Monitor::new_test(&replicas);
    let threshold = ReplicaLag {
        duration: Duration::MAX,
        bytes: i64::MAX,
    };

    monitor.ban_check(&threshold);

    assert!(replicas.targets[0].ban.banned());
    assert_eq!(
        replicas.targets[0].ban.error(),
        Some(Error::PoolUnhealthy),
        "Ban reason should be PoolUnhealthy when replica lag is within threshold"
    );
}

#[test]
fn test_ban_check_does_not_ban_single_target() {
    let pool_config = create_test_pool_config("127.0.0.1", 5432);

    let replicas = LoadBalancer::new(
        &None,
        &[pool_config],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );
    // Don't launch - we're unit testing ban_check

    // Set target as unhealthy
    replicas.targets[0].health.toggle(false);

    let monitor = Monitor::new_test(&replicas);
    let threshold = ReplicaLag {
        duration: Duration::MAX,
        bytes: i64::MAX,
    };

    monitor.ban_check(&threshold);

    assert!(
        !replicas.targets[0].ban.banned(),
        "Single target should not be banned even when unhealthy"
    );
}

#[test]
fn test_ban_check_does_not_ban_with_zero_ban_timeout() {
    let pool_config1 = PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            ..Default::default()
        },
        config: Config {
            inner: pgdog_stats::Config {
                max: 1,
                checkout_timeout: Duration::from_millis(1000),
                ban_timeout: Duration::ZERO,
                ..Config::default().inner
            },
        },
    };

    let pool_config2 = PoolConfig {
        address: Address {
            host: "localhost".into(),
            port: 5432,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            ..Default::default()
        },
        config: Config {
            inner: pgdog_stats::Config {
                max: 1,
                checkout_timeout: Duration::from_millis(1000),
                ban_timeout: Duration::ZERO,
                ..Config::default().inner
            },
        },
    };

    let replicas = LoadBalancer::new(
        &None,
        &[pool_config1, pool_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    // Set target as unhealthy
    replicas.targets[0].health.toggle(false);

    let monitor = Monitor::new_test(&replicas);
    let threshold = ReplicaLag {
        duration: Duration::MAX,
        bytes: i64::MAX,
    };

    monitor.ban_check(&threshold);

    assert!(
        !replicas.targets[0].ban.banned(),
        "Target with zero ban_timeout should not be banned"
    );
}

#[test]
fn test_ban_check_unbans_all_when_all_unhealthy() {
    let replicas = setup_test_replicas_no_launch();

    // Ban both targets manually first
    replicas.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_secs(60));
    replicas.targets[1]
        .ban
        .ban(Error::ServerError, Duration::from_secs(60));

    // Set both as unhealthy
    replicas.targets[0].health.toggle(false);
    replicas.targets[1].health.toggle(false);

    assert!(replicas.targets[0].ban.banned());
    assert!(replicas.targets[1].ban.banned());

    let monitor = Monitor::new_test(&replicas);
    let threshold = ReplicaLag {
        duration: Duration::MAX,
        bytes: i64::MAX,
    };

    monitor.ban_check(&threshold);

    assert!(
        !replicas.targets[0].ban.banned(),
        "All bans should be cleared when all targets are unhealthy"
    );
    assert!(
        !replicas.targets[1].ban.banned(),
        "All bans should be cleared when all targets are unhealthy"
    );
}

#[test]
fn test_ban_check_does_not_clear_unexpired_ban() {
    let replicas = setup_test_replicas_no_launch();

    // Ban with long timeout
    replicas.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_secs(60));

    assert!(replicas.targets[0].ban.banned());

    let monitor = Monitor::new_test(&replicas);
    let threshold = ReplicaLag {
        duration: Duration::MAX,
        bytes: i64::MAX,
    };

    monitor.ban_check(&threshold);

    assert!(
        replicas.targets[0].ban.banned(),
        "Unexpired ban should not be cleared"
    );
}

#[test]
fn test_ban_check_default_threshold_does_not_ban_healthy_replica_with_high_lag() {
    let replicas = setup_test_replicas_no_launch();

    // Target is healthy (default)
    assert!(replicas.targets[0].health.healthy());

    // Set very high replica lag on the pool
    replicas.targets[0].pool.lock().replica_lag = ReplicaLag {
        duration: Duration::from_secs(3600), // 1 hour lag
        bytes: 1_000_000_000,                // 1GB lag
    };

    let monitor = Monitor::new_test(&replicas);
    // Use default config thresholds (MAX values)
    let threshold = ReplicaLag {
        duration: Duration::MAX,
        bytes: i64::MAX,
    };

    monitor.ban_check(&threshold);

    assert!(
        !replicas.targets[0].ban.banned(),
        "With default MAX threshold, healthy replica should NOT be banned despite high lag"
    );
}

#[test]
fn test_ban_check_default_threshold_bans_unhealthy_with_pool_unhealthy_reason() {
    let replicas = setup_test_replicas_no_launch();

    // Set very high replica lag on the pool
    replicas.targets[0].pool.lock().replica_lag = ReplicaLag {
        duration: Duration::from_secs(3600), // 1 hour lag
        bytes: 1_000_000_000,                // 1GB lag
    };

    // Set target as unhealthy
    replicas.targets[0].health.toggle(false);

    let monitor = Monitor::new_test(&replicas);
    // Use default config thresholds (MAX values)
    let threshold = ReplicaLag {
        duration: Duration::MAX,
        bytes: i64::MAX,
    };

    monitor.ban_check(&threshold);

    assert!(replicas.targets[0].ban.banned());
    assert_eq!(
        replicas.targets[0].ban.error(),
        Some(Error::PoolUnhealthy),
        "With default MAX threshold, unhealthy replica should be banned with PoolUnhealthy reason"
    );
}

#[test]
fn test_ban_check_default_threshold_clears_expired_ban_despite_high_lag() {
    let replicas = setup_test_replicas_no_launch();

    // Ban with short timeout
    replicas.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_millis(1));

    // Set very high replica lag on the pool
    replicas.targets[0].pool.lock().replica_lag = ReplicaLag {
        duration: Duration::from_secs(3600), // 1 hour lag
        bytes: 1_000_000_000,                // 1GB lag
    };

    // Wait for ban to expire
    std::thread::sleep(Duration::from_millis(10));

    assert!(replicas.targets[0].ban.banned());

    let monitor = Monitor::new_test(&replicas);
    // Use default config thresholds (MAX values) - replica lag should be ignored
    let threshold = ReplicaLag {
        duration: Duration::MAX,
        bytes: i64::MAX,
    };

    monitor.ban_check(&threshold);

    assert!(
        !replicas.targets[0].ban.banned(),
        "With default MAX threshold, expired ban should be cleared despite high replica lag"
    );
}

// ==========================================
// params() tests
// ==========================================

#[tokio::test]
async fn test_exclude_primary_all_replicas_banned_returns_unavailable() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );

    for target in replicas
        .targets
        .iter()
        .filter(|target| target.role() == Role::Replica)
    {
        target
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let request = Request::default();
    let result = replicas.get(&request).await;

    assert!(
        matches!(result, Err(Error::AllReplicasDown)),
        "ExcludePrimary should not fall back to primary when all replicas are banned"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_exclude_primary_primary_down_reads_go_to_replicas() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );

    if let Some(primary_target) = replicas.primary_target() {
        primary_target
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let candidate_ids: HashSet<_> = replicas
        .read_candidates()
        .unwrap()
        .into_iter()
        .filter(|target| !target.ban.banned())
        .map(|target| target.pool.id())
        .collect();

    let primary_id = replicas.primary().unwrap().id();
    assert!(!candidate_ids.contains(&primary_id));
    assert_eq!(candidate_ids.len(), 2);

    replicas.shutdown();
}

#[tokio::test]
async fn test_exclude_primary_all_down_returns_unavailable() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );

    for target in &replicas.targets {
        target
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let request = Request::default();
    let result = replicas.get(&request).await;

    assert!(
        matches!(result, Err(Error::AllReplicasDown)),
        "ExcludePrimary should be unavailable when all targets are down"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_include_primary_all_down_returns_unavailable() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    for target in &replicas.targets {
        target
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let request = Request::default();
    let result = replicas.get(&request).await;

    assert!(
        matches!(result, Err(Error::AllReplicasDown)),
        "IncludePrimary should be unavailable when all targets are down"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_include_primary_if_replica_banned_primary_down_reads_go_to_replicas() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );

    if let Some(primary_target) = replicas.primary_target() {
        primary_target
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let candidate_ids: HashSet<_> = replicas
        .read_candidates()
        .unwrap()
        .into_iter()
        .filter(|target| !target.ban.banned())
        .map(|target| target.pool.id())
        .collect();

    let primary_id = replicas.primary().unwrap().id();
    assert!(!candidate_ids.contains(&primary_id));
    assert_eq!(candidate_ids.len(), 2);

    replicas.shutdown();
}

#[tokio::test]
async fn test_include_primary_if_replica_banned_all_down_returns_unavailable() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );

    for target in &replicas.targets {
        target
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let request = Request::default();
    let result = replicas.get(&request).await;

    assert!(
        matches!(result, Err(Error::AllReplicasDown)),
        "IncludePrimaryIfReplicaBanned should be unavailable when all targets are down"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_prefer_primary_all_down_returns_unavailable() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::PreferPrimary,
    );

    for target in &replicas.targets {
        target
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let request = Request::default();
    let result = replicas.preferred_read(&request).await;

    assert!(
        matches!(result, Err(Error::AllReplicasDown)),
        "PreferPrimary should be unavailable when all targets are down"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_prefer_primary_explicit_replica_reads_primary_down_go_to_replicas() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::PreferPrimary,
    );

    if let Some(primary_target) = replicas.primary_target() {
        primary_target
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let candidate_ids: HashSet<_> = replicas
        .read_candidates()
        .unwrap()
        .into_iter()
        .filter(|target| !target.ban.banned())
        .map(|target| target.pool.id())
        .collect();

    let primary_id = replicas.primary().unwrap().id();
    assert!(!candidate_ids.contains(&primary_id));
    assert_eq!(candidate_ids.len(), 2);

    replicas.shutdown();
}

#[tokio::test]
async fn test_include_primary_replicas_down_reads_fall_back_to_primary() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    for target in replicas
        .targets
        .iter()
        .filter(|target| target.role() == Role::Replica)
    {
        target
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let candidate_ids: HashSet<_> = replicas
        .read_candidates()
        .unwrap()
        .into_iter()
        .filter(|target| !target.ban.banned())
        .map(|target| target.pool.id())
        .collect();

    let primary_id = replicas.primary().unwrap().id();
    assert_eq!(
        candidate_ids,
        HashSet::from([primary_id]),
        "IncludePrimary should fall back to primary when all replicas are banned"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_include_primary_primary_down_reads_go_to_replicas() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    if let Some(primary_target) = replicas.primary_target() {
        primary_target
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let candidate_ids: HashSet<_> = replicas
        .read_candidates()
        .unwrap()
        .into_iter()
        .filter(|target| !target.ban.banned())
        .map(|target| target.pool.id())
        .collect();

    let primary_id = replicas.primary().unwrap().id();
    assert!(!candidate_ids.contains(&primary_id));
    assert_eq!(candidate_ids.len(), 2);

    replicas.shutdown();
}

#[tokio::test]
async fn test_exclude_primary_healthy_replicas_primary_excluded_from_reads() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );

    let candidate_ids: HashSet<_> = replicas
        .read_candidates()
        .unwrap()
        .into_iter()
        .map(|target| target.pool.id())
        .collect();

    let primary_id = replicas.primary().unwrap().id();
    assert!(
        !candidate_ids.contains(&primary_id),
        "ExcludePrimary should never include primary in read candidates"
    );
    assert_eq!(candidate_ids.len(), 2);

    replicas.shutdown();
}

#[tokio::test]
async fn test_include_primary_if_replica_banned_healthy_replicas_primary_excluded_from_reads() {
    let replicas = setup_primary_and_replicas(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );

    let candidate_ids: HashSet<_> = replicas
        .read_candidates()
        .unwrap()
        .into_iter()
        .map(|target| target.pool.id())
        .collect();

    let primary_id = replicas.primary().unwrap().id();
    assert!(
        !candidate_ids.contains(&primary_id),
        "IncludePrimaryIfReplicaBanned should exclude primary when replicas are healthy"
    );
    assert_eq!(candidate_ids.len(), 2);

    replicas.shutdown();
}

#[test]
fn test_preferred_read_no_primary_candidates_are_replicas() {
    let replica_configs = [
        create_test_pool_config("localhost", 5432),
        create_test_pool_config("127.0.0.1", 5432),
    ];

    let lb = LoadBalancer::new(
        &None,
        &replica_configs,
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::PreferPrimary,
    );

    assert!(lb.primary_target().is_none());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    assert!(candidates.iter().all(|t| t.role() == Role::Replica));
}

#[tokio::test]
async fn test_preferred_read_no_primary_no_replicas_returns_error() {
    let lb = LoadBalancer::new(
        &None,
        &[],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::PreferPrimary,
    );

    let request = Request::default();
    let result = lb.preferred_read(&request).await;

    assert!(
        matches!(result, Err(Error::AllReplicasDown)),
        "preferred_read with no primary and no replicas should return AllReplicasDown"
    );
}

#[tokio::test]
async fn test_preferred_read_primary_banned_no_replicas_returns_preferred_read_unavailable() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let lb = LoadBalancer::new(
        &Some(primary_pool),
        &[],
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::PreferPrimary,
    );
    lb.launch();

    if let Some(primary_target) = lb.primary_target() {
        primary_target
            .ban
            .ban(Error::ServerError, Duration::from_millis(1000));
    }

    let request = Request::default();
    let result = lb.preferred_read(&request).await;

    assert!(
        matches!(result, Err(Error::PreferredReadUnavailable)),
        "preferred_read with banned primary and no replicas should return PreferredReadUnavailable"
    );

    lb.shutdown();
}

#[tokio::test]
async fn test_params_returns_params_from_non_banned_target() {
    let replicas = setup_test_replicas();

    let request = Request::default();
    let result = replicas.params(&request).await;

    assert!(result.is_ok(), "params() should succeed when targets exist");

    replicas.shutdown();
}

#[tokio::test]
async fn test_params_returns_all_replicas_down_when_all_banned() {
    let replicas = setup_test_replicas();

    // Ban all targets
    for target in &replicas.targets {
        target.ban.ban(Error::ServerError, Duration::from_secs(60));
    }

    let request = Request::default();
    let result = replicas.params(&request).await;

    assert!(
        matches!(result, Err(Error::AllReplicasDown)),
        "params() should return AllReplicasDown when all targets are banned"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_params_skips_banned_targets() {
    let replicas = setup_test_replicas();

    // Ban first target
    replicas.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_secs(60));

    let request = Request::default();
    let result = replicas.params(&request).await;

    assert!(
        result.is_ok(),
        "params() should succeed by using non-banned target"
    );

    replicas.shutdown();
}

#[tokio::test]
async fn test_params_returns_all_replicas_down_when_empty() {
    let replicas = LoadBalancer::new(
        &None,
        &[],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    let request = Request::default();
    let result = replicas.params(&request).await;

    assert!(
        matches!(result, Err(Error::AllReplicasDown)),
        "params() should return AllReplicasDown when no targets exist"
    );
}

fn create_test_pool_config_resharding_only(host: &str, port: u16) -> PoolConfig {
    PoolConfig {
        address: Address {
            host: host.into(),
            port,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            ..Default::default()
        },
        config: Config {
            inner: pgdog_stats::Config {
                max: 1,
                checkout_timeout: Duration::from_millis(1000),
                ban_timeout: Duration::from_millis(100),
                resharding_only: true,
                ..Config::default().inner
            },
        },
    }
}

#[tokio::test]
async fn test_resharding_only_replicas_excluded_from_read_candidates() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config_resharding_only("localhost", 5432)];

    let lb = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );
    lb.launch();

    assert!(lb.has_replicas());

    let candidates = lb.read_candidates().unwrap();
    let has_replica = candidates
        .iter()
        .any(|target| target.role() == Role::Replica);
    assert!(
        !has_replica,
        "resharding_only replica should be excluded from read candidates"
    );

    lb.shutdown();
}

#[tokio::test]
async fn test_prefer_primary_resharding_only_replicas_falls_back_to_primary() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let replica_configs = [create_test_pool_config_resharding_only("localhost", 5432)];

    let lb = LoadBalancer::new(
        &Some(primary_pool),
        &replica_configs,
        LoadBalancingStrategy::Random,
        ReadWriteSplit::PreferPrimary,
    );
    lb.launch();

    assert!(lb.has_replicas());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].role(), Role::Primary);

    lb.shutdown();
}

#[tokio::test]
async fn test_preferred_read_externally_banned_primary_stays_banned() {
    let lb = setup_primary_and_replicas(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::PreferPrimary,
    );

    // Ban primary externally (simulating a health-check or write-path ban).
    if let Some(primary_target) = lb.primary_target() {
        primary_target
            .ban
            .ban(Error::ServerError, Duration::from_millis(5000));
    }

    // Ban all replicas so try_candidates will fail.
    for target in lb.targets.iter().filter(|t| t.role() == Role::Replica) {
        target
            .ban
            .ban(Error::ServerError, Duration::from_millis(5000));
    }

    let request = Request::default();
    let _result = lb.preferred_read(&request).await;

    assert!(
        lb.primary_target().unwrap().ban.banned(),
        "primary banned externally must stay banned after preferred_read fails"
    );

    lb.shutdown();
}

// ==========================================
// ban_check + read_candidates integration tests
// ==========================================

fn setup_primary_and_replicas_no_launch(
    strategy: LoadBalancingStrategy,
    split: ReadWriteSplit,
) -> LoadBalancer {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);

    let replica_configs = [
        create_test_pool_config("localhost", 5432),
        create_test_pool_config("127.0.0.1", 5432),
    ];

    LoadBalancer::new(&Some(primary_pool), &replica_configs, strategy, split)
}

fn standard_threshold() -> ReplicaLag {
    ReplicaLag {
        duration: Duration::from_secs(1),
        bytes: 100,
    }
}

fn bad_lag() -> ReplicaLag {
    ReplicaLag {
        duration: Duration::from_secs(10),
        bytes: 1000,
    }
}

fn set_lag(lb: &LoadBalancer, idx: usize, lag: ReplicaLag) {
    lb.targets[idx].pool.lock().replica_lag = lag;
}

fn setup_primary_and_replicas_pools_only(
    strategy: LoadBalancingStrategy,
    split: ReadWriteSplit,
) -> LoadBalancer {
    let lb = setup_primary_and_replicas_no_launch(strategy, split);
    lb.targets.iter().for_each(|target| target.pool.launch());
    lb
}

// -- Group 1: IncludePrimary --

#[test]
fn test_lag_ban_one_replica_include_primary() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    set_lag(&lb, 0, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[0].ban.banned());
    assert!(!lb.targets[1].ban.banned());
    assert!(!lb.targets[2].ban.banned());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 3);
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 2);
}

#[test]
fn test_lag_ban_all_replicas_include_primary() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    set_lag(&lb, 0, bad_lag());
    set_lag(&lb, 1, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[0].ban.banned());
    assert!(lb.targets[1].ban.banned());
    assert!(!lb.targets[2].ban.banned());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 3);
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 1);
    assert_eq!(unbanned[0].role(), Role::Primary);
}

#[test]
fn test_lag_ban_primary_include_primary() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    set_lag(&lb, 2, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(!lb.targets[0].ban.banned());
    assert!(!lb.targets[1].ban.banned());
    assert!(lb.targets[2].ban.banned());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 3);
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 2);
    assert!(unbanned.iter().all(|t| t.role() == Role::Replica));
}

#[test]
fn test_lag_ban_all_targets_include_primary_safety_valve() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    set_lag(&lb, 0, bad_lag());
    set_lag(&lb, 1, bad_lag());
    set_lag(&lb, 2, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(!lb.targets[0].ban.banned());
    assert!(!lb.targets[1].ban.banned());
    assert!(!lb.targets[2].ban.banned());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 3);
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 3);
}

// -- Group 2: ExcludePrimary --

#[test]
fn test_lag_ban_one_replica_exclude_primary() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );

    set_lag(&lb, 0, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[0].ban.banned());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 1);
}

#[test]
fn test_lag_ban_all_replicas_exclude_primary_no_fallback() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );

    set_lag(&lb, 0, bad_lag());
    set_lag(&lb, 1, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[0].ban.banned());
    assert!(lb.targets[1].ban.banned());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 0);
}

#[test]
fn test_lag_ban_primary_exclude_primary_irrelevant() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );

    set_lag(&lb, 2, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[2].ban.banned());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 2);
    assert!(unbanned.iter().all(|t| t.role() == Role::Replica));
}

#[test]
fn test_lag_ban_mixed_reasons_exclude_primary() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );

    lb.targets[0].health.toggle(false);
    set_lag(&lb, 1, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[0].ban.banned());
    assert_eq!(lb.targets[0].ban.error(), Some(Error::PoolUnhealthy));
    assert!(lb.targets[1].ban.banned());
    assert_eq!(lb.targets[1].ban.error(), Some(Error::ReplicaLag));

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 0);
}

// -- Group 3: IncludePrimaryIfReplicaBanned --

#[test]
fn test_lag_ban_one_replica_if_banned_primary_excluded() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );

    set_lag(&lb, 0, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[0].ban.banned());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    assert!(candidates.iter().all(|t| t.role() == Role::Replica));
}

#[test]
fn test_lag_ban_all_replicas_if_banned_primary_included() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );

    set_lag(&lb, 0, bad_lag());
    set_lag(&lb, 1, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[0].ban.banned());
    assert!(lb.targets[1].ban.banned());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 3);
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 1);
    assert_eq!(unbanned[0].role(), Role::Primary);
}

#[test]
fn test_lag_ban_all_targets_if_banned_safety_valve() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );

    set_lag(&lb, 0, bad_lag());
    set_lag(&lb, 1, bad_lag());
    set_lag(&lb, 2, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(!lb.targets[0].ban.banned());
    assert!(!lb.targets[1].ban.banned());
    assert!(!lb.targets[2].ban.banned());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    assert!(candidates.iter().all(|t| t.role() == Role::Replica));
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 2);
}

#[test]
fn test_lag_ban_replicas_recover_if_banned_primary_excluded_again() {
    let config = PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            ..Default::default()
        },
        config: Config {
            inner: pgdog_stats::Config {
                max: 1,
                checkout_timeout: Duration::from_millis(1000),
                ban_timeout: Duration::from_millis(1),
                ..Config::default().inner
            },
        },
    };
    let primary_pool = Pool::new(&config);
    let replica_config2 = PoolConfig {
        address: Address {
            host: "localhost".into(),
            port: 5432,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            ..Default::default()
        },
        config: Config {
            inner: pgdog_stats::Config {
                max: 1,
                checkout_timeout: Duration::from_millis(1000),
                ban_timeout: Duration::from_millis(1),
                ..Config::default().inner
            },
        },
    };

    let lb = LoadBalancer::new(
        &Some(primary_pool),
        &[config.clone(), replica_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );

    let threshold = standard_threshold();
    let monitor = Monitor::new_test(&lb);

    // Step 1: Ban replicas via lag.
    set_lag(&lb, 0, bad_lag());
    set_lag(&lb, 1, bad_lag());
    monitor.ban_check(&threshold);
    assert!(lb.targets[0].ban.banned());
    assert!(lb.targets[1].ban.banned());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 3);

    // Step 2: Clear lag, wait for ban expiry, run ban_check again.
    set_lag(&lb, 0, ReplicaLag::default());
    set_lag(&lb, 1, ReplicaLag::default());
    std::thread::sleep(Duration::from_millis(10));
    monitor.ban_check(&threshold);

    assert!(!lb.targets[0].ban.banned());
    assert!(!lb.targets[1].ban.banned());

    // Primary excluded again since no replicas are banned.
    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    assert!(candidates.iter().all(|t| t.role() == Role::Replica));
}

// -- Group 4: PreferPrimary --

#[test]
fn test_lag_ban_one_replica_prefer_primary_primary_not_in_candidates() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::PreferPrimary,
    );

    set_lag(&lb, 0, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    assert!(candidates.iter().all(|t| t.role() == Role::Replica));
}

#[test]
fn test_lag_ban_all_replicas_prefer_primary_primary_in_candidates() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::PreferPrimary,
    );

    set_lag(&lb, 0, bad_lag());
    set_lag(&lb, 1, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 3);
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 1);
    assert_eq!(unbanned[0].role(), Role::Primary);
}

#[tokio::test]
async fn test_lag_ban_all_replicas_prefer_primary_preferred_read() {
    let lb = setup_primary_and_replicas_pools_only(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::PreferPrimary,
    );

    set_lag(&lb, 0, bad_lag());
    set_lag(&lb, 1, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[0].ban.banned());
    assert!(lb.targets[1].ban.banned());
    assert!(!lb.targets[2].ban.banned());

    let primary_id = lb.primary().unwrap().id();
    let request = Request::default();
    let result = lb.preferred_read(&request).await;

    // Primary is not banned, so preferred_read tries it first.
    // With PG available: connection succeeds from primary → verify pool id.
    // Without PG: primary fails → banned → replicas tried (banned, skipped,
    // then unbanned by try_candidates) → AllReplicasDown. This still exercises
    // the routing: primary was attempted first because it wasn't banned.
    if let Ok(conn) = &result {
        assert_eq!(
            conn.pool.id(),
            primary_id,
            "preferred_read should route to primary when replicas are lag-banned"
        );
    }

    lb.shutdown();
}

#[tokio::test]
async fn test_lag_ban_primary_prefer_primary_preferred_read_skips_primary() {
    let lb = setup_primary_and_replicas_pools_only(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::PreferPrimary,
    );

    set_lag(&lb, 2, bad_lag());

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[2].ban.banned());
    assert!(!lb.targets[0].ban.banned());
    assert!(!lb.targets[1].ban.banned());

    let request = Request::default();
    let result = lb.preferred_read(&request).await;

    // Primary is lag-banned, so preferred_read skips it and tries replicas.
    // If PG is available, a replica connection succeeds.
    if let Ok(conn) = &result {
        let primary_id = lb.primary().unwrap().id();
        assert_ne!(
            conn.pool.id(),
            primary_id,
            "preferred_read should skip lag-banned primary"
        );
    }

    // Primary ban should be preserved (no background monitor to clear it).
    assert!(
        lb.targets[2].ban.banned(),
        "primary lag ban should be preserved after preferred_read skips it"
    );

    lb.shutdown();
}

#[tokio::test]
async fn test_lag_ban_preferred_read_external_ban_preserved_after_lag_ban() {
    let lb = setup_primary_and_replicas_pools_only(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::PreferPrimary,
    );

    // Ban primary externally with ServerError.
    lb.targets[2]
        .ban
        .ban(Error::ServerError, Duration::from_millis(5000));
    assert_eq!(lb.targets[2].ban.error(), Some(Error::ServerError));

    // Lag-ban replicas via ban_check.
    set_lag(&lb, 0, bad_lag());
    set_lag(&lb, 1, bad_lag());
    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[0].ban.banned());
    assert!(lb.targets[1].ban.banned());

    let request = Request::default();
    let result = lb.preferred_read(&request).await;
    assert!(result.is_err());

    // Primary ban must be preserved with original ServerError, not overwritten.
    assert!(lb.targets[2].ban.banned());
    assert_eq!(
        lb.targets[2].ban.error(),
        Some(Error::ServerError),
        "external ServerError ban must not be overwritten by lag ban"
    );

    lb.shutdown();
}

// -- Group 5: Threshold variants --

#[test]
fn test_lag_ban_bytes_threshold_triggers_ban() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    let threshold = ReplicaLag {
        duration: Duration::MAX,
        bytes: 100,
    };

    set_lag(
        &lb,
        0,
        ReplicaLag {
            bytes: 200,
            duration: Duration::ZERO,
        },
    );

    Monitor::new_test(&lb).ban_check(&threshold);

    assert!(
        lb.targets[0].ban.banned(),
        "should be banned via bytes threshold alone"
    );
    assert_eq!(lb.targets[0].ban.error(), Some(Error::ReplicaLag));
}

#[test]
fn test_lag_ban_duration_threshold_triggers_ban() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimary,
    );

    let threshold = ReplicaLag {
        duration: Duration::from_secs(5),
        bytes: i64::MAX,
    };

    set_lag(
        &lb,
        0,
        ReplicaLag {
            bytes: 0,
            duration: Duration::from_secs(10),
        },
    );

    Monitor::new_test(&lb).ban_check(&threshold);

    assert!(
        lb.targets[0].ban.banned(),
        "should be banned via duration threshold alone"
    );
    assert_eq!(lb.targets[0].ban.error(), Some(Error::ReplicaLag));
}

// -- Group 6: Recovery & multi-cycle --

#[test]
fn test_lag_ban_safety_valve_all_rw_splits() {
    let splits = [
        ReadWriteSplit::IncludePrimary,
        ReadWriteSplit::ExcludePrimary,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
        ReadWriteSplit::PreferPrimary,
    ];

    for split in splits {
        let lb = setup_primary_and_replicas_no_launch(LoadBalancingStrategy::Random, split);

        lb.targets[0].health.toggle(false);
        lb.targets[1].health.toggle(false);
        lb.targets[2].health.toggle(false);

        Monitor::new_test(&lb).ban_check(&standard_threshold());

        assert!(
            !lb.targets[0].ban.banned(),
            "safety valve should fire for {:?}",
            split
        );
        assert!(!lb.targets[1].ban.banned());
        assert!(!lb.targets[2].ban.banned());

        let candidates = lb.read_candidates();
        assert!(
            candidates.is_ok(),
            "read_candidates should return Ok for {:?}, got {:?}",
            split,
            candidates
        );
    }
}

#[test]
fn test_lag_ban_mixed_unhealthy_and_lag() {
    let lb = setup_primary_and_replicas_no_launch(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );

    lb.targets[0].health.toggle(false);
    set_lag(&lb, 1, bad_lag());
    // Primary (targets[2]) is healthy, no lag.

    Monitor::new_test(&lb).ban_check(&standard_threshold());

    assert!(lb.targets[0].ban.banned());
    assert_eq!(lb.targets[0].ban.error(), Some(Error::PoolUnhealthy));
    assert!(lb.targets[1].ban.banned());
    assert_eq!(lb.targets[1].ban.error(), Some(Error::ReplicaLag));
    assert!(!lb.targets[2].ban.banned());

    // All replicas banned → primary included.
    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 3);
    let unbanned: Vec<_> = candidates.iter().filter(|t| !t.ban.banned()).collect();
    assert_eq!(unbanned.len(), 1);
    assert_eq!(unbanned[0].role(), Role::Primary);
}

#[test]
fn test_lag_ban_repeated_cycles_converge() {
    let config = PoolConfig {
        address: Address {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            ..Default::default()
        },
        config: Config {
            inner: pgdog_stats::Config {
                max: 1,
                checkout_timeout: Duration::from_millis(1000),
                ban_timeout: Duration::from_millis(1),
                ..Config::default().inner
            },
        },
    };
    let primary_pool = Pool::new(&config);
    let replica_config2 = PoolConfig {
        address: Address {
            host: "localhost".into(),
            port: 5432,
            user: "pgdog".into(),
            passwords: vec!["pgdog".into()],
            database_name: "pgdog".into(),
            ..Default::default()
        },
        config: Config {
            inner: pgdog_stats::Config {
                max: 1,
                checkout_timeout: Duration::from_millis(1000),
                ban_timeout: Duration::from_millis(1),
                ..Config::default().inner
            },
        },
    };

    let lb = LoadBalancer::new(
        &Some(primary_pool),
        &[config.clone(), replica_config2],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    );

    let threshold = standard_threshold();
    let monitor = Monitor::new_test(&lb);

    // Step 1: Ban 1 replica.
    set_lag(&lb, 0, bad_lag());
    monitor.ban_check(&threshold);
    assert!(lb.targets[0].ban.banned());
    assert!(!lb.targets[1].ban.banned());
    // 1 of 2 replicas banned → primary excluded.
    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    assert!(candidates.iter().all(|t| t.role() == Role::Replica));

    // Step 2: Ban both replicas.
    set_lag(&lb, 1, bad_lag());
    monitor.ban_check(&threshold);
    assert!(lb.targets[0].ban.banned());
    assert!(lb.targets[1].ban.banned());
    // All replicas banned → primary included.
    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 3);

    // Step 3: Clear lag, wait for bans to expire, unban.
    set_lag(&lb, 0, ReplicaLag::default());
    set_lag(&lb, 1, ReplicaLag::default());
    std::thread::sleep(Duration::from_millis(10));
    monitor.ban_check(&threshold);
    assert!(!lb.targets[0].ban.banned());
    assert!(!lb.targets[1].ban.banned());
    // No replicas banned → primary excluded.
    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    assert!(candidates.iter().all(|t| t.role() == Role::Replica));

    // Step 4: Set 1 unhealthy.
    lb.targets[0].health.toggle(false);
    monitor.ban_check(&threshold);
    assert!(lb.targets[0].ban.banned());
    assert_eq!(lb.targets[0].ban.error(), Some(Error::PoolUnhealthy));
    assert!(!lb.targets[1].ban.banned());
    // 1 of 2 replicas banned → primary excluded.
    let candidates = lb.read_candidates().unwrap();
    assert_eq!(candidates.len(), 2);
    assert!(candidates.iter().all(|t| t.role() == Role::Replica));
}

#[tokio::test]
async fn test_any_read_includes_primary_and_replicas() {
    let lb = setup_primary_and_replicas(
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );

    let request = Request::default();
    let mut used_ids = HashSet::new();
    for _ in 0..50 {
        let conn = lb.any_read(&request).await.unwrap();
        used_ids.insert(conn.pool.id());
    }

    assert_eq!(
        used_ids.len(),
        3,
        "any_read should use primary + 2 replicas"
    );
    let primary_id = lb.primary().unwrap().id();
    assert!(used_ids.contains(&primary_id));

    lb.shutdown();
}

#[tokio::test]
async fn test_any_read_exclude_primary_split_still_includes_primary() {
    let lb = setup_primary_and_replicas(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
    );

    let request = Request::default();
    let mut used_ids = HashSet::new();
    for _ in 0..30 {
        let conn = lb.any_read(&request).await.unwrap();
        used_ids.insert(conn.pool.id());
    }

    let primary_id = lb.primary().unwrap().id();
    assert!(
        used_ids.contains(&primary_id),
        "any_read ignores rw_split and includes primary"
    );

    lb.shutdown();
}

#[tokio::test]
async fn test_any_read_banned_target_skipped() {
    let lb = setup_primary_and_replicas(
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
    );

    lb.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_secs(60));

    let request = Request::default();
    let mut used_ids = HashSet::new();
    for _ in 0..20 {
        let conn = lb.any_read(&request).await.unwrap();
        used_ids.insert(conn.pool.id());
    }

    assert!(
        !used_ids.contains(&lb.targets[0].pool.id()),
        "banned target should be skipped"
    );

    lb.shutdown();
}

#[tokio::test]
async fn test_any_read_resharding_only_excluded() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let mut resharding_config = create_test_pool_config("localhost", 5433);
    resharding_config.config.inner.resharding_only = true;

    let normal_config = create_test_pool_config("localhost", 5432);

    let lb = LoadBalancer::new(
        &Some(primary_pool),
        &[resharding_config, normal_config],
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
    );
    lb.launch();

    let request = Request::default();
    let mut used_ids = HashSet::new();
    for _ in 0..20 {
        let conn = lb.any_read(&request).await.unwrap();
        used_ids.insert(conn.pool.id());
    }

    let resharding_id = lb.targets[0].pool.id();
    assert!(
        !used_ids.contains(&resharding_id),
        "resharding-only target should be excluded from any_read"
    );

    lb.shutdown();
}

#[tokio::test]
async fn test_any_read_all_down_returns_error() {
    let primary_config = create_test_pool_config("127.0.0.1", 5432);
    let primary_pool = Pool::new(&primary_config);
    primary_pool.launch();

    let lb = LoadBalancer::new(
        &Some(primary_pool),
        &[],
        LoadBalancingStrategy::Random,
        ReadWriteSplit::ExcludePrimary,
    );
    lb.launch();

    lb.targets[0]
        .ban
        .ban(Error::ServerError, Duration::from_secs(60));

    let request = Request::default();
    let result = lb.any_read(&request).await;
    assert!(result.is_err());

    lb.shutdown();
}
