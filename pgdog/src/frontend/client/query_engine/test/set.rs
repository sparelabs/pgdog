use crate::{
    config::ReadWriteSplit,
    expect_message,
    net::{parameter::ParameterValue, CommandComplete, Query, ReadyForQuery},
};

use super::prelude::*;

#[tokio::test]
async fn test_set() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client
        .send_simple(Query::new("SET application_name TO 'test_set'"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("test_set".into()),
    );

    assert!(!test_client.backend_locked());
}

#[tokio::test]
async fn test_set_search_path() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client
        .send_simple(Query::new(
            "SET search_path TO \"$user\", public, acustomer",
        ))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("search_path").unwrap(),
        &ParameterValue::Tuple(vec!["$user".into(), "public".into(), "acustomer".into()]),
    );
}

#[tokio::test]
async fn test_set_inside_transaction() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client.send_simple(Query::new("BEGIN")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "BEGIN"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    assert!(!test_client.backend_locked());

    test_client
        .send_simple(Query::new("SET search_path TO acustomer, public"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client.send_simple(Query::new("COMMIT")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "COMMIT"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("search_path").unwrap(),
        &ParameterValue::Tuple(vec!["acustomer".into(), "public".into()]),
    );

    assert!(!test_client.backend_locked());
}

#[tokio::test]
async fn test_set_inside_transaction_rollback() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    test_client.send_simple(Query::new("BEGIN")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "BEGIN"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client
        .send_simple(Query::new("SET search_path TO acustomer, public"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client.send_simple(Query::new("ROLLBACK")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "ROLLBACK"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client.client().params.get("search_path").is_none(),
        "search_path should not be set",
    );
}

#[tokio::test]
async fn test_reset() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // First set a parameter
    test_client
        .send_simple(Query::new("SET application_name TO 'test_reset'"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("test_reset".into()),
    );

    // Now reset it
    test_client
        .send_simple(Query::new("RESET application_name"))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client
            .client()
            .params
            .get("application_name")
            .is_none(),
        "application_name should be reset"
    );
}

#[tokio::test]
async fn test_reset_all() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Set multiple parameters
    test_client
        .send_simple(Query::new("SET application_name TO 'test_reset_all'"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client
        .send_simple(Query::new("SET statement_timeout TO 5000"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    assert!(test_client
        .client()
        .params
        .get("application_name")
        .is_some());
    assert!(test_client
        .client()
        .params
        .get("statement_timeout")
        .is_some());

    // Reset all
    test_client.send_simple(Query::new("RESET ALL")).await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client
            .client()
            .params
            .get("application_name")
            .is_none(),
        "application_name should be reset"
    );
    assert!(
        test_client
            .client()
            .params
            .get("statement_timeout")
            .is_none(),
        "statement_timeout should be reset"
    );
}

#[tokio::test]
async fn test_reset_inside_transaction_commit() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Set a parameter outside transaction
    test_client
        .send_simple(Query::new("SET application_name TO 'before_reset'"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("before_reset".into()),
    );

    // Begin transaction
    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Reset inside transaction
    test_client
        .send_simple(Query::new("RESET application_name"))
        .await;
    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Commit
    test_client.send_simple(Query::new("COMMIT")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    // Parameter should be reset after commit
    assert!(
        test_client
            .client()
            .params
            .get("application_name")
            .is_none(),
        "application_name should be reset after commit"
    );
}

#[tokio::test]
async fn test_reset_inside_transaction_rollback() {
    let mut test_client = TestClient::new_sharded(Parameters::default()).await;

    // Set a parameter outside transaction
    test_client
        .send_simple(Query::new("SET application_name TO 'before_reset'"))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("before_reset".into()),
    );

    // Begin transaction
    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Reset inside transaction
    test_client
        .send_simple(Query::new("RESET application_name"))
        .await;
    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    // Rollback
    test_client.send_simple(Query::new("ROLLBACK")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    // Parameter should be restored after rollback
    assert_eq!(
        test_client.client().params.get("application_name").unwrap(),
        &ParameterValue::String("before_reset".into()),
        "application_name should be restored after rollback"
    );
}

#[tokio::test]
async fn test_set_pgdog_role_to_prefer_replica_routes_reads_to_replica_but_keeps_writes_on_primary()
{
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    test_client
        .send_simple(Query::new(r#"SET "pgdog.role" TO 'prefer-replica'"#))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("pgdog.role").unwrap(),
        &ParameterValue::String("prefer-replica".into()),
    );

    let before_read = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after_read = assignment_counts();
    assert_assignment_delta(before_read, after_read, 0, 1);

    let before_write = assignment_counts();
    send_simple_and_read_ready(
        &mut test_client,
        "CREATE TEMP TABLE test_set_pgdog_role_to_replica(id BIGINT)",
    )
    .await;
    let after_write = assignment_counts();
    assert_assignment_delta(before_write, after_write, 1, 0);
}

#[tokio::test]
async fn test_reset_pgdog_role_restores_default_read_routing() {
    let mut params = Parameters::default();
    params.insert("pgdog.role", "prefer-replica");

    let mut test_client =
        TestClient::new_replicas_with_read_write_split(params, ReadWriteSplit::PreferPrimary).await;

    test_client
        .send_simple(Query::new(r#"RESET "pgdog.role""#))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "RESET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client.client().params.get("pgdog.role").is_none(),
        "pgdog.role should be cleared after RESET"
    );

    let before = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 1, 0);
}

#[tokio::test]
async fn test_startup_param_pgdog_role_replica_routes_reads_to_replica_but_keeps_writes_on_primary()
{
    let mut params = Parameters::default();
    params.insert("pgdog.role", "prefer-replica");

    let mut test_client =
        TestClient::new_replicas_with_read_write_split(params, ReadWriteSplit::PreferPrimary).await;

    let before_read = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after_read = assignment_counts();
    assert_assignment_delta(before_read, after_read, 0, 1);

    let before_write = assignment_counts();
    send_simple_and_read_ready(
        &mut test_client,
        "CREATE TEMP TABLE test_startup_replica(id BIGINT)",
    )
    .await;
    let after_write = assignment_counts();
    assert_assignment_delta(before_write, after_write, 1, 0);
}

#[tokio::test]
async fn test_set_pgdog_role_inside_transaction_commit_persists_for_reads_but_not_writes() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client
        .send_simple(Query::new(r#"SET "pgdog.role" TO 'prefer-replica'"#))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client.send_simple(Query::new("COMMIT")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        test_client.client().params.get("pgdog.role").unwrap(),
        &ParameterValue::String("prefer-replica".into()),
        "pgdog.role should persist after COMMIT"
    );

    let before_read = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after_read = assignment_counts();
    assert_assignment_delta(before_read, after_read, 0, 1);

    let before_write = assignment_counts();
    send_simple_and_read_ready(
        &mut test_client,
        "CREATE TEMP TABLE test_set_tx_commit(id BIGINT)",
    )
    .await;
    let after_write = assignment_counts();
    assert_assignment_delta(before_write, after_write, 1, 0);
}

#[tokio::test]
async fn test_set_pgdog_role_inside_transaction_rollback_reverts() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'T'
    );

    test_client
        .send_simple(Query::new(r#"SET "pgdog.role" TO 'prefer-replica'"#))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client.send_simple(Query::new("ROLLBACK")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client.client().params.get("pgdog.role").is_none(),
        "pgdog.role should revert after ROLLBACK"
    );

    let before = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 1, 0);
}

#[tokio::test]
async fn test_set_local_pgdog_role_reverts_after_commit() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client
        .send_simple(Query::new(r#"SET LOCAL "pgdog.role" TO 'prefer-replica'"#))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client.send_simple(Query::new("COMMIT")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client.client().params.get("pgdog.role").is_none(),
        "SET LOCAL pgdog.role should revert after COMMIT"
    );

    let before = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 1, 0);
}

#[tokio::test]
async fn test_set_local_pgdog_role_reverts_after_rollback() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    test_client.send_simple(Query::new("BEGIN")).await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client
        .send_simple(Query::new(r#"SET LOCAL "pgdog.role" TO 'prefer-replica'"#))
        .await;
    expect_message!(test_client.read().await, CommandComplete);
    expect_message!(test_client.read().await, ReadyForQuery);

    test_client.send_simple(Query::new("ROLLBACK")).await;
    expect_message!(test_client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    assert!(
        test_client.client().params.get("pgdog.role").is_none(),
        "SET LOCAL pgdog.role should revert after ROLLBACK"
    );

    let before = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 1, 0);
}

#[tokio::test]
async fn test_set_local_pgdog_role_prefer_replica_overrides_connection_prefer_primary_within_transaction(
) {
    let mut params = Parameters::default();
    params.insert("pgdog.role", "prefer-primary");

    let mut test_client =
        TestClient::new_replicas_with_read_write_split(params, ReadWriteSplit::PreferPrimary).await;

    test_client.send_simple(Query::new("BEGIN")).await;
    test_client.read_until('Z').await.unwrap();

    test_client
        .send_simple(Query::new(r#"SET LOCAL "pgdog.role" TO 'prefer-replica'"#))
        .await;
    test_client.read_until('Z').await.unwrap();

    let before_tx_read = assignment_counts();
    test_client.send_simple(Query::new("SELECT 1")).await;
    test_client.read_until('Z').await.unwrap();
    let after_tx_read = assignment_counts();
    assert_assignment_delta(before_tx_read, after_tx_read, 0, 1);

    test_client.send_simple(Query::new("COMMIT")).await;
    test_client.read_until('Z').await.unwrap();

    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;

    let after_commit = assignment_counts();
    assert_assignment_delta(after_tx_read, after_commit, 1, 0);
}

#[tokio::test]
async fn test_set_local_pgdog_role_primary_reverts_to_connection_replica_after_commit() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    test_client
        .send_simple(Query::new(r#"SET "pgdog.role" TO 'prefer-replica'"#))
        .await;
    test_client.read_until('Z').await.unwrap();

    test_client.send_simple(Query::new("BEGIN")).await;
    test_client.read_until('Z').await.unwrap();

    test_client
        .send_simple(Query::new(r#"SET LOCAL "pgdog.role" TO 'prefer-primary'"#))
        .await;
    test_client.read_until('Z').await.unwrap();

    let before_tx_read = assignment_counts();
    test_client.send_simple(Query::new("SELECT 1")).await;
    test_client.read_until('Z').await.unwrap();
    let after_tx_read = assignment_counts();
    assert_assignment_delta(before_tx_read, after_tx_read, 1, 0);

    test_client.send_simple(Query::new("COMMIT")).await;
    test_client.read_until('Z').await.unwrap();

    assert_eq!(
        test_client.client().params.get("pgdog.role").unwrap(),
        &ParameterValue::String("prefer-replica".into()),
        "SET LOCAL should revert to connection-level value after COMMIT, not config default"
    );

    let before = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 0, 1);
}

#[tokio::test]
async fn test_set_local_pgdog_role_primary_reverts_to_connection_replica_after_rollback() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    test_client
        .send_simple(Query::new(r#"SET "pgdog.role" TO 'prefer-replica'"#))
        .await;
    test_client.read_until('Z').await.unwrap();

    test_client.send_simple(Query::new("BEGIN")).await;
    test_client.read_until('Z').await.unwrap();

    test_client
        .send_simple(Query::new(r#"SET LOCAL "pgdog.role" TO 'prefer-primary'"#))
        .await;
    test_client.read_until('Z').await.unwrap();

    let before_tx_read = assignment_counts();
    test_client.send_simple(Query::new("SELECT 1")).await;
    test_client.read_until('Z').await.unwrap();
    let after_tx_read = assignment_counts();
    assert_assignment_delta(before_tx_read, after_tx_read, 1, 0);

    test_client.send_simple(Query::new("ROLLBACK")).await;
    test_client.read_until('Z').await.unwrap();

    assert_eq!(
        test_client.client().params.get("pgdog.role").unwrap(),
        &ParameterValue::String("prefer-replica".into()),
        "SET LOCAL should revert to connection-level value after ROLLBACK"
    );

    let before = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 0, 1);
}

#[tokio::test]
async fn test_set_local_pgdog_role_must_appear_before_first_statement_in_transaction() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    let before = assignment_counts();

    test_client.send_simple(Query::new("BEGIN")).await;
    test_client.read_until('Z').await.unwrap();

    test_client.send_simple(Query::new("SELECT 1")).await;
    test_client.read_until('Z').await.unwrap();

    test_client
        .send_simple(Query::new(r#"SET LOCAL "pgdog.role" TO 'prefer-replica'"#))
        .await;
    test_client.read_until('Z').await.unwrap();

    test_client.send_simple(Query::new("SELECT 2")).await;
    test_client.read_until('Z').await.unwrap();

    test_client
        .send_simple(Query::new(
            "CREATE TEMP TABLE test_set_local_too_late_in_transaction(id BIGINT)",
        ))
        .await;
    test_client.read_until('Z').await.unwrap();

    test_client.send_simple(Query::new("COMMIT")).await;
    test_client.read_until('Z').await.unwrap();

    let after = assignment_counts();
    assert_eq!(after.0 - before.0, 1);
    assert_eq!(after.1 - before.1, 0);
}

#[tokio::test]
async fn test_set_local_pgdog_role_with_comment_primary_override() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    let before = assignment_counts();

    test_client.send_simple(Query::new("BEGIN")).await;
    test_client.read_until('Z').await.unwrap();

    test_client
        .send_simple(Query::new(r#"SET LOCAL "pgdog.role" TO 'prefer-replica'"#))
        .await;
    test_client.read_until('Z').await.unwrap();

    send_simple_and_read_ready(
        &mut test_client,
        "/* pgdog_role: prefer-primary */ SELECT 1",
    )
    .await;

    test_client.send_simple(Query::new("COMMIT")).await;
    test_client.read_until('Z').await.unwrap();

    let after = assignment_counts();
    assert_assignment_delta(before, after, 1, 0);
}

#[tokio::test]
async fn test_set_pgdog_role_to_replica_with_comment_primary_override() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    test_client
        .send_simple(Query::new(r#"SET "pgdog.role" TO 'prefer-replica'"#))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    let before = assignment_counts();
    send_simple_and_read_ready(
        &mut test_client,
        "/* pgdog_role: prefer-primary */ SELECT 1",
    )
    .await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 1, 0);
}

#[tokio::test]
async fn test_prefer_primary_comment_replica_override_routes_to_replica() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    let before = assignment_counts();
    send_simple_and_read_ready(
        &mut test_client,
        "/* pgdog_role: prefer-replica */ SELECT 1",
    )
    .await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 0, 1);
}

#[tokio::test]
async fn test_set_pgdog_role_to_primary_with_comment_replica_override() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    test_client
        .send_simple(Query::new(r#"SET "pgdog.role" TO 'prefer-primary'"#))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    let before = assignment_counts();
    send_simple_and_read_ready(
        &mut test_client,
        "/* pgdog_role: prefer-replica */ SELECT 1",
    )
    .await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 0, 1);
}

#[tokio::test]
async fn test_set_pgdog_role_replica_routes_non_read_eligible_to_replica() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    test_client
        .send_simple(Query::new(r#"SET "pgdog.role" TO 'replica'"#))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    // SELECT 1 is a read — would go to replica even without force.
    // Verify it still goes to replica with the hard replica hint.
    let before = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 0, 1);
}

#[tokio::test]
async fn test_set_pgdog_role_primary_routes_reads_to_primary() {
    let mut test_client = TestClient::new_replicas_with_read_write_split(
        Parameters::default(),
        ReadWriteSplit::PreferPrimary,
    )
    .await;

    test_client
        .send_simple(Query::new(r#"SET "pgdog.role" TO 'primary'"#))
        .await;

    assert_eq!(
        expect_message!(test_client.read().await, CommandComplete).command(),
        "SET"
    );
    assert_eq!(
        expect_message!(test_client.read().await, ReadyForQuery).status,
        'I'
    );

    let before = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 1, 0);
}

#[tokio::test]
async fn test_startup_param_replica_routes_reads_to_replica() {
    let mut params = Parameters::default();
    params.insert("pgdog.role", "replica");

    let mut test_client =
        TestClient::new_replicas_with_read_write_split(params, ReadWriteSplit::PreferPrimary).await;

    let before = assignment_counts();
    send_simple_and_read_ready(&mut test_client, "SELECT 1").await;
    let after = assignment_counts();
    assert_assignment_delta(before, after, 0, 1);
}
