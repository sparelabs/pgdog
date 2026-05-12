use crate::{
    frontend::client::query_engine::test::prelude::assignment_counts,
    net::{parameter::ParameterValue, Parameters, Query},
};

use super::test_client::TestClient;

#[tokio::test]
async fn test_target_session_attrs_standby() {
    let mut params = Parameters::default();
    params.insert("pgdog.role", "replica");

    let mut client = TestClient::new_replicas(params).await;
    assert_eq!(
        client.client().params.get("pgdog.role"),
        Some(&ParameterValue::String("replica".into())),
    );

    // Use a simple read and assert that only the replica assignment count moves.
    // This keeps the test focused on routing rather than read-only DDL errors.
    let before = assignment_counts();

    client.send_simple(Query::new("SELECT 1")).await;
    client.read_until('Z').await.unwrap();

    let after = assignment_counts();
    assert_eq!(after.0 - before.0, 0);
    assert_eq!(after.1 - before.1, 1);
}

#[tokio::test]
async fn test_target_session_attrs_primary() {
    let mut params = Parameters::default();
    params.insert("pgdog.role", "primary");

    let mut client = TestClient::new_replicas(params).await;
    assert_eq!(
        client.client().params.get("pgdog.role"),
        Some(&ParameterValue::String("primary".into())),
    );

    // Same rationale as the standby case: we only care that the connection hint
    // routes to primary, so we assert on pool assignments instead of using DDL
    // as a proxy for successful primary routing.
    let before = assignment_counts();

    client.send_simple(Query::new("SELECT 1")).await;
    client.read_until('Z').await.unwrap();

    let after = assignment_counts();
    assert_eq!(after.0 - before.0, 1);
    assert_eq!(after.1 - before.1, 0);
}
