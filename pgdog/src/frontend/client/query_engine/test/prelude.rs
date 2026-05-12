pub use crate::{
    frontend::{
        client::{
            query_engine::{QueryEngine, QueryEngineContext},
            test::{SpawnedClient, TestClient},
            Client,
        },
        ClientRequest,
    },
    net::{
        bind::Parameter, Bind, Close, Describe, Execute, Flush, Parameters, Parse, Protocol,
        ProtocolMessage, Query, Stream, Sync, Terminate,
    },
};

use crate::{backend::databases::databases, config::Role};

pub(crate) fn assignment_counts() -> (usize, usize) {
    let pools = databases().cluster(("pgdog", "pgdog")).unwrap().shards()[0].pools_with_roles();

    pools.into_iter().fold((0, 0), |mut counts, (role, pool)| {
        let assignments = pool.state().stats.counts.server_assignment_count;
        match role {
            Role::Primary => counts.0 += assignments,
            Role::Replica => counts.1 += assignments,
            Role::Auto => unreachable!("role auto"),
        }
        counts
    })
}

pub(crate) fn assert_assignment_delta(
    before: (usize, usize),
    after: (usize, usize),
    primary: usize,
    replica: usize,
) {
    assert_eq!(after.0 - before.0, primary);
    assert_eq!(after.1 - before.1, replica);
}

pub(crate) async fn send_simple_and_read_ready(test_client: &mut TestClient, sql: &str) {
    test_client.send_simple(Query::new(sql)).await;
    test_client.read_until('Z').await.unwrap();
}
