use std::time::{Duration, Instant};

use pgdog_config::{PoolerMode, QueryParserLevel, QuerySizeLimitAction};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

use bytes::{BufMut, BytesMut};

use crate::{
    backend::{
        databases::{
            PoolReplacement, databases, from_config, reload_from_existing, replace_databases,
        },
        maintenance_mode,
        pool::Request,
        server::test::test_server,
    },
    config::{
        PreparedStatementsLevel, config, load_test, load_test_replicas, load_test_sharded,
        load_test_with_pooler_mode, set,
    },
    frontend::{
        Client,
        client::{BufferEvent, QueryEngine},
        prepared_statements,
    },
    net::{
        Bind, Close, CommandComplete, DataRow, Describe, ErrorResponse, Execute, Field, Flush,
        Format, FromBytes, Message, Parameters, Parse, ProtocolVersion, Query, ReadyForQuery,
        RowDescription, Sync, Terminate, ToBytes, replication::StatusUpdate,
    },
    state::State,
};

use super::Stream;

pub(crate) mod auth;
pub(crate) mod target_session_attrs;
pub(crate) mod test_client;
pub(crate) use test_client::{SpawnedClient, TestClient, read_until};

//
// cargo nextest runs these in separate processes.
// That's important otherwise I'm not sure what would happen.
//

pub(crate) async fn test_client(replicas: bool) -> (TcpStream, Client) {
    if replicas {
        load_test_replicas();
    } else {
        load_test();
    }

    parallel_test_client().await
}

/// Load test client with 4 databases (2 shards, 1 replica each).
pub(crate) async fn test_client_sharded() -> (TcpStream, Client) {
    load_test_sharded();

    parallel_test_client().await
}

pub(crate) async fn parallel_test_client() -> (TcpStream, Client) {
    parallel_test_client_with_params(Parameters::default()).await
}

pub(crate) async fn parallel_test_client_with_params(params: Parameters) -> (TcpStream, Client) {
    let addr = "127.0.0.1:0".to_string();
    let conn_addr = addr.clone();
    let stream = TcpListener::bind(&conn_addr).await.unwrap();
    let port = stream.local_addr().unwrap().port();
    let connect_handle = tokio::spawn(async move {
        let (stream, _) = stream.accept().await.unwrap();
        let stream = Stream::plain(stream, 4096);

        Client::new_test(stream, params)
    });

    let conn = TcpStream::connect(&format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let client = connect_handle.await.unwrap();

    (conn, client)
}

macro_rules! new_client {
    ($replicas:expr_2021) => {{
        crate::logger();
        let (conn, client) = test_client($replicas).await;
        let engine = QueryEngine::from_client(&client).unwrap();

        (conn, client, engine)
    }};
}

/// Read a series of messages from the stream and make sure
/// they arrive in the right order.
pub(crate) async fn read_messages(
    stream: &mut (impl AsyncRead + Unpin),
    codes: &[char],
) -> Vec<Message> {
    let mut result = vec![];
    let mut error = false;

    for code in codes {
        let c = stream.read_u8().await.unwrap();

        if c as char != *code {
            if c as char == 'E' {
                error = true;
            } else {
                panic!("got message code {}, expected {}", c as char, *code);
            }
        }

        let len = stream.read_i32().await.unwrap();
        let mut data = vec![0u8; len as usize - 4];
        stream.read_exact(&mut data).await.unwrap();

        let mut message = BytesMut::new();
        message.put_u8(c);
        message.put_i32(len);
        message.put_slice(&data);

        let message = Message::new(message.freeze());

        if error {
            panic!(
                "{:#}",
                ErrorResponse::from_bytes(message.to_bytes()).unwrap()
            );
        } else {
            result.push(message);
        }
    }

    result
}

macro_rules! buffer {
    ( $( $msg:block ),* ) => {{
        let mut buf = BytesMut::new();

        $(
           buf.put($msg.to_bytes());
        )*

        buf
    }}
}

macro_rules! read_one {
    ($conn:expr_2021) => {{
        let mut buf = BytesMut::new();
        let code = $conn.read_u8().await.unwrap();
        buf.put_u8(code);
        let len = $conn.read_i32().await.unwrap();
        buf.put_i32(len);
        buf.resize(len as usize + 1, 0);
        $conn.read_exact(&mut buf[5..]).await.unwrap();

        buf
    }};
}

macro_rules! read {
    ($conn:expr_2021, $codes:expr_2021) => {{
        let mut result = vec![];
        for c in $codes {
            let buf = read_one!($conn);
            assert_eq!(buf[0] as char, c);
            result.push(buf);
        }

        result
    }};
}

#[tokio::test]
async fn test_multiple_async() {
    let (mut conn, mut client, _) = new_client!(false);

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    let mut buf = vec![];
    for i in 0..50 {
        let q = Query::new(format!("SELECT {}::bigint AS one", i));
        buf.extend(&q.to_bytes())
    }

    conn.write_all(&buf).await.unwrap();

    for i in 0..50 {
        let mut codes = vec![];
        for c in ['T', 'D', 'C', 'Z'] {
            // Buffer.
            let mut b = BytesMut::new();
            // Code
            let code = conn.read_u8().await.unwrap();
            assert_eq!(c, code as char);
            b.put_u8(code);
            // Length
            let len = conn.read_i32().await.unwrap();
            b.put_i32(len);
            b.resize(len as usize + 1, 0);
            // The rest.
            conn.read_exact(&mut b[5..]).await.unwrap();
            match c {
                'T' => {
                    let rd = RowDescription::from_bytes(b.freeze()).unwrap();
                    assert_eq!(rd.field(0).unwrap(), &Field::bigint("one"));
                    codes.push(c);
                }

                'D' => {
                    let dr = DataRow::from_bytes(b.freeze()).unwrap();
                    assert_eq!(dr.get::<i64>(0, Format::Text), Some(i));
                    codes.push(c);
                }

                'C' => {
                    let cc = CommandComplete::from_bytes(b.freeze()).unwrap();
                    assert_eq!(cc.command(), "SELECT 1");
                    codes.push(c);
                }

                'Z' => {
                    let rfq = ReadyForQuery::from_bytes(b.freeze()).unwrap();
                    assert_eq!(rfq.status, 'I');
                    codes.push(c);
                }

                _ => panic!("unexpected code"),
            }
        }

        assert_eq!(codes, ['T', 'D', 'C', 'Z']);
    }

    conn.write_all(&Terminate.to_bytes()).await.unwrap();
    handle.await.unwrap();

    let dbs = databases();
    let cluster = dbs.cluster(("pgdog", "pgdog")).unwrap();
    let shard = cluster.shards()[0].pools()[0].state();
    // Each simple query gets its own server assignment.
    assert_eq!(shard.stats.counts.server_assignment_count, 50);
}

#[tokio::test]
async fn test_abrupt_disconnect() {
    let (conn, mut client, _) = new_client!(false);

    drop(conn);

    let event = client.buffer(State::Idle).await.unwrap();
    assert_eq!(event, BufferEvent::DisconnectAbrupt);
    assert!(client.client_request.messages.is_empty());

    // Client disconnects and returns gracefully.
    let (conn, mut client, _) = new_client!(false);
    drop(conn);
    client.run().await.unwrap();
}

#[tokio::test]
async fn test_force_reload_preserves_split_extended_request() {
    let (mut conn, mut client, mut engine) = new_client!(false);
    conn.write_all(&Parse::named("across_reload", "SELECT 42").to_bytes())
        .await
        .expect("send Parse");

    // Interrupt the read after Parse, while the frontend is waiting for Sync.
    assert!(
        timeout(Duration::from_millis(100), client.buffer(State::Idle))
            .await
            .is_err()
    );
    assert_eq!(client.client_request.messages.len(), 1);
    let request_start = client.request_start;

    let retired = engine.cancellation_token();
    replace_databases(from_config(&config()), PoolReplacement::ForceReload).expect("force reload");
    assert!(retired.is_cancelled());
    engine
        .rebind_after_force_reload(client.in_transaction())
        .await
        .expect("rebind the unbound frontend");

    conn.write_all(&buffer!(
        { Bind::new_statement("across_reload") },
        { Execute::new() },
        { Sync }
    ))
    .await
    .expect("finish the same request");
    assert_eq!(
        timeout(Duration::from_secs(2), client.buffer(State::Idle))
            .await
            .expect("buffered request")
            .expect("complete protocol batch"),
        BufferEvent::HaveRequest
    );
    assert_eq!(client.request_start, request_start);
    timeout(Duration::from_secs(2), client.client_messages(&mut engine))
        .await
        .expect("execute the request")
        .expect("named statement remains available");
    let replies = timeout(
        Duration::from_secs(2),
        read_messages(&mut conn, &['1', '2', 'D', 'C', 'Z']),
    )
    .await
    .expect("receive the complete extended response");
    let row = DataRow::from_bytes(replies[2].to_bytes()).expect("data row");
    assert_eq!(row.get_int(0, true), Some(42));
    assert!(client.client_request.messages.is_empty());
}

#[tokio::test]
async fn test_force_reload_interrupts_first_checkout_in_transaction() {
    let (mut conn, mut client, _) = new_client!(false);
    let mut old_config = (*config()).clone();
    old_config.users.users[0].pool_size = Some(1);
    old_config.users.users[0].min_pool_size = Some(0);
    old_config.config.general.checkout_timeout = 10_000;
    old_config.config.general.connect_timeout = 10_000;
    set(old_config.clone()).expect("configure one available backend");
    reload_from_existing().expect("install the original pool");

    let cluster = databases()
        .cluster(("pgdog", "pgdog"))
        .expect("test cluster");
    let old_pool = cluster.shards()[0].pools()[0].clone();
    let _holder = old_pool
        .get(&Request::default())
        .await
        .expect("occupy the pool");
    let frontend = tokio::spawn(async move { client.spawn_internal().await });

    conn.write_all(&Query::new("BEGIN").to_bytes())
        .await
        .expect("start a buffered transaction");
    timeout(
        Duration::from_secs(2),
        read_messages(&mut conn, &['C', 'Z']),
    )
    .await
    .expect("BEGIN responds without a backend");
    conn.write_all(&Query::new("SELECT 1").to_bytes())
        .await
        .expect("request the first backend in the transaction");
    timeout(Duration::from_secs(2), async {
        while old_pool.state().waiting == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("query is waiting in the original pool");

    // The replacement accepts TCP connections but sends no PostgreSQL reply.
    let stalled_backend = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a backend without a protocol handler");
    let mut replacement = old_config;
    replacement.config.databases[0].port = stalled_backend
        .local_addr()
        .expect("replacement address")
        .port();
    set(replacement.clone()).expect("configure the replacement");
    replace_databases(from_config(&replacement), PoolReplacement::ForceReload)
        .expect("force reload while checkout is blocked");

    let fatal = timeout(Duration::from_secs(2), async { read_one!(conn) })
        .await
        .expect("termination arrives before the checkout timeout");
    let response = ErrorResponse::from_bytes(fatal.freeze()).expect("read termination");
    assert_eq!(response.code, "57P01");
    assert_eq!(old_pool.state().waiting, 0);
    frontend.await.expect("frontend exits after cancellation");
}

/// A replication client's standby status updates reach the server at once,
/// even under maintenance mode. The server only advances the slot's
/// `confirmed_flush_lsn` when it receives them.
#[tokio::test]
async fn test_maintenance_mode_forwards_replication_feedback() {
    let (mut conn, mut client, _) = new_client!(false);
    let mut replication = (*config()).clone();
    replication.users.users[0].replication_mode = true;
    replication.users.users[0].pooler_mode = Some(PoolerMode::Session);
    replication.config.general.query_parser = QueryParserLevel::Off;
    set(replication).expect("configure a replication login");
    reload_from_existing().expect("install the replication pool");

    let name = format!("pgdog_feedback_{}", uuid::Uuid::new_v4().simple());
    let mut primary = test_server().await;
    primary
        .execute_checked(format!("CREATE PUBLICATION {name}"))
        .await
        .expect("create publication");
    let frontend = tokio::spawn(async move { client.spawn_internal().await });

    conn.write_all(
        &Query::new(format!(
            "CREATE_REPLICATION_SLOT {name} TEMPORARY LOGICAL pgoutput"
        ))
        .to_bytes(),
    )
    .await
    .expect("create slot");
    read_until(&mut conn, 'Z').await.expect("slot created");
    conn.write_all(
        &Query::new(format!(
            "START_REPLICATION SLOT {name} LOGICAL 0/0 (proto_version '1', publication_names '{name}')"
        ))
        .to_bytes(),
    )
    .await
    .expect("start replication");
    read_until(&mut conn, 'W').await.expect("stream started");

    maintenance_mode::start(None);

    // WAL moves on, and the client confirms all of it.
    primary
        .execute_checked("SELECT pg_logical_emit_message(true, 'pgdog', 'held')")
        .await
        .expect("write WAL");
    let target: u64 = primary
        .fetch_all::<String>("SELECT (pg_current_wal_lsn() - '0/0')::bigint::text")
        .await
        .expect("current LSN")[0]
        .parse()
        .expect("LSN");
    let feedback = StatusUpdate {
        last_written: target,
        last_flushed: target,
        last_applied: target,
        system_clock: 0,
        reply: 0,
    }
    .wrapped()
    .expect("status update");
    conn.write_all(&feedback.to_bytes())
        .await
        .expect("send feedback");

    let confirmed = timeout(Duration::from_secs(5), async {
        loop {
            let confirmed: u64 = primary
                .fetch_all::<String>(format!(
                    "SELECT (confirmed_flush_lsn - '0/0')::bigint::text FROM pg_replication_slots WHERE slot_name = '{name}'"
                ))
                .await
                .expect("read slot")[0]
                .parse()
                .expect("LSN");
            if confirmed >= target {
                return confirmed;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    maintenance_mode::stop(None);
    drop(conn);
    let _ = timeout(Duration::from_secs(5), frontend).await;
    primary
        .execute_checked(format!("DROP PUBLICATION {name}"))
        .await
        .expect("drop publication");

    let confirmed =
        confirmed.expect("the slot confirms the client's flush position under maintenance mode");
    assert!(confirmed >= target);
}

#[tokio::test]
async fn test_client_idle_timeout() {
    let (mut conn, mut client, _inner) = new_client!(false);

    let mut config = (*config()).clone();
    config.config.general.client_idle_timeout = 25;
    set(config).unwrap();

    let start = Instant::now();
    let res = client.buffer(State::Idle).await.unwrap();
    assert_eq!(res, BufferEvent::DisconnectAbrupt);

    let err = read_one!(conn);
    assert!(start.elapsed() >= Duration::from_millis(25));
    let err = ErrorResponse::from_bytes(err.freeze()).unwrap();
    assert_eq!(err.code, "57P05");

    assert!(
        timeout(
            Duration::from_millis(50),
            client.buffer(State::IdleInTransaction)
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn test_parse_describe_flush_bind_execute_close_sync() {
    let (mut conn, mut client, _) = new_client!(false);

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    let mut buf = BytesMut::new();

    buf.put(Parse::new_anonymous("SELECT 1").to_bytes());
    buf.put(Describe::new_statement("").to_bytes());
    buf.put(Flush.to_bytes());

    conn.write_all(&buf).await.unwrap();

    let _ = read!(conn, ['1', 't', 'T']);

    let mut buf = BytesMut::new();
    buf.put(Bind::new_statement("").to_bytes());
    buf.put(Execute::new().to_bytes());
    buf.put(Close::named("").to_bytes());
    buf.put(Sync.to_bytes());

    conn.write_all(&buf).await.unwrap();

    let _ = read!(conn, ['2', 'D', 'C', '3', 'Z']);

    conn.write_all(&Terminate.to_bytes()).await.unwrap();

    handle.await.unwrap();

    // No portal yet, so no pin: second batch takes a fresh server.
    let dbs = databases();
    let cluster = dbs.cluster(("pgdog", "pgdog")).unwrap();
    let shard = cluster.shards()[0].pools()[0].state();
    assert_eq!(shard.stats.counts.server_assignment_count, 2);
}

/// https://github.com/pgdogdev/pgdog/issues/1330
///
/// Bind creates a portal that lives on one connection until Sync.
#[tokio::test]
async fn test_flush_after_bind_keeps_portal() {
    let (mut conn, mut client, _) = new_client!(false);

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    // What pg-cursor opens with: no Execute in this batch.
    conn.write_all(&buffer!(
        { Parse::new_anonymous("SELECT 1") },
        { Bind::new_statement("") },
        { Describe::new_portal("") },
        { Flush }
    ))
    .await
    .unwrap();

    let _ = read!(conn, ['1', '2', 'T']);

    conn.write_all(&buffer!({ Execute::new() }, { Sync::new() }))
        .await
        .unwrap();

    let _ = read!(conn, ['D', 'C', 'Z']);

    conn.write_all(&Terminate.to_bytes()).await.unwrap();
    handle.await.unwrap();

    let dbs = databases();
    let cluster = dbs.cluster(("pgdog", "pgdog")).unwrap();
    let shard = cluster.shards()[0].pools()[0].state();
    assert_eq!(shard.stats.counts.server_assignment_count, 1);
}

/// Paging: pin survives several Flush batches, only Sync releases.
#[tokio::test]
async fn test_flush_portal_paging_stays_pinned() {
    let (mut conn, mut client, _) = new_client!(false);

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    conn.write_all(&buffer!(
        { Parse::new_anonymous("SELECT * FROM generate_series(1, 3)") },
        { Bind::new_statement("") },
        { Describe::new_portal("") },
        { Flush }
    ))
    .await
    .unwrap();

    let _ = read!(conn, ['1', '2', 'T']);

    // 's' is PortalSuspended.
    for _ in 0..2 {
        conn.write_all(&buffer!({ Execute::new_portal_limit("", 1) }, { Flush }))
            .await
            .unwrap();
        let _ = read!(conn, ['D', 's']);
    }

    conn.write_all(&buffer!({ Execute::new() }, { Sync::new() }))
        .await
        .unwrap();
    let _ = read!(conn, ['D', 'C', 'Z']);

    conn.write_all(&Terminate.to_bytes()).await.unwrap();
    handle.await.unwrap();

    let dbs = databases();
    let cluster = dbs.cluster(("pgdog", "pgdog")).unwrap();
    let shard = cluster.shards()[0].pools()[0].state();
    assert_eq!(shard.stats.counts.server_assignment_count, 1);
}

/// Pin must not outlive the client.
#[tokio::test]
async fn test_flush_portal_released_on_terminate() {
    let (mut conn, mut client, _) = new_client!(false);

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    conn.write_all(&buffer!(
        { Parse::new_anonymous("SELECT 1") },
        { Bind::new_statement("") },
        { Describe::new_portal("") },
        { Flush }
    ))
    .await
    .unwrap();

    let _ = read!(conn, ['1', '2', 'T']);

    // Walk away, never send Sync.
    conn.write_all(&Terminate.to_bytes()).await.unwrap();

    timeout(Duration::from_secs(5), handle)
        .await
        .expect("client did not shut down while holding a portal")
        .unwrap();

    let dbs = databases();
    let cluster = dbs.cluster(("pgdog", "pgdog")).unwrap();
    let pool = &cluster.shards()[0].pools()[0];

    // Check-in cleans the connection first, so it completes asynchronously.
    timeout(Duration::from_secs(5), async {
        while pool.state().checked_out != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("backend leaked");

    let state = pool.state();
    assert_eq!(state.stats.counts.server_assignment_count, 1);
    // Cleanup succeeded: the connection was reused, not discarded.
    assert_eq!(state.force_close, 0, "cleanup failed, connection discarded");
    assert_eq!(state.stats.counts.errors, 0);
}

#[tokio::test]
async fn test_client_login_timeout() {
    use crate::config::config;
    use tokio::time::sleep;

    crate::logger();
    load_test();

    let mut config = (*config()).clone();
    config.config.general.client_login_timeout = 100;
    set(config).unwrap();

    let addr = "127.0.0.1:0".to_string();
    let stream = TcpListener::bind(&addr).await.unwrap();
    let port = stream.local_addr().unwrap().port();

    let handle = tokio::spawn(async move {
        let (stream, addr) = stream.accept().await.unwrap();
        let stream = Stream::plain(stream, 4096);

        let mut params = crate::net::Parameters::default();
        params.insert("user", "pgdog");
        params.insert("database", "pgdog");

        Client::spawn(
            stream,
            params,
            addr,
            crate::config::config(),
            ProtocolVersion::V3_0,
        )
        .await
    });

    let conn = TcpStream::connect(&format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    sleep(Duration::from_millis(150)).await;

    drop(conn);

    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_statement_mode() {
    crate::logger();

    load_test_with_pooler_mode(PoolerMode::Statement);
    let (mut conn, mut client) = parallel_test_client().await;

    tokio::spawn(async move {
        client.run().await.unwrap();
    });

    let req = buffer!({ Query::new("BEGIN") });
    conn.write_all(&req).await.unwrap();

    let msgs = read!(conn, ['E', 'Z']);
    let error = ErrorResponse::from_bytes(msgs[0].clone().freeze()).unwrap();
    assert_eq!(error.code, "58000");
}

#[tokio::test]
async fn test_client_login_timeout_does_not_affect_queries() {
    crate::logger();
    load_test();

    let mut config = (*config()).clone();
    config.config.general.client_login_timeout = 100;
    set(config).unwrap();

    let (mut conn, mut client, _) = new_client!(false);

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    let buf = buffer!({ Query::new("SELECT pg_sleep(0.2)") });
    conn.write_all(&buf).await.unwrap();

    let msgs = read!(conn, ['T', 'D', 'C', 'Z']);
    assert_eq!(msgs[0][0] as char, 'T');
    assert_eq!(msgs[3][0] as char, 'Z');

    conn.write_all(&buffer!({ Terminate })).await.unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn test_anon_prepared_statements() {
    crate::logger();
    load_test();

    let (mut conn, mut client, _) = new_client!(false);

    let mut c = (*config()).clone();
    c.config.general.prepared_statements = PreparedStatementsLevel::ExtendedAnonymous;
    set(c).unwrap();

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    conn.write_all(&buffer!(
        { Parse::new_anonymous("SELECT 1") },
        { Bind::new_params("", &[]) },
        { Execute::new() },
        { Sync }
    ))
    .await
    .unwrap();

    let _ = read!(conn, ['1', '2', 'D', 'C', 'Z']);

    {
        let cache = prepared_statements::PreparedStatements::global();
        let read = cache.read();
        assert!(!read.is_empty());
    }

    conn.write_all(&buffer!({ Terminate })).await.unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn test_anon_prepared_statements_extended() {
    crate::logger();
    load_test();

    let (mut conn, mut client, _) = new_client!(false);

    let mut c = (*config()).clone();
    c.config.general.prepared_statements = PreparedStatementsLevel::Extended;
    set(c).unwrap();

    let handle = tokio::spawn(async move {
        client.run().await.unwrap();
    });

    conn.write_all(&buffer!(
        { Parse::new_anonymous("SELECT 1") },
        { Bind::new_params("", &[]) },
        { Execute::new() },
        { Sync }
    ))
    .await
    .unwrap();

    let _ = read!(conn, ['1', '2', 'D', 'C', 'Z']);

    {
        let cache = prepared_statements::PreparedStatements::global();
        let read = cache.read();
        assert!(read.is_empty());
    }

    conn.write_all(&buffer!({ Terminate })).await.unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn test_query_timeout() {
    crate::logger();
    load_test();

    let (mut conn, mut client, mut engine) = new_client!(false);

    let mut c = (*config()).clone();
    c.config.general.query_timeout = 50;
    set(c).unwrap();

    let buf = buffer!({ Query::new("SELECT pg_sleep(0.2)") });
    conn.write_all(&buf).await.unwrap();

    client.buffer(State::Idle).await.unwrap();
    let result = client.client_messages(&mut engine).await;

    assert!(result.is_err());

    let pools = databases().cluster(("pgdog", "pgdog")).unwrap().shards()[0].pools();
    let state = pools[0].state();
    assert_eq!(state.force_close, 1);
}

#[tokio::test]
async fn test_query_size_limit_block() {
    crate::logger();
    load_test();
    let (mut conn, mut client) = parallel_test_client_with_params(Parameters::default()).await;

    let mut c = (*config()).clone();
    c.config.general.query_size_limit = Some(1024);
    c.config.general.query_size_limit_action = QuerySizeLimitAction::Block;
    set(c).unwrap();

    let handle = tokio::spawn(async move { client.run().await.unwrap() });

    // 100 distinct columns — readable in logs, clearly exceeds the 1024-byte limit.
    let big_query = format!(
        "SELECT {}",
        (1..=100)
            .map(|i| format!("{i} AS col_{i:03}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let msg_size = Query::new(&big_query).to_bytes().len();
    conn.write_all(&Query::new(&big_query).to_bytes())
        .await
        .unwrap();

    let err = read_one!(conn);
    let err = ErrorResponse::from_bytes(err.freeze()).unwrap();
    assert_eq!(err.severity, "FATAL");
    assert_eq!(err.code, "54000");
    assert_eq!(err.message, "query size exceeds query_size_limit");
    assert_eq!(
        err.detail,
        Some(format!(
            "message is {} bytes, query_size_limit is 1024 bytes",
            msg_size
        ))
    );

    handle.await.unwrap();
}

#[tokio::test]
async fn test_query_size_limit_warn() {
    let (mut conn, mut client, _) = new_client!(false);

    let mut c = (*config()).clone();
    c.config.general.query_size_limit = Some(1024);
    c.config.general.query_size_limit_action = QuerySizeLimitAction::Warn;
    set(c).unwrap();

    let handle = tokio::spawn(async move { client.run().await.unwrap() });

    // 100 distinct columns — readable in logs, exceeds the 1024-byte warn limit.
    let big_query = format!(
        "SELECT {}",
        (1..=100)
            .map(|i| format!("{i} AS col_{i:03}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    conn.write_all(&Query::new(&big_query).to_bytes())
        .await
        .unwrap();
    read_messages(&mut conn, &['T', 'D', 'C', 'Z']).await;

    conn.write_all(&Terminate.to_bytes()).await.unwrap();
    handle.await.unwrap();
}
