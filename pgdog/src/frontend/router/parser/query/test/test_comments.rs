use crate::{
    config::{ReadWriteSplit, Role},
    frontend::router::parser::{Cache, Shard},
};

use super::setup::*;

// --- read_eligible tests ---

#[test]
fn test_select_for_update_not_read_eligible() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT * FROM foo FOR UPDATE").into()]);

    assert!(command.route().is_write());
    assert!(!command.route().read_eligible());
}

#[test]
fn test_cte_with_insert_not_read_eligible() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "WITH ins AS (INSERT INTO foo VALUES (1) RETURNING id) SELECT * FROM ins",
    )
    .into()]);

    assert!(command.route().is_write());
    assert!(!command.route().read_eligible());
}

#[test]
fn test_plain_select_is_read_eligible() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT * FROM foo WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert!(command.route().read_eligible());
}

#[test]
fn test_select_for_share_not_read_eligible() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT * FROM foo FOR SHARE").into()]);

    assert!(command.route().is_write());
    assert!(!command.route().read_eligible());
}

#[test]
fn test_sticky_prefer_replica_does_not_redirect_locking_select() {
    let mut test = QueryParserTest::new().with_connection_role(Role::Replica);

    let command = test.execute(vec![Query::new("SELECT * FROM foo FOR UPDATE").into()]);

    assert!(
        command.route().is_write(),
        "SELECT FOR UPDATE must stay on primary even with sticky prefer-replica"
    );
    assert!(!command.route().read_eligible());
}

#[test]
fn test_sticky_prefer_replica_does_not_redirect_cte_write() {
    let mut test = QueryParserTest::new().with_connection_role(Role::Replica);

    let command = test.execute(vec![Query::new(
        "With del AS (DELETE FROM foo RETURNING id) SELECT * FROM del",
    )
    .into()]);

    assert!(
        command.route().is_write(),
        "CTE with writes must stay on primary even with sticky prefer-replica"
    );
    assert!(!command.route().read_eligible());
}

#[test]
fn test_comment_pgdog_role_prefer_primary() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: prefer-primary */ SELECT 1",
    )
    .into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_comment_pgdog_shard() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("/* pgdog_shard: 1234 */ SELECT 1234").into()
    ]);

    assert_eq!(command.route().shard(), &Shard::Direct(1234));
}

#[test]
fn test_comment_pgdog_shard_extended() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Parse::named(
            "__test_comment",
            "/* pgdog_shard: 1234 */ SELECT * FROM sharded WHERE id = $1",
        )
        .into(),
        Bind::new_statement("__test_comment").into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert_eq!(command.route().shard(), &Shard::Direct(1234));
}

/// A shard-commented query that requires a rewrite must not be stored in
/// the cache under the comment-stripped key. If it were, a subsequent
/// uncommented query with the same body would cache-hit and inherit the
/// rewrite plan built against the direct-shard variant.
///
/// `pgdog.unique_id()` is a reliable rewrite trigger: the rewriter replaces
/// the call with a `$N::bigint` parameter in extended protocol.
#[test]
fn test_shard_comment_with_rewrite_not_cached() {
    Cache::reset();
    let mut test = QueryParserTest::new();

    let stripped = "SELECT pgdog.unique_id()";
    let commented = "/* pgdog_shard: 0 */ SELECT pgdog.unique_id()";

    test.execute(vec![
        Parse::named("__shard_comment_rewrite", commented).into(),
        Bind::new_statement("__shard_comment_rewrite").into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let cached = Cache::queries();
    let poisoned = cached.keys().any(|k| k.as_str() == stripped);
    assert!(
        !poisoned,
        "shard-commented query with a non-empty rewrite plan must not be \
         cached under the stripped key; a subsequent uncommented lookup \
         would otherwise receive the direct-shard plan"
    );
}

/// Complement to the previous test: a shard-commented query whose rewrite
/// plan is empty can safely share a cache entry with its uncommented
/// counterpart, since there's no plan to poison. Verifies the cacheable
/// check isn't blanket-dropping every shard-commented query.
#[test]
fn test_shard_comment_without_rewrite_is_cached() {
    Cache::reset();
    let mut test = QueryParserTest::new();

    let stripped = "SELECT 1";
    let commented = "/* pgdog_shard: 0 */ SELECT 1";

    test.execute(vec![
        Parse::named("__shard_comment_no_rewrite", commented).into(),
        Bind::new_statement("__shard_comment_no_rewrite").into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let cached = Cache::queries();
    assert!(
        cached.keys().any(|k| k.as_str() == stripped),
        "shard-commented query with an empty rewrite plan must still be \
         cached under the stripped key"
    );
}

#[test]
fn test_comment_prefer_replica_overrides_sticky_prefer_primary() {
    let mut test = QueryParserTest::new().with_connection_role(Role::Primary);

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: prefer-replica */ SELECT 1",
    )
    .into()]);

    assert!(command.route().is_read());
}

#[test]
fn test_sticky_prefer_primary_without_comment_sends_to_primary() {
    let mut test = QueryParserTest::new().with_connection_role(Role::Primary);

    let command = test.execute(vec![Query::new("SELECT 1").into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_comment_prefer_primary_overrides_sticky_prefer_replica() {
    let mut test = QueryParserTest::new().with_connection_role(Role::Replica);

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: prefer-primary */ SELECT 1",
    )
    .into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_comment_role_override_does_not_leak_to_next_query() {
    let mut test = QueryParserTest::new().with_connection_role(Role::Primary);

    let first = test.execute(vec![Query::new(
        "/* pgdog_role: prefer-replica */ SELECT 1",
    )
    .into()]);
    assert!(first.route().is_read());

    let second = test.execute(vec![Query::new("SELECT 1").into()]);
    assert!(second.route().is_write());
}

// --- SHOW read_eligible tests ---

#[test]
fn test_show_is_read_eligible() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SHOW server_version").into()]);

    assert!(command.route().read_eligible());
}

#[test]
fn test_show_respects_connection_role_prefer_replica() {
    let mut test = QueryParserTest::new().with_connection_role(Role::Replica);

    let command = test.execute(vec![Query::new("SHOW server_version").into()]);

    assert!(command.route().is_read());
}

#[test]
fn test_show_respects_connection_role_prefer_primary() {
    let mut test = QueryParserTest::new().with_connection_role(Role::Primary);

    let command = test.execute(vec![Query::new("SHOW server_version").into()]);

    assert!(command.route().is_write());
}

// --- hard role override tests ---

#[test]
fn test_replica_comment_routes_write_to_replica() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: replica */ CREATE TABLE foo(id INT)",
    )
    .into()]);

    assert!(command.route().is_read());
}

#[test]
fn test_primary_comment_routes_read_to_primary() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("/* pgdog_role: primary */ SELECT 1").into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_replica_param_routes_write_to_replica() {
    let mut test = QueryParserTest::new().with_force_connection_role(Role::Replica);

    let command = test.execute(vec![Query::new("CREATE TABLE foo(id INT)").into()]);

    assert!(command.route().is_read());
}

#[test]
fn test_primary_param_routes_read_to_primary() {
    let mut test = QueryParserTest::new().with_force_connection_role(Role::Primary);

    let command = test.execute(vec![Query::new("SELECT 1").into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_prefer_replica_param_does_not_route_write_to_replica() {
    let mut test = QueryParserTest::new().with_connection_role(Role::Replica);

    let command = test.execute(vec![Query::new("CREATE TABLE foo(id INT)").into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_prefer_replica_comment_does_not_route_write_to_replica() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: prefer-replica */ CREATE TABLE foo(id INT)",
    )
    .into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_prefer_replica_underscore_comment_routes_read_to_replica() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: prefer_replica */ SELECT 1",
    )
    .into()]);

    assert!(command.route().is_read());
}

#[test]
fn test_prefer_primary_underscore_comment_routes_read_to_primary() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: prefer_primary */ SELECT 1",
    )
    .into()]);

    assert!(command.route().is_write());
}

// --- prefer_primary config-level tests ---

#[test]
fn test_prefer_primary_select_uses_primary_preferred_read_route() {
    let mut test = QueryParserTest::new().with_read_write_split(ReadWriteSplit::PreferPrimary);

    let command = test.execute(vec![Query::new("SELECT 1").into()]);

    assert!(command.route().is_read());
    assert!(command.route().prefer_primary());
}

#[test]
fn test_prefer_primary_comment_prefer_replica_overrides_to_replica() {
    let mut test = QueryParserTest::new().with_read_write_split(ReadWriteSplit::PreferPrimary);

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: prefer-replica */ SELECT 1",
    )
    .into()]);

    assert!(command.route().is_read());
    assert!(!command.route().prefer_primary());
}

#[test]
fn test_prefer_primary_sticky_prefer_replica_overrides_config() {
    let mut test = QueryParserTest::new()
        .with_read_write_split(ReadWriteSplit::PreferPrimary)
        .with_connection_role(Role::Replica);

    let command = test.execute(vec![Query::new("SELECT 1").into()]);

    assert!(command.route().is_read());
    assert!(!command.route().prefer_primary());
}

#[test]
fn test_sticky_prefer_replica_does_not_rewrite_writes() {
    let mut test = QueryParserTest::new().with_connection_role(Role::Replica);

    let command = test.execute(vec![Query::new("INSERT INTO foo VALUES (1)").into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_prefer_primary_comment_overrides_sticky_overrides_config() {
    let mut test = QueryParserTest::new()
        .with_read_write_split(ReadWriteSplit::PreferPrimary)
        .with_connection_role(Role::Replica);

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: prefer-primary */ SELECT 1",
    )
    .into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_writes_still_go_to_primary_for_all_read_write_split_modes() {
    for split in [
        ReadWriteSplit::IncludePrimary,
        ReadWriteSplit::ExcludePrimary,
        ReadWriteSplit::PreferPrimary,
        ReadWriteSplit::IncludePrimaryIfReplicaBanned,
    ] {
        let mut test = QueryParserTest::new().with_read_write_split(split);

        let command = test.execute(vec![Query::new("INSERT INTO foo VALUES (1)").into()]);

        assert!(command.route().is_write());
        assert!(!command.route().prefer_primary());
    }
}

#[test]
fn test_prefer_primary_reset_pgdog_role_reverts_to_primary() {
    let mut test = QueryParserTest::new()
        .with_read_write_split(ReadWriteSplit::PreferPrimary)
        .with_connection_role(Role::Replica);

    let command = test.execute(vec![Query::new("SELECT 1").into()]);
    assert!(command.route().is_read());
    assert!(!command.route().prefer_primary());

    test.clear_connection_role();

    let command = test.execute(vec![Query::new("SELECT 1").into()]);
    assert!(command.route().is_read());
    assert!(command.route().prefer_primary());
}

#[test]
fn test_include_primary_sticky_prefer_replica_routes_to_replica() {
    let mut test = QueryParserTest::new()
        .with_read_write_split(ReadWriteSplit::IncludePrimary)
        .with_connection_role(Role::Replica);

    let command = test.execute(vec![Query::new("SELECT 1").into()]);

    assert!(command.route().is_read());
}

#[test]
fn test_prefer_primary_transaction_role_overrides_connection_role() {
    let mut test = QueryParserTest::new()
        .with_read_write_split(ReadWriteSplit::PreferPrimary)
        .with_connection_role(Role::Primary)
        .with_transaction_role(Role::Replica)
        .in_transaction(true);

    let command = test.execute(vec![Query::new("SELECT 1").into()]);

    assert!(command.route().is_read());
    assert!(!command.route().prefer_primary());
}

#[test]
fn test_prefer_primary_transaction_prefer_replica_does_not_rewrite_writes() {
    let mut test = QueryParserTest::new()
        .with_read_write_split(ReadWriteSplit::PreferPrimary)
        .with_transaction_role(Role::Replica)
        .in_transaction(true);

    let command = test.execute(vec![Query::new("INSERT INTO foo VALUES (1)").into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_prefer_primary_comment_overrides_transaction_role() {
    let mut test = QueryParserTest::new()
        .with_read_write_split(ReadWriteSplit::PreferPrimary)
        .with_connection_role(Role::Primary)
        .with_transaction_role(Role::Replica)
        .in_transaction(true);

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: prefer-primary */ SELECT 1",
    )
    .into()]);

    assert!(command.route().is_write());
}

#[test]
fn test_prefer_primary_comment_prefer_replica_overrides_connection_prefer_primary() {
    let mut test = QueryParserTest::new()
        .with_read_write_split(ReadWriteSplit::PreferPrimary)
        .with_connection_role(Role::Primary);

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: prefer-replica */ SELECT 1",
    )
    .into()]);

    assert!(command.route().is_read());
    assert!(!command.route().prefer_primary());
}

// --- `any` role hint tests ---

#[test]
fn test_comment_any_routes_read_to_any_target() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("/* pgdog_role: any */ SELECT 1").into()]);

    assert!(command.route().is_read());
    assert!(command.route().any_target());
    assert!(!command.route().prefer_primary());
}

#[test]
fn test_param_any_routes_read_to_any_target() {
    let mut test = QueryParserTest::new().with_param("pgdog.role", "any");

    let command = test.execute(vec![Query::new("SELECT 1").into()]);

    assert!(command.route().is_read());
    assert!(command.route().any_target());
    assert!(!command.route().prefer_primary());
}

#[test]
fn test_any_does_not_affect_writes() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: any */ INSERT INTO foo VALUES (1)",
    )
    .into()]);

    assert!(command.route().is_write());
    assert!(!command.route().any_target());
}

#[test]
fn test_any_does_not_affect_select_for_update() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new(
        "/* pgdog_role: any */ SELECT * FROM foo FOR UPDATE",
    )
    .into()]);

    assert!(command.route().is_write());
    assert!(!command.route().any_target());
}

#[test]
fn test_any_comment_overrides_prefer_primary_config() {
    let mut test = QueryParserTest::new().with_read_write_split(ReadWriteSplit::PreferPrimary);

    let command = test.execute(vec![Query::new("/* pgdog_role: any */ SELECT 1").into()]);

    assert!(command.route().is_read());
    assert!(command.route().any_target());
    assert!(!command.route().prefer_primary());
}

#[test]
fn test_any_comment_overrides_sticky_connection_role() {
    let mut test = QueryParserTest::new().with_connection_role(Role::Primary);

    let command = test.execute(vec![Query::new("/* pgdog_role: any */ SELECT 1").into()]);

    assert!(command.route().is_read());
    assert!(command.route().any_target());
}

#[test]
fn test_any_does_not_leak_between_queries() {
    let mut test = QueryParserTest::new();

    let first = test.execute(vec![Query::new("/* pgdog_role: any */ SELECT 1").into()]);
    assert!(first.route().any_target());

    let second = test.execute(vec![Query::new("SELECT 1").into()]);
    assert!(!second.route().any_target());
}
