//! Tests that test what the query parser is disabled
//! and we have only one shard (but we have replicas).
//!
//! QueryParser::query_parser_bypass.
//!
use pgdog_config::QueryParserLevel;

use crate::{
    config::config,
    frontend::router::parser::{Error, Shard},
    net::Query,
};

use crate::config::ReadWriteSplit;

use super::setup::QueryParserTest;

fn setup() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::Off;
    QueryParserTest::new_single_shard(&config)
}

fn setup_sharded() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::Off;
    QueryParserTest::new_with_config(&config)
}

const QUERIES: &[&str] = &[
    "SELECT 1",
    "CREATE TABLE test (id BIGINT)",
    "SELECT * FROM test",
    "INSERT INTO test (id) VALUES (1)",
];

#[tokio::test]
async fn test_replica() {
    let mut test = setup().with_param("pgdog.role", "prefer-replica");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert!(result.route().is_read());
        assert_eq!(result.route().shard(), &Shard::Direct(0))
    }
}

#[tokio::test]
async fn test_primary() {
    let mut test = setup().with_param("pgdog.role", "prefer-primary");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert!(result.route().is_write());
        assert_eq!(result.route().shard(), &Shard::Direct(0))
    }
}

#[tokio::test]
async fn test_no_hints() {
    let mut test = setup();

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert!(result.route().is_write());
        assert_eq!(result.route().shard(), &Shard::Direct(0))
    }
}

#[tokio::test]
async fn test_sharded_with_shard() {
    let mut test = setup_sharded().with_param("pgdog.shard", "1");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert!(result.route().is_write());
        assert_eq!(result.route().shard(), &Shard::Direct(1))
    }
}

#[tokio::test]
async fn test_sharded_with_shard_and_replica() {
    let mut test = setup_sharded()
        .with_param("pgdog.shard", "1")
        .with_param("pgdog.role", "prefer-replica");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert!(result.route().is_read());
        assert_eq!(result.route().shard(), &Shard::Direct(1))
    }
}

#[tokio::test]
async fn test_sharded_no_hints() {
    let mut test = setup_sharded();

    for query in QUERIES {
        let result = test
            .try_execute(vec![Query::new(query).into()])
            .unwrap_err();
        assert!(matches!(result, Error::QueryParserRequired));
    }
}

fn setup_prefer_primary() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::Off;
    QueryParserTest::new_single_shard(&config).with_read_write_split(ReadWriteSplit::PreferPrimary)
}

#[tokio::test]
async fn test_prefer_primary_no_hints_defaults_to_write() {
    let mut test = setup_prefer_primary();

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert!(result.route().is_write());
        assert!(!result.route().prefer_primary());
        assert_eq!(result.route().shard(), &Shard::Direct(0));
    }
}

#[tokio::test]
async fn test_prefer_primary_with_replica_hint() {
    let mut test = setup_prefer_primary().with_param("pgdog.role", "prefer-replica");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert!(result.route().is_read());
        assert!(!result.route().prefer_primary());
        assert_eq!(result.route().shard(), &Shard::Direct(0));
    }
}

#[tokio::test]
async fn test_prefer_primary_with_primary_hint() {
    let mut test = setup_prefer_primary().with_param("pgdog.role", "prefer-primary");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert!(result.route().is_write());
        assert_eq!(result.route().shard(), &Shard::Direct(0));
    }
}

#[tokio::test]
async fn test_prefer_primary_hard_replica() {
    let mut test = setup_prefer_primary().with_param("pgdog.role", "replica");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert!(result.route().is_read());
        assert_eq!(result.route().shard(), &Shard::Direct(0));
    }
}

#[tokio::test]
async fn test_prefer_primary_hard_primary() {
    let mut test = setup_prefer_primary().with_param("pgdog.role", "primary");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert!(result.route().is_write());
        assert_eq!(result.route().shard(), &Shard::Direct(0));
    }
}

#[tokio::test]
async fn test_any_bypass() {
    let mut test = setup().with_param("pgdog.role", "any");

    for query in QUERIES {
        let result = test.try_execute(vec![Query::new(query).into()]).unwrap();
        assert!(result.route().is_read());
        assert!(result.route().any_target());
        assert_eq!(result.route().shard(), &Shard::Direct(0));
    }
}
