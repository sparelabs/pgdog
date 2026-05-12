use std::str::FromStr;

use pgdog_config::Role;

use super::parser::Error;
use crate::{
    backend::ShardingSchema,
    frontend::router::{
        parser::{ee::ParserHooks, Schema, Shard, ShardWithPriority, ShardsWithPriority},
        sharding::{ContextBuilder, SchemaSharder},
    },
    net::{parameter::ParameterValue, Parameters},
};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum RoleHint {
    PreferPrimary,
    PreferReplica,
    Primary,
    Replica,
    Any,
}

impl RoleHint {
    pub fn role(&self) -> Role {
        match self {
            RoleHint::PreferPrimary | RoleHint::Primary => Role::Primary,
            RoleHint::PreferReplica | RoleHint::Replica => Role::Replica,
            RoleHint::Any => Role::Auto,
        }
    }

    pub fn respects_read_eligible(&self) -> bool {
        matches!(
            self,
            RoleHint::PreferPrimary | RoleHint::PreferReplica | RoleHint::Any
        )
    }

    pub fn is_prefer_variant(&self) -> bool {
        matches!(self, RoleHint::PreferPrimary | RoleHint::PreferReplica)
    }
}

impl FromStr for RoleHint {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "prefer-primary" | "prefer_primary" => Ok(RoleHint::PreferPrimary),
            "prefer-replica" | "prefer_replica" => Ok(RoleHint::PreferReplica),
            "primary" => Ok(RoleHint::Primary),
            "replica" => Ok(RoleHint::Replica),
            "any" => Ok(RoleHint::Any),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParameterHints<'a> {
    pub search_path: Option<&'a ParameterValue>,
    pub pgdog_shard: Option<&'a ParameterValue>,
    pub pgdog_sharding_key: Option<&'a ParameterValue>,
    pub pgdog_role: Option<&'a ParameterValue>,
    hooks: ParserHooks,
    cached_role: Option<RoleHint>,
}

impl<'a> From<&'a Parameters> for ParameterHints<'a> {
    fn from(value: &'a Parameters) -> Self {
        let pgdog_role = value.get("pgdog.role");
        let cached_role = match pgdog_role {
            Some(ParameterValue::String(val)) => val.parse::<RoleHint>().ok(),
            _ => None,
        };
        Self {
            search_path: value.search_path(),
            pgdog_shard: value.get("pgdog.shard"),
            pgdog_role,
            pgdog_sharding_key: value.get("pgdog.sharding_key"),
            hooks: ParserHooks::default(),
            cached_role,
        }
    }
}

impl ParameterHints<'_> {
    /// Compute shard from parameters.
    pub(crate) fn compute_shard(
        &self,
        shards: &mut ShardsWithPriority,
        sharding_schema: &ShardingSchema,
    ) -> Result<(), Error> {
        let mut schema_sharder = SchemaSharder::default();

        if let Some(ParameterValue::Integer(val)) = self.pgdog_shard {
            let shard = Shard::Direct(*val as usize);
            self.hooks.record_set_shard(&shard);
            shards.push(ShardWithPriority::new_set(shard));
        }
        if let Some(ParameterValue::String(val)) = self.pgdog_shard {
            if let Ok(shard) = val.parse() {
                let shard = Shard::Direct(shard);
                self.hooks.record_set_shard(&shard);
                shards.push(ShardWithPriority::new_set(shard));
            }
        }
        if let Some(ParameterValue::String(val)) = self.pgdog_sharding_key {
            if sharding_schema.schemas.is_empty() {
                let ctx =
                    ContextBuilder::infer_from_from_and_config(val.as_str(), sharding_schema)?
                        .shards(sharding_schema.shards)
                        .build()?;
                let shard = ctx.apply()?;
                self.hooks.record_set_sharding_key(&shard, val);
                shards.push(ShardWithPriority::new_set(shard));
            } else {
                schema_sharder.resolve(Some(Schema::from(val.as_str())), &sharding_schema.schemas);

                if let Some((shard, schema)) = schema_sharder.get() {
                    self.hooks.record_sharded_schema(&shard, schema);
                    shards.push(ShardWithPriority::new_set(shard));
                }
            }
        }
        if let Some(search_path) = self.search_path {
            match search_path {
                ParameterValue::String(search_path) => {
                    let schema = Schema::from(search_path.as_str());
                    schema_sharder.resolve(Some(schema), &sharding_schema.schemas);
                }
                ParameterValue::Tuple(search_paths) => {
                    for schema in search_paths {
                        let schema = Schema::from(schema.as_str());
                        schema_sharder.resolve(Some(schema), &sharding_schema.schemas);
                    }
                }

                _ => (),
            }

            if let Some((shard, schema)) = schema_sharder.get() {
                self.hooks.record_sharded_schema(&shard, schema);
                shards.push(ShardWithPriority::new_search_path(shard, schema));
            }
        }
        Ok(())
    }

    /// Compute role from parameter value.
    pub(crate) fn compute_role(&self) -> Option<RoleHint> {
        if let Some(hint) = &self.cached_role {
            self.hooks.record_set_role(&hint.role());
        }
        self.cached_role
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::replication::ShardedSchemas;
    use pgdog_config::sharding::ShardedSchema;

    fn make_sharding_schema(schemas: &[(&str, usize)]) -> ShardingSchema {
        let sharded_schemas: Vec<ShardedSchema> = schemas
            .iter()
            .map(|(name, shard)| ShardedSchema {
                database: "test".to_string(),
                name: Some(name.to_string()),
                shard: *shard,
                all: false,
            })
            .collect();

        ShardingSchema {
            shards: schemas.len(),
            schemas: ShardedSchemas::new(sharded_schemas),
            ..Default::default()
        }
    }

    #[test]
    fn test_any_from_str() {
        assert_eq!("any".parse::<RoleHint>(), Ok(RoleHint::Any));
        assert_eq!(RoleHint::Any.role(), Role::Auto);
        assert!(RoleHint::Any.respects_read_eligible());
        assert!(!RoleHint::Any.is_prefer_variant());
    }

    #[test]
    fn test_sharding_key_with_schema_name() {
        let sharding_schema = make_sharding_schema(&[("sales", 1)]);

        let sharding_key = ParameterValue::String("sales".to_string());
        let hints = ParameterHints {
            search_path: None,
            pgdog_shard: None,
            pgdog_sharding_key: Some(&sharding_key),
            pgdog_role: None,
            hooks: ParserHooks::default(),
            cached_role: None,
        };

        let mut shards = ShardsWithPriority::default();
        hints.compute_shard(&mut shards, &sharding_schema).unwrap();

        let result = shards.shard();
        assert_eq!(*result, Shard::Direct(1));
    }

    #[test]
    fn test_sharding_key_takes_priority_over_search_path() {
        let sharding_schema = make_sharding_schema(&[("sales", 0), ("inventory", 1)]);

        let sharding_key = ParameterValue::String("sales".to_string());
        let search_path = ParameterValue::String("inventory".to_string());
        let hints = ParameterHints {
            search_path: Some(&search_path),
            pgdog_shard: None,
            pgdog_sharding_key: Some(&sharding_key),
            pgdog_role: None,
            hooks: ParserHooks::default(),
            cached_role: None,
        };

        let mut shards = ShardsWithPriority::default();
        hints.compute_shard(&mut shards, &sharding_schema).unwrap();

        let result = shards.shard();
        assert_eq!(*result, Shard::Direct(0));
    }
}
