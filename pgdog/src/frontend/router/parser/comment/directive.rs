use once_cell::sync::Lazy;
use regex::Regex;

use crate::backend::ShardingSchema;
use crate::frontend::router::parameter_hints::RoleHint;
use crate::frontend::router::sharding::ContextBuilder;

use super::super::Error;
use super::super::Shard;

pub(super) static SHARD: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"pgdog_shard: *([0-9]+)"#).unwrap());
pub(super) static SHARDING_KEY: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"pgdog_sharding_key: *(?:"([^"]*)"|'([^']*)'|([0-9a-zA-Z-]+))"#).unwrap()
});
pub(super) static ROLE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"pgdog_role: *(prefer[-_]primary|prefer[-_]replica|primary|replica|any)"#).unwrap()
});

pub(super) fn get_matched_value<'a>(caps: &'a regex::Captures<'a>) -> Option<&'a str> {
    caps.get(1)
        .or_else(|| caps.get(2))
        .or_else(|| caps.get(3))
        .map(|m| m.as_str())
}

pub(super) fn shard_role_from_comment(
    comment: &str,
    schema: &ShardingSchema,
) -> Result<(Option<Shard>, Option<RoleHint>), Error> {
    let mut role = None;

    if let Some(cap) = ROLE.captures(comment) {
        if let Some(r) = cap.get(1) {
            role = r.as_str().parse::<RoleHint>().ok();
        }
    }
    if let Some(cap) = SHARDING_KEY.captures(comment) {
        if let Some(sharding_key) = get_matched_value(&cap) {
            if let Some(schema) = schema.schemas.get(Some(sharding_key.into())) {
                return Ok((Some(schema.shard().into()), role));
            }
            let ctx = ContextBuilder::infer_from_from_and_config(sharding_key, schema)?
                .shards(schema.shards)
                .build()?;
            return Ok((Some(ctx.apply()?), role));
        }
    }
    if let Some(cap) = SHARD.captures(comment) {
        if let Some(shard) = cap.get(1) {
            return Ok((
                Some(
                    shard
                        .as_str()
                        .parse::<usize>()
                        .ok()
                        .map(Shard::Direct)
                        .unwrap_or(Shard::All),
                ),
                role,
            ));
        }
    }

    Ok((None, role))
}
