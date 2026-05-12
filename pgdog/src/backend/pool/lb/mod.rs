//! Load balanced connection pool.

use std::{
    sync::{
        atomic::{AtomicI64, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use rand::seq::SliceRandom;
use tokio::{sync::Notify, time::timeout};
use tracing::warn;

use crate::net::messages::BackendKeyData;
use crate::{
    config::{LoadBalancingStrategy, ReadWriteSplit, Role},
    net::Parameters,
};

use super::{Error, Guard, Pool, PoolConfig, Request};

pub mod ban;
pub mod monitor;
pub mod target_health;

use ban::Ban;
use monitor::*;
pub use target_health::*;

type Candidates<'a> = Vec<&'a Target>;

#[cfg(test)]
mod test;

/// Read query load balancer target.
#[derive(Clone, Debug)]
pub struct Target {
    pub pool: Pool,
    pub ban: Ban,
    role: Arc<AtomicU8>,
    pub health: TargetHealth,
    /// Smooth weighted round-robin current weight tracker.
    current_weight: Arc<AtomicI64>,
}

impl Target {
    pub(super) fn new(pool: Pool, role: Role) -> Self {
        let ban = Ban::new(&pool);
        Self {
            ban,
            role: Arc::new(AtomicU8::new(role.into())),
            health: pool.inner().health.clone(),
            pool,
            current_weight: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Get role.
    pub(super) fn role(&self) -> Role {
        let role = self.role.load(Ordering::Relaxed);
        role.try_into().expect("valid role")
    }

    /// Set role.
    pub(super) fn set_role(&self, role: Role) -> bool {
        let value = u8::from(role);
        let old = self.role.swap(value, Ordering::Relaxed);
        value != old
    }
}

/// Load balancer.
#[derive(Debug)]
pub struct LoadBalancer {
    /// Read/write targets.
    pub(super) targets: Vec<Target>,
    /// Checkout timeout.
    pub(super) checkout_timeout: Duration,
    /// Round robin atomic counter.
    pub(super) round_robin: Arc<AtomicUsize>,
    /// Chosen load balancing strategy.
    pub(super) lb_strategy: LoadBalancingStrategy,
    /// Maintenance. notification.
    pub(super) maintenance: Arc<Notify>,
    /// Role detection waiter.
    pub(super) role_detection: Arc<Notify>,
    /// Read/write split.
    pub(super) rw_split: ReadWriteSplit,
    /// Cached index of the primary target (`usize::MAX` = none).
    pub(super) primary_idx: AtomicUsize,
}

impl Default for LoadBalancer {
    fn default() -> Self {
        Self {
            targets: Vec::new(),
            checkout_timeout: Duration::ZERO,
            round_robin: Arc::new(AtomicUsize::new(0)),
            lb_strategy: LoadBalancingStrategy::default(),
            maintenance: Arc::new(Notify::new()),
            role_detection: Arc::new(Notify::new()),
            rw_split: ReadWriteSplit::default(),
            primary_idx: AtomicUsize::new(usize::MAX),
        }
    }
}

impl Clone for LoadBalancer {
    fn clone(&self) -> Self {
        Self {
            targets: self.targets.clone(),
            checkout_timeout: self.checkout_timeout,
            round_robin: self.round_robin.clone(),
            lb_strategy: self.lb_strategy,
            maintenance: self.maintenance.clone(),
            role_detection: self.role_detection.clone(),
            rw_split: self.rw_split,
            primary_idx: AtomicUsize::new(self.primary_idx.load(Ordering::Acquire)),
        }
    }
}

impl LoadBalancer {
    /// Create new replicas pools.
    pub fn new(
        primary: &Option<Pool>,
        addrs: &[PoolConfig],
        lb_strategy: LoadBalancingStrategy,
        rw_split: ReadWriteSplit,
    ) -> LoadBalancer {
        let checkout_timeout = primary
            .as_ref()
            .map(|primary| primary.config().checkout_timeout)
            .unwrap_or(Duration::ZERO)
            + addrs
                .iter()
                .map(|c| c.config.checkout_timeout)
                .sum::<Duration>();

        let mut targets: Vec<_> = addrs
            .iter()
            .map(|config| Target::new(Pool::new(config), config.address.configured_role))
            .collect();

        let primary_target = primary
            .as_ref()
            .map(|pool| Target::new(pool.clone(), Role::Primary));

        let primary_idx = if primary_target.is_some() {
            targets.len()
        } else {
            usize::MAX
        };

        if let Some(primary) = primary_target {
            targets.push(primary);
        }

        Self {
            targets,
            checkout_timeout,
            round_robin: Arc::new(AtomicUsize::new(0)),
            lb_strategy,
            maintenance: Arc::new(Notify::new()),
            role_detection: Arc::new(Notify::new()),
            rw_split,
            primary_idx: AtomicUsize::new(primary_idx),
        }
    }

    /// Get the primary pool, if configured.
    pub fn primary(&self) -> Option<&Pool> {
        self.primary_target().map(|target| &target.pool)
    }

    /// Get the primary read target containing the pool, ban state, and health.
    ///
    /// Unlike [`primary()`], this returns the full target struct which allows
    /// access to ban and health state for monitoring and testing purposes.
    ///
    /// Acquire/Release ordering on `primary_idx` ensures that a store from
    /// `redetect_roles` is visible before the next `primary_target` read.
    pub fn primary_target(&self) -> Option<&Target> {
        let idx = self.primary_idx.load(Ordering::Acquire);
        if idx == usize::MAX {
            return None;
        }
        if let Some(target) = self.targets.get(idx).filter(|t| t.role() == Role::Primary) {
            return Some(target);
        }
        // Fallback linear scan — only runs when the cached index is stale
        // (e.g. during failover). Self-heals once the cache is updated.
        let found = self.targets.iter().rposition(|t| t.role() == Role::Primary);
        if let Some(idx) = found {
            self.primary_idx.store(idx, Ordering::Release);
            return self.targets.get(idx);
        }
        self.primary_idx.store(usize::MAX, Ordering::Release);
        None
    }

    /// Detect database roles from pg_is_in_recovery() and
    /// return new primary (if any), and replicas.
    pub fn redetect_roles(&self) -> bool {
        let mut promoted = false;

        let mut targets = self
            .targets
            .clone()
            .into_iter()
            .map(|target| (target.pool.lsn_stats(), target))
            .collect::<Vec<_>>();

        // Pick primary by latest data. The one with the most
        // up-to-date lsn number and pg_is_in_recovery() = false
        // is the new primary.
        //
        // The old primary is still part of the config and will be demoted
        // to replica. If it's down, it will be banned from serving traffic.
        //
        let now = SystemTime::now();
        targets.sort_by_cached_key(|target| target.0.lsn_age(now));

        let primary = targets
            .iter()
            .position(|target| !target.0.replica && target.0.valid());

        if let Some(primary) = primary {
            promoted = targets[primary].1.set_role(Role::Primary);

            if promoted {
                warn!("new primary chosen: {}", targets[primary].1.pool.addr());

                // Demote everyone else to replicas.
                targets
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != primary)
                    .for_each(|(_, target)| {
                        target.1.set_role(Role::Replica);
                    });
            }
        }

        if promoted {
            self.role_detection.notify_one();
        }

        let new_idx = self.targets.iter().rposition(|t| t.role() == Role::Primary);
        self.primary_idx
            .store(new_idx.unwrap_or(usize::MAX), Ordering::Release);

        promoted
    }

    /// Launch replica pools and start the monitor.
    pub fn launch(&self) {
        self.targets.iter().for_each(|target| target.pool.launch());
        Monitor::spawn(self);
    }

    /// Check that the load balancer targets are all launched.
    pub fn online(&self) -> bool {
        self.targets.iter().all(|target| target.pool.lock().online)
    }

    /// Get a live connection from the pool.
    pub async fn get(&self, request: &Request) -> Result<Guard, Error> {
        match timeout(self.checkout_timeout, self.get_internal(request)).await {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(Error::ReplicaCheckoutTimeout),
        }
    }

    /// Get parameters from first non-banned connection pool.
    pub async fn params(&self, request: &Request) -> Result<&Parameters, Error> {
        if let Some(target) = self.targets.iter().find(|target| !target.ban.banned()) {
            return target.pool.params(request).await;
        }

        Err(Error::AllReplicasDown)
    }

    /// Move connections from this replica set to another.
    pub fn move_conns_to(&self, destination: &LoadBalancer) -> Result<(), Error> {
        assert_eq!(self.targets.len(), destination.targets.len());

        for (from, to) in self.targets.iter().zip(destination.targets.iter()) {
            from.pool.move_conns_to(&to.pool)?;

            // Carry over detected roles and LSN stats so the new load balancer
            // doesn't briefly appear read-only before the role detector runs.
            to.set_role(from.role());
            *to.pool.inner().lsn_stats.write() = from.pool.lsn_stats();
        }

        Ok(())
    }

    /// The two replica sets are referring to the same databases.
    pub fn can_move_conns_to(&self, destination: &LoadBalancer) -> bool {
        self.targets.len() == destination.targets.len()
            && self
                .targets
                .iter()
                .zip(destination.targets.iter())
                .all(|(a, b)| a.pool.can_move_conns_to(&b.pool))
    }

    /// True if the LB has any target that can serve replica reads.
    ///
    /// An `Auto` target counts as a potential replica until role detection
    /// converges, so callers may briefly route reads to a target that turns
    /// out to be the primary.
    pub fn has_replicas(&self) -> bool {
        self.targets
            .iter()
            .any(|target| matches!(target.role(), Role::Replica | Role::Auto))
    }

    /// Cancel a query if one is running.
    pub async fn cancel(&self, id: &BackendKeyData) -> Result<(), super::super::Error> {
        for target in &self.targets {
            target.pool.cancel(id).await?;
        }

        Ok(())
    }

    /// Replica pools handle.
    pub fn pools(&self) -> Vec<&Pool> {
        self.targets.iter().map(|target| &target.pool).collect()
    }

    /// Collect all connection pools used for read queries.
    pub fn pools_with_roles_and_bans(&self) -> Vec<(Role, Ban, Pool)> {
        let result: Vec<_> = self
            .targets
            .iter()
            .map(|target| (target.role(), target.ban.clone(), target.pool.clone()))
            .collect();

        result
    }

    /// Block until role detection has assigned every `Auto` target to
    /// `Primary` or `Replica`. The wakeup is driven by `pick_primary`, so
    /// if no primary is ever elected (e.g. LSN stats never populate),
    /// callers will block until their `checkout_timeout` fires.
    async fn wait_roles_detected(&self) {
        if !self.roles_detected() {
            self.role_detection.notified().await;
            // Chain the wakeup so any other waiter that arrived after us
            // also gets released without needing another promotion event.
            self.role_detection.notify_one();
        }
    }

    /// True once no target is still in the `Auto` state.
    pub fn roles_detected(&self) -> bool {
        !self
            .targets
            .iter()
            .any(|target| target.role() == Role::Auto)
    }

    pub(super) async fn get_primary(&self, request: &Request) -> Result<Guard, Error> {
        match timeout(self.checkout_timeout, self.get_primary_internal(request)).await {
            Ok(Ok(guard)) => Ok(guard),
            Err(_) => Err(Error::CheckoutTimeout),
            Ok(Err(err)) => Err(err),
        }
    }

    async fn get_primary_internal(&self, request: &Request) -> Result<Guard, Error> {
        self.wait_roles_detected().await;
        self.primary_target()
            .ok_or(Error::NoPrimary)?
            .pool
            .get(request)
            .await
    }

    /// Collect candidate targets for a read query.
    fn read_candidates(&self) -> Result<Candidates<'_>, Error> {
        use ReadWriteSplit::*;

        let mut candidates: Candidates<'_> = Vec::with_capacity(self.targets.len());
        let mut primaries: Candidates<'_> = Vec::new();
        let mut total_replicas: u32 = 0;
        let mut banned_replicas: u32 = 0;

        for target in &self.targets {
            if target.pool.config().resharding_only {
                continue;
            }
            match target.role() {
                Role::Primary => {
                    primaries.push(target);
                }
                Role::Replica => {
                    total_replicas += 1;
                    if target.ban.banned() {
                        banned_replicas += 1;
                    }
                    candidates.push(target);
                }
                Role::Auto => {
                    candidates.push(target);
                }
            }
        }

        let include_primary = match self.rw_split {
            IncludePrimary => true,
            IncludePrimaryIfReplicaBanned | PreferPrimary => total_replicas == banned_replicas,
            ExcludePrimary => total_replicas == 0,
        };

        if include_primary {
            candidates.extend(primaries);
        }

        if candidates.is_empty() {
            return Err(Error::AllReplicasDown);
        }

        Ok(candidates)
    }

    fn order_candidates(&self, candidates: &mut [&Target]) {
        use LoadBalancingStrategy::*;

        match self.lb_strategy {
            Random => candidates.shuffle(&mut rand::rng()),
            RoundRobin => {
                let first = self.round_robin.fetch_add(1, Ordering::Relaxed) % candidates.len();
                candidates.rotate_left(first);
            }
            LeastActiveConnections => {
                candidates.sort_by_cached_key(|target| target.pool.checked_out_count());
            }
            // Relaxed ordering means concurrent readers may see slightly stale
            // weights, producing imperfect distribution under high concurrency.
            // Acceptable for load-balancing fairness.
            WeightedRoundRobin => {
                let total_weight: i64 = candidates
                    .iter()
                    .map(|target| target.pool.config().lb_weight as i64)
                    .sum();

                if total_weight > 0 {
                    for target in candidates.iter() {
                        target
                            .current_weight
                            .fetch_add(target.pool.config().lb_weight as i64, Ordering::Relaxed);
                    }

                    let max_idx = candidates
                        .iter()
                        .enumerate()
                        .max_by_key(|(_, t)| t.current_weight.load(Ordering::Relaxed))
                        .map(|(idx, _)| idx)
                        .unwrap_or_default();

                    candidates[max_idx]
                        .current_weight
                        .fetch_sub(total_weight, Ordering::Relaxed);

                    candidates.swap(0, max_idx);
                }
            }
        }
    }

    async fn try_candidates(
        &self,
        candidates: &mut [&Target],
        request: &Request,
    ) -> Result<Guard, Error> {
        self.order_candidates(candidates);

        let bannable = candidates.len() > 1;

        for target in candidates.iter() {
            if target.ban.banned() {
                continue;
            }
            match target.pool.get(request).await {
                Ok(conn) => return Ok(conn),
                Err(Error::Offline) => continue,
                Err(err) => {
                    if bannable {
                        target.ban.ban(err, target.pool.config().ban_timeout);
                    }
                }
            }
        }

        candidates.iter().for_each(|target| target.ban.unban(true));

        Err(Error::AllReplicasDown)
    }

    /// Try the primary first; fall back to replicas if the primary is banned or
    /// fails checkout. Used by `PreferPrimary` read-write split mode.
    pub(crate) async fn preferred_read(&self, request: &Request) -> Result<Guard, Error> {
        let primary = self.primary_target();
        let primary_was_banned = primary.is_none_or(|t| t.ban.banned());

        if let Some(p) = primary.filter(|target| !target.ban.banned()) {
            match p.pool.get(request).await {
                Ok(conn) => return Ok(conn),
                Err(Error::Offline) => {}
                Err(err) => {
                    if self.has_replicas() {
                        p.ban.ban(err, p.pool.config().ban_timeout);
                    } else {
                        return Err(err);
                    }
                }
            }
        }

        let mut candidates: Candidates<'_> = self
            .targets
            .iter()
            .filter(|target| target.role() == Role::Replica)
            .filter(|target| !target.pool.config().resharding_only)
            .collect();

        if candidates.is_empty() {
            return if primary.is_some() {
                Err(Error::PreferredReadUnavailable)
            } else {
                Err(Error::AllReplicasDown)
            };
        }

        let result = self.try_candidates(&mut candidates, request).await;
        // Only unban the primary if preferred_read itself banned it above.
        // If the primary was already banned externally (health check, write
        // path), preserve that ban to avoid masking a legitimate outage.
        if result.is_err() && !primary_was_banned {
            if let Some(p) = primary {
                p.ban.unban(true);
            }
        }
        result
    }

    pub(crate) async fn any_read(&self, request: &Request) -> Result<Guard, Error> {
        let mut candidates: Candidates<'_> = self
            .targets
            .iter()
            .filter(|target| !target.pool.config().resharding_only)
            .collect();

        if candidates.is_empty() {
            return Err(Error::AllReplicasDown);
        }

        self.try_candidates(&mut candidates, request).await
    }

    async fn get_internal(&self, request: &Request) -> Result<Guard, Error> {
        let mut candidates = self.read_candidates()?;
        self.try_candidates(&mut candidates, request).await
    }

    /// Shutdown replica pools.
    ///
    /// N.B. The primary pool is managed by `super::Shard`.
    pub fn shutdown(&self) {
        for target in &self.targets {
            target.pool.shutdown();
        }

        self.maintenance.notify_waiters();
    }
}
