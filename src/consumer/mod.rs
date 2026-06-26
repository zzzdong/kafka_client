//! Consumer — high-level Kafka message consumer
//!
//! Architecture:
//!
//! ```text
//! Consumer (unified facade, delegates internally)
//!   ├── SimpleConsumer (no group_id, direct fetch)
//!   └── GroupConsumer
//!         ├── cmd_tx → ConsumerReactor (single tokio task)
//!         ├── record_rx ← ConsumerReactor
//!         └── running: Arc<AtomicBool>
//!
//! ConsumerReactor (event loop, pure async, zero blocking)
//!   tokio::select! {
//!       _ = heartbeat_tick    => { send heartbeat, detect rebalance }
//!       _ = commit_tick       => { auto-commit offsets }
//!       cmd = cmd_rx          => { handle commands }
//!       Some(result) = fetch_result_rx => { handle per-partition fetch result }
//!   }
//!
//! Fetch pipeline (fully async, non-blocking):
//!   1. try_send_fetches() — decision: which partitions to fetch?
//!      → sends FetchRequestTask to background fetch manager
//!   2. Background fetch manager — per-broker serial tasks
//!   3. handle_fetch_result() — process CompletedFetch from result channel
//!   4. drain_and_maybe_fetch() — drain records to user, fire next round
//! ```

mod config;
mod group;
mod reactor;
mod simple;
mod types;
mod util;

// Re-export public API types
pub use config::{AutoOffsetReset, ConsumerConfig, PartitionAssignmentStrategy};
pub use group::{GroupConsumer, GroupHandle, OffsetHandle};
pub use simple::SimpleConsumer;
pub use types::ConsumerRecord;

use std::sync::Arc;
use tokio::sync::mpsc;

use crate::cluster::ClusterClient;
use crate::consumer::types::ConsumerCommand;
use crate::error::Result;

// ===========================================================================
// Consumer (unified facade — backward compatible)
// ===========================================================================
pub struct Consumer {
    inner: ConsumerInner,
    offset_handle: OffsetHandle,
    group_handle: GroupHandle,
}

enum ConsumerInner {
    Simple(SimpleConsumer),
    Group(GroupConsumer),
}

fn dead_channel() -> mpsc::UnboundedSender<ConsumerCommand> {
    let (tx, _rx) = mpsc::unbounded_channel();
    tx
}

impl Consumer {
    pub(crate) fn new(cluster: Arc<ClusterClient>, config: ConsumerConfig) -> Self {
        let (inner, cmd_tx) = if config.group_id.is_empty() {
            (
                ConsumerInner::Simple(SimpleConsumer::new(cluster, config)),
                dead_channel(),
            )
        } else {
            let gc = GroupConsumer::new(cluster, config);
            let cmd_tx = gc.cmd_tx.clone();
            (ConsumerInner::Group(gc), cmd_tx)
        };
        Self {
            inner,
            offset_handle: OffsetHandle {
                cmd_tx: cmd_tx.clone(),
            },
            group_handle: GroupHandle { cmd_tx },
        }
    }

    pub async fn subscribe(&mut self, topics: Vec<String>) -> Result<()> {
        match &mut self.inner {
            ConsumerInner::Simple(s) => s.subscribe(topics).await,
            ConsumerInner::Group(g) => g.subscribe(topics).await,
        }
    }

    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        match &mut self.inner {
            ConsumerInner::Simple(s) => s.poll().await,
            ConsumerInner::Group(g) => g.poll().await,
        }
    }

    pub async fn poll_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Vec<ConsumerRecord>> {
        match &mut self.inner {
            ConsumerInner::Simple(s) => s.poll_timeout(timeout).await,
            ConsumerInner::Group(g) => g.poll_timeout(timeout).await,
        }
    }

    pub async fn try_poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        match &mut self.inner {
            ConsumerInner::Simple(s) => s.try_poll().await,
            ConsumerInner::Group(g) => g.try_poll().await,
        }
    }

    pub async fn close(&self) -> Result<()> {
        match &self.inner {
            ConsumerInner::Simple(s) => s.close().await,
            ConsumerInner::Group(g) => g.close().await,
        }
    }

    pub fn offsets(&self) -> &OffsetHandle {
        &self.offset_handle
    }

    pub fn group(&self) -> &GroupHandle {
        &self.group_handle
    }

    pub fn is_group(&self) -> bool {
        matches!(self.inner, ConsumerInner::Group(_))
    }
}
