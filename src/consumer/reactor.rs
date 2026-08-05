//! ConsumerOrchestrator — unified background task for both Direct and Group modes
//!
//! Architecture:
//!
//! ```text
//! Consumer (public API)
//!   └── cmd_tx → ConsumerOrchestrator (single tokio background task)
//!                  ├── FetchManager (existing, unchanged)
//!                  ├── GroupCoordinator (actor, group mode only)
//!                  └── Manages: offsets, buffer, fetch scheduling
//! ```
//!
//! The orchestrator uses a flat event loop (no nested loops) and a `shutdown`
//! flag for clean lifecycle management. Group coordination is delegated to a
//! spawned `GroupCoordinator` actor that communicates via `GroupEvent`.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::interval;
use tracing::{debug, warn};

use crate::cluster::ClusterClient;
use crate::consumer::config::{AutoOffsetReset, ConsumerConfig};
use crate::consumer::group_coordinator::{
    GroupCommand, GroupCoordinatorHandle, GroupEvent, spawn_group_coordinator,
};
use crate::consumer::types::{
    CompletedFetch, ConsumerCommand, ConsumerMode, ConsumerRecord, FetchParams, FetchRequestTask,
    RecordBatchCursor,
};
use crate::consumer::util::{fetch_committed_offsets_raw, find_coordinator_raw, list_offset_for};
use crate::error::{KafkaError, Result};
use crate::protocol::{
    FetchPartition, FetchRequest, FetchTopic, HeartbeatRequest, LeaveGroupRequest,
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_client_protocol::KafkaErrorCode;

// Sentinel timestamps for ListOffsets requests.
const LATEST_TIMESTAMP: i64 = -1;
const EARLIEST_TIMESTAMP: i64 = -2;

// ===========================================================================
// Spawn entry point (kept for backward compat with mod.rs)
// ===========================================================================

pub(crate) fn spawn_consumer_task(
    cluster: Arc<ClusterClient>,
    config: ConsumerConfig,
    cmd_rx: mpsc::UnboundedReceiver<ConsumerCommand>,
    record_tx: mpsc::Sender<Vec<ConsumerRecord>>,
) {
    let mode = if config.group_id.is_none() {
        ConsumerMode::Direct
    } else {
        ConsumerMode::Group
    };

    let fetch_params = FetchParams {
        timeout_ms: config.max_wait.as_millis() as i32,
        min_bytes: config.min_bytes,
        max_bytes: config.max_bytes,
        partition_max_bytes: config.partition_max_bytes,
    };

    let (fetch_task_tx, fetch_task_rx) = mpsc::unbounded_channel();
    let (fetch_result_tx, fetch_result_rx) = mpsc::unbounded_channel();

    spawn_fetch_manager(cluster.clone(), fetch_task_rx, fetch_result_tx);

    // Spawn GroupCoordinator for group mode
    let (group_coordinator, group_event_rx) = if mode == ConsumerMode::Group {
        let (handle, rx) = spawn_group_coordinator(cluster.clone(), config.clone());
        (Some(handle), Some(rx))
    } else {
        (None, None)
    };

    tokio::spawn(async move {
        let mut orch = ConsumerOrchestrator {
            mode,
            cluster,
            config,
            fetch_params,
            offsets: HashMap::new(),
            consumed_offsets: HashMap::new(),
            next_in_line_records: HashMap::new(),
            pending_fetches: HashSet::new(),
            assigned_partitions: HashMap::new(),
            subscribed_topics: Vec::new(),
            cmd_rx,
            record_tx,
            fetch_task_tx,
            fetch_result_rx,
            group_coordinator,
            group_event_rx,
            // Cached group state (updated via GroupEvent)
            coordinator: None,
            group_active: false,
            group_member_id: String::new(),
            group_generation_id: 0,
            consumer_ready: false,
            shutdown: false,
            last_metadata_refresh: None,
            metadata_refresh_interval: Duration::from_secs(1),
            pending_subscribe_reply: None,
            pending_subscribe_deadline: None,
        };
        orch.run().await;
    });
}

// ===========================================================================
// FetchManager — unchanged from original
// ===========================================================================

fn spawn_fetch_manager(
    cluster: Arc<ClusterClient>,
    task_rx: mpsc::UnboundedReceiver<FetchRequestTask>,
    result_tx: mpsc::UnboundedSender<CompletedFetch>,
) {
    tokio::spawn(async move {
        let mut task_rx = task_rx;
        let mut broker_tasks: HashMap<SocketAddr, mpsc::UnboundedSender<FetchRequestTask>> =
            HashMap::new();

        loop {
            let Some(task) = task_rx.recv().await else {
                break;
            };
            let addr = task.broker_addr;
            let forward_tx = if let Some(tx) = broker_tasks.get(&addr) {
                if !tx.is_closed() {
                    tx.clone()
                } else {
                    let (tx, rx) = mpsc::unbounded_channel();
                    spawn_broker_fetch_task(cluster.clone(), addr, rx, result_tx.clone());
                    broker_tasks.insert(addr, tx.clone());
                    tx
                }
            } else {
                let (tx, rx) = mpsc::unbounded_channel();
                spawn_broker_fetch_task(cluster.clone(), addr, rx, result_tx.clone());
                broker_tasks.insert(addr, tx.clone());
                tx
            };

            if forward_tx.send(task).is_err() {
                warn!("Broker fetch task for {} died", addr);
                broker_tasks.remove(&addr);
            }
        }
    });
}

fn spawn_broker_fetch_task(
    cluster: Arc<ClusterClient>,
    broker_addr: SocketAddr,
    mut task_rx: mpsc::UnboundedReceiver<FetchRequestTask>,
    result_tx: mpsc::UnboundedSender<CompletedFetch>,
) {
    tokio::spawn(async move {
        while let Some(task) = task_rx.recv().await {
            let response: Result<crate::protocol::FetchResponse> =
                cluster.send_to_broker(broker_addr, &task.request).await;
            match response {
                Ok(fetch_response) => {
                    for tr in fetch_response.responses {
                        let topic_name = if !tr.topic.is_empty() {
                            tr.topic.clone()
                        } else {
                            String::new()
                        };
                        for pr in tr.partitions {
                            let _ = result_tx.send(CompletedFetch {
                                topic: topic_name.clone(),
                                topic_id: tr.topic_id,
                                partition: pr.partition_index,
                                error_code: pr.error_code,
                                records: pr.records,
                            });
                        }
                    }
                }
                Err(e) => {
                    debug!("Fetch request to {} failed: {}", broker_addr, e);
                    for (topic, partition) in &task.partitions {
                        let _ = result_tx.send(CompletedFetch {
                            topic: topic.clone(),
                            topic_id: uuid::Uuid::nil(),
                            partition: *partition,
                            error_code: -1,
                            records: None,
                        });
                    }
                }
            }
        }
    });
}

// ===========================================================================
// ConsumerOrchestrator
// ===========================================================================

struct ConsumerOrchestrator {
    mode: ConsumerMode,
    cluster: Arc<ClusterClient>,
    config: ConsumerConfig,
    fetch_params: FetchParams,

    // Offset tracking
    offsets: HashMap<String, HashMap<i32, i64>>,
    consumed_offsets: HashMap<String, HashMap<i32, i64>>,

    // Fetch pipeline
    next_in_line_records: HashMap<(String, i32), RecordBatchCursor>,
    pending_fetches: HashSet<(String, i32)>,

    // Partition assignment
    assigned_partitions: HashMap<String, Vec<i32>>,
    subscribed_topics: Vec<String>,

    // Channels
    cmd_rx: mpsc::UnboundedReceiver<ConsumerCommand>,
    record_tx: mpsc::Sender<Vec<ConsumerRecord>>,
    fetch_task_tx: mpsc::UnboundedSender<FetchRequestTask>,
    fetch_result_rx: mpsc::UnboundedReceiver<CompletedFetch>,

    // Group mode
    group_coordinator: Option<GroupCoordinatorHandle>,
    group_event_rx: Option<mpsc::UnboundedReceiver<GroupEvent>>,
    /// Cached coordinator address (updated from GroupEvent).
    coordinator: Option<SocketAddr>,
    /// Tracks whether we currently hold a group assignment (ignores stale events).
    group_active: bool,
    group_member_id: String,
    group_generation_id: i32,

    /// Whether the consumer has signaled readiness via poll() or into_stream().
    /// Fetches only start after this flag is set, to avoid buffering records
    /// before any receiver is reading from record_tx.
    consumer_ready: bool,

    // Lifecycle
    shutdown: bool,
    /// Last time metadata was refreshed (debounce).
    last_metadata_refresh: Option<std::time::Instant>,
    /// Minimum interval between metadata refreshes.
    metadata_refresh_interval: Duration,
    /// Pending reply for a group-mode `subscribe()` call. Resolved when the
    /// group receives its first partition assignment and offsets are initialized.
    pending_subscribe_reply: Option<oneshot::Sender<Result<()>>>,
    /// Deadline for resolving a pending group-mode `subscribe()` call, so a
    /// coordinator that never assigns partitions cannot block the caller
    /// forever.
    pending_subscribe_deadline: Option<tokio::time::Instant>,
}

impl ConsumerOrchestrator {
    // ==================================================================
    // Event loop
    // ==================================================================

    async fn run(&mut self) {
        let mut commit_interval = interval(self.config.auto_commit_interval);
        commit_interval.reset();

        loop {
            if self.shutdown {
                break;
            }

            tokio::select! {
                biased;
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd).await,
                        None => self.shutdown = true,
                    }
                }
                Some(result) = self.fetch_result_rx.recv() => {
                    self.handle_fetch_result(result).await;
                    self.drain_and_send().await;
                }
                event = async {
                    self.group_event_rx.as_mut()?.recv().await
                }, if self.group_event_rx.is_some() => {
                    if let Some(event) = event {
                        self.handle_group_event(event).await;
                    }
                }
                _ = commit_interval.tick(), if self.mode == ConsumerMode::Group && self.config.auto_commit => {
                    if let Err(e) = self.do_commit().await {
                        warn!("Auto commit failed: {}", e);
                    }
                }
                _ = Self::subscribe_deadline(self.pending_subscribe_deadline),
                    if self.pending_subscribe_deadline.is_some() =>
                {
                    if let Some(reply) = self.pending_subscribe_reply.take() {
                        let _ = reply.send(Err(KafkaError::RequestTimeout));
                    }
                    self.pending_subscribe_deadline = None;
                    warn!("subscribe() timed out waiting for a group assignment");
                }
            }
        }

        // --- Clean shutdown ---
        // 1. Resolve any pending subscribe() so callers don't hang
        if let Some(reply) = self.pending_subscribe_reply.take() {
            let _ = reply.send(Err(KafkaError::ConnectionClosed));
        }
        self.pending_subscribe_deadline = None;
        // 2. Send leave group if active
        if self.group_active {
            let _ = self.send_leave_group().await;
        }
        // 3. Shutdown GroupCoordinator
        if let Some(gc) = self.group_coordinator.take() {
            gc.shutdown().await;
        }
        // 4. fetch_task_tx dropped → FetchManager shuts down naturally
        debug!("ConsumerOrchestrator shut down");
    }

    // ==================================================================
    // Command handling (unified for Direct and Group)
    // ==================================================================

    async fn handle_command(&mut self, cmd: ConsumerCommand) {
        match cmd {
            ConsumerCommand::Subscribe { topics, reply } => {
                self.subscribed_topics = topics.clone();
                match self.mode {
                    ConsumerMode::Direct => {
                        self.assign_all_partitions(&topics).await;
                        self.init_offsets_for_assignment().await;
                        self.try_send_fetches().await;
                        if let Some(reply) = reply {
                            let _ = reply.send(Ok(()));
                        }
                    }
                    ConsumerMode::Group => {
                        // If a previous subscribe() is still awaiting its first
                        // assignment, resolve it with an error so it doesn't hang.
                        if let Some(old_reply) = self.pending_subscribe_reply.take() {
                            let _ = old_reply.send(Err(KafkaError::ConnectionClosed));
                        }
                        self.pending_subscribe_deadline = None;
                        if let Some(ref gc) = self.group_coordinator {
                            let _ = gc.cmd_tx.send(GroupCommand::Join { topics });
                        }
                        // Wait for the first partition assignment before
                        // resolving subscribe(). This satisfies the public API
                        // contract that subscribe() blocks until assignment and
                        // offset initialization are complete — but not forever.
                        self.pending_subscribe_reply = reply;
                        self.pending_subscribe_deadline = Some(
                            tokio::time::Instant::now()
                                + self.config.rebalance_timeout
                                + self.config.session_timeout,
                        );
                    }
                }
            }
            ConsumerCommand::Assign {
                topic,
                partitions,
                reply,
            } => {
                self.assigned_partitions
                    .entry(topic.clone())
                    .or_default()
                    .extend(partitions);
                self.init_offsets_for_assignment().await;
                if self.consumer_ready {
                    self.try_send_fetches().await;
                }
                if let Some(reply) = reply {
                    let _ = reply.send(Ok(()));
                }
            }
            ConsumerCommand::Seek {
                topic,
                partition,
                offset,
            } => {
                self.offsets
                    .entry(topic.clone())
                    .or_default()
                    .insert(partition, offset);
                self.consumed_offsets
                    .entry(topic.clone())
                    .or_default()
                    .insert(partition, offset);
                // Clear cached data and pending fetch — force re-fetch
                let key = (topic, partition);
                self.next_in_line_records.remove(&key);
                self.pending_fetches.remove(&key);
                if self.consumer_ready {
                    self.try_send_fetches().await;
                }
            }
            ConsumerCommand::Commit { reply } => {
                let _ = reply.send(self.do_commit().await);
            }
            ConsumerCommand::GetOffset {
                topic,
                partition,
                reply,
            } => {
                let val = self
                    .offsets
                    .get(&topic)
                    .and_then(|m| m.get(&partition))
                    .copied();
                let _ = reply.send(val);
            }
            ConsumerCommand::SetOffset {
                topic,
                partition,
                offset,
            } => {
                self.offsets
                    .entry(topic.clone())
                    .or_default()
                    .insert(partition, offset);
                self.consumed_offsets
                    .entry(topic)
                    .or_default()
                    .insert(partition, offset);
            }
            ConsumerCommand::Heartbeat { reply } => {
                let result = self.send_heartbeat_raw().await;
                let _ = reply.send(result);
            }
            ConsumerCommand::Leave { reply } => {
                // Tell GroupCoordinator to stop heartbeat
                if let Some(ref gc) = self.group_coordinator {
                    let _ = gc.cmd_tx.send(GroupCommand::Leave);
                }
                // Send LeaveGroup RPC
                let r = self.send_leave_group().await;
                // Reset local state
                self.group_active = false;
                self.coordinator = None;
                self.group_member_id.clear();
                self.group_generation_id = 0;
                self.subscribed_topics.clear();
                self.assigned_partitions.clear();
                self.next_in_line_records.clear();
                self.pending_fetches.clear();
                let _ = reply.send(r);
            }
            ConsumerCommand::GetAssignment { reply } => {
                let _ = reply.send(self.assigned_partitions.clone());
            }
            ConsumerCommand::GetGroupMetadata { reply } => {
                let _ = reply.send((self.group_generation_id, self.group_member_id.clone()));
            }
            ConsumerCommand::Unsubscribe { reply } => {
                // Group mode: leave the group first
                if self.group_active {
                    if let Some(ref gc) = self.group_coordinator {
                        let _ = gc.cmd_tx.send(GroupCommand::Leave);
                    }
                    let _ = self.send_leave_group().await;
                }
                // Reset all assignment state
                self.group_active = false;
                self.coordinator = None;
                self.group_member_id.clear();
                self.group_generation_id = 0;
                self.subscribed_topics.clear();
                self.assigned_partitions.clear();
                self.next_in_line_records.clear();
                self.pending_fetches.clear();
                self.offsets.clear();
                self.consumed_offsets.clear();
                let _ = reply.send(Ok(()));
            }
            ConsumerCommand::StartPolling => {
                self.consumer_ready = true;
                self.try_send_fetches().await;
            }
            ConsumerCommand::Shutdown => {
                self.shutdown = true;
            }
        }
    }

    // ==================================================================
    // Group event handling
    // ==================================================================

    async fn handle_group_event(&mut self, event: GroupEvent) {
        match event {
            GroupEvent::AssignmentChanged {
                partitions,
                coordinator,
                member_id,
                generation_id,
            } => {
                self.group_active = true;
                self.assigned_partitions = partitions;
                self.coordinator = Some(coordinator);
                self.group_member_id = member_id;
                self.group_generation_id = generation_id;

                // Initialize offsets for newly assigned partitions
                let init_result = self.init_offsets_for_group().await;
                if let Err(ref e) = init_result {
                    warn!("Failed to init offsets for group assignment: {}", e);
                }
                // Resolve any pending subscribe() reply now that assignment and
                // offset initialization are complete.
                if let Some(reply) = self.pending_subscribe_reply.take() {
                    let _ = reply.send(init_result);
                }
                self.pending_subscribe_deadline = None;
                // Start fetching only if consumer is ready (poll/into_stream called).
                // Otherwise wait for StartPolling to avoid buffering records
                // before any receiver is reading record_tx.
                if self.consumer_ready {
                    self.try_send_fetches().await;
                }
            }
            GroupEvent::RebalanceRequired => {
                // Commit offsets before rebalancing to avoid duplicate
                // consumption in enable_at_least_once mode.
                if let Err(e) = self.do_commit().await {
                    warn!("Failed to commit offsets before rebalance: {}", e);
                }
                self.group_active = false;
                self.next_in_line_records.clear();
                self.pending_fetches.clear();
                self.assigned_partitions.clear();
                self.coordinator = None;
                // Re-trigger join via GroupCoordinator
                if let Some(ref gc) = self.group_coordinator {
                    let topics = self.subscribed_topics.clone();
                    let _ = gc.cmd_tx.send(GroupCommand::Join { topics });
                }
            }
            GroupEvent::FatalError(e) => {
                warn!("Group fatal error: {}", e);
                self.group_active = false;
                self.coordinator = None;
                self.group_member_id.clear();
                self.group_generation_id = 0;
                self.assigned_partitions.clear();
                self.next_in_line_records.clear();
                self.pending_fetches.clear();
                if let Some(reply) = self.pending_subscribe_reply.take() {
                    let _ = reply.send(Err(e.clone()));
                }
                self.pending_subscribe_deadline = None;
            }
        }
    }

    /// Future that fires when the pending subscribe() deadline elapses.
    /// Pends forever while no deadline is set (so the select! branch stays
    /// inert without allocating timers).
    async fn subscribe_deadline(deadline: Option<tokio::time::Instant>) {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending::<()>().await,
        }
    }

    // ==================================================================
    // Direct mode helpers
    // ==================================================================

    async fn assign_all_partitions(&mut self, topics: &[String]) {
        for topic in topics {
            if let Some(partitions) = self.cluster.metadata().get_partitions(topic).await {
                self.assigned_partitions.insert(topic.clone(), partitions);
            }
        }
    }

    /// Initialise offsets for newly assigned partitions (both Direct and Group).
    async fn init_offsets_for_assignment(&mut self) {
        let needs_init: Vec<(String, i32, i64)> = self
            .assigned_partitions
            .iter()
            .flat_map(|(topic, parts)| {
                parts.iter().filter_map(|p| {
                    let off = self
                        .offsets
                        .get(topic)
                        .and_then(|m| m.get(p))
                        .copied()
                        .unwrap_or(-1);
                    if off < 0 {
                        let ts = match self.config.auto_offset_reset {
                            AutoOffsetReset::Latest => LATEST_TIMESTAMP,
                            AutoOffsetReset::Earliest => EARLIEST_TIMESTAMP,
                            AutoOffsetReset::None => return None,
                        };
                        Some((topic.clone(), *p, ts))
                    } else {
                        None
                    }
                })
            })
            .collect();

        for (topic, partition, timestamp) in needs_init {
            match list_offset_for(&self.cluster, &topic, partition, timestamp).await {
                Ok(off) => {
                    self.offsets
                        .entry(topic.clone())
                        .or_default()
                        .insert(partition, off);
                    self.consumed_offsets
                        .entry(topic)
                        .or_default()
                        .insert(partition, off);
                }
                Err(e) => warn!("Failed to init offset for {}/{}: {}", topic, partition, e),
            }
        }
    }

    /// Initialise offsets for group-mode partitions (reads committed offsets).
    async fn init_offsets_for_group(&mut self) -> Result<()> {
        let committed = if let Some(coord) = self.coordinator {
            fetch_committed_offsets_raw(
                &self.cluster,
                self.config.group_id.as_deref().unwrap(),
                coord,
                &self.assigned_partitions,
            )
            .await
            .unwrap_or_default()
        } else {
            HashMap::new()
        };

        let default_offset = match self.config.auto_offset_reset {
            AutoOffsetReset::Earliest => EARLIEST_TIMESTAMP,
            AutoOffsetReset::Latest => LATEST_TIMESTAMP,
            AutoOffsetReset::None => return Err(KafkaError::NoOffsetStored),
        };

        for (topic, partitions) in &self.assigned_partitions {
            for partition in partitions {
                if self
                    .offsets
                    .get(topic.as_str())
                    .map(|m| m.contains_key(partition))
                    .unwrap_or(false)
                {
                    continue;
                }
                let raw_off = committed
                    .get(topic)
                    .and_then(|m| m.get(partition))
                    .copied()
                    .unwrap_or(default_offset);

                // Resolve negative sentinel offsets (-2=Earliest, -1=Latest) to
                // real offsets immediately. Otherwise fetchable_partitions() will
                // skip partitions with offset < 0 and no fetch will ever be sent.
                let off = if raw_off < 0 {
                    match list_offset_for(&self.cluster, topic, *partition, raw_off).await {
                        Ok(o) => o,
                        Err(e) => {
                            warn!(
                                "Failed to resolve {} for {}/{}: {}, falling back to offset 0",
                                if raw_off == -2 { "Earliest" } else { "Latest" },
                                topic,
                                partition,
                                e
                            );
                            // Fall back to offset 0 so the partition is still
                            // consumable rather than being permanently skipped.
                            0
                        }
                    }
                } else {
                    raw_off
                };

                self.offsets
                    .entry(topic.clone())
                    .or_default()
                    .insert(*partition, off);
                self.consumed_offsets
                    .entry(topic.clone())
                    .or_default()
                    .insert(*partition, off);
            }
        }
        Ok(())
    }

    // ==================================================================
    // Fetch scheduling
    // ==================================================================

    fn fetchable_partitions(&self) -> Vec<(String, i32, i64)> {
        self.assigned_partitions
            .iter()
            .flat_map(|(topic, parts)| {
                parts.iter().filter_map(|p| {
                    let tp = (topic.clone(), *p);
                    if self.pending_fetches.contains(&tp) {
                        return None;
                    }
                    // A cursor in next_in_line_records means we already have a
                    // batch of records for this partition. Sending another fetch
                    // would request data at the NEXT offset (past the current
                    // batch), which usually returns empty — and each empty fetch
                    // wastes max_wait_ms (default 500ms). Instead, keep draining
                    // the existing cursor until exhausted.
                    if self.next_in_line_records.contains_key(&tp) {
                        return None;
                    }
                    let offset = self.offsets.get(topic).and_then(|m| m.get(p)).copied()?;
                    if offset < 0 {
                        return None;
                    }
                    Some((topic.clone(), *p, offset))
                })
            })
            .collect()
    }

    async fn build_fetch_request(
        &self,
        partitions: &[(String, i32, i64)],
        params: &FetchParams,
    ) -> Result<FetchRequest> {
        let mut topic_map: HashMap<String, Vec<(i32, i64)>> = HashMap::new();
        for (topic, partition, offset) in partitions {
            topic_map
                .entry(topic.clone())
                .or_default()
                .push((*partition, *offset));
        }

        let mut topics = Vec::with_capacity(topic_map.len());
        for (topic_name, parts) in topic_map {
            let topic_id = self
                .cluster
                .metadata()
                .get_topic(&topic_name)
                .await
                .map(|t| t.topic_id)
                .unwrap_or_else(uuid::Uuid::nil);
            let fetch_partitions: Vec<FetchPartition> = parts
                .into_iter()
                .map(|(partition, fetch_offset)| FetchPartition {
                    partition,
                    current_leader_epoch: -1,
                    fetch_offset,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: params.partition_max_bytes,
                    replica_directory_id: uuid::Uuid::nil(),
                    high_watermark: 0,
                })
                .collect();
            topics.push(FetchTopic {
                topic: topic_name,
                topic_id,
                partitions: fetch_partitions,
            });
        }

        Ok(FetchRequest {
            cluster_id: None,
            replica_id: -1,
            replica_state: Default::default(),
            max_wait_ms: params.timeout_ms,
            min_bytes: params.min_bytes,
            max_bytes: params.max_bytes,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics,
            forgotten_topics_data: vec![],
            rack_id: String::new(),
        })
    }

    async fn refresh_metadata_debounced(&mut self) -> Result<()> {
        let now = std::time::Instant::now();
        if let Some(last) = self.last_metadata_refresh
            && now.duration_since(last) < self.metadata_refresh_interval
        {
            return Ok(());
        }
        self.last_metadata_refresh = Some(now);
        self.cluster.refresh_metadata().await
    }

    async fn try_send_fetches(&mut self) -> bool {
        if self.cluster.metadata().is_expired().await {
            let _ = self.refresh_metadata_debounced().await;
        }

        let fetchable = self.fetchable_partitions();
        if fetchable.is_empty() {
            return false;
        }

        let mut by_broker: HashMap<SocketAddr, Vec<(String, i32, i64)>> = HashMap::new();
        for (topic, partition, offset) in &fetchable {
            if let Some(leader) = self
                .cluster
                .metadata()
                .get_partition_leader(topic, *partition)
                .await
            {
                by_broker
                    .entry(leader)
                    .or_default()
                    .push((topic.clone(), *partition, *offset));
            }
        }

        for (broker_addr, partitions) in by_broker {
            match self
                .build_fetch_request(&partitions, &self.fetch_params)
                .await
            {
                Ok(request) => {
                    let partitions_info: Vec<(String, i32)> =
                        partitions.iter().map(|(t, p, _)| (t.clone(), *p)).collect();
                    let task = FetchRequestTask {
                        broker_addr,
                        request,
                        partitions: partitions_info.clone(),
                    };
                    if self.fetch_task_tx.send(task).is_ok() {
                        for (topic, partition) in partitions_info {
                            self.pending_fetches.insert((topic, partition));
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to build fetch request for {}: {}", broker_addr, e);
                }
            }
        }
        true
    }

    // ==================================================================
    // Fetch result processing
    // ==================================================================

    async fn handle_fetch_result(&mut self, mut result: CompletedFetch) {
        if result.topic.is_empty()
            && !result.topic_id.is_nil()
            && let Some(name) = self
                .cluster
                .metadata()
                .get_topic_name_by_id(result.topic_id)
                .await
        {
            result.topic = name;
        }
        if result.topic.is_empty() {
            debug!(
                "Fetch result with unknown topic (topic_id={})",
                result.topic_id
            );
            return;
        }

        let tp = (result.topic.clone(), result.partition);
        self.pending_fetches.remove(&tp);

        match KafkaErrorCode::from_i16(result.error_code) {
            KafkaErrorCode::NONE => {
                if let Some(batch) = result.records
                    && !batch.records.is_empty()
                {
                    let next_offset = batch.base_offset + batch.last_offset_delta as i64 + 1;
                    if batch.is_control_batch() {
                        // Transaction markers (abort/commit) occupy offsets
                        // but carry no user data — advance the fetch position
                        // without yielding records.
                        debug!(
                            "Skipping control batch for {}/{}",
                            result.topic, result.partition
                        );
                        self.offsets
                            .entry(result.topic.clone())
                            .or_default()
                            .insert(result.partition, next_offset);
                    } else {
                        let cursor =
                            RecordBatchCursor::new(result.topic.clone(), result.partition, batch);
                        self.next_in_line_records.insert(tp, cursor);
                        self.offsets
                            .entry(result.topic.clone())
                            .or_default()
                            .insert(result.partition, next_offset);
                    }
                }
            }
            KafkaErrorCode::OFFSET_OUT_OF_RANGE => {
                warn!(
                    "OFFSET_OUT_OF_RANGE for {}/{}",
                    result.topic, result.partition
                );
                let ts = match self.config.auto_offset_reset {
                    AutoOffsetReset::Latest => LATEST_TIMESTAMP,
                    AutoOffsetReset::Earliest => EARLIEST_TIMESTAMP,
                    AutoOffsetReset::None => {
                        warn!(
                            "auto_offset_reset=None but OFFSET_OUT_OF_RANGE for {}/{}. Keeping current offset.",
                            result.topic, result.partition
                        );
                        return;
                    }
                };
                if let Ok(new_offset) =
                    list_offset_for(&self.cluster, &result.topic, result.partition, ts).await
                {
                    debug!(
                        "Reset offset for {}/{} to {}",
                        result.topic, result.partition, new_offset
                    );
                    self.offsets
                        .entry(result.topic.clone())
                        .or_default()
                        .insert(result.partition, new_offset);
                    self.consumed_offsets
                        .entry(result.topic.clone())
                        .or_default()
                        .insert(result.partition, new_offset);
                }
            }
            KafkaErrorCode::REBALANCE_IN_PROGRESS => {
                debug!(
                    "REBALANCE_IN_PROGRESS from fetch for {}/{}",
                    result.topic, result.partition
                );
            }
            code if code.code() == -1 => {
                debug!("Network error for {}/{}", result.topic, result.partition);
            }
            KafkaErrorCode::UNKNOWN_TOPIC_OR_PARTITION
            | KafkaErrorCode::LEADER_NOT_AVAILABLE
            | KafkaErrorCode::NOT_LEADER_OR_FOLLOWER
            | KafkaErrorCode::REPLICA_NOT_AVAILABLE
            | KafkaErrorCode::FENCED_LEADER_EPOCH
            | KafkaErrorCode::UNKNOWN_LEADER_EPOCH
            | KafkaErrorCode::UNKNOWN_TOPIC_ID
            | KafkaErrorCode::INCONSISTENT_TOPIC_ID => {
                debug!(
                    "Recoverable fetch error code={} for {}/{}, refreshing metadata",
                    result.error_code, result.topic, result.partition
                );
                let _ = self.refresh_metadata_debounced().await;
            }
            code => {
                warn!(
                    "Fetch error for {}/{}: {}",
                    result.topic, result.partition, code
                );
            }
        }
    }

    // ==================================================================
    // Record draining
    // ==================================================================

    async fn drain_and_send(&mut self) {
        loop {
            if self.next_in_line_records.is_empty() {
                self.try_send_fetches().await;
                return;
            }

            let max = self.config.max_poll_records;
            let mut records = Vec::with_capacity(max);
            let mut exhausted_keys = Vec::new();
            let mut made_progress = true;

            // Round-robin across all non-exhausted cursors.
            // Each outer iteration tries one record per partition until
            // max_poll_records is reached or no partition has more data.
            while records.len() < max && made_progress {
                made_progress = false;
                for (key, cursor) in &mut self.next_in_line_records {
                    if records.len() >= max {
                        break;
                    }
                    if cursor.is_exhausted() {
                        continue;
                    }
                    if let Some(record) = cursor.next() {
                        records.push(record);
                        made_progress = true;
                    }
                    if cursor.is_exhausted() {
                        exhausted_keys.push(key.clone());
                    }
                }
            }

            // Remove exhausted cursors from the map.
            for key in &exhausted_keys {
                self.next_in_line_records.remove(key);
            }
            let has_exhausted = !exhausted_keys.is_empty();
            exhausted_keys.clear();

            if records.is_empty() || self.record_tx.is_closed() {
                return;
            }

            if self.config.enable_at_least_once {
                for record in &records {
                    self.consumed_offsets
                        .entry(record.topic.clone())
                        .or_default()
                        .insert(record.partition, record.offset + 1);
                }
            }

            let _ = self.record_tx.send(records).await;

            if has_exhausted {
                self.try_send_fetches().await;
            }

            tokio::task::yield_now().await;
        }
    }

    // ==================================================================
    // Offset commit & heartbeat (group RPCs with cached state)
    // ==================================================================

    async fn do_commit(&mut self) -> Result<()> {
        if self.config.group_id.is_none() {
            return Ok(());
        }
        let commit_offsets = if self.config.enable_at_least_once {
            &self.consumed_offsets
        } else {
            &self.offsets
        };
        if commit_offsets.is_empty() {
            return Ok(());
        }
        let group_id = self.config.group_id.as_deref().unwrap();

        let topic_partitions: HashMap<String, Vec<(i32, i64)>> = commit_offsets
            .iter()
            .map(|(t, m)| (t.clone(), m.iter().map(|(p, o)| (*p, *o)).collect()))
            .collect();

        let mut topic_ids = Vec::with_capacity(topic_partitions.len());
        for topic in topic_partitions.keys() {
            let tid = self
                .cluster
                .metadata()
                .get_topic(topic)
                .await
                .map(|t| t.topic_id)
                .unwrap_or_else(uuid::Uuid::nil);
            topic_ids.push(tid);
        }

        let topics: Vec<OffsetCommitRequestTopic> = topic_partitions
            .into_iter()
            .zip(topic_ids)
            .map(|((name, partitions), topic_id)| OffsetCommitRequestTopic {
                name,
                topic_id,
                partitions: partitions
                    .into_iter()
                    .map(
                        |(partition_index, committed_offset)| OffsetCommitRequestPartition {
                            partition_index,
                            committed_offset,
                            committed_leader_epoch: -1,
                            committed_metadata: None,
                        },
                    )
                    .collect(),
            })
            .collect();

        let request = OffsetCommitRequest {
            group_id: group_id.to_string(),
            generation_id_or_member_epoch: self.group_generation_id,
            member_id: self.group_member_id.clone(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics,
        };

        let max_attempts = self.config.retries.max(1) as u32;
        let mut backoff = self.config.retry_backoff;

        for attempt in 0..max_attempts {
            let coord = match self.coordinator {
                Some(addr) => addr,
                None => match find_coordinator_raw(&self.cluster, group_id).await {
                    Ok(addr) => {
                        self.coordinator = Some(addr);
                        addr
                    }
                    Err(_) if attempt + 1 < max_attempts => {
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.mul_f32(2.0);
                        continue;
                    }
                    Err(_) => return Err(KafkaError::NoCoordinator),
                },
            };

            let response: Result<crate::protocol::OffsetCommitResponse> =
                self.cluster.send_to_broker(coord, &request).await;

            match response {
                Ok(resp) => {
                    let has_retriable_error =
                        resp.topics.iter().flat_map(|t| &t.partitions).any(|p| {
                            let code = KafkaErrorCode::from_i16(p.error_code);
                            code.is_retriable() || code == KafkaErrorCode::NOT_COORDINATOR
                        });
                    let not_coordinator = resp
                        .topics
                        .iter()
                        .flat_map(|t| &t.partitions)
                        .any(|p| p.error_code == KafkaErrorCode::NOT_COORDINATOR.code());

                    if has_retriable_error && attempt + 1 < max_attempts {
                        debug!(
                            "Commit got retriable error (attempt {}/{}), retrying...",
                            attempt + 1,
                            max_attempts
                        );
                        if not_coordinator
                            && let Ok(addr) = find_coordinator_raw(&self.cluster, group_id).await
                        {
                            self.coordinator = Some(addr);
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.mul_f32(2.0);
                        continue;
                    }

                    for tr in resp.topics {
                        for pr in tr.partitions {
                            if pr.error_code != 0 {
                                return Err(KafkaError::OffsetCommitError(
                                    KafkaErrorCode::from_i16(pr.error_code),
                                ));
                            }
                        }
                    }
                    return Ok(());
                }
                Err(KafkaError::ConnectionClosed | KafkaError::Io(_))
                    if attempt + 1 < max_attempts =>
                {
                    debug!(
                        "Commit connection failed (attempt {}/{}), retrying...",
                        attempt + 1,
                        max_attempts
                    );
                    if let Ok(addr) = find_coordinator_raw(&self.cluster, group_id).await {
                        self.coordinator = Some(addr);
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.mul_f32(2.0);
                }
                Err(e) => return Err(e),
            }
        }
        Err(KafkaError::NoCoordinator)
    }

    async fn send_leave_group(&self) -> Result<()> {
        if self.group_member_id.is_empty() {
            return Ok(());
        }
        let Some(coord) = self.coordinator else {
            return Ok(());
        };
        let group_id = self.config.group_id.as_deref().unwrap();
        use crate::protocol::leave_group_request::MemberIdentity;
        let request = LeaveGroupRequest {
            group_id: group_id.to_string(),
            member_id: self.group_member_id.clone(),
            members: vec![MemberIdentity {
                member_id: self.group_member_id.clone(),
                group_instance_id: None,
                reason: None,
            }],
        };
        let response: crate::protocol::LeaveGroupResponse =
            self.cluster.send_to_broker(coord, &request).await?;
        if response.error_code != 0 {
            warn!("LeaveGroup failed: error {}", response.error_code);
        }
        Ok(())
    }

    async fn send_heartbeat_raw(&self) -> Result<()> {
        if self.group_member_id.is_empty() {
            return Ok(());
        }
        let Some(group_id) = self.config.group_id.as_deref() else {
            return Ok(());
        };
        // Fallback: find coordinator fresh if not cached
        let coord = match self.coordinator {
            Some(addr) => addr,
            None => find_coordinator_raw(&self.cluster, group_id).await?,
        };
        let request = HeartbeatRequest {
            group_id: group_id.to_string(),
            generation_id: self.group_generation_id,
            member_id: self.group_member_id.clone(),
            group_instance_id: None,
        };
        let response: crate::protocol::HeartbeatResponse =
            self.cluster.send_to_broker(coord, &request).await?;
        crate::consumer::util::map_heartbeat_error(
            response.error_code,
            self.group_generation_id,
            &self.group_member_id,
        )
    }
}
