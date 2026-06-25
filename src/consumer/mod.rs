//! Consumer - high-level Kafka message consumer
//!
//! Uses a **Facade pattern**: [`Consumer`] provides a simplified core API
//! (`subscribe`, `poll`, `close`), while [`OffsetHandle`] and [`GroupHandle`]
//! expose advanced offset / group operations.
//!
//! Internally, a reactor pattern drives group membership, continuous fetching,
//! heartbeats, and offset commits. Fetched records are pushed into an mpsc
//! channel; `poll()` simply reads from that channel.

use bytes::Bytes;
use kafka_client_protocol::Message;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use tokio::time::interval;
use tracing::{debug, warn};

use crate::cluster::ClusterClient;
use crate::error::{KafkaError, Result};
use crate::protocol::{
    ConsumerProtocolAssignment, FetchPartition, FetchRequest, FetchResponse, FetchTopic,
    FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest, JoinGroupRequest,
    JoinGroupRequestProtocol, JoinGroupResponse, LeaveGroupRequest, ListOffsetsPartition,
    ListOffsetsRequest, ListOffsetsTopic, OffsetCommitRequest, OffsetCommitRequestPartition,
    OffsetCommitRequestTopic, OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
    OffsetFetchRequestTopics, SyncGroupRequest, SyncGroupRequestAssignment, SyncGroupResponse,
    TopicPartition,
};

// ---------------------------------------------------------------------------
// Reactor states
// ---------------------------------------------------------------------------
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReactorState {
    /// Waiting for initial subscribe command
    Init,
    /// Attempting to join consumer group
    Joining,
    /// Successfully joined; continuously fetching and heartbeating
    Fetching,
    /// Need to rejoin (rebalance or heartbeat failure)
    Rebalancing,
}

// ---------------------------------------------------------------------------
// Commands sent to the reactor via channel
// ---------------------------------------------------------------------------
enum ConsumerCommand {
    /// Subscribe/change topic list
    Subscribe { topics: Vec<String> },
}

// ---------------------------------------------------------------------------
// Fetch parameters
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
struct FetchParams {
    timeout_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    partition_max_bytes: i32,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Consumer configuration
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub group_id: String,
    pub auto_commit: bool,
    pub auto_commit_interval: Duration,
    pub auto_offset_reset: AutoOffsetReset,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub partition_max_bytes: i32,
    pub max_wait: Duration,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub partition_assignment_strategy: PartitionAssignmentStrategy,
}

/// Offset reset strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoOffsetReset {
    Earliest,
    Latest,
    None,
}

/// Partition assignment strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionAssignmentStrategy {
    Range,
    RoundRobin,
    CooperativeSticky,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: format!("{}-consumer", crate::NAME),
            auto_commit: true,
            auto_commit_interval: Duration::from_secs(5),
            auto_offset_reset: AutoOffsetReset::Latest,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            partition_max_bytes: 1024 * 1024,
            max_wait: Duration::from_millis(500),
            session_timeout: Duration::from_secs(10),
            rebalance_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(3),
            partition_assignment_strategy: PartitionAssignmentStrategy::Range,
        }
    }
}

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------

/// Message header
#[derive(Debug, Clone)]
pub struct Header {
    pub key: String,
    pub value: Bytes,
}

/// Consumer record
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<Header>,
}

// ---------------------------------------------------------------------------
// Shared group state
// ---------------------------------------------------------------------------
#[derive(Debug, Clone, Default)]
struct GroupState {
    member_id: String,
    generation_id: i32,
    leader: String,
    protocol_name: Option<String>,
    assigned_partitions: HashMap<String, Vec<i32>>,
}

// ---------------------------------------------------------------------------
// Shared state bag — avoids repeating Arc fields across Consumer & handles
// ---------------------------------------------------------------------------
#[derive(Clone)]
struct SharedConsumerState {
    cluster: Arc<ClusterClient>,
    config: ConsumerConfig,
    offsets: Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    group_state: Arc<RwLock<GroupState>>,
    coordinator: Arc<RwLock<Option<std::net::SocketAddr>>>,
    running: Arc<std::sync::atomic::AtomicBool>,
}

// ===========================================================================
// OffsetHandle — offset management (commit, get, set)
// ===========================================================================

/// Handle for offset management operations.
///
/// Obtained via [`Consumer::offsets`].
pub struct OffsetHandle {
    shared: SharedConsumerState,
}

impl OffsetHandle {
    /// Commit current offsets to the coordinator.
    pub async fn commit(&self) -> Result<()> {
        let (generation_id, member_id) = {
            let gs = self.shared.group_state.read().await;
            (
                gs.generation_id,
                if gs.member_id.is_empty() {
                    None
                } else {
                    Some(gs.member_id.clone())
                },
            )
        };
        do_commit(
            &self.shared.cluster,
            &self.shared.config.group_id,
            &self.shared.offsets,
            &self.shared.coordinator,
            generation_id,
            member_id,
        )
        .await
    }

    /// Get current offset for a topic/partition.
    pub fn get(&self, topic: &str, partition: i32) -> Option<i64> {
        let offsets = self.shared.offsets.blocking_read();
        offsets.get(topic)?.get(&partition).copied()
    }

    /// Manually set offset (will be used on next fetch).
    pub async fn set(&self, topic: &str, partition: i32, offset: i64) {
        let mut offsets = self.shared.offsets.write().await;
        offsets
            .entry(topic.to_string())
            .or_insert_with(HashMap::new)
            .insert(partition, offset);
    }
}

// ===========================================================================
// GroupHandle — group coordination (heartbeat, leave, assignment)
// ===========================================================================

/// Handle for consumer group operations.
///
/// Obtained via [`Consumer::group`].
pub struct GroupHandle {
    shared: SharedConsumerState,
}

impl GroupHandle {
    /// Send heartbeat to the coordinator.
    pub async fn heartbeat(&self) -> Result<()> {
        let group_state = self.shared.group_state.read().await;
        if group_state.member_id.is_empty() {
            return Ok(());
        }
        let coordinator =
            find_coordinator(&self.shared.cluster, &self.shared.config.group_id, &self.shared.coordinator).await?;
        let request = HeartbeatRequest {
            group_id: self.shared.config.group_id.clone(),
            generation_id: group_state.generation_id,
            member_id: group_state.member_id.clone(),
            group_instance_id: None,
        };
        let response: crate::protocol::HeartbeatResponse =
            self.shared.cluster.send_to_broker(coordinator, &request).await?;
        if response.error_code != 0 {
            if response.error_code == 27 {
                warn!("Heartbeat indicates rebalance needed, rejoining group");
                *self.shared.coordinator.write().await = None;
                return Err(KafkaError::Protocol("Rebalance required".to_string()));
            }
            return Err(KafkaError::Protocol(format!(
                "Heartbeat failed: error {}",
                response.error_code
            )));
        }
        Ok(())
    }

    /// Leave the consumer group.
    pub async fn leave(&self) -> Result<()> {
        let member_id = {
            let gs = self.shared.group_state.read().await;
            gs.member_id.clone()
        };
        if member_id.is_empty() {
            return Ok(());
        }
        if self.shared.config.auto_commit {
            if let Err(e) = self.commit().await {
                warn!("Commit before leave failed: {}", e);
            }
        }
        let coordinator =
            find_coordinator(&self.shared.cluster, &self.shared.config.group_id, &self.shared.coordinator).await?;
        use crate::protocol::leave_group_request::MemberIdentity;
        let request = LeaveGroupRequest {
            group_id: self.shared.config.group_id.clone(),
            member_id: member_id.clone(),
            members: vec![MemberIdentity {
                member_id,
                group_instance_id: None,
                reason: None,
            }],
        };
        let response: crate::protocol::LeaveGroupResponse =
            self.shared.cluster.send_to_broker(coordinator, &request).await?;
        if response.error_code != 0 {
            warn!("LeaveGroup failed: error {}", response.error_code);
        }
        Ok(())
    }

    /// Get current assigned partitions.
    pub async fn assignment(&self) -> HashMap<String, Vec<i32>> {
        self.shared.group_state.read().await.assigned_partitions.clone()
    }

    /// Convenience: commit offsets (same as [`OffsetHandle::commit`]).
    pub async fn commit(&self) -> Result<()> {
        let (generation_id, member_id) = {
            let gs = self.shared.group_state.read().await;
            (
                gs.generation_id,
                if gs.member_id.is_empty() {
                    None
                } else {
                    Some(gs.member_id.clone())
                },
            )
        };
        do_commit(
            &self.shared.cluster,
            &self.shared.config.group_id,
            &self.shared.offsets,
            &self.shared.coordinator,
            generation_id,
            member_id,
        )
        .await
    }
}

// ===========================================================================
// Consumer — simplified facade (subscribe / poll / close)
// ===========================================================================

/// High-level Kafka Consumer.
///
/// Uses a **Facade pattern**:
/// - [`Consumer`] provides the essential message-consuming lifecycle:
///   [`subscribe`](Self::subscribe), [`poll`](Self::poll), [`close`](Self::close).
/// - Advanced operations (offset management, group coordination) are accessible
///   via [`offsets`](Self::offsets) and [`group`](Self::group) handles.
///
/// Internally a reactor task manages group membership, continuous fetching,
/// heartbeats, and offset commits — users never deal with state machines.
pub struct Consumer {
    shared: SharedConsumerState,
    /// Channel receiving records pushed by the reactor
    record_rx: mpsc::UnboundedReceiver<Vec<ConsumerRecord>>,
    /// Channel sending commands to the reactor
    command_tx: mpsc::UnboundedSender<ConsumerCommand>,
    /// Subscribed topics (for simple consumers without group_id)
    subscribed_topics: Vec<String>,
    /// Offset handle
    offset_handle: OffsetHandle,
    /// Group handle
    group_handle: GroupHandle,
}

impl Consumer {
    /// Create a new Consumer and start the internal reactor background task.
    pub(crate) fn new(cluster: Arc<ClusterClient>, config: ConsumerConfig) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (record_tx, record_rx) = mpsc::unbounded_channel();

        let shared = SharedConsumerState {
            offsets: Arc::new(RwLock::new(HashMap::new())),
            group_state: Arc::new(RwLock::new(GroupState::default())),
            coordinator: Arc::new(RwLock::new(None)),
            running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            cluster: cluster.clone(),
            config: config.clone(),
        };

        let consumer = Self {
            offset_handle: OffsetHandle {
                shared: shared.clone(),
            },
            group_handle: GroupHandle {
                shared: shared.clone(),
            },
            shared,
            record_rx,
            command_tx,
            subscribed_topics: Vec::new(),
        };

        // Start the reactor in background
        if !config.group_id.is_empty() {
            spawn_reactor(
                cluster,
                config,
                consumer.shared.offsets.clone(),
                consumer.shared.group_state.clone(),
                consumer.shared.coordinator.clone(),
                consumer.shared.running.clone(),
                record_tx,
                command_rx,
            );
        }

        consumer
    }

    // -----------------------------------------------------------------------
    // Public API — core lifecycle
    // -----------------------------------------------------------------------

    /// Subscribe to topics.
    ///
    /// For group consumers (`group_id` not empty), this triggers a rebalance.
    /// The reactor will join the group, get assigned partitions, and start
    /// fetching — all transparently. `poll()` will return data once available.
    ///
    /// For simple consumers (empty `group_id`), offsets are initialized
    /// immediately based on `auto_offset_reset`.
    pub async fn subscribe(&mut self, topics: Vec<String>) -> Result<()> {
        if self.shared.config.group_id.is_empty() {
            self.subscribed_topics = topics.clone();
            self.init_offsets_simple(topics).await?;
        } else {
            let _ = self.command_tx.send(ConsumerCommand::Subscribe {
                topics,
            });
        }
        Ok(())
    }

    /// Poll for messages.
    ///
    /// Returns any buffered records immediately, or waits up to `timeout_ms`
    /// for new records from the reactor's continuous fetch loop.
    ///
    /// For **group consumers**: the reactor handles joining, offset resolution,
    /// heartbeats, rebalances, and continuous fetching. `poll()` simply reads
    /// from the internal channel. Empty results mean no new data arrived
    /// within the timeout.
    ///
    /// For **simple consumers** (no group_id): fetches directly.
    pub async fn poll(&mut self, timeout_ms: i32) -> Result<Vec<ConsumerRecord>> {
        if self.shared.config.group_id.is_empty() {
            return self.poll_simple(timeout_ms).await;
        }
        self.poll_group(timeout_ms).await
    }

    /// Close the consumer.
    pub async fn close(&self) -> Result<()> {
        self.shared
            .running
            .store(false, std::sync::atomic::Ordering::Relaxed);
        if !self.shared.config.group_id.is_empty() {
            if let Err(e) = self.group_handle.leave().await {
                warn!("Error leaving group: {}", e);
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Sub-handle accessors
    // -----------------------------------------------------------------------

    /// Access offset management operations.
    pub fn offsets(&self) -> &OffsetHandle {
        &self.offset_handle
    }

    /// Access consumer group operations.
    pub fn group(&self) -> &GroupHandle {
        &self.group_handle
    }

    // -----------------------------------------------------------------------
    // Simple consumer (no group_id): poll-driven fetch
    // -----------------------------------------------------------------------

    async fn poll_simple(&mut self, timeout_ms: i32) -> Result<Vec<ConsumerRecord>> {
        let fetch_targets: Vec<(String, i32, i64)> = {
            let offsets = self.shared.offsets.read().await;
            let mut targets = Vec::new();
            for topic in &self.subscribed_topics {
                if let Some(partitions) = self.shared.cluster.metadata().get_partitions(topic).await {
                    for partition in partitions {
                        let offset = offsets
                            .get(topic)
                            .and_then(|t| t.get(&partition))
                            .copied()
                            .unwrap_or(-1);
                        if offset >= 0 {
                            targets.push((topic.clone(), partition, offset));
                        }
                    }
                }
            }
            targets
        };

        if fetch_targets.is_empty() {
            lazy_init_offsets_simple(
                &self.shared.cluster,
                &self.shared.offsets,
                self.shared.config.auto_offset_reset,
            )
            .await;
            let targets = collect_fetch_targets_simple(
                &self.shared.offsets,
                &self.subscribed_topics,
                &self.shared.cluster,
            )
            .await;
            if targets.is_empty() {
                return Ok(Vec::new());
            }
            return execute_fetch(
                &self.shared.cluster,
                &self.shared.offsets,
                &self.shared.config,
                targets,
                timeout_ms,
            )
            .await;
        }

        execute_fetch(
            &self.shared.cluster,
            &self.shared.offsets,
            &self.shared.config,
            fetch_targets,
            timeout_ms,
        )
        .await
    }

    // -----------------------------------------------------------------------
    // Group consumer: channel-based poll
    // -----------------------------------------------------------------------

    async fn poll_group(&mut self, timeout_ms: i32) -> Result<Vec<ConsumerRecord>> {
        let timeout = Duration::from_millis(timeout_ms as u64);

        // First drain any already buffered records
        let mut records = Vec::new();
        while let Ok(batch) = self.record_rx.try_recv() {
            records.extend(batch);
        }
        if !records.is_empty() {
            return Ok(records);
        }

        // Wait for new records with timeout
        match tokio::time::timeout(timeout, self.record_rx.recv()).await {
            Ok(Some(batch)) => {
                records = batch;
                while let Ok(batch) = self.record_rx.try_recv() {
                    records.extend(batch);
                }
                Ok(records)
            }
            Ok(None) => Ok(Vec::new()),
            Err(_) => Ok(Vec::new()),
        }
    }

    // -----------------------------------------------------------------------
    // Helpers (simple consumer)
    // -----------------------------------------------------------------------

    async fn init_offsets_simple(&self, topics: Vec<String>) -> Result<()> {
        let mut needs_init: Vec<(String, i32)> = Vec::new();
        {
            let mut offsets = self.shared.offsets.write().await;
            for topic in &topics {
                let partitions = self
                    .shared
                    .cluster
                    .metadata()
                    .get_partitions(topic)
                    .await
                    .ok_or_else(|| KafkaError::TopicNotFound(topic.clone()))?;
                let topic_offsets = offsets.entry(topic.clone()).or_insert_with(HashMap::new);
                for partition in &partitions {
                    if !topic_offsets.contains_key(partition) {
                        topic_offsets.insert(*partition, -1);
                        needs_init.push((topic.clone(), *partition));
                    }
                }
            }
        }
        for (topic, partition) in needs_init {
            let timestamp = match self.shared.config.auto_offset_reset {
                AutoOffsetReset::Latest => -1i64,
                AutoOffsetReset::Earliest => -2i64,
                AutoOffsetReset::None => return Err(KafkaError::NoOffsetStored),
            };
            let offset = list_offset_for(&self.shared.cluster, &topic, partition, timestamp).await?;
            let mut offsets = self.shared.offsets.write().await;
            offsets
                .entry(topic)
                .or_insert_with(HashMap::new)
                .insert(partition, offset);
        }
        Ok(())
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.shared
            .running
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

// ===========================================================================
// Reactor — background task for group consumers
// ===========================================================================

/// Spawn the reactor background task for group consumers.
fn spawn_reactor(
    cluster: Arc<ClusterClient>,
    config: ConsumerConfig,
    offsets: Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    group_state: Arc<RwLock<GroupState>>,
    coordinator: Arc<RwLock<Option<std::net::SocketAddr>>>,
    running: Arc<std::sync::atomic::AtomicBool>,
    record_tx: mpsc::UnboundedSender<Vec<ConsumerRecord>>,
    mut command_rx: mpsc::UnboundedReceiver<ConsumerCommand>,
) {
    tokio::spawn(async move {
        let mut state = ReactorState::Init;
        let mut subscribed_topics: Vec<String> = Vec::new();

        loop {
            if !running.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            match state {
                ReactorState::Init => {
                    match command_rx.recv().await {
                        Some(ConsumerCommand::Subscribe { topics }) => {
                            subscribed_topics = topics;
                            state = ReactorState::Joining;
                        }
                        None => break,
                    }
                }

                ReactorState::Joining => {
                    match join_group(
                        &cluster,
                        &config,
                        &offsets,
                        &group_state,
                        &coordinator,
                        &subscribed_topics,
                    )
                    .await
                    {
                        Ok(()) => {
                            lazy_init_offsets_group(
                                &cluster,
                                &offsets,
                                &group_state,
                                config.auto_offset_reset,
                            )
                            .await;
                            state = ReactorState::Fetching;
                        }
                        Err(e) => {
                            warn!("Join group failed: {:?}, will retry", e);
                            {
                                let mut gs = group_state.write().await;
                                gs.assigned_partitions.clear();
                                gs.member_id.clear();
                                *coordinator.write().await = None;
                            }
                            tokio::time::sleep(Duration::from_millis(
                                1000 + (rand::random::<u64>() % 1000),
                            ))
                            .await;
                        }
                    }
                }

                ReactorState::Fetching => {
                    let mut commit_interval = interval(config.auto_commit_interval);
                    let mut heartbeat_interval = interval(config.heartbeat_interval);
                    let fetch_params = FetchParams {
                        timeout_ms: config.max_wait.as_millis() as i32,
                        min_bytes: config.min_bytes,
                        max_bytes: config.max_bytes,
                        partition_max_bytes: config.partition_max_bytes,
                    };
                    let mut rejoin_flag = false;

                    loop {
                        if !running.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }

                        tokio::select! {
                            _ = commit_interval.tick() => {
                                if config.auto_commit {
                                    let (generation_id, member_id) = {
                                        let gs = group_state.read().await;
                                        (gs.generation_id, if gs.member_id.is_empty() { None } else { Some(gs.member_id.clone()) })
                                    };
                                    if let Err(e) = do_commit(
                                        &cluster,
                                        &config.group_id,
                                        &offsets,
                                        &coordinator,
                                        generation_id,
                                        member_id,
                                    ).await {
                                        debug!("Auto commit failed: {}", e);
                                    }
                                }
                            }
                            _ = heartbeat_interval.tick() => {
                                match background_heartbeat(
                                    &cluster,
                                    &config.group_id,
                                    &group_state,
                                    &coordinator,
                                ).await {
                                    Ok(()) => {}
                                    Err(e) => {
                                        debug!("Heartbeat failed: {}", e);
                                        rejoin_flag = true;
                                    }
                                }
                            }
                            cmd = command_rx.recv() => {
                                match cmd {
                                    Some(ConsumerCommand::Subscribe { topics }) => {
                                        subscribed_topics = topics;
                                        rejoin_flag = true;
                                    }
                                    None => break,
                                }
                            }
                            _ = async {
                                let targets = collect_fetch_targets_group(
                                    &offsets,
                                    &group_state,
                                ).await;
                                if targets.is_empty() {
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                    return;
                                }
                                match execute_fetch(&cluster, &offsets, &config, targets, fetch_params.timeout_ms).await {
                                    Ok(records) => {
                                        if !records.is_empty() && !record_tx.is_closed() {
                                            let _ = record_tx.send(records);
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Fetch in reactor failed: {}", e);
                                    }
                                }
                            } => {}
                        }

                        if rejoin_flag {
                            break;
                        }
                    }

                    state = ReactorState::Rebalancing;
                    if !running.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }

                ReactorState::Rebalancing => {
                    {
                        let mut gs = group_state.write().await;
                        gs.assigned_partitions.clear();
                        gs.member_id.clear();
                        *coordinator.write().await = None;
                    }
                    state = ReactorState::Joining;
                }
            }
        }

        // Cleanup: send LeaveGroup
        if !config.group_id.is_empty() {
            let coord_addr = { *coordinator.read().await };
            if let Some(coord_addr) = coord_addr {
                let member_id = { group_state.read().await.member_id.clone() };
                if !member_id.is_empty() {
                    use crate::protocol::leave_group_request::MemberIdentity;
                    let request = LeaveGroupRequest {
                        group_id: config.group_id.clone(),
                        member_id: member_id.clone(),
                        members: vec![MemberIdentity {
                            member_id,
                            group_instance_id: None,
                            reason: None,
                        }],
                    };
                    let leave_res: std::result::Result<crate::protocol::LeaveGroupResponse, _> =
                        cluster.send_to_broker(coord_addr, &request).await;
                    if let Err(e) = leave_res {
                        debug!("LeaveGroup on shutdown failed: {}", e);
                    }
                }
            }
        }
    });
}

// ===========================================================================
// Free functions — shared by Consumer, handles, and reactor
// ===========================================================================

/// Find the coordinator address for a consumer group.
async fn find_coordinator(
    cluster: &Arc<ClusterClient>,
    group_id: &str,
    coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
) -> Result<std::net::SocketAddr> {
    let mut coord = coordinator.write().await;
    if let Some(addr) = *coord {
        return Ok(addr);
    }
    let request = FindCoordinatorRequest {
        key: group_id.to_string(),
        key_type: 0,
        coordinator_keys: vec![group_id.to_string()],
    };
    let response: FindCoordinatorResponse = cluster.send_to_any_broker(&request).await?;
    if response.error_code != 0 {
        return Err(KafkaError::NoCoordinator);
    }
    if let Some(coord) = response.coordinators.first() {
        if coord.error_code != 0 {
            return Err(KafkaError::NoCoordinator);
        }
    }
    let (host, port) = if !response.host.is_empty() {
        (response.host.clone(), response.port)
    } else if let Some(coord) = response.coordinators.first() {
        (coord.host.clone(), coord.port)
    } else {
        return Err(KafkaError::NoCoordinator);
    };
    let addr: std::net::SocketAddr = format!("{}:{}", host, port)
        .to_socket_addrs()
        .map_err(|_| KafkaError::NoCoordinator)?
        .next()
        .ok_or(KafkaError::NoCoordinator)?;
    *coord = Some(addr);
    Ok(addr)
}

/// Resolve unresolved offsets (negative values) via ListOffsets for a group consumer.
async fn lazy_init_offsets_group(
    cluster: &Arc<ClusterClient>,
    offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    group_state: &Arc<RwLock<GroupState>>,
    auto_offset_reset: AutoOffsetReset,
) {
    let needs_init: Vec<(String, i32, i64)> = {
        let gs = group_state.read().await;
        let off = offsets.read().await;
        let mut result = Vec::new();
        for (topic, partitions) in &gs.assigned_partitions {
            for partition in partitions {
                let offset = off
                    .get(topic)
                    .and_then(|t| t.get(partition))
                    .copied()
                    .unwrap_or(-1);
                if offset < 0 {
                    let timestamp = match auto_offset_reset {
                        AutoOffsetReset::Latest => -1i64,
                        AutoOffsetReset::Earliest => -2i64,
                        AutoOffsetReset::None => continue,
                    };
                    result.push((topic.clone(), *partition, timestamp));
                }
            }
        }
        result
    };

    for (topic, partition, timestamp) in needs_init {
        match list_offset_for(cluster, &topic, partition, timestamp).await {
            Ok(real_offset) => {
                let mut off = offsets.write().await;
                off.entry(topic.clone())
                    .or_insert_with(HashMap::new)
                    .insert(partition, real_offset);
            }
            Err(e) => {
                warn!("Failed to resolve offset for {}/{}: {}", topic, partition, e);
            }
        }
    }
}

/// Collect fetch targets (partitions with resolved offsets) for a group consumer.
async fn collect_fetch_targets_group(
    offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    group_state: &Arc<RwLock<GroupState>>,
) -> Vec<(String, i32, i64)> {
    let gs = group_state.read().await;
    let off = offsets.read().await;

    let assigned = if !gs.assigned_partitions.is_empty() {
        gs.assigned_partitions.clone()
    } else {
        return Vec::new();
    };

    let mut targets = Vec::new();
    for (topic, partitions) in assigned {
        for partition in partitions {
            let offset = off
                .get(&topic)
                .and_then(|t| t.get(&partition))
                .copied()
                .unwrap_or(-1);
            if offset >= 0 {
                targets.push((topic.clone(), partition, offset));
            }
        }
    }
    targets
}

/// Lazily initialize offsets for a simple consumer from metadata / auto_offset_reset.
async fn lazy_init_offsets_simple(
    cluster: &Arc<ClusterClient>,
    offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    auto_offset_reset: AutoOffsetReset,
) {
    let topics_to_init: Vec<(String, i32)> = {
        let off = offsets.read().await;
        off.iter()
            .flat_map(|(topic, partitions)| {
                partitions
                    .iter()
                    .filter(|&(_, &v)| v < 0)
                    .map(|(p, _)| (topic.clone(), *p))
                    .collect::<Vec<_>>()
            })
            .collect()
    };

    for (topic, partition) in topics_to_init {
        let timestamp = match auto_offset_reset {
            AutoOffsetReset::Earliest => -2i64,
            AutoOffsetReset::Latest => -1i64,
            AutoOffsetReset::None => continue,
        };
        if let Err(e) = list_offset_for(cluster, &topic, partition, timestamp).await {
            warn!("Failed to init offset for {}@{}: {}", topic, partition, e);
        }
    }
}

/// Collect fetch targets for a simple consumer.
async fn collect_fetch_targets_simple(
    offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    subscribed_topics: &[String],
    cluster: &Arc<ClusterClient>,
) -> Vec<(String, i32, i64)> {
    let off = offsets.read().await;
    let mut targets = Vec::new();
    for topic in subscribed_topics {
        if let Some(partitions) = cluster.metadata().get_partitions(topic).await {
            for partition in partitions {
                let offset = off
                    .get(topic)
                    .and_then(|t| t.get(&partition))
                    .copied()
                    .unwrap_or(-1);
                if offset >= 0 {
                    targets.push((topic.clone(), partition, offset));
                }
            }
        }
    }
    targets
}

/// Join consumer group: find coordinator, send JoinGroup/SyncGroup.
async fn join_group(
    cluster: &Arc<ClusterClient>,
    config: &ConsumerConfig,
    offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    group_state: &Arc<RwLock<GroupState>>,
    coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
    topics: &[String],
) -> Result<()> {
    // 1. Find coordinator
    let coord_addr = {
        let request = FindCoordinatorRequest {
            key: config.group_id.clone(),
            key_type: 0,
            coordinator_keys: vec![config.group_id.clone()],
        };
        let response: FindCoordinatorResponse = cluster.send_to_any_broker(&request).await?;
        if response.error_code == 15 || response.error_code != 0 {
            return Err(KafkaError::NoCoordinator);
        }
        let (host, port) = if !response.host.is_empty() {
            (response.host.clone(), response.port)
        } else if let Some(coord) = response.coordinators.first() {
            if coord.error_code != 0 {
                return Err(KafkaError::NoCoordinator);
            }
            (coord.host.clone(), coord.port)
        } else {
            return Err(KafkaError::NoCoordinator);
        };
        format!("{}:{}", host, port)
            .to_socket_addrs()
            .map_err(|_| KafkaError::NoCoordinator)?
            .next()
            .ok_or(KafkaError::NoCoordinator)?
    };
    *coordinator.write().await = Some(coord_addr);

    // 2. Build protocol metadata
    let protocol_metadata = {
        let mut buf = bytes::BytesMut::new();
        use bytes::BufMut;
        buf.put_i16(2);
        buf.put_i32(topics.len() as i32);
        for t in topics {
            buf.put_i16(t.len() as i16);
            buf.put_slice(t.as_bytes());
        }
        buf.put_i32(-1);
        buf.put_i32(0);
        buf.put_i32(-1);
        buf.freeze()
    };

    // 3. JoinGroup loop (with MEMBER_ID_REQUIRED handling)
    let mut member_id = String::new();
    for _join_attempt in 0..10u32 {
        let request = JoinGroupRequest {
            group_id: config.group_id.clone(),
            session_timeout_ms: config.session_timeout.as_millis() as i32,
            rebalance_timeout_ms: config.rebalance_timeout.as_millis() as i32,
            member_id: member_id.clone(),
            group_instance_id: None,
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: match config.partition_assignment_strategy {
                    PartitionAssignmentStrategy::Range => "range".to_string(),
                    PartitionAssignmentStrategy::RoundRobin => "roundrobin".to_string(),
                    PartitionAssignmentStrategy::CooperativeSticky => {
                        "cooperative-sticky".to_string()
                    }
                },
                metadata: protocol_metadata.clone(),
            }],
            reason: None,
        };

        let response: JoinGroupResponse = cluster.send_to_broker(coord_addr, &request).await?;

        if response.error_code == 0 {
            let generation_id = response.generation_id;
            let is_leader = response.leader == response.member_id;
            let protocol_name = response.protocol_name.clone();
            let new_member_id = response.member_id.clone();

            let assignment_bytes = if is_leader {
                compute_assignment(topics, &response, cluster, config.partition_assignment_strategy)
                    .await?
            } else {
                Bytes::new()
            };

            let sync_request = SyncGroupRequest {
                group_id: config.group_id.clone(),
                generation_id,
                member_id: new_member_id.clone(),
                group_instance_id: None,
                protocol_type: Some("consumer".to_string()),
                protocol_name: protocol_name.clone(),
                assignments: if is_leader {
                    response
                        .members
                        .iter()
                        .map(|m| SyncGroupRequestAssignment {
                            member_id: m.member_id.clone(),
                            assignment: if m.member_id == response.leader {
                                assignment_bytes.clone()
                            } else {
                                Bytes::new()
                            },
                        })
                        .collect()
                } else {
                    vec![]
                },
            };

            let sync_response: SyncGroupResponse =
                cluster.send_to_broker(coord_addr, &sync_request).await?;
            if sync_response.error_code != 0 {
                return Err(KafkaError::Protocol(format!(
                    "SyncGroup error: {}",
                    sync_response.error_code
                )));
            }

            let assignment = {
                let mut buf_data = sync_response.assignment;
                let assignment_result = ConsumerProtocolAssignment::decode(&mut buf_data, 0)
                    .map_err(|e| KafkaError::Protocol(e.to_string()))?;
                let mut result = HashMap::new();
                for tp in assignment_result.assigned_partitions {
                    result.insert(tp.topic, tp.partitions);
                }
                result
            };

            {
                let mut gs = group_state.write().await;
                gs.member_id = new_member_id;
                gs.generation_id = generation_id;
                gs.leader = response.leader;
                gs.protocol_name = protocol_name;
                gs.assigned_partitions = assignment.clone();
            }

            init_offsets_for_group(
                cluster,
                &config.group_id,
                offsets,
                coordinator,
                &assignment,
                config.auto_offset_reset,
            )
            .await?;

            debug!("Joined group, assigned partitions={:?}", assignment);
            return Ok(());
        }

        if response.error_code == 79 {
            if response.member_id.is_empty() || response.member_id == member_id {
                return Err(KafkaError::Protocol(
                    "MEMBER_ID_REQUIRED with no new member_id".to_string(),
                ));
            }
            warn!(
                "MEMBER_ID_REQUIRED, retrying with member_id={}",
                response.member_id
            );
            member_id = response.member_id.clone();
            continue;
        }

        return Err(KafkaError::Protocol(format!(
            "JoinGroup error: {}",
            response.error_code
        )));
    }

    Err(KafkaError::Protocol(
        "JoinGroup retry exhausted".to_string(),
    ))
}

/// Compute partition assignment for the group leader.
async fn compute_assignment(
    topics: &[String],
    join_response: &JoinGroupResponse,
    cluster: &Arc<ClusterClient>,
    strategy: PartitionAssignmentStrategy,
) -> Result<Bytes> {
    let all_members: Vec<&str> = join_response
        .members
        .iter()
        .map(|m| m.member_id.as_str())
        .collect();

    if all_members.is_empty() {
        return Ok(Bytes::new());
    }

    let mut assignments: HashMap<String, Vec<TopicPartition>> = HashMap::new();
    for member_id in &all_members {
        assignments.insert(member_id.to_string(), Vec::new());
    }

    for topic in topics {
        let partitions = cluster
            .metadata()
            .get_partitions(topic)
            .await
            .unwrap_or_default();

        match strategy {
            PartitionAssignmentStrategy::Range | PartitionAssignmentStrategy::CooperativeSticky => {
                let n = all_members.len();
                let per = partitions.len() / n;
                let rem = partitions.len() % n;
                let mut idx = 0;
                for (i, member_id) in all_members.iter().enumerate() {
                    let count = per + if i < rem { 1 } else { 0 };
                    if count > 0 {
                        let member_parts: Vec<i32> = partitions[idx..idx + count].to_vec();
                        assignments
                            .get_mut(*member_id)
                            .unwrap()
                            .push(TopicPartition {
                                topic: topic.to_string(),
                                partitions: member_parts,
                            });
                        idx += count;
                    }
                }
            }
            PartitionAssignmentStrategy::RoundRobin => {
                for (i, &partition) in partitions.iter().enumerate() {
                    let member_id = all_members[i % all_members.len()];
                    assignments
                        .get_mut(member_id)
                        .unwrap()
                        .push(TopicPartition {
                            topic: topic.to_string(),
                            partitions: vec![partition],
                        });
                }
            }
        }
    }

    let current_member_id = join_response.member_id.clone();
    let current_assignment = assignments
        .get(current_member_id.as_str())
        .cloned()
        .unwrap_or_default();

    let protocol_assignment = ConsumerProtocolAssignment {
        assigned_partitions: current_assignment,
        user_data: None,
    };

    let mut buf = bytes::BytesMut::new();
    protocol_assignment
        .encode(&mut buf, 0)
        .map_err(|e| KafkaError::Protocol(e.to_string()))?;
    Ok(buf.freeze())
}

/// Send heartbeat.
async fn background_heartbeat(
    cluster: &Arc<ClusterClient>,
    group_id: &str,
    group_state: &Arc<RwLock<GroupState>>,
    coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
) -> Result<()> {
    let (generation_id, member_id) = {
        let gs = group_state.read().await;
        if gs.assigned_partitions.is_empty() || gs.member_id.is_empty() {
            return Ok(());
        }
        (gs.generation_id, gs.member_id.clone())
    };

    let coord_addr = match *coordinator.read().await {
        Some(addr) => addr,
        None => return Ok(()),
    };

    let request = HeartbeatRequest {
        group_id: group_id.to_string(),
        generation_id,
        member_id,
        group_instance_id: None,
    };

    let response: crate::protocol::HeartbeatResponse =
        cluster.send_to_broker(coord_addr, &request).await?;

    if response.error_code == 27 {
        warn!("Heartbeat REBALANCE_IN_PROGRESS");
        let mut gs = group_state.write().await;
        gs.assigned_partitions.clear();
        gs.member_id.clear();
        *coordinator.write().await = None;
        return Err(KafkaError::Protocol("Rebalance required".to_string()));
    } else if response.error_code != 0 {
        return Err(KafkaError::Protocol(format!(
            "Heartbeat failed: error {}",
            response.error_code
        )));
    }
    Ok(())
}

/// Fetch committed offsets from coordinator.
async fn fetch_committed_offsets(
    cluster: &Arc<ClusterClient>,
    group_id: &str,
    coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
    assignment: &HashMap<String, Vec<i32>>,
) -> Result<HashMap<String, HashMap<i32, i64>>> {
    let coord_addr = match *coordinator.read().await {
        Some(addr) => addr,
        None => return Err(KafkaError::NoCoordinator),
    };

    let mut topic_ids = Vec::with_capacity(assignment.len());
    for topic in assignment.keys() {
        let tid = cluster
            .metadata()
            .get_topic(topic)
            .await
            .map(|t| t.topic_id)
            .unwrap_or_else(uuid::Uuid::nil);
        topic_ids.push(tid);
    }

    let topics: Vec<OffsetFetchRequestTopics> = assignment
        .iter()
        .zip(topic_ids)
        .map(|((topic, partitions), topic_id)| OffsetFetchRequestTopics {
            name: topic.clone(),
            topic_id,
            partition_indexes: partitions.clone(),
        })
        .collect();

    let legacy_topics: Vec<OffsetFetchRequestTopic> = assignment
        .iter()
        .map(|(topic, partitions)| OffsetFetchRequestTopic {
            name: topic.clone(),
            partition_indexes: partitions.clone(),
        })
        .collect();

    let request = OffsetFetchRequest {
        group_id: String::new(),
        topics: if !legacy_topics.is_empty() {
            Some(legacy_topics)
        } else {
            None
        },
        groups: vec![OffsetFetchRequestGroup {
            group_id: group_id.to_string(),
            member_id: None,
            member_epoch: -1,
            topics: Some(topics),
        }],
        require_stable: false,
    };

    let response: crate::protocol::OffsetFetchResponse =
        cluster.send_to_broker(coord_addr, &request).await?;

    let mut result: HashMap<String, HashMap<i32, i64>> = HashMap::new();
    for group in response.groups {
        if group.group_id == group_id {
            for topic in group.topics {
                let topic_name = if !topic.name.is_empty() {
                    topic.name.clone()
                } else if !topic.topic_id.is_nil() {
                    cluster
                        .metadata()
                        .get_topic_name_by_id(topic.topic_id)
                        .await
                        .unwrap_or_else(|| format!("unknown-{}", topic.topic_id))
                } else {
                    continue;
                };
                let entry = result.entry(topic_name).or_default();
                for p in topic.partitions {
                    if p.error_code == 0 && p.committed_offset >= 0 {
                        entry.insert(p.partition_index, p.committed_offset);
                    }
                }
            }
        }
    }
    Ok(result)
}

/// Initialize offsets for a group (committed or auto_offset_reset fallback).
async fn init_offsets_for_group(
    cluster: &Arc<ClusterClient>,
    group_id: &str,
    offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
    assignment: &HashMap<String, Vec<i32>>,
    auto_offset_reset: AutoOffsetReset,
) -> Result<()> {
    let committed = fetch_committed_offsets(cluster, group_id, coordinator, assignment)
        .await
        .unwrap_or_default();

    let default_offset = match auto_offset_reset {
        AutoOffsetReset::Earliest => -2i64,
        AutoOffsetReset::Latest => -1i64,
        AutoOffsetReset::None => return Err(KafkaError::NoOffsetStored),
    };

    let mut off = offsets.write().await;
    for (topic, partitions) in assignment {
        let topic_offsets = off.entry(topic.clone()).or_insert_with(HashMap::new);
        let committed_topic = committed.get(topic);
        for partition in partitions {
            if topic_offsets.contains_key(partition) {
                continue;
            }
            if let Some(offset) = committed_topic.and_then(|p| p.get(partition)) {
                topic_offsets.insert(*partition, *offset);
            } else {
                topic_offsets.insert(*partition, default_offset);
            }
        }
    }
    Ok(())
}

/// Execute a fetch for the given targets, updating offsets on success.
async fn execute_fetch(
    cluster: &Arc<ClusterClient>,
    offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    config: &ConsumerConfig,
    fetch_targets: Vec<(String, i32, i64)>,
    timeout_ms: i32,
) -> Result<Vec<ConsumerRecord>> {
    let mut all_records = Vec::new();
    let fetch_params = FetchParams {
        timeout_ms,
        min_bytes: config.min_bytes,
        max_bytes: config.max_bytes,
        partition_max_bytes: config.partition_max_bytes,
    };

    let futures: Vec<_> = fetch_targets
        .into_iter()
        .map(|(topic, partition, offset)| {
            let cluster = cluster.clone();
            let params = fetch_params.clone();
            async move { fetch_partition(&cluster, &topic, partition, offset, &params).await }
        })
        .collect();

    let results = futures::future::join_all(futures).await;

    {
        let mut off = offsets.write().await;
        for result in results {
            match result {
                Ok(records) => {
                    if let Some(last) = records.last() {
                        let topic_offsets = off
                            .entry(last.topic.clone())
                            .or_insert_with(HashMap::new);
                        topic_offsets.insert(last.partition, last.offset + 1);
                    }
                    all_records.extend(records);
                }
                Err(e) => {
                    warn!("Failed to fetch partition: {}", e);
                }
            }
        }
    }

    Ok(all_records)
}

/// Fetch records from a single partition.
async fn fetch_partition(
    cluster: &Arc<ClusterClient>,
    topic: &str,
    partition: i32,
    offset: i64,
    params: &FetchParams,
) -> Result<Vec<ConsumerRecord>> {
    if cluster.metadata().is_expired().await {
        cluster.refresh_metadata().await?;
    }

    let leader_addr = cluster
        .metadata()
        .get_partition_leader(topic, partition)
        .await
        .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;

    let topic_id = cluster
        .metadata()
        .get_topic(topic)
        .await
        .map(|t| t.topic_id)
        .unwrap_or_else(uuid::Uuid::nil);

    let request = FetchRequest {
        cluster_id: None,
        replica_id: -1,
        replica_state: Default::default(),
        max_wait_ms: params.timeout_ms,
        min_bytes: params.min_bytes,
        max_bytes: params.max_bytes,
        isolation_level: 0,
        session_id: 0,
        session_epoch: -1,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            topic_id,
            partitions: vec![FetchPartition {
                partition,
                current_leader_epoch: -1,
                fetch_offset: offset,
                last_fetched_epoch: -1,
                log_start_offset: -1,
                partition_max_bytes: params.partition_max_bytes,
                replica_directory_id: uuid::Uuid::nil(),
                high_watermark: 0,
            }],
        }],
        forgotten_topics_data: vec![],
        rack_id: String::new(),
    };

    let response: FetchResponse = cluster.send_to_broker(leader_addr, &request).await?;
    parse_fetch_response(response, topic, topic_id, partition)
}

/// Parse a FetchResponse into ConsumerRecords.
fn parse_fetch_response(
    response: FetchResponse,
    topic_name: &str,
    topic_id: uuid::Uuid,
    partition_index: i32,
) -> Result<Vec<ConsumerRecord>> {
    let mut records = Vec::new();

    for topic_response in response.responses {
        let name_matches = !topic_response.topic.is_empty() && topic_response.topic == topic_name;
        let id_matches = !topic_response.topic_id.is_nil() && topic_response.topic_id == topic_id;
        if !name_matches && !id_matches {
            continue;
        }

        for partition_response in topic_response.partitions {
            if partition_response.partition_index != partition_index {
                continue;
            }

            if partition_response.error_code != 0 {
                if partition_response.error_code == 27 {
                    return Err(KafkaError::ProduceError(partition_response.error_code));
                }
                warn!(
                    "Fetch error for partition {}: error_code={}",
                    partition_response.partition_index, partition_response.error_code
                );
                continue;
            }

            let Some(batch) = partition_response.records else {
                continue;
            };

            let base_offset = batch.base_offset;
            let first_timestamp = batch.first_timestamp;

            for (idx, record) in batch.records.into_iter().enumerate() {
                let offset = base_offset + idx as i64;
                let timestamp = first_timestamp + record.timestamp_delta;
                let headers = record
                    .headers
                    .into_iter()
                    .map(|h| Header {
                        key: h.key,
                        value: h.value.unwrap_or_default(),
                    })
                    .collect();

                records.push(ConsumerRecord {
                    topic: topic_name.to_string(),
                    partition: partition_index,
                    offset,
                    timestamp,
                    key: record.key,
                    value: record.value.unwrap_or_default(),
                    headers,
                });
            }
        }
    }

    Ok(records)
}

/// Resolve offset for a partition via ListOffsets.
async fn list_offset_for(
    cluster: &Arc<ClusterClient>,
    topic: &str,
    partition: i32,
    timestamp: i64,
) -> Result<i64> {
    let leader_addr = cluster
        .metadata()
        .get_partition_leader(topic, partition)
        .await
        .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;

    let request = ListOffsetsRequest {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopic {
            name: topic.to_string(),
            partitions: vec![ListOffsetsPartition {
                partition_index: partition,
                current_leader_epoch: -1,
                timestamp,
            }],
        }],
        timeout_ms: -1,
    };

    let response = cluster
        .send_to_broker::<ListOffsetsRequest, crate::protocol::ListOffsetsResponse>(
            leader_addr,
            &request,
        )
        .await?;

    for topic_response in response.topics {
        if topic_response.name == topic {
            for partition_response in topic_response.partitions {
                if partition_response.partition_index == partition {
                    if partition_response.error_code != 0 {
                        return Err(KafkaError::OffsetNotFound(topic.to_string(), partition));
                    }
                    return Ok(partition_response.offset);
                }
            }
        }
    }
    Err(KafkaError::OffsetNotFound(topic.to_string(), partition))
}

/// Commit offsets to coordinator.
async fn do_commit(
    cluster: &Arc<ClusterClient>,
    group_id: &str,
    offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
    generation_id: i32,
    member_id: Option<String>,
) -> Result<()> {
    if group_id.is_empty() {
        return Ok(());
    }

    let topic_partitions: HashMap<String, Vec<(i32, i64)>> = {
        let off = offsets.read().await;
        off.iter()
            .map(|(topic, partitions)| {
                let entries: Vec<(i32, i64)> =
                    partitions.iter().map(|(p, o)| (*p, *o)).collect();
                (topic.clone(), entries)
            })
            .collect()
    };

    if topic_partitions.is_empty() {
        return Ok(());
    }

    let coordinator_addr = {
        let mut coord = coordinator.write().await;
        if let Some(addr) = *coord {
            addr
        } else {
            let request = FindCoordinatorRequest {
                key: group_id.to_string(),
                key_type: 0,
                coordinator_keys: vec![group_id.to_string()],
            };
            let response: FindCoordinatorResponse =
                cluster.send_to_any_broker(&request).await?;
            if response.error_code != 0 {
                return Err(KafkaError::NoCoordinator);
            }
            if let Some(coord) = response.coordinators.first() {
                if coord.error_code != 0 {
                    return Err(KafkaError::NoCoordinator);
                }
            }
            let (host, port) = if !response.host.is_empty() {
                (response.host.clone(), response.port)
            } else if let Some(coord) = response.coordinators.first() {
                (coord.host.clone(), coord.port)
            } else {
                return Err(KafkaError::NoCoordinator);
            };
            let addr: std::net::SocketAddr = format!("{}:{}", host, port)
                .to_socket_addrs()
                .map_err(|_| KafkaError::NoCoordinator)?
                .next()
                .ok_or(KafkaError::NoCoordinator)?;
            *coord = Some(addr);
            addr
        }
    };

    let topic_ids: Vec<uuid::Uuid> = {
        let mut ids = Vec::with_capacity(topic_partitions.len());
        for topic in topic_partitions.keys() {
            let tid = cluster
                .metadata()
                .get_topic(topic)
                .await
                .map(|t| t.topic_id)
                .unwrap_or_else(uuid::Uuid::nil);
            ids.push(tid);
        }
        ids
    };

    let topics: Vec<OffsetCommitRequestTopic> = topic_partitions
        .into_iter()
        .zip(topic_ids)
        .map(|((topic, partitions), topic_id)| OffsetCommitRequestTopic {
            name: topic,
            topic_id,
            partitions: partitions
                .into_iter()
                .map(|(partition_index, committed_offset)| OffsetCommitRequestPartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch: -1,
                    committed_metadata: None,
                })
                .collect(),
        })
        .collect();

    let request = OffsetCommitRequest {
        group_id: group_id.to_string(),
        generation_id_or_member_epoch: generation_id,
        member_id: member_id.unwrap_or_default(),
        group_instance_id: None,
        retention_time_ms: -1,
        topics,
    };

    let response: Result<crate::protocol::OffsetCommitResponse> =
        cluster.send_to_broker(coordinator_addr, &request).await;

    match response {
        Ok(resp) => {
            for topic_response in resp.topics {
                for partition_response in topic_response.partitions {
                    if partition_response.error_code != 0 {
                        return Err(KafkaError::OffsetCommitError(partition_response.error_code));
                    }
                }
            }
            Ok(())
        }
        Err(KafkaError::ConnectionClosed | KafkaError::Io(_)) => {
            warn!(
                "Connection to coordinator {} closed, clearing cache",
                coordinator_addr
            );
            *coordinator.write().await = None;
            Err(KafkaError::ConnectionClosed)
        }
        Err(e) => Err(e),
    }
}
