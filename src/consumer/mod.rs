//! Consumer - high-level Kafka message consumer

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

/// Command sent to Consumer background event loop
enum ConsumerCommand {
    /// Subscribe/change topic list
    Subscribe { topics: Vec<String> },
}

/// Fetch parameters for partition fetching
#[derive(Debug, Clone)]
struct FetchParams {
    timeout_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    partition_max_bytes: i32,
}

/// Consumer configuration
///
/// Controls consumer behavior including group membership, offset management,
/// and fetch parameters.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Consumer group identifier for coordinated consumption
    pub group_id: String,
    /// Enable automatic offset commit at intervals
    pub auto_commit: bool,
    /// Interval in milliseconds between automatic commits
    pub auto_commit_interval_ms: u64,
    /// Offset reset strategy when no committed offset exists
    pub auto_offset_reset: AutoOffsetReset,
    /// Minimum bytes to wait for in fetch response
    pub min_bytes: i32,
    /// Maximum bytes to return in fetch response
    pub max_bytes: i32,
    /// Maximum bytes per partition in fetch response
    pub partition_max_bytes: i32,
    /// Maximum time in milliseconds to wait for fetch response
    pub max_wait_ms: i32,
    /// Session timeout in milliseconds for consumer group
    pub session_timeout_ms: i32,
    /// Rebalance timeout in milliseconds for consumer group
    pub rebalance_timeout_ms: i32,
    /// Interval in milliseconds between heartbeat requests
    pub heartbeat_interval_ms: u64,
    /// Strategy for partition assignment among group members
    pub partition_assignment_strategy: PartitionAssignmentStrategy,
}

/// Offset reset strategy when no committed offset exists
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoOffsetReset {
    /// Start from the earliest available offset
    Earliest,
    /// Start from the latest offset (new messages only)
    Latest,
    /// Fail if no committed offset exists
    None,
}

/// Partition assignment strategy for consumer groups
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionAssignmentStrategy {
    /// Assign partitions to consumers based on topic partition ranges
    Range,
    /// Assign partitions to consumers in round-robin fashion
    RoundRobin,
    /// Cooperative sticky assignment (incremental rebalancing)
    CooperativeSticky,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: format!("{}-consumer", crate::NAME),
            auto_commit: true,
            auto_commit_interval_ms: 5000,
            auto_offset_reset: AutoOffsetReset::Latest,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            partition_max_bytes: 1024 * 1024,
            max_wait_ms: 500,
            session_timeout_ms: 45000,
            rebalance_timeout_ms: 300000,
            heartbeat_interval_ms: 3000,
            partition_assignment_strategy: PartitionAssignmentStrategy::Range,
        }
    }
}

/// Message header
///
/// Key-value pair attached to a Kafka message.
#[derive(Debug, Clone)]
pub struct Header {
    /// Header key name
    pub key: String,
    /// Header value data
    pub value: Bytes,
}

/// Consumer record
///
/// A single message consumed from Kafka with metadata.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    /// Topic name where the message was published
    pub topic: String,
    /// Partition number within the topic
    pub partition: i32,
    /// Offset of the message within the partition
    pub offset: i64,
    /// Message timestamp (milliseconds since epoch)
    pub timestamp: i64,
    /// Optional message key
    pub key: Option<Bytes>,
    /// Message value data
    pub value: Bytes,
    /// Message headers
    pub headers: Vec<Header>,
}

/// Consumer group member state
#[derive(Debug, Clone, Default)]
struct GroupState {
    member_id: String,
    generation_id: i32,
    leader: String,
    protocol_name: Option<String>,
    assigned_partitions: HashMap<String, Vec<i32>>,
}

/// High-level Kafka Consumer
///
/// Provides automatic consumer group management including:
/// - Group coordination and partition assignment
/// - Heartbeat maintenance
/// - Automatic offset commit
///
/// Uses a single background event loop to handle all group operations,
/// avoiding spawning multiple background tasks.
pub struct Consumer {
    cluster: Arc<ClusterClient>,
    subscribed_topics: Vec<String>,
    offsets: Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    config: ConsumerConfig,
    group_state: Arc<RwLock<GroupState>>,
    coordinator: Arc<RwLock<Option<std::net::SocketAddr>>>,
    running: Arc<std::sync::atomic::AtomicBool>,
    command_tx: mpsc::UnboundedSender<ConsumerCommand>,
}

impl Consumer {
    /// Create Consumer instance (does not start background loop)
    pub(crate) fn new(cluster: Arc<ClusterClient>, config: ConsumerConfig) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let consumer = Self {
            cluster: cluster.clone(),
            subscribed_topics: Vec::new(),
            offsets: Arc::new(RwLock::new(HashMap::new())),
            config: config.clone(),
            group_state: Arc::new(RwLock::new(GroupState::default())),
            coordinator: Arc::new(RwLock::new(None)),
            running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            command_tx,
        };

        consumer.start(cluster, config, command_rx);
        consumer
    }

    /// Start background event loop
    fn start(
        &self,
        cluster: Arc<ClusterClient>,
        config: ConsumerConfig,
        mut command_rx: mpsc::UnboundedReceiver<ConsumerCommand>,
    ) {
        let offsets = self.offsets.clone();
        let group_state = self.group_state.clone();
        let coordinator = self.coordinator.clone();
        let running = self.running.clone();

        let mut commit_interval = interval(Duration::from_millis(config.auto_commit_interval_ms));
        let mut heartbeat_interval = interval(Duration::from_millis(config.heartbeat_interval_ms));

        let mut current_topics: Vec<String> = Vec::new();
        let mut needs_rejoin = false;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = commit_interval.tick() => {
                        if !running.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }
                        if config.auto_commit && !config.group_id.is_empty() {
                            let (generation_id, member_id) = {
                                let gs = group_state.read().await;
                                (gs.generation_id, if gs.member_id.is_empty() { None } else { Some(gs.member_id.clone()) })
                            };
                            if let Err(e) = Self::do_commit(
                                &cluster,
                                &config.group_id,
                                &offsets,
                                &coordinator,
                                generation_id,
                                member_id,
                            )
                            .await
                            {
                                debug!("Auto commit failed: {}", e);
                            }
                        }
                    }
                    _ = heartbeat_interval.tick() => {
                        if !running.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }
                        if !config.group_id.is_empty() {
                            if let Err(e) = Self::background_heartbeat(
                                &cluster,
                                &config.group_id,
                                &group_state,
                                &coordinator,
                            )
                            .await
                            {
                                debug!("Heartbeat failed: {}", e);
                                needs_rejoin = true;
                            }
                        }
                    }
                    cmd = command_rx.recv() => {
                        if !running.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }
                        match cmd {
                            Some(ConsumerCommand::Subscribe { topics }) => {
                                current_topics = topics;
                                needs_rejoin = true;
                                let mut gs = group_state.write().await;
                                gs.assigned_partitions.clear();
                                gs.member_id.clear();
                                *coordinator.write().await = None;
                            }
                            None => break,
                        }
                    }
                }

                if needs_rejoin && !current_topics.is_empty() && !config.group_id.is_empty() {
                    needs_rejoin = false;
                    match Self::background_join_group(
                        &cluster,
                        &config.group_id,
                        &group_state,
                        &coordinator,
                        &offsets,
                        &config,
                        &current_topics,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(e) => {
                            warn!("background_join_group failed: {:?}, will retry", e);
                            let mut gs = group_state.write().await;
                            gs.assigned_partitions.clear();
                            gs.member_id.clear();
                            *coordinator.write().await = None;
                            needs_rejoin = true;
                        }
                    }
                }
            }
        });
    }

    /// Subscribe to topic list
    ///
    /// For consumer groups, this triggers group rebalance to assign partitions.
    /// For simple consumers (no group_id), this initializes offsets directly.
    pub async fn subscribe(&mut self, topics: Vec<String>) -> Result<()> {
        self.subscribed_topics = topics.clone();

        if self.config.group_id.is_empty() {
            self.init_offsets_simple(topics).await?;
        } else {
            let _ = self.command_tx.send(ConsumerCommand::Subscribe {
                topics: topics.clone(),
            });
        }

        Ok(())
    }

    /// Background heartbeat
    async fn background_heartbeat(
        cluster: &Arc<ClusterClient>,
        group_id: &str,
        group_state: &Arc<RwLock<GroupState>>,
        coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
    ) -> Result<()> {
        if group_id.is_empty() {
            return Ok(());
        }

        let (generation_id, member_id) = {
            let gs = group_state.read().await;
            if gs.assigned_partitions.is_empty() || gs.member_id.is_empty() {
                return Ok(());
            }
            (gs.generation_id, gs.member_id.clone())
        };

        let coord_addr = {
            let c = coordinator.read().await;
            match *c {
                Some(addr) => addr,
                None => return Ok(()),
            }
        };

        let request = HeartbeatRequest {
            group_id: group_id.to_string(),
            generation_id,
            member_id: member_id.clone(),
            group_instance_id: None,
        };

        let response: crate::protocol::HeartbeatResponse =
            cluster.send_to_broker(coord_addr, &request).await?;

        if response.error_code == 27 {
            warn!(
                "Heartbeat REBALANCE_IN_PROGRESS for {}, will rejoin on next tick",
                member_id
            );
            let mut gs = group_state.write().await;
            gs.assigned_partitions.clear();
            gs.member_id.clear();
            *coordinator.write().await = None;
            return Err(KafkaError::Protocol("Rebalance required".to_string()));
        } else if response.error_code != 0 {
            debug!(
                "Heartbeat error for {}: error_code={}",
                member_id, response.error_code
            );
            return Err(KafkaError::Protocol(format!(
                "Heartbeat failed: error {}",
                response.error_code
            )));
        }

        Ok(())
    }

    /// Compute partition assignment
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
                PartitionAssignmentStrategy::Range => {
                    let partitions_per_member = partitions.len() / all_members.len();
                    let remainder = partitions.len() % all_members.len();

                    let mut partition_idx = 0;
                    for (i, member_id) in all_members.iter().enumerate() {
                        let count = partitions_per_member + if i < remainder { 1 } else { 0 };
                        if count > 0 {
                            let member_partitions: Vec<i32> =
                                partitions[partition_idx..partition_idx + count].to_vec();
                            assignments
                                .get_mut(*member_id)
                                .unwrap()
                                .push(TopicPartition {
                                    topic: topic.to_string(),
                                    partitions: member_partitions,
                                });
                            partition_idx += count;
                        }
                    }
                }
                PartitionAssignmentStrategy::RoundRobin => {
                    for (i, &partition) in partitions.iter().enumerate() {
                        let member_idx = i % all_members.len();
                        let member_id = all_members[member_idx];
                        assignments
                            .get_mut(member_id)
                            .unwrap()
                            .push(TopicPartition {
                                topic: topic.to_string(),
                                partitions: vec![partition],
                            });
                    }
                }
                PartitionAssignmentStrategy::CooperativeSticky => {
                    let partitions_per_member = partitions.len() / all_members.len();
                    let remainder = partitions.len() % all_members.len();

                    let mut partition_idx = 0;
                    for (i, member_id) in all_members.iter().enumerate() {
                        let count = partitions_per_member + if i < remainder { 1 } else { 0 };
                        if count > 0 {
                            let member_partitions: Vec<i32> =
                                partitions[partition_idx..partition_idx + count].to_vec();
                            assignments
                                .get_mut(*member_id)
                                .unwrap()
                                .push(TopicPartition {
                                    topic: topic.to_string(),
                                    partitions: member_partitions,
                                });
                            partition_idx += count;
                        }
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
            .encode(&mut buf, 0, false)
            .map_err(|e| KafkaError::Protocol(e.to_string()))?;
        Ok(buf.freeze())
    }

    /// Initialize offsets based on auto_offset_reset
    async fn init_offsets_for_partitions(&self, partitions: Vec<(String, i32)>) -> Result<()> {
        if partitions.is_empty() {
            return Ok(());
        }

        let timestamp = match self.config.auto_offset_reset {
            AutoOffsetReset::Latest => -1i64,
            AutoOffsetReset::Earliest => -2i64,
            AutoOffsetReset::None => return Err(KafkaError::NoOffsetStored),
        };

        for (topic, partition) in partitions {
            let offset = self.list_offset(&topic, partition, timestamp).await?;
            let mut offsets = self.offsets.write().await;
            offsets
                .entry(topic)
                .or_insert_with(HashMap::new)
                .insert(partition, offset);
        }

        Ok(())
    }

    async fn init_offsets_simple(&self, topics: Vec<String>) -> Result<()> {
        let mut needs_initialization: Vec<(String, i32)> = Vec::new();

        {
            let mut offsets = self.offsets.write().await;

            for topic in &topics {
                let partitions = self
                    .cluster
                    .metadata()
                    .get_partitions(topic)
                    .await
                    .ok_or_else(|| KafkaError::TopicNotFound(topic.clone()))?;

                let topic_offsets = offsets.entry(topic.clone()).or_insert_with(HashMap::new);
                for partition in &partitions {
                    if !topic_offsets.contains_key(partition) {
                        topic_offsets.insert(*partition, -1);
                        needs_initialization.push((topic.clone(), *partition));
                    }
                }
            }
        }

        self.init_offsets_for_partitions(needs_initialization)
            .await?;

        Ok(())
    }

    async fn list_offset(&self, topic: &str, partition: i32, timestamp: i64) -> Result<i64> {
        let leader_addr = self
            .cluster
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

        let response = self
            .cluster
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

    /// Background join group
    async fn background_join_group(
        cluster: &Arc<ClusterClient>,
        group_id: &str,
        group_state: &Arc<RwLock<GroupState>>,
        coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
        offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
        config: &ConsumerConfig,
        topics: &[String],
    ) -> Result<()> {
        // 1. Find coordinator (with retryable error handling)
        let coord_addr = {
            let request = FindCoordinatorRequest {
                key: group_id.to_string(),
                key_type: 0,
                coordinator_keys: vec![group_id.to_string()],
            };

            let response: FindCoordinatorResponse = cluster.send_to_any_broker(&request).await?;

            // error_code 15 = GROUP_COORDINATOR_NOT_AVAILABLE (retryable)
            if response.error_code == 15 {
                return Err(KafkaError::NoCoordinator);
            }

            if response.error_code != 0 {
                return Err(KafkaError::NoCoordinator);
            }

            let (host, port) = if !response.host.is_empty() {
                (response.host.clone(), response.port)
            } else if let Some(coord) = response.coordinators.first() {
                if coord.error_code == 15 {
                    return Err(KafkaError::NoCoordinator);
                }
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

        // 3. JoinGroup loop
        let mut member_id = String::new();
        for _join_attempt in 0..10u32 {
            let request = JoinGroupRequest {
                group_id: group_id.to_string(),
                session_timeout_ms: config.session_timeout_ms,
                rebalance_timeout_ms: config.rebalance_timeout_ms,
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
                    Self::compute_assignment(
                        topics,
                        &response,
                        cluster,
                        config.partition_assignment_strategy,
                    )
                    .await?
                } else {
                    Bytes::new()
                };

                let sync_request = SyncGroupRequest {
                    group_id: group_id.to_string(),
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
                    let assignment_result =
                        ConsumerProtocolAssignment::decode(&mut buf_data, 0, false)
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

                Self::init_offsets_for_group(
                    cluster,
                    group_id,
                    offsets,
                    coordinator,
                    &assignment,
                    config.auto_offset_reset,
                )
                .await?;

                debug!(
                    "background_join_group: joined group, assigned partitions={:?}",
                    assignment
                );
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

    /// Fetch committed offsets from coordinator
    async fn fetch_committed_offsets(
        cluster: &Arc<ClusterClient>,
        group_id: &str,
        coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
        assignment: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>> {
        let coord_addr = {
            let c = coordinator.read().await;
            match *c {
                Some(addr) => addr,
                None => return Err(KafkaError::NoCoordinator),
            }
        };

        // Build topics with topic_id (needed for v10+)
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

        // v0-7: legacy topic-based format
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
                    // v0-7: name is populated; v10+: topic_id is populated
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

    /// Initialize offsets for consumer group
    async fn init_offsets_for_group(
        cluster: &Arc<ClusterClient>,
        group_id: &str,
        offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
        coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
        assignment: &HashMap<String, Vec<i32>>,
        auto_offset_reset: AutoOffsetReset,
    ) -> Result<()> {
        let committed = Self::fetch_committed_offsets(cluster, group_id, coordinator, assignment)
            .await
            .unwrap_or_default();

        let default_offset = match auto_offset_reset {
            AutoOffsetReset::Earliest => -2i64,
            AutoOffsetReset::Latest => -1i64,
            AutoOffsetReset::None => return Err(KafkaError::NoOffsetStored),
        };

        let mut offsets_writer = offsets.write().await;
        for (topic, partitions) in assignment {
            let topic_offsets = offsets_writer
                .entry(topic.clone())
                .or_insert_with(HashMap::new);
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

    /// Send heartbeat
    pub async fn heartbeat(&self) -> Result<()> {
        let group_state = self.group_state.read().await;
        if group_state.member_id.is_empty() {
            return Ok(());
        }

        let coordinator = self.find_coordinator().await?;

        let request = HeartbeatRequest {
            group_id: self.config.group_id.clone(),
            generation_id: group_state.generation_id,
            member_id: group_state.member_id.clone(),
            group_instance_id: None,
        };

        let response: crate::protocol::HeartbeatResponse =
            self.cluster.send_to_broker(coordinator, &request).await?;

        if response.error_code != 0 {
            if response.error_code == 27 {
                warn!("Heartbeat indicates rebalance needed, rejoining group");
                *self.coordinator.write().await = None;
                return Err(KafkaError::Protocol("Rebalance required".to_string()));
            }
            return Err(KafkaError::Protocol(format!(
                "Heartbeat failed: error {}",
                response.error_code
            )));
        }

        Ok(())
    }

    /// Poll messages
    ///
    /// Fetches messages from assigned partitions. Returns empty vector
    /// if no messages are available within the timeout.
    pub async fn poll(&mut self, timeout_ms: i32) -> Result<Vec<ConsumerRecord>> {
        if !self.config.group_id.is_empty() {
            let needs_join = {
                let gs = self.group_state.read().await;
                gs.assigned_partitions.is_empty()
            };
            if needs_join {
                return Ok(Vec::new());
            }
        }

        let fetch_targets: Vec<(String, i32, i64)> = {
            let group_state = self.group_state.read().await;
            let offsets = self.offsets.read().await;

            let assigned = if !group_state.assigned_partitions.is_empty() {
                group_state.assigned_partitions.clone()
            } else {
                let mut all: HashMap<String, Vec<i32>> = HashMap::new();
                for topic in &self.subscribed_topics {
                    if let Some(partitions) = self.cluster.metadata().get_partitions(topic).await {
                        all.insert(topic.clone(), partitions);
                    }
                }
                all
            };

            let mut targets = Vec::new();
            for (topic, partitions) in assigned {
                for partition in partitions {
                    let offset = offsets
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
        };

        if fetch_targets.is_empty() {
            self.lazy_init_offsets().await;
            let targets = self.collect_fetch_targets().await;
            if targets.is_empty() {
                return Ok(Vec::new());
            }
            return self.execute_fetch(targets, timeout_ms).await;
        }

        self.execute_fetch(fetch_targets, timeout_ms).await
    }

    async fn lazy_init_offsets(&self) {
        let needs_init: Vec<(String, i32)> = {
            let offsets = self.offsets.read().await;
            let group_state = self.group_state.read().await;
            let assigned = if !group_state.assigned_partitions.is_empty() {
                group_state.assigned_partitions.clone()
            } else {
                HashMap::new()
            };
            let mut result = Vec::new();
            for (topic, partitions) in assigned {
                for partition in partitions {
                    let offset = offsets
                        .get(&topic)
                        .and_then(|t| t.get(&partition))
                        .copied()
                        .unwrap_or(-1);
                    if offset < 0 {
                        result.push((topic.clone(), partition));
                    }
                }
            }
            result
        };

        for (topic, partition) in needs_init {
            let timestamp = match self.config.auto_offset_reset {
                AutoOffsetReset::Latest => -1i64,
                AutoOffsetReset::Earliest => -2i64,
                AutoOffsetReset::None => continue,
            };
            match self.list_offset(&topic, partition, timestamp).await {
                Ok(real_offset) => {
                    let mut offsets = self.offsets.write().await;
                    offsets
                        .entry(topic.clone())
                        .or_insert_with(HashMap::new)
                        .insert(partition, real_offset);
                }
                Err(e) => {
                    warn!(
                        "Failed to resolve offset for {}/{}: {}",
                        topic, partition, e
                    );
                }
            }
        }
    }

    async fn collect_fetch_targets(&self) -> Vec<(String, i32, i64)> {
        let group_state = self.group_state.read().await;
        let offsets = self.offsets.read().await;

        let assigned = if !group_state.assigned_partitions.is_empty() {
            group_state.assigned_partitions.clone()
        } else {
            return Vec::new();
        };

        let mut targets = Vec::new();
        for (topic, partitions) in assigned {
            for partition in partitions {
                let offset = offsets
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

    async fn execute_fetch(
        &self,
        fetch_targets: Vec<(String, i32, i64)>,
        timeout_ms: i32,
    ) -> Result<Vec<ConsumerRecord>> {
        let mut all_records = Vec::new();
        let fetch_params = FetchParams {
            timeout_ms,
            min_bytes: self.config.min_bytes,
            max_bytes: self.config.max_bytes,
            partition_max_bytes: self.config.partition_max_bytes,
        };

        let futures: Vec<_> =
            fetch_targets
                .into_iter()
                .map(|(topic, partition, offset)| {
                    let cluster = self.cluster.clone();
                    let params = fetch_params.clone();

                    async move {
                        Self::fetch_partition(&cluster, &topic, partition, offset, &params).await
                    }
                })
                .collect();

        let results = futures::future::join_all(futures).await;

        {
            let mut offsets = self.offsets.write().await;
            for result in results {
                match result {
                    Ok(records) => {
                        if let Some(last) = records.last() {
                            let topic_offsets = offsets
                                .entry(last.topic.clone())
                                .or_insert_with(HashMap::new);
                            topic_offsets.insert(last.partition, last.offset + 1);
                        }
                        all_records.extend(records);
                    }
                    Err(e) => {
                        warn!("Failed to fetch partition: {}", e);
                        return Err(e);
                    }
                }
            }
        }

        Ok(all_records)
    }

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
                    high_watermark: 0, // default (i64::default()), skip tagged encoding
                }],
            }],
            forgotten_topics_data: vec![],
            rack_id: String::new(),
        };

        let response: FetchResponse = cluster.send_to_broker(leader_addr, &request).await?;
        Self::parse_fetch_response(response, topic, topic_id, partition)
    }

    fn parse_fetch_response(
        response: FetchResponse,
        topic_name: &str,
        topic_id: uuid::Uuid,
        partition_index: i32,
    ) -> Result<Vec<ConsumerRecord>> {
        let mut records = Vec::new();

        for topic_response in response.responses {
            // v0-12: name-based match; v13+: uuid-based match
            let name_matches =
                !topic_response.topic.is_empty() && topic_response.topic == topic_name;
            let id_matches =
                !topic_response.topic_id.is_nil() && topic_response.topic_id == topic_id;
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

    /// Commit offsets to coordinator
    ///
    /// Manually commits current offsets to the consumer group coordinator.
    /// This is in addition to automatic commits if `auto_commit` is enabled.
    pub async fn commit(&self) -> Result<()> {
        let (generation_id, member_id) = {
            let gs = self.group_state.read().await;
            (
                gs.generation_id,
                if gs.member_id.is_empty() {
                    None
                } else {
                    Some(gs.member_id.clone())
                },
            )
        };

        Self::do_commit(
            &self.cluster,
            &self.config.group_id,
            &self.offsets,
            &self.coordinator,
            generation_id,
            member_id,
        )
        .await
    }

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
            let offsets_guard = offsets.read().await;
            offsets_guard
                .iter()
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
                            return Err(KafkaError::OffsetCommitError(
                                partition_response.error_code,
                            ));
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

    async fn find_coordinator(&self) -> Result<std::net::SocketAddr> {
        let mut coord = self.coordinator.write().await;
        if let Some(addr) = *coord {
            return Ok(addr);
        }

        let request = FindCoordinatorRequest {
            key: self.config.group_id.clone(),
            key_type: 0,
            coordinator_keys: vec![self.config.group_id.clone()],
        };

        let response: FindCoordinatorResponse = self.cluster.send_to_any_broker(&request).await?;

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

    /// Leave consumer group
    pub async fn leave_group(&self) -> Result<()> {
        let group_state = self.group_state.read().await;
        if group_state.member_id.is_empty() {
            return Ok(());
        }

        if self.config.auto_commit {
            if let Err(e) = self.commit().await {
                warn!("Commit before leave failed: {}", e);
            }
        }

        let coordinator = self.find_coordinator().await?;

        let request = LeaveGroupRequest {
            group_id: self.config.group_id.clone(),
            member_id: group_state.member_id.clone(),
            ..Default::default()
        };

        let response: crate::protocol::LeaveGroupResponse =
            self.cluster.send_to_broker(coordinator, &request).await?;

        if response.error_code != 0 {
            warn!("LeaveGroup failed: error {}", response.error_code);
        }

        Ok(())
    }

    /// Get current offset
    pub fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let offsets = self.offsets.blocking_read();
        offsets.get(topic)?.get(&partition).copied()
    }

    /// Set offset
    pub async fn set_offset(&self, topic: &str, partition: i32, offset: i64) {
        let mut offsets = self.offsets.write().await;
        offsets
            .entry(topic.to_string())
            .or_insert_with(HashMap::new)
            .insert(partition, offset);
    }

    /// Get current assigned partitions
    pub async fn assignment(&self) -> HashMap<String, Vec<i32>> {
        self.group_state.read().await.assigned_partitions.clone()
    }

    /// Close consumer
    pub async fn close(&self) -> Result<()> {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);

        if !self.config.group_id.is_empty() {
            if let Err(e) = self.leave_group().await {
                warn!("Error leaving group: {}", e);
            }
        }

        Ok(())
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
}
