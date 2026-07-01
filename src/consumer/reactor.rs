use bytes::{Bytes, BytesMut};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, warn};

use crate::cluster::ClusterClient;
use crate::consumer::config::{AutoOffsetReset, ConsumerConfig, PartitionAssignmentStrategy};
use crate::consumer::types::{
    CompletedFetch, ConsumerCommand, ConsumerRecord, FetchParams, FetchRequestTask, ReactorState,
    RecordBatchCursor,
};
use crate::consumer::util::{
    build_protocol_metadata, compute_all_assignments, fetch_committed_offsets_raw,
    find_coordinator_raw, list_offset_for, map_heartbeat_error,
};
use crate::error::{KafkaError, Result};
use crate::protocol::{
    ConsumerProtocolAssignment, FetchPartition, FetchRequest, FetchTopic, HeartbeatRequest,
    JoinGroupRequest, JoinGroupRequestProtocol, JoinGroupResponse, LeaveGroupRequest,
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic, SyncGroupRequest,
    SyncGroupRequestAssignment, SyncGroupResponse,
};
use kafka_client_protocol::KafkaErrorCode;
use kafka_client_protocol::Message;

// ===========================================================================
// ConsumerReactor — background task for group consumers
// ===========================================================================

pub(crate) fn spawn_reactor(
    cluster: Arc<ClusterClient>,
    config: ConsumerConfig,
    cmd_rx: mpsc::UnboundedReceiver<ConsumerCommand>,
    record_tx: mpsc::Sender<Vec<ConsumerRecord>>,
    running: Arc<AtomicBool>,
) {
    let fetch_params = FetchParams {
        timeout_ms: config.max_wait.as_millis() as i32,
        min_bytes: config.min_bytes,
        max_bytes: config.max_bytes,
        partition_max_bytes: config.partition_max_bytes,
    };

    let (fetch_task_tx, fetch_task_rx) = mpsc::unbounded_channel();
    let (fetch_result_tx, fetch_result_rx) = mpsc::unbounded_channel();

    spawn_fetch_manager(cluster.clone(), fetch_task_rx, fetch_result_tx.clone());

    tokio::spawn(async move {
        let mut r = ConsumerReactor {
            cluster,
            config,
            fetch_params,
            offsets: HashMap::new(),
            member_id: String::new(),
            generation_id: 0,
            leader: String::new(),
            protocol_name: None,
            subscribed_topics: Vec::new(),
            coordinator: None,
            assigned_partitions: HashMap::new(),
            cmd_rx,
            record_tx,
            running,
            state: ReactorState::Init,
            rebalance_needed: false,
            next_in_line_records: HashMap::new(),
            pending_fetches: HashSet::new(),
            fetch_task_tx,
            fetch_result_rx,
        };
        r.run().await;
    });
}

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
                            // In protocol v13+, topic field is empty — use topic_id
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

struct ConsumerReactor {
    cluster: Arc<ClusterClient>,
    config: ConsumerConfig,
    fetch_params: FetchParams,
    offsets: HashMap<String, HashMap<i32, i64>>,
    member_id: String,
    generation_id: i32,
    leader: String,
    protocol_name: Option<String>,
    subscribed_topics: Vec<String>,
    coordinator: Option<SocketAddr>,
    assigned_partitions: HashMap<String, Vec<i32>>,
    cmd_rx: mpsc::UnboundedReceiver<ConsumerCommand>,
    record_tx: mpsc::Sender<Vec<ConsumerRecord>>,
    running: Arc<AtomicBool>,
    state: ReactorState,
    rebalance_needed: bool,
    next_in_line_records: HashMap<(String, i32), RecordBatchCursor>,
    pending_fetches: HashSet<(String, i32)>,
    fetch_task_tx: mpsc::UnboundedSender<FetchRequestTask>,
    fetch_result_rx: mpsc::UnboundedReceiver<CompletedFetch>,
}

impl ConsumerReactor {
    async fn run(&mut self) {
        loop {
            if !self.running.load(Ordering::Relaxed) {
                break;
            }

            match self.state.clone() {
                ReactorState::Init | ReactorState::Stopped => match self.cmd_rx.recv().await {
                    Some(ConsumerCommand::Subscribe { topics }) => {
                        self.subscribed_topics = topics;
                        self.state = ReactorState::Joining;
                    }
                    None => break,
                    _ => {}
                },
                ReactorState::Joining => {
                    self.run_joining().await;
                }
                ReactorState::Fetching => {
                    self.fetching_loop().await;
                    if self.subscribed_topics.is_empty() || self.member_id.is_empty() {
                        self.state = ReactorState::Stopped;
                    } else {
                        self.state = ReactorState::Rebalancing;
                    }
                }
                ReactorState::Rebalancing => {
                    self.next_in_line_records.clear();
                    self.pending_fetches.clear();
                    self.rebalance_needed = false;
                    self.assigned_partitions.clear();
                    self.member_id.clear();
                    self.coordinator = None;
                    self.state = ReactorState::Joining;
                }
            }
        }
        let _ = self.send_leave_group().await;
    }

    async fn run_joining(&mut self) {
        loop {
            loop {
                match self.cmd_rx.try_recv() {
                    Ok(ConsumerCommand::Leave { reply }) => {
                        self.state = ReactorState::Stopped;
                        self.subscribed_topics.clear();
                        self.member_id.clear();
                        self.coordinator = None;
                        let _ = reply.send(Ok(()));
                        return;
                    }
                    Ok(ConsumerCommand::Subscribe { topics }) => {
                        self.subscribed_topics = topics;
                        break;
                    }
                    Ok(cmd) => {
                        self.handle_command(cmd).await;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => return,
                }
            }

            match self.join_group().await {
                Ok(()) => {
                    self.resolve_offsets().await;
                    self.try_send_fetches().await;
                    self.state = ReactorState::Fetching;
                    return;
                }
                Err(e) => {
                    warn!("Join group failed: {:?}, will retry", e);
                    self.assigned_partitions.clear();
                    self.member_id.clear();
                    self.coordinator = None;
                    tokio::time::sleep(Duration::from_millis(1000 + rand::random::<u64>() % 1000))
                        .await;
                }
            }
        }
    }

    async fn fetching_loop(&mut self) {
        let mut commit_interval = interval(self.config.auto_commit_interval);
        commit_interval.reset();
        let mut heartbeat_interval = interval(self.config.heartbeat_interval);
        heartbeat_interval.reset();

        loop {
            if !self.running.load(Ordering::Relaxed) || self.rebalance_needed {
                break;
            }

            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    match self.background_heartbeat().await {
                        Err(KafkaError::RebalanceRequired)
                        | Err(KafkaError::IllegalGeneration(_))
                        | Err(KafkaError::UnknownMemberId(_)) => break,
                        Err(e) => debug!("Heartbeat failed (non-fatal): {}", e),
                        Ok(()) => {}
                    }
                }
                _ = commit_interval.tick() => {
                    if self.config.auto_commit && let Err(e) = self.do_commit().await {
                        warn!("Auto commit failed: {}", e);
                    }
                }
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            if self.handle_command(cmd).await {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                Some(result) = self.fetch_result_rx.recv() => {
                    self.handle_fetch_result(result).await;
                    self.drain_and_maybe_fetch().await;
                }
            }
        }
    }

    fn fetchable_partitions(&self) -> Vec<(String, i32, i64)> {
        self.assigned_partitions
            .iter()
            .flat_map(|(topic, parts)| {
                parts.iter().filter_map(|p| {
                    let tp = (topic.clone(), *p);
                    if self.pending_fetches.contains(&tp) {
                        return None;
                    }
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

    async fn try_send_fetches(&mut self) -> bool {
        if self.cluster.metadata().is_expired().await
            && let Err(e) = self.cluster.refresh_metadata().await
        {
            debug!("Metadata refresh failed: {}", e);
            return false;
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

    async fn handle_fetch_result(&mut self, mut result: CompletedFetch) {
        // Resolve topic name — in protocol v13+ the response may have
        // an empty topic string; fall back to topic_id lookup.
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
                    let cursor =
                        RecordBatchCursor::new(result.topic.clone(), result.partition, batch);
                    self.next_in_line_records.insert(tp, cursor);
                    self.offsets
                        .entry(result.topic.clone())
                        .or_default()
                        .insert(result.partition, next_offset);
                }
            }
            KafkaErrorCode::OFFSET_OUT_OF_RANGE => {
                warn!(
                    "OFFSET_OUT_OF_RANGE for {}/{}",
                    result.topic, result.partition
                );
                let ts = match self.config.auto_offset_reset {
                    AutoOffsetReset::Latest => -1i64,
                    AutoOffsetReset::Earliest => -2i64,
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
                }
            }
            KafkaErrorCode::REBALANCE_IN_PROGRESS => {
                debug!(
                    "REBALANCE_IN_PROGRESS from fetch for {}/{}",
                    result.topic, result.partition
                );
                self.rebalance_needed = true;
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
                if let Err(e) = self.cluster.refresh_metadata().await {
                    warn!("Metadata refresh failed: {}", e);
                }
            }
            code => {
                warn!(
                    "Fetch error for {}/{}: {}",
                    result.topic, result.partition, code
                );
            }
        }
    }

    async fn drain_and_maybe_fetch(&mut self) {
        let max = self.config.max_poll_records;
        if max == 0 {
            self.try_send_fetches().await;
            return;
        }

        let mut keys: Vec<(String, i32)> = self
            .next_in_line_records
            .iter()
            .filter(|(_, cursor)| !cursor.is_exhausted())
            .map(|(k, _)| k.clone())
            .collect();

        if keys.is_empty() {
            self.try_send_fetches().await;
            return;
        }

        let mut total = 0usize;
        let mut records = Vec::with_capacity(max);
        let mut idx = 0usize;

        while total < max && !keys.is_empty() {
            if idx >= keys.len() {
                idx = 0;
                keys.retain(|k| {
                    self.next_in_line_records
                        .get(k)
                        .is_some_and(|c| !c.is_exhausted())
                });
                if keys.is_empty() {
                    break;
                }
            }

            let key = keys[idx].clone();
            if let Some(cursor) = self.next_in_line_records.get_mut(&key) {
                if cursor.is_exhausted() {
                    keys.remove(idx);
                    self.next_in_line_records.remove(&key);
                    continue;
                }
                if let Some(record) = cursor.next() {
                    records.push(record);
                    total += 1;
                }
                if cursor.is_exhausted() {
                    keys.remove(idx);
                    self.next_in_line_records.remove(&key);
                    continue;
                }
            }
            idx += 1;
        }

        if !records.is_empty() && !self.record_tx.is_closed() {
            let _ = self.record_tx.send(records).await;
        }

        self.try_send_fetches().await;
    }

    async fn handle_command(&mut self, cmd: ConsumerCommand) -> bool {
        match cmd {
            ConsumerCommand::Subscribe { topics } => {
                self.subscribed_topics = topics;
                self.coordinator = None;
                self.assigned_partitions.clear();
                self.next_in_line_records.clear();
                self.pending_fetches.clear();
                self.member_id.clear();
                true
            }
            ConsumerCommand::Commit { reply } => {
                let _ = reply.send(self.do_commit().await);
                false
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
                false
            }
            ConsumerCommand::SetOffset {
                topic,
                partition,
                offset,
            } => {
                self.offsets
                    .entry(topic)
                    .or_default()
                    .insert(partition, offset);
                false
            }
            ConsumerCommand::Heartbeat { reply } => {
                let _ = reply.send(self.send_heartbeat_raw().await);
                false
            }
            ConsumerCommand::Leave { reply } => {
                let r = self.send_leave_group().await;
                self.member_id.clear();
                self.coordinator = None;
                self.subscribed_topics.clear();
                self.assigned_partitions.clear();
                self.protocol_name = None;
                self.next_in_line_records.clear();
                self.pending_fetches.clear();
                self.rebalance_needed = true;
                let _ = reply.send(r);
                true
            }
            ConsumerCommand::GetAssignment { reply } => {
                let _ = reply.send(self.assigned_partitions.clone());
                false
            }
            ConsumerCommand::TryFetch => {
                self.try_send_fetches().await;
                false
            }
        }
    }

    async fn join_group(&mut self) -> Result<()> {
        let coord_addr = find_coordinator_raw(&self.cluster, &self.config.group_id).await?;
        self.coordinator = Some(coord_addr);

        let protocol_metadata = build_protocol_metadata(&self.subscribed_topics);

        let mut member_id = String::new();
        for _ in 0..10 {
            let name = match self.config.partition_assignment_strategy {
                PartitionAssignmentStrategy::Range => "range",
                PartitionAssignmentStrategy::RoundRobin => "roundrobin",
                PartitionAssignmentStrategy::CooperativeSticky => "cooperative-sticky",
            }
            .to_string();

            let request = JoinGroupRequest {
                group_id: self.config.group_id.clone(),
                session_timeout_ms: self.config.session_timeout.as_millis() as i32,
                rebalance_timeout_ms: self.config.rebalance_timeout.as_millis() as i32,
                member_id: member_id.clone(),
                group_instance_id: None,
                protocol_type: "consumer".to_string(),
                protocols: vec![JoinGroupRequestProtocol {
                    name,
                    metadata: protocol_metadata.clone(),
                }],
                reason: None,
            };
            let response: JoinGroupResponse =
                self.cluster.send_to_broker(coord_addr, &request).await?;

            if response.error_code == 0 {
                self.member_id = response.member_id.clone();
                self.generation_id = response.generation_id;
                self.leader = response.leader.clone();
                self.protocol_name = response.protocol_name.clone();

                let all_assignments = compute_all_assignments(
                    &self.subscribed_topics,
                    &response,
                    &self.cluster,
                    self.config.partition_assignment_strategy,
                )
                .await?;

                let sync_assignments: Vec<SyncGroupRequestAssignment> = response
                    .members
                    .iter()
                    .map(|m| {
                        let assignment_bytes = all_assignments
                            .get(&m.member_id)
                            .map(|a| {
                                let mut buf = BytesMut::new();
                                a.encode(&mut buf, 0)
                                    .map_err(|e| KafkaError::Protocol(e.to_string()))?;
                                Ok::<_, KafkaError>(buf.freeze())
                            })
                            .unwrap_or(Ok(Bytes::new()))?;
                        Ok(SyncGroupRequestAssignment {
                            member_id: m.member_id.clone(),
                            assignment: assignment_bytes,
                        })
                    })
                    .collect::<Result<_>>()?;

                let sync_request = SyncGroupRequest {
                    group_id: self.config.group_id.clone(),
                    generation_id: self.generation_id,
                    member_id: self.member_id.clone(),
                    group_instance_id: None,
                    protocol_type: Some("consumer".to_string()),
                    protocol_name: self.protocol_name.clone(),
                    assignments: sync_assignments,
                };
                let sync_response: SyncGroupResponse = self
                    .cluster
                    .send_to_broker(coord_addr, &sync_request)
                    .await?;
                if sync_response.error_code != 0 {
                    return Err(KafkaError::Protocol(format!(
                        "SyncGroup error: {}",
                        sync_response.error_code
                    )));
                }

                let mut buf_data = sync_response.assignment;
                let assignment_result = ConsumerProtocolAssignment::decode(&mut buf_data, 0)
                    .map_err(|e| KafkaError::Protocol(e.to_string()))?;
                for tp in assignment_result.assigned_partitions {
                    self.assigned_partitions
                        .entry(tp.topic)
                        .or_default()
                        .extend(tp.partitions);
                }

                self.init_offsets_for_group().await?;
                debug!(
                    "Joined group, assigned partitions={:?}",
                    self.assigned_partitions
                );
                return Ok(());
            }

            if response.error_code == KafkaErrorCode::MEMBER_ID_REQUIRED.code() {
                if response.member_id.is_empty() || response.member_id == member_id {
                    return Err(KafkaError::Protocol(
                        "MEMBER_ID_REQUIRED with no new member_id".into(),
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
        Err(KafkaError::Protocol("JoinGroup retry exhausted".into()))
    }

    async fn resolve_offsets(&mut self) {
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
                            AutoOffsetReset::Latest => -1i64,
                            AutoOffsetReset::Earliest => -2i64,
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
            if let Ok(off) = list_offset_for(&self.cluster, &topic, partition, timestamp).await {
                self.offsets
                    .entry(topic)
                    .or_default()
                    .insert(partition, off);
            }
        }
    }

    async fn init_offsets_for_group(&mut self) -> Result<()> {
        let committed = if let Some(coord) = self.coordinator {
            fetch_committed_offsets_raw(
                &self.cluster,
                &self.config.group_id,
                coord,
                &self.assigned_partitions,
            )
            .await
            .unwrap_or_default()
        } else {
            HashMap::new()
        };

        let default_offset = match self.config.auto_offset_reset {
            AutoOffsetReset::Earliest => -2i64,
            AutoOffsetReset::Latest => -1i64,
            AutoOffsetReset::None => return Err(KafkaError::NoOffsetStored),
        };

        for (topic, partitions) in &self.assigned_partitions {
            for partition in partitions {
                if self
                    .offsets
                    .get(topic)
                    .map(|m| m.contains_key(partition))
                    .unwrap_or(false)
                {
                    continue;
                }
                let off = committed
                    .get(topic)
                    .and_then(|m| m.get(partition))
                    .copied()
                    .unwrap_or(default_offset);
                self.offsets
                    .entry(topic.clone())
                    .or_default()
                    .insert(*partition, off);
            }
        }
        Ok(())
    }

    async fn background_heartbeat(&self) -> Result<()> {
        if self.assigned_partitions.is_empty() || self.member_id.is_empty() {
            return Ok(());
        }
        let Some(coord) = self.coordinator else {
            return Ok(());
        };
        let request = HeartbeatRequest {
            group_id: self.config.group_id.clone(),
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: None,
        };
        let response: crate::protocol::HeartbeatResponse =
            self.cluster.send_to_broker(coord, &request).await?;
        map_heartbeat_error(response.error_code, self.generation_id, &self.member_id)
    }

    async fn send_heartbeat_raw(&self) -> Result<()> {
        if self.member_id.is_empty() {
            return Ok(());
        }
        let coord = find_coordinator_raw(&self.cluster, &self.config.group_id).await?;
        let request = HeartbeatRequest {
            group_id: self.config.group_id.clone(),
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: None,
        };
        let response: crate::protocol::HeartbeatResponse =
            self.cluster.send_to_broker(coord, &request).await?;
        map_heartbeat_error(response.error_code, self.generation_id, &self.member_id)
    }

    async fn do_commit(&mut self) -> Result<()> {
        if self.config.group_id.is_empty() || self.offsets.is_empty() {
            return Ok(());
        }

        let topic_partitions: HashMap<String, Vec<(i32, i64)>> = self
            .offsets
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
            group_id: self.config.group_id.clone(),
            generation_id_or_member_epoch: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics,
        };

        for attempt in 0..2 {
            let coord = self.coordinator.ok_or(KafkaError::NoCoordinator)?;
            let response: Result<crate::protocol::OffsetCommitResponse> =
                self.cluster.send_to_broker(coord, &request).await;

            match response {
                Ok(resp) => {
                    let not_coord = resp
                        .topics
                        .iter()
                        .flat_map(|t| &t.partitions)
                        .any(|p| p.error_code == KafkaErrorCode::NOT_COORDINATOR.code());
                    if not_coord && attempt == 0 {
                        debug!(
                            "Commit got NOT_COORDINATOR (16), re-finding coordinator and retrying..."
                        );
                        if let Ok(addr) =
                            find_coordinator_raw(&self.cluster, &self.config.group_id).await
                        {
                            self.coordinator = Some(addr);
                            continue;
                        }
                    }
                    for tr in resp.topics {
                        for pr in tr.partitions {
                            if pr.error_code != 0 {
                                return Err(KafkaError::OffsetCommitError(pr.error_code));
                            }
                        }
                    }
                    return Ok(());
                }
                Err(KafkaError::ConnectionClosed | KafkaError::Io(_)) => {
                    if attempt == 0 {
                        debug!("Commit connection failed, re-finding coordinator and retrying...");
                        if let Ok(addr) =
                            find_coordinator_raw(&self.cluster, &self.config.group_id).await
                        {
                            self.coordinator = Some(addr);
                            continue;
                        }
                    }
                    return Err(KafkaError::ConnectionClosed);
                }
                Err(e) => return Err(e),
            }
        }
        Err(KafkaError::NoCoordinator)
    }

    async fn send_leave_group(&self) -> Result<()> {
        if self.member_id.is_empty() {
            return Ok(());
        }
        let Some(coord) = self.coordinator else {
            return Ok(());
        };
        use crate::protocol::leave_group_request::MemberIdentity;
        let request = LeaveGroupRequest {
            group_id: self.config.group_id.clone(),
            member_id: self.member_id.clone(),
            members: vec![MemberIdentity {
                member_id: self.member_id.clone(),
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
}
