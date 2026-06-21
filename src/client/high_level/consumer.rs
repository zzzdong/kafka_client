use bytes::Bytes;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, watch};
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::client::low_level::KafkaClient;
use crate::error::{KafkaError, Result};
use crate::protocol::Message;
use crate::protocol::{
    ConsumerProtocolAssignment, FetchPartition, FetchRequest, FetchResponse, FetchTopic,
    FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest, JoinGroupRequest,
    JoinGroupRequestProtocol, JoinGroupResponse, LeaveGroupRequest, ListOffsetsPartition,
    ListOffsetsRequest, ListOffsetsTopic, OffsetCommitRequest, OffsetCommitRequestPartition,
    OffsetCommitRequestTopic, OffsetFetchRequest, OffsetFetchRequestGroup,
    OffsetFetchRequestTopics, SyncGroupRequest, SyncGroupRequestAssignment, SyncGroupResponse,
    TopicPartition,
};

/// 消费者配置
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub group_id: String,
    pub auto_commit: bool,
    pub auto_commit_interval_ms: u64,
    pub auto_offset_reset: AutoOffsetReset,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub partition_max_bytes: i32,
    pub max_wait_ms: i32,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub heartbeat_interval_ms: u64,
    pub partition_assignment_strategy: PartitionAssignmentStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoOffsetReset {
    Earliest,
    Latest,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionAssignmentStrategy {
    /// Range 分配策略：按主题分区范围分配
    Range,
    /// RoundRobin 分配策略：轮询分配
    RoundRobin,
    /// CooperativeSticky 分配策略：协作式粘性分配
    CooperativeSticky,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: "rust-consumer".to_string(),
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

/// 消息头
#[derive(Debug, Clone)]
pub struct Header {
    pub key: String,
    pub value: Bytes,
}

/// 消费记录
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

/// 消费者组成员状态
#[derive(Debug, Clone, Default)]
struct GroupState {
    member_id: String,
    generation_id: i32,
    leader: String,
    protocol_name: Option<String>,
    /// 当前分配到的分区
    assigned_partitions: HashMap<String, Vec<i32>>,
}

/// 高级消费者
pub struct Consumer {
    client: Arc<Mutex<KafkaClient>>,
    /// 订阅的主题列表
    subscribed_topics: Vec<String>,
    /// 当前分配和管理的分区 -> 偏移量
    offsets: Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
    config: ConsumerConfig,
    /// 消费者组状态
    group_state: Arc<RwLock<GroupState>>,
    /// 协调者地址缓存
    coordinator: Arc<RwLock<Option<std::net::SocketAddr>>>,
    /// 是否正在运行
    running: Arc<std::sync::atomic::AtomicBool>,
    /// 通知后台任务变更主题订阅
    subscribed_topics_tx: watch::Sender<Vec<String>>,
}

impl Consumer {
    pub async fn new(client: Arc<Mutex<KafkaClient>>, config: ConsumerConfig) -> Self {
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (subscribed_topics_tx, subscribed_topics_rx) =
            watch::channel::<Vec<String>>(Vec::new());

        let consumer = Self {
            client: client.clone(),
            subscribed_topics: Vec::new(),
            offsets: Arc::new(RwLock::new(HashMap::new())),
            config: config.clone(),
            group_state: Arc::new(RwLock::new(GroupState::default())),
            coordinator: Arc::new(RwLock::new(None)),
            running: running.clone(),
            subscribed_topics_tx,
        };

        // 启动自动提交任务
        if consumer.config.auto_commit {
            let offsets = consumer.offsets.clone();
            let client = consumer.client.clone();
            let group_id = consumer.config.group_id.clone();
            let coordinator = consumer.coordinator.clone();
            let running = running.clone();

            tokio::spawn(async move {
                let mut timer = interval(Duration::from_millis(
                    consumer.config.auto_commit_interval_ms,
                ));
                loop {
                    timer.tick().await;
                    if !running.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    if let Err(e) =
                        Self::do_commit(&client, &group_id, &offsets, &coordinator).await
                    {
                        debug!("Auto commit failed: {}", e);
                    }
                }
            });
        }

        // 启动后台心跳任务（仅在消费者组模式下）
        if !config.group_id.is_empty() {
            let hb_client = consumer.client.clone();
            let hb_group_id = config.group_id.clone();
            let hb_group_state = consumer.group_state.clone();
            let hb_coordinator = consumer.coordinator.clone();
            let hb_running = running.clone();
            let hb_heartbeat_interval = Duration::from_millis(config.heartbeat_interval_ms);

            tokio::spawn(async move {
                eprintln!("[heartbeat-{}] TASK STARTED", hb_group_id);
                let mut timer = tokio::time::interval(hb_heartbeat_interval);
                let mut tick_count = 0u64;
                loop {
                    timer.tick().await;
                    tick_count += 1;
                    if !hb_running.load(std::sync::atomic::Ordering::Relaxed) {
                        eprintln!("[heartbeat-{}] stopping", hb_group_id);
                        break;
                    }

                    let (generation_id, member_id) = {
                        let gs = hb_group_state.read().await;
                        if gs.assigned_partitions.is_empty() || gs.member_id.is_empty() {
                            eprintln!(
                                "[heartbeat-{}] tick={}: skipping (no partitions/member)",
                                hb_group_id, tick_count
                            );
                            continue;
                        }
                        (gs.generation_id, gs.member_id.clone())
                    };

                    let coord_addr = {
                        let c = hb_coordinator.read().await;
                        match *c {
                            Some(addr) => addr,
                            None => {
                                eprintln!(
                                    "[heartbeat-{}] tick={}: skipping (no coordinator)",
                                    hb_group_id, tick_count
                                );
                                continue;
                            }
                        }
                    };

                    let request = HeartbeatRequest {
                        group_id: hb_group_id.clone(),
                        generation_id,
                        member_id: member_id.clone(),
                        group_instance_id: None,
                    };

                    eprintln!(
                        "[heartbeat-{}] tick={}: sending heartbeat to {}, member={}",
                        hb_group_id, tick_count, coord_addr, member_id
                    );
                    match hb_client
                        .lock()
                        .await
                        .send_request::<_, crate::protocol::HeartbeatResponse>(
                            coord_addr, 12, &request,
                        )
                        .await
                    {
                        Ok(resp) => {
                            eprintln!(
                                "[heartbeat-{}] tick={}: response error_code={}",
                                hb_group_id, tick_count, resp.error_code
                            );
                            if resp.error_code == 27 {
                                warn!(
                                    "Heartbeat REBALANCE_IN_PROGRESS for {}, will rejoin on next poll",
                                    member_id
                                );
                                let mut gs = hb_group_state.write().await;
                                gs.assigned_partitions.clear();
                                gs.member_id.clear();
                                *hb_coordinator.write().await = None;
                            } else if resp.error_code != 0 {
                                debug!(
                                    "Heartbeat error for {}: error_code={}",
                                    member_id, resp.error_code
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "[heartbeat-{}] tick={}: send error: {:?}",
                                hb_group_id, tick_count, e
                            );
                            debug!("Heartbeat request failed for {}: {}", member_id, e);
                        }
                    }
                }
            });

            // 启动后台加入消费者组任务
            let bg_client = consumer.client.clone();
            let bg_group_id = config.group_id.clone();
            let bg_group_state = consumer.group_state.clone();
            let bg_coordinator = consumer.coordinator.clone();
            let bg_offsets = consumer.offsets.clone();
            let bg_config = config.clone();
            let bg_running = running.clone();
            let mut bg_topics_rx = subscribed_topics_rx.clone();

            tokio::spawn(async move {
                // 等待第一个主题通知
                let mut topics = loop {
                    bg_topics_rx.changed().await.ok();
                    let t = bg_topics_rx.borrow_and_update().clone();
                    if !t.is_empty() {
                        break t;
                    }
                };

                loop {
                    if !bg_running.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }

                    // 尝试加入组
                    eprintln!(
                        "[bg_task] calling background_join_group for group={}, topics={:?}",
                        bg_group_id, topics
                    );
                    match Self::background_join_group(
                        &bg_client,
                        &bg_group_id,
                        &bg_group_state,
                        &bg_coordinator,
                        &bg_offsets,
                        &bg_config,
                        &topics,
                    )
                    .await
                    {
                        Ok(()) => {
                            eprintln!(
                                "[bg_task] background_join_group succeeded for group={}",
                                bg_group_id
                            );
                            info!(
                                "background_join_group: group joined, waiting for rebalance trigger"
                            );
                        }
                        Err(e) => {
                            eprintln!(
                                "[bg_task] background_join_group FAILED for group={}: {:?}",
                                bg_group_id, e
                            );
                            warn!(
                                "background_join_group failed: {:?}, will retry on next topic change",
                                e
                            );
                            // 清空状态以便 poll 感知到需要重试
                            let mut gs = bg_group_state.write().await;
                            gs.assigned_partitions.clear();
                            gs.member_id.clear();
                            *bg_coordinator.write().await = None;
                        }
                    }

                    // 等待主题变更（新 subscribe 调用），或者心跳检测到再均衡
                    // 使用 tokio::select 在主题变更和心跳检测之间等待
                    let wait_for_rebalance = async {
                        loop {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            let gs = bg_group_state.read().await;
                            if gs.assigned_partitions.is_empty() || gs.member_id.is_empty() {
                                return;
                            }
                        }
                    };

                    tokio::select! {
                        result = bg_topics_rx.changed() => {
                            if result.is_ok() {
                                topics = bg_topics_rx.borrow_and_update().clone();
                                info!("background_join_group: topics changed to {:?}, rejoining", topics);
                                // 清空状态
                                let mut gs = bg_group_state.write().await;
                                gs.assigned_partitions.clear();
                                gs.member_id.clear();
                                *bg_coordinator.write().await = None;
                            } else {
                                break;
                            }
                        }
                        _ = wait_for_rebalance => {
                            info!("background_join_group: rebalance detected, rejoining group");
                        }
                    }
                }
            });
        }

        consumer
    }

    /// 订阅主题列表
    pub async fn subscribe(&mut self, topics: Vec<String>) -> Result<()> {
        self.subscribed_topics = topics.clone();

        if self.config.group_id.is_empty() {
            // 没有消费者组，直接根据 metadata 获取偏移量
            let mut all_offsets = HashMap::new();
            {
                let client = self.client.lock().await;
                for topic in &topics {
                    let partitions = client
                        .metadata()
                        .get_partitions(topic)
                        .await
                        .ok_or_else(|| KafkaError::TopicNotFound(topic.clone()))?;
                    all_offsets.insert(topic.clone(), partitions.clone());
                }
            }
            self.init_offsets_simple(topics).await?;
        } else {
            // 有消费者组：通知后台任务开始加入组
            debug!("subscribe: group_id set, notifying background task to join group");
            let _ = self.subscribed_topics_tx.send(topics.clone());
        }

        Ok(())
    }

    /// 加入消费者组（可选，当有 group_id 时使用，支持 MEMBER_ID_REQUIRED 重试）
    async fn join_consumer_group(&self) -> Result<()> {
        let topics = self.subscribed_topics.clone();
        if topics.is_empty() {
            return Ok(());
        }

        // 1. 查找协调者
        let coordinator = self.find_coordinator().await?;
        {
            let mut coord = self.coordinator.write().await;
            *coord = Some(coordinator);
        }

        // 2. 加入组（可能重试 MEMBER_ID_REQUIRED error=79）
        let protocol_metadata = self.build_subscription_metadata(&topics)?;
        let mut member_id = String::new();
        let mut attempt = 0u32;

        loop {
            let request = JoinGroupRequest {
                group_id: self.config.group_id.clone(),
                session_timeout_ms: self.config.session_timeout_ms,
                rebalance_timeout_ms: self.config.rebalance_timeout_ms,
                member_id: member_id.clone(),
                group_instance_id: None,
                protocol_type: "consumer".to_string(),
                protocols: vec![JoinGroupRequestProtocol {
                    name: match self.config.partition_assignment_strategy {
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

            debug!(?request, "JoinGroup request");
            let mut client = self.client.lock().await;
            let response: JoinGroupResponse =
                client.send_request(coordinator, 11, &request).await?;
            drop(client);
            debug!(?response, "JoinGroup response");

            if response.error_code == 0 {
                let generation_id = response.generation_id;
                let is_leader = response.leader == response.member_id;
                let protocol_name = response.protocol_name.clone();
                let new_member_id = response.member_id.clone();

                debug!(
                    "Joined group: member_id={}, generation_id={}, leader={}",
                    new_member_id, generation_id, is_leader
                );

                // 3. 如果是 leader，计算分区分配
                let assignment_bytes = if is_leader {
                    Self::compute_assignment(
                        &topics,
                        &response,
                        &self.client,
                        self.config.partition_assignment_strategy,
                    )
                    .await?
                } else {
                    Bytes::new()
                };

                // 4. 同步组
                let sync_request = SyncGroupRequest {
                    group_id: self.config.group_id.clone(),
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

                let mut client = self.client.lock().await;
                let sync_response: SyncGroupResponse =
                    client.send_request(coordinator, 14, &sync_request).await?;
                drop(client);

                if sync_response.error_code != 0 {
                    return Err(KafkaError::Protocol(format!(
                        "SyncGroup failed: error {}",
                        sync_response.error_code
                    )));
                }

                // 5. 解析分配
                let assignment = self.parse_assignment(sync_response.assignment)?;

                // 6. 初始化偏移量并更新状态
                let mut group_state = self.group_state.write().await;
                group_state.member_id = new_member_id;
                group_state.generation_id = generation_id;
                group_state.leader = response.leader;
                group_state.protocol_name = protocol_name;
                group_state.assigned_partitions = assignment;

                // 初始化偏移量
                self.fetch_committed_offsets(&group_state.assigned_partitions)
                    .await?;

                return Ok(());
            }

            if response.error_code == 79 {
                // MEMBER_ID_REQUIRED: broker assigned a member_id
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
                attempt += 1;
                if attempt > 5 {
                    return Err(KafkaError::Protocol(
                        "MEMBER_ID_REQUIRED retry exhausted".to_string(),
                    ));
                }
                continue;
            }

            return Err(KafkaError::Protocol(format!(
                "JoinGroup failed: error {}",
                response.error_code
            )));
        }
    }

    fn build_subscription_metadata(&self, topics: &[String]) -> Result<Bytes> {
        // Manually encode the Kafka consumer-group subscription metadata.
        // The generated ConsumerProtocolSubscription type uses flexible RPC encoding,
        // but the on-wire consumer protocol is a separate versioned byte stream.
        // Use version 2 so modern brokers accept the metadata.
        let mut buf = bytes::BytesMut::new();
        use bytes::BufMut;
        buf.put_i16(2); // ConsumerProtocolSubscription version 2
        buf.put_i32(topics.len() as i32);
        for t in topics {
            buf.put_i16(t.len() as i16);
            buf.put_slice(t.as_bytes());
        }
        buf.put_i32(-1); // null user_data
        // owned_partitions: empty array
        buf.put_i32(0);
        // generation_id
        buf.put_i32(-1);
        Ok(buf.freeze())
    }

    /// 计算分区分配
    async fn compute_assignment(
        topics: &[String],
        join_response: &JoinGroupResponse,
        client: &Arc<Mutex<KafkaClient>>,
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

        let client_guard = client.lock().await;

        for topic in topics {
            let partitions = client_guard
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
                    // 简化为 range 策略
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

        // 找到当前成员的分配
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

    fn parse_assignment(&self, data: Bytes) -> Result<HashMap<String, Vec<i32>>> {
        let mut buf = data;
        let assignment = ConsumerProtocolAssignment::decode(&mut buf, 0)
            .map_err(|e| KafkaError::Protocol(e.to_string()))?;

        let mut result = HashMap::new();
        for tp in assignment.assigned_partitions {
            result.insert(tp.topic, tp.partitions);
        }
        Ok(result)
    }

    /// 从协调者获取已提交的偏移量
    async fn fetch_committed_offsets(&self, assigned: &HashMap<String, Vec<i32>>) -> Result<()> {
        let coordinator = self.find_coordinator().await?;

        // 为每个分区初始化偏移量，如果没有提交的偏移量则使用 auto_offset_reset
        let mut needs_initialization: Vec<(String, i32)> = Vec::new();

        {
            let mut offsets = self.offsets.write().await;
            for (topic, partitions) in assigned {
                let topic_offsets = offsets.entry(topic.clone()).or_insert_with(HashMap::new);
                for partition in partitions {
                    // 先尝试获取已提交的偏移量
                    match self
                        .fetch_offset_for_partition(coordinator, topic, *partition)
                        .await
                    {
                        Ok(Some(committed_offset)) => {
                            // 使用已提交偏移量
                            topic_offsets.insert(*partition, committed_offset);
                            if committed_offset < 0 {
                                needs_initialization.push((topic.clone(), *partition));
                            }
                        }
                        Ok(None) => {
                            // 没有已提交偏移量，待初始化
                            needs_initialization.push((topic.clone(), *partition));
                            topic_offsets.insert(*partition, -1);
                        }
                        Err(_) => {
                            needs_initialization.push((topic.clone(), *partition));
                            topic_offsets.insert(*partition, -1);
                        }
                    }
                }
            }
        }

        // 根据 auto_offset_reset 初始化未设置的偏移量
        self.init_offsets_for_partitions(needs_initialization)
            .await?;

        Ok(())
    }

    async fn fetch_offset_for_partition(
        &self,
        coordinator: std::net::SocketAddr,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>> {
        // 使用新的 OffsetFetchRequest 格式 (version 8+)
        let request = OffsetFetchRequest {
            group_id: String::new(), // version 0-7 字段，新版本忽略
            topics: None,            // version 0-7 字段
            groups: vec![OffsetFetchRequestGroup {
                group_id: self.config.group_id.clone(),
                member_id: None,
                member_epoch: -1,
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: Some(topic.to_string()),
                    topic_id: None,
                    partition_indexes: vec![partition],
                }]),
            }],
            require_stable: false,
        };

        let mut client = self.client.lock().await;
        let response: crate::protocol::OffsetFetchResponse =
            client.send_request(coordinator, 9, &request).await?;

        for group in response.groups {
            if group.group_id == self.config.group_id {
                for t in group.topics {
                    if t.name.as_deref() == Some(topic) {
                        for p in t.partitions {
                            if p.partition_index == partition
                                && p.error_code == 0 {
                                    if p.committed_offset >= 0 {
                                        return Ok(Some(p.committed_offset));
                                    }
                                    return Ok(None);
                                }
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// 根据 auto_offset_reset 初始化偏移量
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
            let client = self.client.lock().await;
            let mut offsets = self.offsets.write().await;

            for topic in &topics {
                let partitions = client
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
        let mut client = self.client.lock().await;
        let leader_addr = client
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

        let response = client
            .send_request::<ListOffsetsRequest, crate::protocol::ListOffsetsResponse>(
                leader_addr,
                2,
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
                        info!(
                            topic,
                            partition,
                            offset = partition_response.offset,
                            "list_offset result"
                        );
                        return Ok(partition_response.offset);
                    }
                }
            }
        }

        Err(KafkaError::OffsetNotFound(topic.to_string(), partition))
    }

    /// 查找协调者，支持重试（协调者可能尚未就绪，error_code=15）
    pub async fn find_coordinator(&self) -> Result<std::net::SocketAddr> {
        // 检查缓存
        if let Some(addr) = *self.coordinator.read().await {
            return Ok(addr);
        }

        let mut last_error = KafkaError::NoCoordinator;
        for attempt in 0..10 {
            let mut client = self.client.lock().await;
            let broker_addr = match client.any_broker_address() {
                Some(addr) => addr,
                None => {
                    last_error = KafkaError::NoBrokerAvailable;
                    drop(client);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            let request = FindCoordinatorRequest {
                key: self.config.group_id.clone(),
                key_type: 0,
                coordinator_keys: vec![],
            };

            let response: FindCoordinatorResponse =
                match client.send_request(broker_addr, 10, &request).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        last_error = e;
                        drop(client);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                };
            // Release the lock before potentially sleeping
            drop(client);
            debug!(?response, "FindCoordinator response");

            if response.error_code == 0 {
                let host = if response.host.is_empty() {
                    response
                        .coordinators
                        .first()
                        .map(|c| c.host.clone())
                        .unwrap_or_default()
                } else {
                    response.host
                };

                let port = if response.port == 0 {
                    response
                        .coordinators
                        .first()
                        .map(|c| c.port)
                        .unwrap_or(9092)
                } else {
                    response.port
                };

                let addr: std::net::SocketAddr = match format!("{}:{}", host, port)
                    .to_socket_addrs()
                {
                    Ok(mut addrs) => match addrs.next() {
                        Some(a) => a,
                        None => {
                            last_error = KafkaError::NoCoordinator;
                            tokio::time::sleep(Duration::from_millis(500 * (attempt as u64 + 1)))
                                .await;
                            continue;
                        }
                    },
                    Err(_) => {
                        last_error = KafkaError::NoCoordinator;
                        tokio::time::sleep(Duration::from_millis(500 * (attempt as u64 + 1))).await;
                        continue;
                    }
                };

                // 缓存
                *self.coordinator.write().await = Some(addr);
                return Ok(addr);
            }

            // error_code 15 = COORDINATOR_NOT_AVAILABLE — retry
            last_error = KafkaError::NoCoordinator;
            tokio::time::sleep(Duration::from_millis(500 * (attempt as u64 + 1))).await;
        }

        Err(last_error)
    }

    /// 后台任务使用的加入消费者组（静态方法，通过共享状态工作）
    async fn background_join_group(
        client: &Arc<Mutex<KafkaClient>>,
        group_id: &str,
        group_state: &Arc<RwLock<GroupState>>,
        coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
        offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
        config: &ConsumerConfig,
        topics: &[String],
    ) -> Result<()> {
        if topics.is_empty() {
            eprintln!("[bg_join] topics empty, returning");
            return Ok(());
        }

        eprintln!(
            "[bg_join] starting join for group={}, topics={:?}",
            group_id, topics
        );

        // 1. 查找协调者（重试）
        let coord_addr = {
            let mut last_error = KafkaError::NoCoordinator;
            let mut found = None;
            for attempt in 0..10 {
                let mut c = client.lock().await;
                let broker_addr = match c.any_broker_address() {
                    Some(addr) => addr,
                    None => {
                        last_error = KafkaError::NoBrokerAvailable;
                        drop(c);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                };

                let request = FindCoordinatorRequest {
                    key: group_id.to_string(),
                    key_type: 0,
                    coordinator_keys: vec![],
                };

                let response: FindCoordinatorResponse =
                    match c.send_request(broker_addr, 10, &request).await {
                        Ok(resp) => resp,
                        Err(e) => {
                            last_error = e;
                            drop(c);
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                    };
                drop(c);

                if response.error_code == 0 {
                    let host = if response.host.is_empty() {
                        response
                            .coordinators
                            .first()
                            .map(|c| c.host.clone())
                            .unwrap_or_default()
                    } else {
                        response.host
                    };
                    let port = if response.port == 0 {
                        response
                            .coordinators
                            .first()
                            .map(|c| c.port)
                            .unwrap_or(9092)
                    } else {
                        response.port
                    };
                    if let Ok(mut addrs) = format!("{}:{}", host, port).to_socket_addrs() {
                        if let Some(a) = addrs.next() {
                            found = Some(a);
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(500 * (attempt as u64 + 1))).await;
            }
            match found {
                Some(addr) => addr,
                None => return Err(last_error),
            }
        };

        // 缓存协调者
        *coordinator.write().await = Some(coord_addr);
        eprintln!("[bg_join] coordinator found: {}", coord_addr);

        // 2. 构建协议元数据（同 build_subscription_metadata）
        let protocol_metadata = {
            let mut buf = bytes::BytesMut::new();
            use bytes::BufMut;
            buf.put_i16(2); // ConsumerProtocolSubscription version 2
            buf.put_i32(topics.len() as i32);
            for t in topics {
                buf.put_i16(t.len() as i16);
                buf.put_slice(t.as_bytes());
            }
            buf.put_i32(-1); // null user_data
            buf.put_i32(0); // owned_partitions: empty
            buf.put_i32(-1); // generation_id
            buf.freeze()
        };

        // 3. JoinGroup 循环（处理 MEMBER_ID_REQUIRED）
        let mut member_id = String::new();
        for join_attempt in 0..10u32 {
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

            let mut c = client.lock().await;
            eprintln!(
                "[bg_join] sending JoinGroup attempt={}, member_id={:?}",
                join_attempt, member_id
            );
            let response: JoinGroupResponse = match c.send_request(coord_addr, 11, &request).await {
                Ok(resp) => resp,
                Err(e) => {
                    eprintln!("[bg_join] JoinGroup send error: {:?}", e);
                    drop(c);
                    return Err(e);
                }
            };
            drop(c);

            eprintln!(
                "[bg_join] JoinGroup response: error_code={}, member_id={}, leader={}, generation_id={}",
                response.error_code, response.member_id, response.leader, response.generation_id
            );

            if response.error_code == 0 {
                let generation_id = response.generation_id;
                let is_leader = response.leader == response.member_id;
                let protocol_name = response.protocol_name.clone();
                let new_member_id = response.member_id.clone();

                // 计算分配（如果 leader）
                let assignment_bytes = if is_leader {
                    Self::compute_assignment(
                        topics,
                        &response,
                        client,
                        config.partition_assignment_strategy,
                    )
                    .await?
                } else {
                    Bytes::new()
                };

                // SyncGroup
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

                eprintln!("[bg_join] sending SyncGroup, is_leader={}", is_leader);
                let mut c = client.lock().await;
                let sync_response: SyncGroupResponse =
                    match c.send_request(coord_addr, 14, &sync_request).await {
                        Ok(resp) => resp,
                        Err(e) => {
                            eprintln!("[bg_join] SyncGroup send error: {:?}", e);
                            drop(c);
                            return Err(e);
                        }
                    };
                drop(c);
                eprintln!(
                    "[bg_join] SyncGroup response: error_code={}, assignment.len={}",
                    sync_response.error_code,
                    sync_response.assignment.len()
                );

                if sync_response.error_code != 0 {
                    // SyncGroup 失败（如 REBALANCE_IN_PROGRESS），返回错误让后台任务在下一个 tick 重试
                    return Err(KafkaError::Protocol(format!(
                        "SyncGroup error: {}",
                        sync_response.error_code
                    )));
                }

                // 解析分配（同 parse_assignment）
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

                // 更新状态
                {
                    let mut gs = group_state.write().await;
                    gs.member_id = new_member_id;
                    gs.generation_id = generation_id;
                    gs.leader = response.leader;
                    gs.protocol_name = protocol_name;
                    gs.assigned_partitions = assignment.clone();
                }

                // 初始化偏移量
                Self::init_offsets_for_group(
                    client,
                    group_id,
                    offsets,
                    &assignment,
                    config.auto_offset_reset,
                )
                .await?;

                debug!(
                    "background_join_group: joined group, assigned partitions={:?}",
                    assignment
                );
                info!("background_join_group SUCCESS for group={}", group_id);
                return Ok(());
            }

            if response.error_code == 79 {
                eprintln!(
                    "[bg_join] MEMBER_ID_REQUIRED, retrying with member_id={:?}",
                    response.member_id
                );
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

            eprintln!(
                "[bg_join] JoinGroup unexpected error_code={}",
                response.error_code
            );
            return Err(KafkaError::Protocol(format!(
                "JoinGroup error: {}",
                response.error_code
            )));
        }

        Err(KafkaError::Protocol(
            "JoinGroup retry exhausted".to_string(),
        ))
    }

    /// 初始化消费者组的偏移量（供后台任务使用）
    #[allow(unused_variables)]
    async fn init_offsets_for_group(
        _client: &Arc<Mutex<KafkaClient>>,
        _group_id: &str,
        offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
        assignment: &HashMap<String, Vec<i32>>,
        auto_offset_reset: AutoOffsetReset,
    ) -> Result<()> {
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
            for partition in partitions {
                if !topic_offsets.contains_key(partition) {
                    topic_offsets.insert(*partition, default_offset);
                }
            }
        }
        drop(offsets_writer);

        debug!(
            "init_offsets_for_group: initialized offsets to {:?} for assigned partitions",
            default_offset
        );
        Ok(())
    }

    /// 发送心跳
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

        let mut client = self.client.lock().await;
        let response: crate::protocol::HeartbeatResponse =
            client.send_request(coordinator, 12, &request).await?;

        if response.error_code != 0 {
            // 如果收到 REBALANCE_IN_PROGRESS 错误，需要重新加入组
            if response.error_code == 27 {
                // Not in sync group, need to rejoin
                warn!("Heartbeat indicates rebalance needed, rejoining group");
                // 清除协调者缓存，下次 find_coordinator 会重新查找
                *self.coordinator.write().await = None;
                // 这里只是标记，实际 rejoin 由外部调用
                return Err(KafkaError::Protocol("Rebalance required".to_string()));
            }
            return Err(KafkaError::Protocol(format!(
                "Heartbeat failed: error {}",
                response.error_code
            )));
        }

        Ok(())
    }

    /// 拉取消息
    pub async fn poll(&mut self, timeout_ms: i32) -> Result<Vec<ConsumerRecord>> {
        // 如果有消费者组但尚未加入（后台任务正在进行），返回空结果
        if !self.config.group_id.is_empty() {
            let needs_join = {
                let gs = self.group_state.read().await;
                gs.assigned_partitions.is_empty()
            };
            if needs_join {
                // 后台任务正在处理组加入，poll 不阻塞，直接返回空
                return Ok(Vec::new());
            }
        }

        // 确定要拉取的分区
        let fetch_targets: Vec<(String, i32, i64)> = {
            let group_state = self.group_state.read().await;
            let offsets = self.offsets.read().await;

            // 从 assigned_partitions 中获取
            let assigned = if !group_state.assigned_partitions.is_empty() {
                group_state.assigned_partitions.clone()
            } else {
                // 如果没有消费者组分配，使用 subscribed_topics 的所有分区
                let mut all: HashMap<String, Vec<i32>> = HashMap::new();
                let client = self.client.lock().await;
                for topic in &self.subscribed_topics {
                    if let Some(partitions) = client.metadata().get_partitions(topic).await {
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
            // 尝试延迟初始化偏移量（poll 第一次调用时 offset 可能为 -1/-2）
            self.lazy_init_offsets().await;
            // 重新获取 targets
            let targets = self.collect_fetch_targets().await;
            if targets.is_empty() {
                info!("poll: no fetch targets");
                return Ok(Vec::new());
            }
            return self.execute_fetch(targets, timeout_ms).await;
        }

        info!(?fetch_targets, "poll: fetch targets");
        self.execute_fetch(fetch_targets, timeout_ms).await
    }

    /// 延迟初始化尚未解析的偏移量（-1 = Latest, -2 = Earliest）
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

    /// 收集所有可 fetch 的目标（offset >= 0）
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

    /// 执行 fetch 请求并解析响应
    async fn execute_fetch(
        &self,
        fetch_targets: Vec<(String, i32, i64)>,
        timeout_ms: i32,
    ) -> Result<Vec<ConsumerRecord>> {
        let mut all_records = Vec::new();
        let futures: Vec<_> = fetch_targets
            .into_iter()
            .map(|(topic, partition, offset)| {
                let client = self.client.clone();
                let min_bytes = self.config.min_bytes;
                let max_bytes = self.config.max_bytes;
                let partition_max_bytes = self.config.partition_max_bytes;

                async move {
                    Self::fetch_partition(
                        &client,
                        &topic,
                        partition,
                        offset,
                        timeout_ms,
                        min_bytes,
                        max_bytes,
                        partition_max_bytes,
                    )
                    .await
                }
            })
            .collect();

        // 并发执行所有 fetch 请求
        let results = futures::future::join_all(futures).await;

        // 处理结果并更新偏移量
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
                    }
                }
            }
        }

        Ok(all_records)
    }

    async fn fetch_partition(
        client: &Arc<Mutex<KafkaClient>>,
        topic: &str,
        partition: i32,
        offset: i64,
        timeout_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        partition_max_bytes: i32,
    ) -> Result<Vec<ConsumerRecord>> {
        let mut client_guard = client.lock().await;

        // 刷新元数据（如果过期）
        if client_guard.metadata().is_expired().await {
            let _ = client_guard.refresh_metadata().await;
        }

        let leader_addr = client_guard
            .metadata()
            .get_partition_leader(topic, partition)
            .await
            .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;

        // 获取连接 Arc 后释放 KafkaClient 锁，避免长时间持有影响心跳
        let conn = client_guard.get_broker_connection(leader_addr).await?;
        let conn_arc = conn.clone();
        drop(client_guard);

        let request = FetchRequest {
            cluster_id: None,
            replica_id: -1,
            replica_state: Default::default(),
            max_wait_ms: timeout_ms,
            min_bytes,
            max_bytes,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![FetchTopic {
                topic: Some(topic.to_string()),
                topic_id: None,
                partitions: vec![FetchPartition {
                    partition,
                    current_leader_epoch: -1,
                    fetch_offset: offset,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes,
                    replica_directory_id: None,
                    high_watermark: 9223372036854775807,
                }],
            }],
            forgotten_topics_data: vec![],
            rack_id: Some(String::new()),
        };

        // 直接通过连接发送请求，不持有 KafkaClient 锁
        let mut conn_guard = conn_arc.lock().await;
        let response: FetchResponse = conn_guard.send_request(&request).await?;
        drop(conn_guard);
        info!(topic, partition, offset, leader = %leader_addr, "fetch_partition response received");
        Self::parse_fetch_response(response, topic, partition)
    }

    fn parse_fetch_response(
        response: FetchResponse,
        topic_name: &str,
        partition_index: i32,
    ) -> Result<Vec<ConsumerRecord>> {
        let mut records = Vec::new();
        info!(
            "parse_fetch_response: topic={}, partition={}",
            topic_name, partition_index
        );

        for topic_response in response.responses {
            if topic_response.topic.as_deref() != Some(topic_name) {
                info!(
                    "  skip topic response: {:?} != {}",
                    topic_response.topic, topic_name
                );
                continue;
            }

            for partition_response in topic_response.partitions {
                info!(
                    "  partition in response: index={}, error_code={}, records={:?}",
                    partition_response.partition_index,
                    partition_response.error_code,
                    partition_response.records.as_ref().map(|r| r.records.len())
                );

                if partition_response.partition_index != partition_index {
                    info!(
                        "  skip partition {} != requested {}",
                        partition_response.partition_index, partition_index
                    );
                    continue;
                }

                if partition_response.error_code != 0 {
                    if partition_response.error_code == 27 {
                        // 如果返回 NOT_LEADER_OR_FOLLOWER，返回特殊错误
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

                info!(
                    "parse_fetch_response: base_offset={}, n_records={}, first_timestamp={}",
                    batch.base_offset,
                    batch.records.len(),
                    batch.first_timestamp
                );

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
                    info!(
                        "parse_fetch_response: record[{}] offset={}, partition={}, key={:?}, val_len={:?}",
                        idx,
                        offset,
                        partition_index,
                        record
                            .key
                            .as_ref()
                            .map(|k| String::from_utf8_lossy(k).to_string()),
                        record.value.as_ref().map(|v| v.len())
                    );

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

    /// 提交偏移量到协调者
    pub async fn commit(&self) -> Result<()> {
        Self::do_commit(
            &self.client,
            &self.config.group_id,
            &self.offsets,
            &self.coordinator,
        )
        .await
    }

    async fn do_commit(
        client: &Arc<Mutex<KafkaClient>>,
        group_id: &str,
        offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
        coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
    ) -> Result<()> {
        if group_id.is_empty() {
            return Ok(());
        }

        // 获取要提交的偏移量
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

        // 查找协调者
        let coordinator_addr = {
            let mut coord = coordinator.write().await;
            if let Some(addr) = *coord {
                addr
            } else {
                // 查找协调者
                let mut client_guard = client.lock().await;
                let broker_addr = client_guard
                    .any_broker_address()
                    .ok_or(KafkaError::NoBrokerAvailable)?;

                let request = FindCoordinatorRequest {
                    key: group_id.to_string(),
                    key_type: 0,
                    coordinator_keys: vec![],
                };

                let response: FindCoordinatorResponse =
                    client_guard.send_request(broker_addr, 10, &request).await?;
                if response.error_code != 0 {
                    return Err(KafkaError::NoCoordinator);
                }

                let host = if response.host.is_empty() {
                    response
                        .coordinators
                        .first()
                        .map(|c| c.host.clone())
                        .unwrap_or_default()
                } else {
                    response.host
                };

                let port = if response.port == 0 {
                    response
                        .coordinators
                        .first()
                        .map(|c| c.port)
                        .unwrap_or(9092)
                } else {
                    response.port
                };

                use std::net::ToSocketAddrs;
                let addr: std::net::SocketAddr = format!("{}:{}", host, port)
                    .to_socket_addrs()
                    .map_err(|_| KafkaError::NoCoordinator)?
                    .next()
                    .ok_or(KafkaError::NoCoordinator)?;
                *coord = Some(addr);
                addr
            }
        };

        let topics: Vec<OffsetCommitRequestTopic> = topic_partitions
            .into_iter()
            .map(|(topic, partitions)| OffsetCommitRequestTopic {
                name: Some(topic),
                topic_id: None,
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
            generation_id_or_member_epoch: -1,
            member_id: None,
            group_instance_id: None,
            retention_time_ms: -1,
            topics,
        };

        let mut client_guard = client.lock().await;
        let response: Result<crate::protocol::OffsetCommitResponse> = client_guard
            .send_request(coordinator_addr, 8, &request)
            .await;

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
                // 连接已关闭，清除协调者缓存并报告错误，下次自动重试
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

    /// 离开消费者组
    pub async fn leave_group(&self) -> Result<()> {
        let group_state = self.group_state.read().await;
        if group_state.member_id.is_empty() {
            return Ok(());
        }

        // 提交偏移量
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

        let mut client = self.client.lock().await;
        let response: crate::protocol::LeaveGroupResponse =
            client.send_request(coordinator, 13, &request).await?;

        if response.error_code != 0 {
            warn!("LeaveGroup failed: error {}", response.error_code);
        }

        Ok(())
    }

    /// 获取当前偏移量
    pub fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let offsets = self.offsets.blocking_read();
        offsets.get(topic)?.get(&partition).copied()
    }

    /// 设置偏移量
    pub async fn set_offset(&self, topic: &str, partition: i32, offset: i64) {
        let mut offsets = self.offsets.write().await;
        offsets
            .entry(topic.to_string())
            .or_insert_with(HashMap::new)
            .insert(partition, offset);
    }

    /// 获取当前分配的分区
    pub async fn assignment(&self) -> HashMap<String, Vec<i32>> {
        self.group_state.read().await.assigned_partitions.clone()
    }

    /// 关闭消费者
    pub async fn close(&self) -> Result<()> {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // 离开消费者组
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
