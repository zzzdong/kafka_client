use bytes::Bytes;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::client::core::KafkaClient;
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

/// 发送给 Consumer 后台事件循环的命令
enum ConsumerCommand {
    /// 订阅/变更主题列表
    Subscribe { topics: Vec<String> },
}

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
///
/// 内部使用单一后台事件循环处理加入组、心跳、自动提交等任务，
/// 避免在构造函数中分散 spawn 多个后台任务。
pub struct Consumer {
    client: Arc<KafkaClient>,
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
    /// 发送命令到后台事件循环
    command_tx: mpsc::UnboundedSender<ConsumerCommand>,
}

impl Consumer {
    /// 创建 Consumer 实例（不启动后台循环）
    pub fn new(client: Arc<KafkaClient>, config: ConsumerConfig) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let consumer = Self {
            client: client.clone(),
            subscribed_topics: Vec::new(),
            offsets: Arc::new(RwLock::new(HashMap::new())),
            config: config.clone(),
            group_state: Arc::new(RwLock::new(GroupState::default())),
            coordinator: Arc::new(RwLock::new(None)),
            running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            command_tx,
        };

        consumer.start(client, config, command_rx);
        consumer
    }

    /// 启动后台事件循环
    fn start(
        &self,
        client: Arc<KafkaClient>,
        config: ConsumerConfig,
        mut command_rx: mpsc::UnboundedReceiver<ConsumerCommand>,
    ) {
        let offsets = self.offsets.clone();
        let group_state = self.group_state.clone();
        let coordinator = self.coordinator.clone();
        let running = self.running.clone();

        // 自动提交与心跳定时器
        let mut commit_interval = interval(Duration::from_millis(config.auto_commit_interval_ms));
        let mut heartbeat_interval = interval(Duration::from_millis(config.heartbeat_interval_ms));

        // 当前订阅主题与是否需要加入组的标记
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
                                &client,
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
                                &client,
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
                                // 清空组状态，触发重新加入
                                let mut gs = group_state.write().await;
                                gs.assigned_partitions.clear();
                                gs.member_id.clear();
                                *coordinator.write().await = None;
                            }
                            None => break,
                        }
                    }
                }

                // 如果需要加入组且主题非空，尝试加入
                if needs_rejoin && !current_topics.is_empty() && !config.group_id.is_empty() {
                    needs_rejoin = false;
                    match Self::background_join_group(
                        &client,
                        &config.group_id,
                        &group_state,
                        &coordinator,
                        &offsets,
                        &config,
                        &current_topics,
                    )
                    .await
                    {
                        Ok(()) => {
                            info!("background_join_group: group joined");
                        }
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

    /// 订阅主题列表
    pub async fn subscribe(&mut self, topics: Vec<String>) -> Result<()> {
        self.subscribed_topics = topics.clone();

        if self.config.group_id.is_empty() {
            // 没有消费者组，直接根据 metadata 获取偏移量
            let mut all_offsets = HashMap::new();
            for topic in &topics {
                let partitions = self
                    .client
                    .metadata()
                    .get_partitions(topic)
                    .await
                    .ok_or_else(|| KafkaError::TopicNotFound(topic.clone()))?;
                all_offsets.insert(topic.clone(), partitions.clone());
            }
            self.init_offsets_simple(topics).await?;
        } else {
            // 有消费者组：通知后台事件循环重新加入组
            debug!("subscribe: group_id set, notifying event loop to rejoin group");
            let _ = self.command_tx.send(ConsumerCommand::Subscribe {
                topics: topics.clone(),
            });
        }

        Ok(())
    }

    /// 后台心跳发送
    async fn background_heartbeat(
        client: &Arc<KafkaClient>,
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
            client.send_request(coord_addr, 12, &request).await?;

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

    /// 计算分区分配
    async fn compute_assignment(
        topics: &[String],
        join_response: &JoinGroupResponse,
        client: &Arc<KafkaClient>,
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

        let client_guard = client;

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
            let client = &self.client;
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
        let client = &self.client;
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
            let client = &self.client;
            let broker_addr = match client.any_broker_address().await {
                Some(addr) => addr,
                None => {
                    last_error = KafkaError::NoBrokerAvailable;
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
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                };
            // Release the lock before potentially sleeping
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
        client: &Arc<KafkaClient>,
        group_id: &str,
        group_state: &Arc<RwLock<GroupState>>,
        coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
        offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
        config: &ConsumerConfig,
        topics: &[String],
    ) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }

        // 1. 查找协调者（重试）
        let coord_addr = {
            let mut last_error = KafkaError::NoCoordinator;
            let mut found = None;
            for attempt in 0..10 {
                let c = client;
                let broker_addr = match c.any_broker_address().await {
                    Some(addr) => addr,
                    None => {
                        last_error = KafkaError::NoBrokerAvailable;
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
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                    };

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

            let c = client;
            let response: JoinGroupResponse = match c.send_request(coord_addr, 11, &request).await {
                Ok(resp) => resp,
                Err(e) => {
                    return Err(e);
                }
            };

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

                let c = client;
                let sync_response: SyncGroupResponse =
                    match c.send_request(coord_addr, 14, &sync_request).await {
                        Ok(resp) => resp,
                        Err(e) => {
                            return Err(e);
                        }
                    };

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
                    coordinator,
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

    /// 查询协调者已提交的偏移量
    async fn fetch_committed_offsets(
        client: &Arc<KafkaClient>,
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

        let topics: Vec<OffsetFetchRequestTopics> = assignment
            .iter()
            .map(|(topic, partitions)| OffsetFetchRequestTopics {
                name: Some(topic.clone()),
                topic_id: None,
                partition_indexes: partitions.clone(),
            })
            .collect();

        let request = OffsetFetchRequest {
            group_id: String::new(), // version 0-7 字段，新版本忽略
            topics: None,
            groups: vec![OffsetFetchRequestGroup {
                group_id: group_id.to_string(),
                member_id: None,
                member_epoch: -1,
                topics: Some(topics),
            }],
            require_stable: false,
        };

        let response: crate::protocol::OffsetFetchResponse =
            client.send_request(coord_addr, 9, &request).await?;

        let mut result: HashMap<String, HashMap<i32, i64>> = HashMap::new();
        for group in response.groups {
            if group.group_id == group_id {
                for topic in group.topics {
                    let topic_name = match topic.name.as_deref() {
                        Some(name) => name.to_string(),
                        None => continue,
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

    /// 初始化消费者组的偏移量（供后台任务使用）
    async fn init_offsets_for_group(
        client: &Arc<KafkaClient>,
        group_id: &str,
        offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
        coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
        assignment: &HashMap<String, Vec<i32>>,
        auto_offset_reset: AutoOffsetReset,
    ) -> Result<()> {
        // 1. 先尝试从协调者获取已提交偏移量
        let committed = Self::fetch_committed_offsets(client, group_id, coordinator, assignment)
            .await
            .unwrap_or_default();

        let default_offset = match auto_offset_reset {
            AutoOffsetReset::Earliest => -2i64,
            AutoOffsetReset::Latest => -1i64,
            AutoOffsetReset::None => return Err(KafkaError::NoOffsetStored),
        };

        let mut offsets_writer = offsets.write().await;
        let mut initialized_from_commit = 0usize;
        let mut initialized_from_default = 0usize;
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
                    initialized_from_commit += 1;
                } else {
                    topic_offsets.insert(*partition, default_offset);
                    initialized_from_default += 1;
                }
            }
        }
        drop(offsets_writer);

        debug!(
            "init_offsets_for_group: {} from committed offsets, {} from auto_offset_reset",
            initialized_from_commit, initialized_from_default
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

        let client = &self.client;
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
                let client = &self.client;
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
    ///
    /// 任一分区失败都会直接返回错误，避免调用方在无感知的情况下丢失消息。
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

        // 处理结果并更新偏移量，任一失败即返回错误
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
        client: &Arc<KafkaClient>,
        topic: &str,
        partition: i32,
        offset: i64,
        timeout_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        partition_max_bytes: i32,
    ) -> Result<Vec<ConsumerRecord>> {
        let client_guard = client;

        // 刷新元数据（如果过期），刷新失败向上传播
        if client_guard.metadata().is_expired().await {
            client_guard.refresh_metadata().await?;
        }

        let leader_addr = client_guard
            .metadata()
            .get_partition_leader(topic, partition)
            .await
            .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;

        // 获取连接 Arc 后释放 KafkaClient 锁，避免长时间持有影响心跳
        let conn = client_guard.get_broker_connection(leader_addr).await?;
        let conn_arc = conn.clone();

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
        // 从 group_state 获取 generation_id 和 member_id，以便协商的 OffsetCommit 版本能正确处理
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
            &self.client,
            &self.config.group_id,
            &self.offsets,
            &self.coordinator,
            generation_id,
            member_id,
        )
        .await
    }

    async fn do_commit(
        client: &Arc<KafkaClient>,
        group_id: &str,
        offsets: &Arc<RwLock<HashMap<String, HashMap<i32, i64>>>>,
        coordinator: &Arc<RwLock<Option<std::net::SocketAddr>>>,
        generation_id: i32,
        member_id: Option<String>,
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
                let client_guard = client;
                let broker_addr = client_guard
                    .any_broker_address()
                    .await
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
            generation_id_or_member_epoch: generation_id,
            member_id,
            group_instance_id: None,
            retention_time_ms: -1,
            topics,
        };

        let client_guard = client;
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

        let client = &self.client;
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
