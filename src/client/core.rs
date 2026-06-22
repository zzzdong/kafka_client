use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::client::broker_manager::BrokerManager;
use crate::client::metadata::MetadataCache;
use crate::connection::Connection;
use crate::error::{KafkaError, Result};
use crate::protocol::{MetadataRequest, MetadataRequestTopic, MetadataResponse};
use crate::transport::SecurityProtocol;

/// 客户端配置
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub bootstrap_servers: Vec<SocketAddr>,
    pub security_protocol: SecurityProtocol,
    pub client_id: String,
    pub metadata_ttl: std::time::Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec![],
            security_protocol: SecurityProtocol::Plaintext,
            client_id: "rust-kafka-client".to_string(),
            metadata_ttl: std::time::Duration::from_secs(300),
        }
    }
}

/// 核心 Kafka 客户端
///
/// 内部通过 `Arc<BrokerManager>` 与 `Arc<MetadataCache>` 实现共享与并发，
/// `BrokerManager` 自身已按 broker 拆分锁，方法签名以 `&self` 为主，
/// 调用方可以安全地用 `Arc<KafkaClient>` 共享而无需额外加锁。
pub struct KafkaClient {
    broker_manager: Arc<BrokerManager>,
    metadata: Arc<MetadataCache>,
    config: ClientConfig,
    /// 串行化 metadata 刷新，避免并发发送重复请求
    metadata_refresh_lock: Mutex<()>,
}

impl KafkaClient {
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        debug!("KafkaClient::connect starting");
        let broker_manager = Arc::new(BrokerManager::new(
            config.bootstrap_servers.clone(),
            config.security_protocol.clone(),
            config.client_id.clone(),
            crate::NAME.to_string(),
            crate::VERSION.to_string(),
        ));

        debug!("bootstrapping broker manager");
        broker_manager.bootstrap().await.map_err(|e| {
            debug!(error = ?e, "bootstrap failed");
            e
        })?;
        debug!("bootstrap succeeded");

        let client = Self {
            broker_manager,
            metadata: Arc::new(MetadataCache::new(config.metadata_ttl)),
            config,
            metadata_refresh_lock: Mutex::new(()),
        };

        debug!("refreshing metadata");
        client.refresh_metadata().await.map_err(|e| {
            debug!(error = ?e, "refresh metadata failed");
            e
        })?;
        debug!("metadata refreshed successfully");
        Ok(client)
    }

    pub async fn send_request<Req, Resp>(
        &self,
        broker_addr: SocketAddr,
        _api_key: i16,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: kafka_client_protocol::Request,
        Resp: kafka_client_protocol::Response,
    {
        let conn = self.broker_manager.get_connection(broker_addr).await?;
        let mut conn_guard = conn.lock().await;
        let result = conn_guard.send_request(request).await;
        if result.is_err() {
            self.broker_manager.mark_unhealthy(broker_addr);
        }
        result
    }

    /// 获取到指定 broker 的连接
    pub async fn get_broker_connection(
        &self,
        broker_addr: SocketAddr,
    ) -> Result<Arc<Mutex<Connection>>> {
        self.broker_manager.get_connection(broker_addr).await
    }

    /// 获取任意一个健康 broker 的地址
    pub async fn any_broker_address(&self) -> Option<SocketAddr> {
        self.broker_manager
            .get_any_healthy_broker()
            .map(|(addr, _)| addr)
    }

    pub async fn send_to_partition<Req, Resp>(
        &self,
        topic: &str,
        partition: i32,
        api_key: i16,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: kafka_client_protocol::Request,
        Resp: kafka_client_protocol::Response,
    {
        // 如果 metadata 过期，先刷新
        if self.metadata.is_expired().await {
            debug!("Metadata expired, refreshing before sending to partition");
            if let Err(e) = self.refresh_metadata().await {
                warn!("Failed to refresh expired metadata: {}", e);
            }
        }

        let leader_addr = self
            .metadata
            .get_partition_leader(topic, partition)
            .await
            .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;

        match self.send_request(leader_addr, api_key, request).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                // 发送失败时尝试刷新 metadata（leader 可能已变更）
                warn!("Send to partition failed, refreshing metadata: {}", e);
                self.refresh_metadata().await?;
                let leader_addr = self
                    .metadata
                    .get_partition_leader(topic, partition)
                    .await
                    .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;
                self.send_request(leader_addr, api_key, request).await
            }
        }
    }

    pub async fn refresh_metadata(&self) -> Result<MetadataResponse> {
        let _guard = self.metadata_refresh_lock.lock().await;

        let request = MetadataRequest {
            topics: None,
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };

        let response: MetadataResponse = self.send_to_any_broker(&request).await?;
        self.broker_manager
            .refresh_from_metadata(response.brokers.clone())
            .await?;

        self.metadata.update(response.clone()).await;
        debug!("Metadata refreshed successfully");
        Ok(response)
    }

    pub async fn get_metadata_for_topics(&self, topics: Vec<String>) -> Result<MetadataResponse> {
        let request_topics: Vec<MetadataRequestTopic> = topics
            .into_iter()
            .map(|name| MetadataRequestTopic {
                topic_id: None,
                name: Some(name),
            })
            .collect();

        let request = MetadataRequest {
            topics: Some(request_topics),
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };

        let response: MetadataResponse = self.send_to_any_broker(&request).await?;
        self.metadata.update(response.clone()).await;
        Ok(response)
    }

    /// 将请求发送到任意一个健康的 broker
    async fn send_to_any_broker<Req, Resp>(&self, request: &Req) -> Result<Resp>
    where
        Req: kafka_client_protocol::Request,
        Resp: kafka_client_protocol::Response,
    {
        // 优先使用已连接的健康 broker
        if let Some((addr, conn)) = self.broker_manager.get_any_healthy_broker() {
            debug!(broker = %addr, "trying healthy broker");
            let mut conn_guard = conn.lock().await;
            match conn_guard.send_request(request).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    debug!(broker = %addr, error = ?e, "healthy broker failed");
                    warn!("Request to healthy broker {} failed: {}", addr, e);
                    self.broker_manager.mark_unhealthy(addr);
                }
            }
        } else {
            debug!("no healthy broker available");
        }

        // 尝试所有已知 broker 地址
        let addresses: Vec<SocketAddr> = self.broker_manager.all_broker_addresses();
        debug!(count = addresses.len(), "trying all broker addresses");
        for addr in addresses {
            debug!(broker = %addr, "trying broker");
            let conn = self.broker_manager.get_connection(addr).await?;
            let mut conn_guard = conn.lock().await;
            match conn_guard.send_request(request).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    debug!(broker = %addr, error = ?e, "broker failed");
                    warn!("Request to broker {} failed: {}", addr, e);
                    self.broker_manager.mark_unhealthy(addr);
                }
            }
        }

        Err(KafkaError::NoBrokerAvailable)
    }

    pub fn metadata(&self) -> &MetadataCache {
        &self.metadata
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// 关闭所有连接
    pub async fn close(&self) -> Result<()> {
        self.broker_manager.close().await
    }
}
