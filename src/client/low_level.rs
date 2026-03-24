use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::connection::BrokerConnection;
use crate::transport::SecurityProtocol;
use crate::client::metadata::MetadataCache;
use crate::protocol::*;
use crate::error::{Result, KafkaError};

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

/// 低级 Kafka 客户端
pub struct KafkaClient {
    connections: HashMap<SocketAddr, Arc<Mutex<BrokerConnection>>>,
    metadata: Arc<MetadataCache>,
    config: ClientConfig,
}

impl KafkaClient {
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        let mut client = Self {
            connections: HashMap::new(),
            metadata: Arc::new(MetadataCache::new(config.metadata_ttl)),
            config,
        };

        client.bootstrap().await?;
        Ok(client)
    }

    async fn bootstrap(&mut self) -> Result<()> {
        for addr in &self.config.bootstrap_servers {
            match self.connect_to_broker(*addr).await {
                Ok(conn) => {
                    self.connections.insert(*addr, Arc::new(Mutex::new(conn)));
                    self.refresh_metadata().await?;
                    return Ok(());
                }
                Err(e) => {
                    warn!("Failed to connect to {}: {}", addr, e);
                    continue;
                }
            }
        }
        Err(KafkaError::NoBootstrapBrokerAvailable)
    }

    async fn connect_to_broker(&self, addr: SocketAddr) -> Result<BrokerConnection> {
        BrokerConnection::connect(
            addr,
            self.config.security_protocol.clone(),
            self.config.client_id.clone(),
        ).await
    }

    async fn get_connection(&mut self, addr: SocketAddr) -> Result<Arc<Mutex<BrokerConnection>>> {
        if !self.connections.contains_key(&addr) {
            let conn = self.connect_to_broker(addr).await?;
            self.connections.insert(addr, Arc::new(Mutex::new(conn)));
        }
        Ok(self.connections.get(&addr).unwrap().clone())
    }

    pub async fn send_request<Req, Resp>(
        &mut self,
        broker_addr: SocketAddr,
        api_key: i16,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: VersionedKafkaEncode,
        Resp: VersionedKafkaDecode,
    {
        let conn = self.get_connection(broker_addr).await?;
        let mut conn_guard = conn.lock().await;
        conn_guard.send_request(api_key, request).await
    }

    pub async fn send_to_partition<Req, Resp>(
        &mut self,
        topic: &str,
        partition: i32,
        api_key: i16,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: VersionedKafkaEncode,
        Resp: VersionedKafkaDecode,
    {
        let leader_addr = self.metadata.get_partition_leader(topic, partition).await
            .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;
        self.send_request(leader_addr, api_key, request).await
    }

    pub async fn refresh_metadata(&mut self) -> Result<MetadataResponse> {
        // Get any available connection
        let (_addr, conn) = self.connections.iter().next()
            .ok_or(KafkaError::NoBrokerAvailable)?;

        let request = MetadataRequest::all_topics();
        let mut conn_guard = conn.lock().await;
        let response: MetadataResponse = conn_guard.send_request(3, &request).await?;
        drop(conn_guard);

        self.metadata.update(response.clone()).await;
        debug!("Metadata refreshed successfully");
        Ok(response)
    }

    pub async fn get_metadata_for_topics(&mut self, topics: Vec<String>) -> Result<MetadataResponse> {
        let (_addr, conn) = self.connections.iter().next()
            .ok_or(KafkaError::NoBrokerAvailable)?;

        let request = MetadataRequest::for_topics(topics);
        let mut conn_guard = conn.lock().await;
        let response: MetadataResponse = conn_guard.send_request(3, &request).await?;
        drop(conn_guard);

        self.metadata.update(response.clone()).await;
        Ok(response)
    }

    pub fn metadata(&self) -> &MetadataCache {
        &self.metadata
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// 关闭所有连接
    pub async fn close(mut self) -> Result<()> {
        for (addr, conn) in self.connections.drain() {
            if let Ok(conn) = Arc::try_unwrap(conn) {
                let conn = conn.into_inner();
                if let Err(e) = conn.close().await {
                    warn!("Error closing connection to {}: {}", addr, e);
                }
            }
        }
        Ok(())
    }
}
