//! Cluster layer - broker management and metadata caching

mod broker;
mod metadata;

pub(crate) use broker::BrokerManager;
pub use metadata::MetadataCache;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use crate::error::{KafkaError, Result};
use crate::sasl::SaslCredentials;
use crate::transport::SecurityProtocol;
use kafka_client_protocol::{self as protocol, Request, Response};

/// Cluster connection configuration (crate-internal)
#[derive(Debug, Clone)]
pub(crate) struct ClusterConfig {
    pub bootstrap_servers: Vec<SocketAddr>,
    pub security_protocol: SecurityProtocol,
    pub client_id: String,
    pub metadata_ttl: Duration,
    /// SASL authentication credentials (None = no authentication)
    pub sasl: Option<SaslCredentials>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec![],
            security_protocol: SecurityProtocol::Plaintext,
            client_id: "rust-kafka-client".to_string(),
            metadata_ttl: Duration::from_secs(300),
            sasl: None,
        }
    }
}

/// Cluster client — shared core for Producer and Consumer.
///
/// **Crate-internal only.** External users interact via [`Client`](crate::Client).
///
/// Responsibilities:
/// - Manage connection pool to all brokers
/// - Cache and refresh cluster metadata
/// - Provide request routing to partition leaders (with retry + metadata refresh)
pub(crate) struct ClusterClient {
    broker_manager: Arc<BrokerManager>,
    metadata: Arc<MetadataCache>,
}

impl ClusterClient {
    /// Connect to cluster: bootstrap → ApiVersions negotiation → refresh metadata
    pub(crate) async fn connect(config: ClusterConfig) -> Result<Self> {
        let broker_manager = Arc::new(BrokerManager::new(
            config.bootstrap_servers.clone(),
            config.security_protocol.clone(),
            config.client_id.clone(),
            crate::NAME.to_string(),
            crate::VERSION.to_string(),
            config.sasl.clone(),
        ));

        broker_manager.bootstrap().await.map_err(|e| {
            debug!(error = ?e, "bootstrap failed");
            e
        })?;

        let client = Self {
            broker_manager,
            metadata: Arc::new(MetadataCache::new(config.metadata_ttl)),
        };

        client.refresh_metadata().await.map_err(|e| {
            debug!(error = ?e, "refresh metadata failed");
            e
        })?;

        Ok(client)
    }

    /// Close all broker connections
    pub(crate) async fn close(&self) -> Result<()> {
        self.broker_manager.close().await
    }

    // ================================================================
    // Request routing
    // ================================================================

    /// Send request to partition leader — pure routing, no retry.
    ///
    /// Refreshes expired metadata, then sends the request to the current
    /// partition leader. Does **not** retry on failure — retry logic
    /// belongs to the caller (Producer/Consumer), which can classify
    /// errors and apply appropriate backoff.
    pub(crate) async fn send_to_partition<Req, Resp>(
        &self,
        topic: &str,
        partition: i32,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        // Opportunistic cache maintenance — refresh stale metadata,
        // but don't fail the request if refresh itself fails.
        if self.metadata.is_expired().await {
            debug!("Metadata expired, refreshing before routing");
            if let Err(e) = self.refresh_metadata().await {
                warn!("Failed to refresh expired metadata: {}", e);
            }
        }

        let leader_addr = self
            .metadata
            .get_partition_leader(topic, partition)
            .await
            .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;

        self.send_to_broker(leader_addr, request).await
    }

    /// Send request to specific broker
    ///
    /// On `CorrelationIdMismatch` or `Protocol` decode errors, force-closes
    /// the broken connection and retries once. Marks broker as unhealthy on
    /// other failures.
    pub(crate) async fn send_to_broker<Req, Resp>(
        &self,
        broker_addr: SocketAddr,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        let handle = self.broker_manager.get_connection(broker_addr).await?;
        let result = handle.send_request(request).await;

        match result {
            Ok(resp) => Ok(resp),
            Err(e @ KafkaError::CorrelationIdMismatch { .. })
            | Err(e @ KafkaError::Protocol(_)) => {
                warn!(
                    "Broker {} returned protocol error ({}), force-closing connection",
                    broker_addr, e
                );
                self.broker_manager.mark_unhealthy(broker_addr);
                self.broker_manager
                    .force_close_connection(broker_addr)
                    .await;

                let _ = self.refresh_metadata().await;

                let fresh_handle = self.broker_manager.get_connection(broker_addr).await?;
                let retry_result = fresh_handle.send_request(request).await;
                if let Err(ref e) = retry_result {
                    warn!(
                        "Retry to broker {} also failed: {}, api_key={}",
                        broker_addr,
                        e,
                        request.api_key(),
                    );
                }
                retry_result
            }
            Err(e) => {
                self.broker_manager.mark_unhealthy(broker_addr);
                Err(e)
            }
        }
    }

    /// Send request to any available broker
    ///
    /// Used for Metadata, FindCoordinator, CreateTopics, etc.
    /// Prefers healthy connected brokers, falls back to all known addresses.
    pub(crate) async fn send_to_any_broker<Req, Resp>(&self, request: &Req) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        let mut errors: Vec<String> = Vec::new();

        // Try healthy broker first
        if let Some((addr, handle)) = self.broker_manager.get_any_healthy_broker() {
            match handle.send_request(request).await {
                Ok(resp) => return Ok(resp),
                Err(e @ KafkaError::CorrelationIdMismatch { .. })
                | Err(e @ KafkaError::Protocol(_)) => {
                    warn!("Broker {} protocol error ({}), force-closing", addr, e);
                    self.broker_manager.mark_unhealthy(addr);
                    self.broker_manager.force_close_connection(addr).await;
                    errors.push(format!("{}: {}", addr, e));
                }
                Err(e) => {
                    warn!("Request to healthy broker {} failed: {}", addr, e);
                    self.broker_manager.mark_unhealthy(addr);
                    errors.push(format!("{}: {}", addr, e));
                }
            }
        }

        // Try all known broker addresses
        let addresses: Vec<SocketAddr> = self.broker_manager.all_broker_addresses();

        for addr in addresses {
            let handle = self.broker_manager.get_connection(addr).await?;
            match handle.send_request(request).await {
                Ok(resp) => return Ok(resp),
                Err(e @ KafkaError::CorrelationIdMismatch { .. })
                | Err(e @ KafkaError::Protocol(_)) => {
                    warn!(
                        "Broker {} api_key {} protocol error ({}), force-closing",
                        addr,
                        request.api_key(),
                        e
                    );
                    self.broker_manager.mark_unhealthy(addr);
                    self.broker_manager.force_close_connection(addr).await;
                    errors.push(format!("{}: {}", addr, e));
                }
                Err(e) => {
                    warn!(
                        "Request api_key: {} to broker {} failed: {}",
                        request.api_key(),
                        addr,
                        e
                    );
                    self.broker_manager.mark_unhealthy(addr);
                    errors.push(format!("{}: {}", addr, e));
                }
            }
        }

        Err(KafkaError::NoBrokerAvailable(errors.join("; ")))
    }

    // ================================================================
    // Metadata
    // ================================================================

    /// Get metadata cache reference
    pub(crate) fn metadata(&self) -> &Arc<MetadataCache> {
        &self.metadata
    }

    /// Get any broker address (for admin operations)
    #[allow(dead_code)]
    pub(crate) fn any_broker_address(&self) -> Option<SocketAddr> {
        self.broker_manager.all_broker_addresses().first().copied()
    }

    /// Force refresh cluster metadata
    pub(crate) async fn refresh_metadata(&self) -> Result<()> {
        let _guard = self.metadata.acquire_refresh_lock().await;

        let request = protocol::MetadataRequest {
            topics: None,
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };

        let response: protocol::MetadataResponse = self.send_to_any_broker(&request).await?;
        self.broker_manager
            .refresh_from_metadata(response.brokers.clone())
            .await?;

        self.metadata.update(response).await;
        debug!("Metadata refreshed successfully");
        Ok(())
    }

    /// Fetch metadata for specific topics
    /// 预留功能，用于特定主题的元数据刷新
    #[allow(dead_code)]
    pub(crate) async fn fetch_metadata_for_topics(&self, topics: &[String]) -> Result<()> {
        let request_topics: Vec<protocol::MetadataRequestTopic> = topics
            .iter()
            .map(|name| protocol::MetadataRequestTopic {
                topic_id: uuid::Uuid::nil(),
                name: Some(name.clone()),
            })
            .collect();

        let request = protocol::MetadataRequest {
            topics: Some(request_topics),
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };

        let response: protocol::MetadataResponse = self.send_to_any_broker(&request).await?;
        self.metadata.update(response).await;
        Ok(())
    }
}
