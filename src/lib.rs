//! Kafka Rust Client
//!
//! A pure Rust Kafka client library based on Tokio async runtime.
//! Supports SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
//! and uses a layered architecture with low-level protocol API
//! and high-level producer/consumer API.
//!
//! # Quick Start
//!
//! ```ignore
//! use kafka_client::Client;
//!
//! // Create client — connects to cluster, discovers all brokers
//! let client = Client::builder(vec!["localhost:9092".parse()?])
//!     .with_plaintext()
//!     .build()
//!     .await?;
//!
//! // Producer — send messages
//! let producer = client.producer_default().await;
//! producer.send(ProducerRecord::new("my-topic", b"hello".into())).await?;
//!
//! // Consumer — read messages
//! let mut consumer = client.consumer_default();
//! consumer.subscribe(vec!["my-topic".into()]).await?;
//! let records = consumer.poll().await?;
//!
//! client.close().await?;
//! ```
//!
//! # Advanced Configuration
//!
//! ```ignore
//! // Custom producer config
//! let producer = client.producer(
//!     ProducerConfig::new().with_acks(-1).with_retries(3)
//! ).await;
//!
//! // Consumer with group coordination
//! let mut consumer = client.consumer(
//!     ConsumerConfig::new("my-group").with_earliest()
//! );
//! ```

// Internal modules (layered architecture)
pub mod admin;
mod cluster;
pub mod connection; // Public for advanced users who need low-level access
mod consumer;
mod error;
mod producer;
mod sasl;
pub mod transport; // Public for advanced users who need low-level access
mod wire;

// Public re-exports
pub use error::{KafkaError, Result};
pub use kafka_client_protocol as protocol;
pub use sasl::{SaslCredentials, SaslMechanismType};
pub use transport::{SecurityProtocol, TlsConfig};

// Producer types
pub use producer::{
    Header, PartitionRouter, PartitionRouting, Producer, ProducerConfig, ProducerRecord,
    RecordMetadata,
};

// Consumer types
pub use consumer::{
    AutoOffsetReset, Consumer, ConsumerConfig, ConsumerRecord, GroupConsumer, GroupHandle,
    OffsetHandle, PartitionAssignmentStrategy, SimpleConsumer,
};

// Metadata types (read-only queries)
pub use cluster::MetadataCache;

/// Library name
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::cluster::ClusterClient;

// ===========================================================================
// Client — unified entry point
// ===========================================================================

/// Unified Kafka client.
///
/// Manages the lifecycle of the connection to a Kafka cluster internally.
/// Provides factory methods for creating [`Producer`] and [`Consumer`] instances.
///
/// # Examples
///
/// ```ignore
/// use kafka_client::Client;
///
/// let client = Client::builder(vec!["localhost:9092".parse().unwrap()])
///     .with_plaintext()
///     .build()
///     .await?;
///
/// let producer = client.producer_default().await?;
/// let consumer = client.consumer_default().await?;
/// ```
pub struct Client {
    cluster: Arc<ClusterClient>,
}

impl Client {
    /// Create a builder for constructing the client.
    pub fn builder(bootstrap_servers: Vec<SocketAddr>) -> ClientBuilder {
        ClientBuilder::new(bootstrap_servers)
    }

    // ------------------------------------------------------------------
    // Producer factories
    // ------------------------------------------------------------------

    /// Create a [`Producer`] with default configuration.
    ///
    /// Equivalent to `client.producer(ProducerConfig::default()).await`.
    pub async fn producer_default(&self) -> Producer {
        Producer::new(self.cluster.clone(), ProducerConfig::default()).await
    }

    /// Create a [`Producer`] with custom configuration.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let producer = client.producer(
    ///     ProducerConfig::new().with_acks(-1).with_retries(5)
    /// ).await?;
    /// ```
    pub async fn producer(&self, config: ProducerConfig) -> Producer {
        Producer::new(self.cluster.clone(), config).await
    }

    // ------------------------------------------------------------------
    // Consumer factories
    // ------------------------------------------------------------------

    /// Create a [`Consumer`] with default configuration.
    ///
    /// Uses a default group id, enabling group-coordinated consumption.
    pub fn consumer_default(&self) -> Consumer {
        Consumer::new(self.cluster.clone(), ConsumerConfig::default())
    }

    /// Create a [`Consumer`] with custom configuration.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Simple consumer (no consumer group)
    /// let consumer = client.consumer(ConsumerConfig::default()).await?;
    ///
    /// // Group consumer
    /// let consumer = client.consumer(
    ///     ConsumerConfig::new("my-group").with_earliest()
    /// ).await?;
    /// ```
    pub fn consumer(&self, config: ConsumerConfig) -> Consumer {
        Consumer::new(self.cluster.clone(), config)
    }

    // ------------------------------------------------------------------
    // Admin client
    // ------------------------------------------------------------------

    /// Create an [`AdminClient`](admin::AdminClient) for cluster management.
    ///
    /// Used for creating/deleting topics, listing groups, describing
    /// the cluster, and other administrative operations.
    pub fn admin(&self) -> admin::AdminClient {
        admin::AdminClient::new(self.cluster.clone())
    }

    // ------------------------------------------------------------------
    // Metadata (read-only)
    // ------------------------------------------------------------------

    /// Get a reference to the metadata cache.
    ///
    /// Useful for discovering topics, partitions, and broker addresses
    /// without sending RPC requests.
    pub fn metadata(&self) -> &MetadataCache {
        self.cluster.metadata()
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Force a metadata refresh from the cluster.
    ///
    /// Useful when you need up-to-date partition leadership information
    /// before admin operations.
    pub async fn refresh_metadata(&self) -> Result<()> {
        self.cluster.refresh_metadata().await
    }

    /// Send a request to any available broker (advanced usage).
    ///
    /// Useful for admin operations like creating/deleting topics,
    /// or custom protocol requests.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use kafka_client::protocol::{CreateTopicsRequest, CreateTopicsResponse};
    ///
    /// let response: CreateTopicsResponse = client.send_to_any_broker(&request).await?;
    /// ```
    pub async fn send_to_any_broker<Req, Resp>(&self, request: &Req) -> Result<Resp>
    where
        Req: kafka_client_protocol::Request,
        Resp: kafka_client_protocol::Response,
    {
        self.cluster.send_to_any_broker(request).await
    }

    /// Close the client, releasing all broker connections.
    pub async fn close(&self) -> Result<()> {
        self.cluster.close().await
    }
}

// ===========================================================================
// ClientBuilder
// ===========================================================================

/// Builder for constructing a [`Client`].
///
/// Supports plaintext, TLS, SASL, and SASL+TLS configurations.
pub struct ClientBuilder {
    bootstrap_servers: Vec<SocketAddr>,
    security_protocol: crate::transport::SecurityProtocol,
    client_id: String,
    sasl_credentials: Option<crate::sasl::SaslCredentials>,
    metadata_ttl: Duration,
}

impl ClientBuilder {
    /// Create a new builder with the given bootstrap servers.
    pub fn new(bootstrap_servers: Vec<SocketAddr>) -> Self {
        Self {
            bootstrap_servers,
            security_protocol: crate::transport::SecurityProtocol::Plaintext,
            client_id: NAME.to_string(),
            sasl_credentials: None,
            metadata_ttl: Duration::from_secs(300),
        }
    }

    // --- Security protocol ---

    /// Use plaintext (no encryption, no authentication).
    pub fn with_plaintext(mut self) -> Self {
        self.security_protocol = crate::transport::SecurityProtocol::Plaintext;
        self
    }

    /// Use TLS encryption with the given domain.
    pub fn with_tls(mut self, domain: impl Into<String>) -> Self {
        self.security_protocol =
            crate::transport::SecurityProtocol::Ssl(crate::transport::TlsConfig {
                domain: domain.into(),
                ..Default::default()
            });
        self
    }

    /// Use TLS with full custom configuration.
    pub fn with_tls_config(mut self, tls_config: crate::transport::TlsConfig) -> Self {
        self.security_protocol = crate::transport::SecurityProtocol::Ssl(tls_config);
        self
    }

    // --- SASL ---

    /// Configure SASL authentication with a custom mechanism.
    ///
    /// # Example
    /// ```ignore
    /// let client = Client::builder(vec![addr])
    ///     .with_sasl(SaslMechanismType::ScramSha256, "user", "pass")
    ///     .build()
    ///     .await?;
    /// ```
    pub fn with_sasl(
        mut self,
        mechanism: crate::sasl::SaslMechanismType,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.security_protocol = crate::transport::SecurityProtocol::SaslPlaintext;
        self.sasl_credentials = Some(crate::sasl::SaslCredentials::new(
            mechanism, username, password,
        ));
        self
    }

    /// Configure SASL + TLS authentication.
    pub fn with_sasl_tls(
        mut self,
        tls_config: crate::transport::TlsConfig,
        mechanism: crate::sasl::SaslMechanismType,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.security_protocol = crate::transport::SecurityProtocol::SaslSsl(tls_config);
        self.sasl_credentials = Some(crate::sasl::SaslCredentials::new(
            mechanism, username, password,
        ));
        self
    }

    // Convenience SASL shortcuts

    /// SASL PLAIN without TLS.
    pub fn with_sasl_plaintext(
        self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.with_sasl(crate::sasl::SaslMechanismType::Plain, username, password)
    }

    /// SASL PLAIN with TLS (domain-based config).
    pub fn with_sasl_ssl(
        self,
        domain: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        let tls_config = crate::transport::TlsConfig {
            domain: domain.into(),
            ..Default::default()
        };
        self.with_sasl_tls(
            tls_config,
            crate::sasl::SaslMechanismType::Plain,
            username,
            password,
        )
    }

    // --- Other settings ---

    /// Set a custom client ID (sent to Kafka brokers).
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = client_id.into();
        self
    }

    /// Override the metadata cache TTL. Default is 5 minutes.
    pub fn with_metadata_ttl(mut self, ttl: Duration) -> Self {
        self.metadata_ttl = ttl;
        self
    }

    // --- Build ---

    /// Connect to the cluster and build the [`Client`].
    pub async fn build(self) -> Result<Client> {
        let config = crate::cluster::ClusterConfig {
            bootstrap_servers: self.bootstrap_servers,
            security_protocol: self.security_protocol,
            client_id: self.client_id,
            metadata_ttl: self.metadata_ttl,
            sasl: self.sasl_credentials,
        };

        let cluster = ClusterClient::connect(config).await?;
        Ok(Client {
            cluster: Arc::new(cluster),
        })
    }
}

/// Convenience builder function — equivalent to `Client::builder(...)`.
pub fn builder(bootstrap_servers: Vec<SocketAddr>) -> ClientBuilder {
    ClientBuilder::new(bootstrap_servers)
}

// ===========================================================================
// Backward-compatibility aliases (deprecated)
// ===========================================================================

#[deprecated(since = "0.2.0", note = "Use `Client` instead")]
pub type KafkaClient = Client;

#[deprecated(since = "0.2.0", note = "Use `ClientBuilder` instead")]
pub type KafkaClientBuilder = ClientBuilder;
