//! Kafka Rust Client
//!
//! A pure Rust Kafka client library based on Tokio async runtime.
//! Supports SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
//! and uses a layered architecture with both low-level protocol API
//! and high-level producer/consumer API.
//!
//! # Example
//!
//! ```ignore
//! use kafka_client::KafkaClient;
//!
//! let client = KafkaClient::builder(vec![("localhost:9092".parse()?])
//!     .with_sasl_scram_sha256("user", "pass")
//!     .build()
//!     .await?;
//!
//! let producer = client.producer(ProducerConfig::new()).await?;
//! let consumer = client.consumer(ConsumerConfig::default()).await?;
//! ```

// Internal modules (layered architecture)
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

// Metadata types (for advanced users)
pub use cluster::{ClusterClient, ClusterConfig, MetadataCache};

/// Library name
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Kafka client - unified entry point
///
/// Provides factory methods for creating Producer and Consumer.
pub struct KafkaClient {
    cluster: Arc<ClusterClient>,
}

impl KafkaClient {
    /// Create builder
    pub fn builder(bootstrap_servers: Vec<SocketAddr>) -> KafkaClientBuilder {
        KafkaClientBuilder::new(bootstrap_servers)
    }

    /// Create Producer
    pub async fn producer(&self, config: ProducerConfig) -> Result<Producer> {
        Ok(Producer::new(self.cluster.clone(), config).await)
    }

    /// Create Consumer
    pub fn consumer(&self, config: ConsumerConfig) -> Consumer {
        Consumer::new(self.cluster.clone(), config)
    }

    /// Get metadata cache (read-only queries)
    pub fn metadata(&self) -> &MetadataCache {
        self.cluster.metadata()
    }

    /// Get cluster client (for advanced low-level operations)
    pub fn cluster(&self) -> &Arc<ClusterClient> {
        &self.cluster
    }

    /// Close client, release all connections
    pub async fn close(&self) -> Result<()> {
        self.cluster.close().await
    }
}

/// Kafka client builder
pub struct KafkaClientBuilder {
    bootstrap_servers: Vec<SocketAddr>,
    security_protocol: crate::transport::SecurityProtocol,
    client_id: String,
    sasl_credentials: Option<crate::sasl::SaslCredentials>,
    metadata_ttl: Duration,
}

impl KafkaClientBuilder {
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
    pub fn with_plaintext(mut self) -> Self {
        self.security_protocol = crate::transport::SecurityProtocol::Plaintext;
        self
    }

    pub fn with_tls(mut self, domain: impl Into<String>) -> Self {
        self.security_protocol =
            crate::transport::SecurityProtocol::Ssl(crate::transport::TlsConfig {
                domain: domain.into(),
                ..Default::default()
            });
        self
    }

    pub fn with_tls_config(mut self, tls_config: crate::transport::TlsConfig) -> Self {
        self.security_protocol = crate::transport::SecurityProtocol::Ssl(tls_config);
        self
    }

    // --- SASL ---
    /// Configure SASL authentication with custom mechanism
    ///
    /// # Arguments
    /// * `mechanism` - SASL mechanism type (Plain, ScramSha256, ScramSha512)
    /// * `username` - SASL username
    /// * `password` - SASL password
    ///
    /// # Example
    /// ```ignore
    /// let client = KafkaClient::builder(vec![addr])
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
        self.sasl_credentials = Some(crate::sasl::SaslCredentials {
            mechanism,
            username: username.into(),
            password: password.into(),
            authzid: None,
        });
        self
    }

    /// Configure SASL + TLS with custom mechanism
    ///
    /// # Arguments
    /// * `tls_config` - TLS configuration
    /// * `mechanism` - SASL mechanism type (Plain, ScramSha256, ScramSha512)
    /// * `username` - SASL username
    /// * `password` - SASL password
    ///
    /// # Example
    /// ```ignore
    /// let tls = TlsConfig { domain: "kafka.example.com".into(), ..Default::default() };
    /// let client = KafkaClient::builder(vec![addr])
    ///     .with_sasl_tls(tls, SaslMechanismType::ScramSha256, "user", "pass")
    ///     .build()
    ///     .await?;
    /// ```
    pub fn with_sasl_tls(
        mut self,
        tls_config: crate::transport::TlsConfig,
        mechanism: crate::sasl::SaslMechanismType,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.security_protocol = crate::transport::SecurityProtocol::SaslSsl(tls_config);
        self.sasl_credentials = Some(crate::sasl::SaslCredentials {
            mechanism,
            username: username.into(),
            password: password.into(),
            authzid: None,
        });
        self
    }

    // Convenience methods for common configurations
    pub fn with_sasl_plaintext(
        self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.with_sasl(crate::sasl::SaslMechanismType::Plain, username, password)
    }

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

    // --- Other ---
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = client_id.into();
        self
    }

    pub fn with_metadata_ttl(mut self, ttl: Duration) -> Self {
        self.metadata_ttl = ttl;
        self
    }

    // --- Build ---
    pub async fn build(self) -> Result<KafkaClient> {
        let config = ClusterConfig {
            bootstrap_servers: self.bootstrap_servers,
            security_protocol: self.security_protocol,
            client_id: self.client_id,
            metadata_ttl: self.metadata_ttl,
            sasl: self.sasl_credentials,
        };

        let cluster = ClusterClient::connect(config).await?;
        Ok(KafkaClient {
            cluster: Arc::new(cluster),
        })
    }
}

/// Convenience builder function
pub fn builder(bootstrap_servers: Vec<SocketAddr>) -> KafkaClientBuilder {
    KafkaClientBuilder::new(bootstrap_servers)
}
