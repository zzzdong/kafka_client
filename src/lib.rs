//! Kafka Rust Client
//!
//! A pure Rust Kafka client library based on Tokio async runtime.
//! Supports SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
//! and uses a layered architecture with both low-level protocol API
//! and high-level producer/consumer API.

pub mod client;
mod codec;
pub mod connection;
pub mod error;
pub mod sasl;
pub mod transport;

// Re-export commonly used types
pub use connection::{Connection, NegotiatedVersions};
pub use error::{KafkaError, ProtocolError, Result, SaslError};
pub use kafka_client_protocol as protocol;
pub use sasl::{SaslCredentials, SaslMechanismType};
pub use transport::{SecurityProtocol, TlsConfigData};

// Re-export client types
pub use client::{KafkaClientBuilder, builder};

pub use client::low_level::{ClientConfig, KafkaClient as LowLevelClient};

pub use client::metadata::MetadataCache;

pub use client::high_level::{
    AutoOffsetReset,
    // Consumer
    Consumer,
    ConsumerConfig,
    Header,
    // Partition routing
    PartitionRouter,
    PartitionRouting,
    // Producer
    Producer,
    ProducerConfig,
    ProducerRecord,
    RecordMetadata,
};

/// Library name
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
