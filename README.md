# kafka_client

A pure Rust Kafka client library built on Tokio async runtime. Supports SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) and TLS encryption.

[![Crates.io](https://img.shields.io/crates/v/kafka_client.svg)](https://crates.io/crates/kafka_client)
[![Documentation](https://docs.rs/kafka_client/badge.svg)](https://docs.rs/kafka_client)
[![License](https://img.shields.io/crates/l/kafka_client.svg)](https://github.com/zzzdong/kafka_client#license)

## Features

- **Pure Rust** - No C bindings or external dependencies required
- **Async/Await** - Built on Tokio for modern async Rust
- **SASL Authentication** - Supports PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
- **TLS Encryption** - Secure connections with configurable TLS
- **Layered Architecture** - Clean separation between transport, protocol, and API layers
- **High-Level API** - Easy-to-use Producer, Consumer, and Admin interfaces
- **Low-Level Access** - Direct protocol access for advanced use cases
- **Connection Pooling** - Efficient broker connection management
- **Metadata Caching** - Automatic cluster metadata refresh

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
kafka_client = "0.1"
```

## Quick Start

### Basic Connection

```rust
use kafka_client::Client;

let client = Client::builder(vec!["127.0.0.1:9092".parse()?])
    .with_client_id("my-app")
    .build()
    .await?;

// Query cluster metadata
let brokers = client.metadata().get_all_brokers().await;
println!("Discovered {} brokers", brokers.len());
```

### Producer

```rust
use kafka_client::{Client, ProducerConfig, ProducerRecord};
use bytes::Bytes;

let client = Client::builder(vec!["127.0.0.1:9092".parse()?])
    .build()
    .await?;

// Default config
let producer = client.producer_default().await;

// Or with custom config
let producer = client.producer(
    ProducerConfig::new().with_acks(-1).with_retries(3)
).await;

// Send messages
let record = ProducerRecord::new("my-topic", Bytes::from("hello world"));
let metadata = producer.send(record).await?;
println!("Sent to partition {} at offset {}", metadata.partition, metadata.offset);

// Ensure all messages are delivered
producer.flush().await?;
```

### Consumer

```rust
use kafka_client::{Client, ConsumerConfig};

let client = Client::builder(vec!["127.0.0.1:9092".parse()?])
    .build()
    .await?;

// Default group consumer
let mut consumer = client.consumer_default();

// Or with custom config
let mut consumer = client.consumer(
    ConsumerConfig::new("my-consumer-group")
        .with_earliest()
);

consumer.subscribe(vec!["my-topic".to_string()]).await?;

// Poll for messages
let records = consumer.poll_timeout(Duration::from_millis(5000)).await?;
for record in records {
    println!("Received: {:?}", record.value);
}
```

### Admin

```rust
use kafka_client::{Client, admin::NewTopic};

let client = Client::builder(vec!["127.0.0.1:9092".parse()?])
    .build()
    .await?;
let admin = client.admin();

// Create a topic
admin.create_topic(&NewTopic::new("orders", 3, 3)).await?;

// List all topics
let topics = admin.list_topics().await?;
for t in &topics { println!("{}", t.name); }

// Describe cluster
let info = admin.describe_cluster().await?;
println!("{} brokers, controller: {:?}", info.brokers.len(), info.controller_id);
```

### SASL Authentication

```rust
use kafka_client::{Client, SaslMechanismType};

// PLAIN authentication
let client = Client::builder(vec!["127.0.0.1:9092".parse()?])
    .with_sasl(SaslMechanismType::Plain, "username", "password")
    .build()
    .await?;

// SCRAM-SHA-256 authentication
let client = Client::builder(vec!["127.0.0.1:9092".parse()?])
    .with_sasl(SaslMechanismType::ScramSha256, "username", "password")
    .build()
    .await?;
```

### TLS Encryption

```rust
use kafka_client::{Client, TlsConfig};

let tls = TlsConfig {
    domain: "kafka.example.com".to_string(),
    verify_certificate: true,
    ..Default::default()
};

// TLS only
let client = Client::builder(vec!["127.0.0.1:9093".parse()?])
    .with_tls_config(tls)
    .build()
    .await?;

// TLS + SASL
let client = Client::builder(vec!["127.0.0.1:9093".parse()?])
    .with_sasl_tls(tls, SaslMechanismType::ScramSha256, "username", "password")
    .build()
    .await?;
```

## Architecture

The library uses a layered architecture for clean separation of concerns:

```
┌─────────────────────────────────────────┐
│         High-Level API Layer            │
│  (Client, Producer, Consumer, Admin)    │
├─────────────────────────────────────────┤
│         Cluster Layer                   │
│  (ClusterClient, BrokerManager,         │
│   MetadataCache)  [crate-internal]      │
├─────────────────────────────────────────┤
│         Connection Layer                │
│  (Connection, ConnectionPool)           │
├─────────────────────────────────────────┤
│         Protocol Layer                  │
│  (Wire Codec, Request/Response)         │
├─────────────────────────────────────────┤
│         Transport Layer                 │
│  (TCP, TLS, SASL)                       │
└─────────────────────────────────────────┘
```

## Examples

See the [examples](./examples/) directory for complete working examples:

- `basic_connect.rs` - Simple connection and metadata query
- `produce_consume.rs` - Complete produce/consume workflow
- `sasl_auth.rs` - SASL authentication with different mechanisms
- `tls_connect.rs` - TLS encryption and TLS+SASL
- `admin_operations.rs` - Topic management operations
- `raw_connection.rs` - Low-level Connection API

Run an example:

```bash
# Basic connection
cargo run --example basic_connect

# SASL authentication
SASL_MECHANISM=SCRAM-SHA-256 \
SASL_USERNAME=user \
SASL_PASSWORD=pass \
cargo run --example sasl_auth

# TLS connection
KAFKA_DOMAIN=kafka.example.com \
cargo run --example tls_connect
```

## Documentation

- [API Documentation](https://docs.rs/kafka_client)
- [Examples](./examples/)
- [Changelog](./CHANGELOG.md)

## Requirements

- Rust 1.85 or later
- Kafka 0.10.0 or later (supports Kafka 3.x+)

## Testing

Integration tests require a running Kafka broker:

```bash
# Using Docker
docker run -d --name kafka -p 9092:9092 apache/kafka:latest

# Run tests
cargo test --features integration_tests
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT License ([LICENSE-MIT](./LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request


## Author

- **zzzdong** - [GitHub](https://github.com/zzzdong) - kuwater@163.com
