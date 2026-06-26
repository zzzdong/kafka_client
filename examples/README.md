# Kafka Client Examples

This directory contains example programs demonstrating how to use the `kafka-client` library.

## Example Overview

| Example | Description | Level |
|---------|-------------|-------|
| `basic_connect.rs` | Simple connection and metadata query | Basic |
| `produce_consume.rs` | Complete workflow: create topic → produce → consume | Intermediate |
| `raw_connection.rs` | Low-level Connection API (for debugging) | Advanced |
| `sasl_auth.rs` | SASL authentication (PLAIN, SCRAM) | Advanced |
| `tls_connect.rs` | TLS encryption and TLS+SASL | Advanced |
| `admin_operations.rs` | Topic management (create/delete) | Intermediate |

## Running Examples

### Prerequisites

- A running Kafka broker (default: `localhost:9092`)
- Rust toolchain with Tokio support

### Basic Examples

```bash
# Connect to localhost:9092
cargo run --example basic_connect

# Connect to custom server
KAFKA_BOOTSTRAP=192.168.1.100:9092 cargo run --example basic_connect
```

### Produce and Consume

```bash
# Default configuration
cargo run --example produce_consume

# Custom topic
KAFKA_TOPIC=my-topic cargo run --example produce_consume
```

### SASL Authentication

```bash
# PLAIN mechanism
KAFKA_BOOTSTRAP=localhost:9092 \
SASL_MECHANISM=PLAIN \
SASL_USERNAME=user \
SASL_PASSWORD=pass \
cargo run --example sasl_auth

# SCRAM-SHA-256 mechanism
KAFKA_BOOTSTRAP=localhost:9092 \
SASL_MECHANISM=SCRAM-SHA-256 \
SASL_USERNAME=user \
SASL_PASSWORD=pass \
cargo run --example sasl_auth

# SCRAM-SHA-512 mechanism
KAFKA_BOOTSTRAP=localhost:9092 \
SASL_MECHANISM=SCRAM-SHA-512 \
SASL_USERNAME=user \
SASL_PASSWORD=pass \
cargo run --example sasl_auth
```

### TLS Connection

```bash
# TLS only (no SASL)
KAFKA_BOOTSTRAP=localhost:9093 \
KAFKA_DOMAIN=kafka.example.com \
cargo run --example tls_connect

# TLS with SASL PLAIN
KAFKA_BOOTSTRAP=localhost:9093 \
KAFKA_DOMAIN=kafka.example.com \
SASL_MECHANISM=PLAIN \
SASL_USERNAME=user \
SASL_PASSWORD=pass \
cargo run --example tls_connect

# TLS with SASL SCRAM-SHA-256
KAFKA_BOOTSTRAP=localhost:9093 \
KAFKA_DOMAIN=kafka.example.com \
SASL_MECHANISM=SCRAM-SHA-256 \
SASL_USERNAME=user \
SASL_PASSWORD=pass \
cargo run --example tls_connect
```

### Admin Operations

```bash
# Create and delete topics
cargo run --example admin_operations
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP` | Bootstrap server address | `127.0.0.1:9092` |
| `KAFKA_TOPIC` | Topic name for produce/consume | `example-topic` |
| `KAFKA_DOMAIN` | TLS domain for SNI | `localhost` |
| `SASL_MECHANISM` | SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) | - |
| `SASL_USERNAME` | SASL username | - |
| `SASL_PASSWORD` | SASL password | - |

## Example Structure

### Basic Level

- **`basic_connect.rs`**: Demonstrates the simplest way to connect to Kafka and query cluster metadata. Ideal for first-time users.

### Intermediate Level

- **`produce_consume.rs`**: Shows a complete workflow including topic creation, message production, and consumption. Demonstrates proper configuration and error handling.

- **`admin_operations.rs`**: Demonstrates administrative operations like creating and deleting topics.

### Advanced Level

- **`raw_connection.rs`**: Shows how to use the low-level Connection API directly. Use this only for debugging or when you need direct protocol access.

- **`sasl_auth.rs`**: Demonstrates SASL authentication with different mechanisms (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).

- **`tls_connect.rs`**: Demonstrates TLS encryption and TLS+SASL configurations.

## Learning Path

1. Start with `basic_connect.rs` to understand connection basics
2. Move to `produce_consume.rs` to learn the core workflow
3. Try `admin_operations.rs` for topic management
4. Explore `sasl_auth.rs` and `tls_connect.rs` for security features
5. Use `raw_connection.rs` only when you need low-level access

## Common Patterns

### Connection

```rust
let client = KafkaClient::builder(vec!["localhost:9092".parse().unwrap()])
    .with_client_id("my-app")
    .build()
    .await?;
```

### SASL Authentication

```rust
// PLAIN
let client = KafkaClient::builder(vec![addr])
    .with_sasl(SaslMechanismType::Plain, "user", "pass")
    .build()
    .await?;

// SCRAM-SHA-256
let client = KafkaClient::builder(vec![addr])
    .with_sasl(SaslMechanismType::ScramSha256, "user", "pass")
    .build()
    .await?;

// Convenience method (PLAIN only)
let client = KafkaClient::builder(vec![addr])
    .with_sasl_plaintext("user", "pass")
    .build()
    .await?;
```

### TLS + SASL

```rust
let tls = TlsConfig {
    domain: "kafka.example.com".into(),
    verify_certificate: true,
    ..Default::default()
};

// TLS + SASL with custom mechanism
let client = KafkaClient::builder(vec![addr])
    .with_sasl_tls(tls, SaslMechanismType::ScramSha256, "user", "pass")
    .build()
    .await?;

// Convenience method (TLS + PLAIN)
let client = KafkaClient::builder(vec![addr])
    .with_sasl_ssl("kafka.example.com", "user", "pass")
    .build()
    .await?;
```

### Producer

```rust
let producer = client.producer(ProducerConfig::new()).await?;
let record = ProducerRecord::new("topic", Bytes::from("message"));
producer.send(record).await?;
producer.flush().await?;
```

### Consumer

```rust
let consumer = client.consumer(
    ConsumerConfig::new("my-group")
        .with_earliest()
);
consumer.subscribe(vec!["topic".to_string()]).await?;
let records = consumer.poll_timeout(Duration::from_millis(5000)).await?;
```

### Admin Operations

```rust
let request = CreateTopicsRequest { ... };
let response: CreateTopicsResponse = client.cluster()
    .send_to_any_broker(&request)
    .await?;
```