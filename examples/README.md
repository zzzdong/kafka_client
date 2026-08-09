# Kafka Client Examples

This directory contains example programs demonstrating how to use the `kafka-client` library.

## Example Overview

| Example | Description | Level |
|---------|-------------|-------|
| `basic_connect.rs` | Simple connection and metadata query | Basic |
| `produce_consume.rs` | Complete workflow: create topic → produce → consume | Intermediate |
| `raw_connection.rs` | Low-level Connection API (for debugging) | Advanced |
| `framed_relay.rs` | Raw 1:1 frame relay via `build_framed()` (proxy/gateway) | Advanced |
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

### Raw Frame Relay (proxy / gateway)

```bash
cargo run --example framed_relay
```

Demonstrates `Builder::build_framed()`, which returns an authenticated
`wire::KafkaFramed` with no reactor — the caller drives the socket and owns
correlation IDs. Call `.into_inner().split()` to obtain independent read/write
halves for full-duplex relaying. Use it when forwarding frames verbatim between
a downstream client and a broker.

Choosing the right level of access:

| Need | Use |
|------|-----|
| Normal produce/consume/admin | `Client` |
| Typed request with auto correlation ID over a pooled connection | `ConnectionHandle::send_request` |
| Strict one-request-at-a-time, no reactor | `Builder::build_sequential` |
| 1:1 frame relay, full-duplex, caller-owned correlation IDs | `Builder::build_framed` |

For a `KafkaFramed` obtained from `build_framed()` / `into_framed()`, pick the
request helper by how much you want to encode yourself:

| You provide | Use |
|-------------|-----|
| A typed `Request` value | `send_request` (library encodes header + body + correlation ID) |
| `api_key`, `api_version`, `is_flexible`, a pre-encoded body | `send_frame` (library encodes header + owns correlation ID) |
| The entire header + body byte string | `send_raw_frame` (library only adds the length prefix) |

Note that `build_framed()` returns an **already authenticated** connection:
never forward a downstream client's `ApiVersions`, `SaslHandshake`, or
`SaslAuthenticate` frames to the broker. Answer those locally using the
`NegotiatedVersions` returned alongside the stream.

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
let client = Client::builder(vec!["localhost:9092".to_string()])
    .with_client_id("my-app")
    .build()
    .await?;
```

### SASL Authentication

```rust
// PLAIN
let client = Client::builder(vec![addr])
    .with_sasl(SaslMechanismType::Plain, "user", "pass")
    .build()
    .await?;

// SCRAM-SHA-256
let client = Client::builder(vec![addr])
    .with_sasl(SaslMechanismType::ScramSha256, "user", "pass")
    .build()
    .await?;

// Convenience method (PLAIN only)
let client = Client::builder(vec![addr])
    .with_sasl_plaintext("user", "pass")
    .build()
    .await?;
```

### TLS + SASL

```rust
let tls = TlsConfig {
    domain: "kafka.example.com".into(),
    ..Default::default()
};

// TLS + SASL with custom mechanism
let client = Client::builder(vec![addr])
    .with_sasl_tls(tls, SaslMechanismType::ScramSha256, "user", "pass")
    .build()
    .await?;

// Convenience method (TLS + PLAIN)
let client = Client::builder(vec![addr])
    .with_sasl_ssl("kafka.example.com", "user", "pass")
    .build()
    .await?;
```

### Producer

```rust
let producer = client.producer(ProducerConfig::new()).await;
let record = ProducerRecord::new("topic", Bytes::from("message"));
producer.send(record).await?;
producer.flush().await?;
```

### Consumer

```rust
let consumer = client.consumer(
    ConsumerConfig::new().with_group_id("my-group")
        .with_earliest()
);
consumer.subscribe(vec!["topic".to_string()]).await?;
let records = consumer.poll_timeout(Duration::from_millis(5000)).await?;
```

### Admin Operations

```rust
use kafka_client::admin::NewTopic;

let admin = client.admin();

// Create a topic
admin.create_topic(&NewTopic::new("orders", 3, 3)).await?;

// List all topics; describe the cluster
let topics = admin.list_topics().await?;
let info = admin.describe_cluster().await?;

// Describe & delete
admin.describe_topics(&["orders"]).await?;
admin.delete_topic("orders").await?;
```