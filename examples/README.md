# Kafka Client Examples

This directory contains example programs demonstrating how to use the `kafka-client` library.

The examples are organized by **layer** of the library, from the high-level
`Client` down to the raw frame stream. Start at the top; reach lower layers only
when you need finer control.

## Architecture in one line

```
L4  Client (produce / consume / admin)
L3  connection::ConnectionHandle   (pipelined, out-of-order)   <- build()
    connection::SequentialConnection (serial, pre-auth)         <- build_sequential()
L2  wire::KafkaFramed              (pure frame send/recv)      <- build_framed()
L1  transport::NetworkStream       (TCP / TLS)
```

## Example Overview (by layer)

| Layer | Example | Description |
|-------|---------|-------------|
| **L4** | `basic_connect.rs` | Simple connection and metadata query via `Client` |
| **L4** | `produce_consume.rs` | Complete workflow: create topic → produce → consume |
| **L4** | `admin_operations.rs` | Topic management (create/delete) |
| **L3** | `raw_connection.rs` | Typed `ConnectionHandle::send_request` (pipelined) |
| **L3** | `request_frame.rs` | Structured `RequestHeader` + raw body (`send_request_frame`) |
| **L3** | `sequential_connection.rs` | Serial `SequentialConnection` → promote to pipelined |
| **L2** | `framed_relay.rs` | Pure frame relay via `build_framed()` (proxy/gateway) |
| **L1/security** | `sasl_auth.rs` | SASL authentication (PLAIN, SCRAM) |
| **L1/security** | `tls_connect.rs` | TLS encryption and TLS+SASL |

## Running Examples

### Prerequisites

- A running Kafka broker (default: `localhost:9092`)
- Rust toolchain with Tokio support

### Basic Examples (L4)

```bash
# Connect to localhost:9092
cargo run --example basic_connect

# Connect to custom server
KAFKA_BOOTSTRAP=192.168.1.100:9092 cargo run --example basic_connect
```

### Produce and Consume (L4)

```bash
# Default configuration
cargo run --example produce_consume

# Custom topic
KAFKA_TOPIC=my-topic cargo run --example produce_consume
```

### Admin Operations (L4)

```bash
# Create and delete topics
cargo run --example admin_operations
```

### Raw Connection (L3: typed pipelined)

```bash
cargo run --example raw_connection
```

Sends a typed `MetadataRequest` over a `ConnectionHandle`, which runs a reactor
that correlates responses to callers out-of-order.

### Request Frame (L3: structured header + raw body)

```bash
cargo run --example request_frame
```

Demonstrates `ConnectionHandle::send_request_frame`: you build a structured
`kafka_client::protocol::RequestHeader` (choosing the correlation ID yourself)
and hand a raw encoded body; you get back a structured
`kafka_client::protocol::ResponseHeader` plus the raw body.

### Sequential Connection (L3: serial phase)

```bash
cargo run --example sequential_connection
```

Demonstrates `Builder::build_sequential()`, which returns a strict
one-request-at-a-time `SequentialConnection`, then promotes it to a pipelined
`ConnectionHandle` via `into_pipeline()`.

### Raw Frame Relay (L2: proxy / gateway)

```bash
cargo run --example framed_relay
```

Demonstrates `Builder::build_framed()`, which returns an authenticated
`wire::KafkaFramed` with no reactor — the caller drives the socket and owns
correlation IDs. Call `.into_inner().split()` to obtain independent read/write
halves for full-duplex relaying. Use it when forwarding frames verbatim between
a downstream client and a broker.

### SASL Authentication (security)

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

### TLS Connection (security)

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

## Choosing the right level of access

| Need | Use |
|------|-----|
| Normal produce/consume/admin | `Client` (L4) |
| Typed request, auto correlation ID, pipelined | `ConnectionHandle::send_request` (L3) |
| Structured header + raw body, pipelined | `ConnectionHandle::send_request_frame` (L3) |
| Strict one-request-at-a-time, then promote | `Builder::build_sequential` (L3) |
| 1:1 frame relay, full-duplex, caller-owned correlation IDs | `Builder::build_framed` (L2) |

### The `KafkaFramed` primitive (L2)

A `KafkaFramed` obtained from `build_framed()` is a pure frame stream: it has no
request/response semantics, no correlation-ID bookkeeping, and no header
encoding. Drive it with its raw primitives, or `into_inner().split()` it into
independent read/write halves:

| You want | Use |
|----------|-----|
| Send one frame (length prefix added for you) | `send_frame(Bytes)` |
| Read the next frame (length prefix stripped) | `recv_frame()` / `recv_response()` |
| Independent read/write halves for full-duplex relaying | `into_inner().split()` |

There is only one way to write at this layer (`send_frame`) and one way to read
(`recv_frame`). A serial "send then read" is just those two calls in sequence;
timeouts and correlation-ID matching are the caller's responsibility.

For correlation-ID dispatch on a single multiplexed connection, use
`ConnectionHandle` (reactor) instead — it correlates responses to callers
out-of-order. `SequentialConnection` covers the strict serial, pre-auth phase.

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

### L4 — High-level `Client`

- **`basic_connect.rs`**: The simplest way to connect to Kafka and query cluster metadata. Ideal for first-time users.
- **`produce_consume.rs`**: A complete workflow including topic creation, message production, and consumption.
- **`admin_operations.rs`**: Administrative operations like creating and deleting topics.

### L3 — Connection layer

- **`raw_connection.rs`**: Typed `ConnectionHandle::send_request` over a pipelined reactor. Use when you need direct protocol access but want typed messages and automatic correlation.
- **`request_frame.rs`**: `ConnectionHandle::send_request_frame` — structured header + raw body. Use when you need to control the header (especially correlation ID) but still want reactor framing.
- **`sequential_connection.rs`**: `SequentialConnection` for the strict serial, pre-auth phase, then `into_pipeline()` to a pipelined handle.

### L2 — Frame layer

- **`framed_relay.rs`**: `Builder::build_framed()` returning an authenticated `KafkaFramed` for 1:1 frame relay (proxy/gateway).

### Security

- **`sasl_auth.rs`**: SASL authentication with different mechanisms (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
- **`tls_connect.rs`**: TLS encryption and TLS+SASL configurations.

## Learning Path

1. Start with `basic_connect.rs` to understand connection basics (L4)
2. Move to `produce_consume.rs` to learn the core workflow (L4)
3. Try `admin_operations.rs` for topic management (L4)
4. Explore `sasl_auth.rs` and `tls_connect.rs` for security features
5. Use `raw_connection.rs` and `request_frame.rs` when you need direct connection control (L3)
6. Reach `framed_relay.rs` only when you need a raw frame relay/proxy (L2)

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
