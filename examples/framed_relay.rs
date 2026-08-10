//! Framed relay example — demonstrates the **L2/L3-framed** layer.
//!
//! [`kafka_client::connection::Builder::build_framed`] returns an
//! authenticated [`kafka_client::wire::KafkaFramed`]: a pure frame stream that
//! understands Kafka's 4-byte length prefix and nothing else. It performs **no**
//! request/response semantics, no correlation-ID bookkeeping, and no header
//! encoding — you drive the wire directly.
//!
//! **Use this when you need:**
//! - A 1:1 frame relay / proxy between a downstream client and an upstream
//!   broker (frames pass through verbatim, so payloads are never re-encoded)
//! - Full control over correlation IDs and byte layout
//! - A connection that stays entirely under your control
//!
//! For typed request/response on a shared connection, prefer
//! [`kafka_client::connection::ConnectionHandle::send_request`] or
//! [`send_request_frame`](kafka_client::connection::ConnectionHandle::send_request_frame).
//!
//! # Usage
//!
//! ```bash
//! # Default: connects to localhost:9092
//! cargo run --example framed_relay
//!
//! # Custom bootstrap server
//! KAFKA_BOOTSTRAP=192.168.1.100:9092 cargo run --example framed_relay
//! ```

use futures::StreamExt;
use kafka_client::connection::Builder as ConnectionBuilder;
use kafka_client::protocol::{MetadataRequest, Request};
use kafka_client::transport::SecurityProtocol;
use std::net::SocketAddr;

fn get_bootstrap_addr() -> SocketAddr {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    bootstrap
        .parse()
        .expect("Invalid bootstrap address format. Expected: host:port")
}

#[tokio::main]
async fn main() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let addr = get_bootstrap_addr();
    println!("=== Framed Relay Example (L2/L3-framed) ===");
    println!("Target: {}", addr);
    println!("Protocol: Plaintext (no TLS/SASL)");
    println!();

    // [1] Build an authenticated KafkaFramed via build_framed().
    //
    // The builder still performs the ApiVersions handshake and SASL
    // authentication for us, but hands back a raw frame stream instead of a
    // typed ConnectionHandle. We own correlation IDs and byte layout from here.
    println!("[1] Building authenticated KafkaFramed...");
    let (mut framed, negotiated) = match ConnectionBuilder::new(
        addr,
        SecurityProtocol::Plaintext,
        "kafka-client".to_string(),
        "0.1.0".to_string(),
    )
    .with_client_id("framed-relay-example".to_string())
    .build_framed()
    .await
    {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("ERROR: Failed to build framed connection: {}", e);
            std::process::exit(1);
        }
    };
    println!("  negotiated: {:?}", negotiated);

    // [2] Encode a Metadata request as a full frame (header + body).
    //
    // `Request::encode_frame` builds the request header (api_key, api_version,
    // correlation ID, client_id) and appends the encoded body. At this layer we
    // pick the correlation ID ourselves.
    println!("\n[2] Encoding a Metadata request frame...");
    let api_version = 0; // Metadata v0 is simple and flexible-free
    let correlation_id = 0x0A0B0C0D;
    let meta_req = MetadataRequest {
        topics: None,
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
    };
    let frame_bytes = match meta_req.encode_frame(
        api_version,
        correlation_id,
        Some("framed-relay-example".to_string()),
    ) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("ERROR: failed to encode request frame: {}", e);
            std::process::exit(1);
        }
    };
    println!(
        "  request frame: {} bytes, corr_id=0x{:08x}",
        frame_bytes.len(),
        correlation_id
    );

    // [3] Send the frame and read the next response frame.
    //
    // `send_frame` adds the 4-byte length prefix; `recv_frame` strips it on the
    // way back. Both are pure I/O with no timeout — serial relay pairs them up.
    println!("\n[3] Sending frame and reading response...");
    if let Err(e) = framed.send_frame(frame_bytes).await {
        eprintln!("ERROR: send_frame failed: {}", e);
        std::process::exit(1);
    }
    let response = match framed.recv_frame().await {
        Ok(frame) => frame,
        Err(e) => {
            eprintln!("ERROR: recv_frame failed: {}", e);
            std::process::exit(1);
        }
    };
    // The response begins with the correlation ID (response headers carry no
    // api_key/version, so it sits at bytes [0..4]).
    let echoed = i32::from_be_bytes(response[..4].try_into().unwrap());
    println!(
        "  response frame: {} bytes, corr_id=0x{:08x}",
        response.len(),
        echoed
    );
    assert_eq!(
        echoed, correlation_id,
        "correlation_id must round-trip 1:1 in serial relay"
    );

    // [4] 1:1 relaying via `into_inner().split()`.
    //
    // For a true proxy you typically split the authenticated frame stream into
    // independent read/write halves and pump frames between two endpoints. The
    // example below is schematic: split into halves and show they can be driven
    // concurrently.
    println!("\n[4] Splitting into read/write halves (schematic relay)...");
    let (_sink, _stream) = framed.into_inner().split();
    println!("  split: sink + stream obtained; pump frames between endpoints here");
    // Example relay plumbing: feed `_sink` from a downstream read half and
    // drain `_stream` into a downstream write half, e.g.
    //   let mut rx = kafka_client::wire::Framed::new(downstream_read, kafka_client::wire::KafkaCodec::new());
    //   let mut tx = kafka_client::wire::Framed::new(downstream_write, kafka_client::wire::KafkaCodec::new());
    //   tokio::join!(copy(rx, _sink), copy(_stream, tx));

    println!("\nDone.");
}
