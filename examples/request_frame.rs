//! Request-frame example — demonstrates **L3** structured header + raw body.
//!
//! [`kafka_client::connection::ConnectionHandle::send_request_frame`] sends a
//! request built from a **structured request header** plus a **raw body
//! [`bytes::Bytes`]**, and returns the structured response header plus the raw
//! response body.
//!
//! This sits between the fully-typed [`send_request`](kafka_client::connection::ConnectionHandle::send_request)
//! and the fully-raw frame stream (see `framed_relay`):
//!
//! - the header is a typed [`kafka_client::protocol::RequestHeader`] — you pick
//!   the correlation ID as a real field, no byte offsets;
//! - the body is caller-owned bytes appended verbatim (never re-encoded);
//! - you get back a typed [`kafka_client::protocol::ResponseHeader`] plus the
//!   undecoded response body.
//!
//! **Use this when you need:** full control over the header fields (especially
//! correlation ID) while still delegating framing/correlation matching to the
//! reactor, on an already-established [`ConnectionHandle`](kafka_client::connection::ConnectionHandle).
//!
//! # Usage
//!
//! ```bash
//! # Default: connects to localhost:9092
//! cargo run --example request_frame
//!
//! # Custom bootstrap server
//! KAFKA_BOOTSTRAP=192.168.1.100:9092 cargo run --example request_frame
//! ```

use bytes::BytesMut;
use kafka_client::connection::Builder as ConnectionBuilder;
use kafka_client::protocol::{Message, MetadataRequest, RequestHeader};
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
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let addr = get_bootstrap_addr();
    println!("=== Request-Frame Example (L3: structured header + raw body) ===");
    println!("Target: {}", addr);
    println!("Protocol: Plaintext (no TLS/SASL)");
    println!();

    // [1] Build a typed ConnectionHandle (reactor does correlation matching).
    println!("[1] Building ConnectionHandle...");
    let conn = match ConnectionBuilder::new(
        addr,
        SecurityProtocol::Plaintext,
        "kafka-client".to_string(),
        "0.1.0".to_string(),
    )
    .with_client_id("request-frame-example".to_string())
    .build()
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: Failed to connect: {}", e);
            std::process::exit(1);
        }
    };

    // [2] Build a structured request header. The correlation ID is a real
    // field you choose — no byte-offset arithmetic. Metadata's API key is 3.
    println!("\n[2] Building structured RequestHeader...");
    let api_key = 3_i16; // Metadata
    let api_version = 0;
    let correlation_id = 0x0A0B0C0D;
    let header = RequestHeader::new_v1(
        api_key,
        api_version,
        correlation_id,
        Some("request-frame-example".to_string()),
    );
    println!(
        "  api_key={} version={} corr_id=0x{:08x}",
        api_key, api_version, correlation_id
    );

    // [3] Encode the body ourselves (typed), hand it to send_request_frame as
    // raw bytes. The connection never re-serialises it.
    println!("\n[3] Encoding request body...");
    let meta_req = MetadataRequest {
        topics: None,
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
    };
    let mut body = BytesMut::with_capacity(64);
    Message::encode(&meta_req, &mut body, api_version).expect("failed to encode request body");
    println!("  body: {} bytes", body.len());

    // [4] Send header + raw body, get structured response header + raw body.
    println!("\n[4] Sending request frame...");
    let (response_header, response_body) =
        match conn.send_request_frame(header, body.freeze()).await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("ERROR: send_request_frame failed: {}", e);
                std::process::exit(1);
            }
        };

    // [5] Read the structured response header (correlation ID etc.) and
    // optionally decode the body yourself.
    println!("\n[5] Response header:");
    println!("  corr_id: 0x{:08x}", response_header.correlation_id());
    assert_eq!(
        response_header.correlation_id(),
        correlation_id,
        "response correlation ID must match the request"
    );
    println!(
        "  body: {} bytes (undecoded; decode it with Message::decode if needed)",
        response_body.len()
    );

    println!("\nDone. Connection will be closed on exit.");
}
