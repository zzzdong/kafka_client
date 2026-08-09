//! Framed relay example — 1:1 raw frame forwarding via `build_framed()`.
//!
//! Demonstrates the lowest-level connection API: an authenticated Kafka frame
//! stream that you drive yourself, with no reactor and no request/response
//! decoding. This is the building block for a Kafka proxy or gateway.
//!
//! **Use this only when you need:**
//! - Verbatim frame forwarding (the payload must not be re-encoded)
//! - Full control over correlation IDs
//! - Independent read/write halves for full-duplex relaying
//!
//! For normal usage, prefer `Client`. For occasional raw sends over a normal
//! pooled connection, prefer `ConnectionHandle::send_raw_frame`.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example framed_relay
//! KAFKA_BOOTSTRAP=192.168.1.100:9092 cargo run --example framed_relay
//! ```

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use kafka_client::connection::Builder as ConnectionBuilder;
use kafka_client::protocol::{Message, MetadataRequest, Request};
use kafka_client::transport::SecurityProtocol;
use kafka_client::wire::KafkaFrame;
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
    println!("=== Framed Relay Example ===");
    println!("Target: {}", addr);
    println!();

    // [1] Build an authenticated framed stream instead of a reactor handle.
    println!("[1] Establishing framed connection...");
    let builder = ConnectionBuilder::new(
        addr,
        SecurityProtocol::Plaintext,
        "kafka-client".to_string(),
        "0.1.0".to_string(),
    )
    .with_client_id("framed-relay-example".to_string())
    // Relaying large record batches? Raise the codec limit.
    .with_max_frame_size(256 * 1024 * 1024);

    let (mut framed, negotiated) = match builder.build_framed().await {
        Ok(pair) => {
            println!("Connection established (handshake + auth complete)");
            pair
        }
        Err(e) => {
            eprintln!("ERROR: Failed to connect: {}", e);
            std::process::exit(1);
        }
    };

    let request = MetadataRequest {
        topics: None,
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
    };

    // The negotiated versions come from the handshake, so a proxy can answer a
    // downstream ApiVersions request locally instead of forwarding it.
    let metadata_version = match negotiated.get_version(request.api_key()) {
        Some(v) => v,
        None => {
            eprintln!("ERROR: broker does not support Metadata");
            std::process::exit(1);
        }
    };
    println!("    Negotiated Metadata version: {}", metadata_version);

    // [1.5] Mid-level helper: `send_frame`. We encode just the *body* ourselves,
    // pass the api_key/version, and let the library pick the correct header
    // (v1 vs v2 flexible) and own the correlation ID. The body is obtained via
    // `Message::encode` on the request (header excluded).
    println!("\n[1.5] Sending via send_frame (library-owned correlation_id)...");
    let mut body = bytes::BytesMut::new();
    kafka_client::protocol::Message::encode(&request, &mut body, metadata_version)
        .expect("failed to encode request body");
    let is_flexible = MetadataRequest::is_flexible_version(metadata_version);
    match framed
        .send_frame(
            request.api_key(),
            metadata_version,
            is_flexible,
            Some("framed-relay-example".to_string()),
            body.freeze(),
        )
        .await
    {
        Ok(response_body) => {
            println!(
                "  send_frame ok, {} response bytes (header stripped).",
                response_body.len()
            );
        }
        Err(e) => {
            eprintln!("ERROR: send_frame failed: {}", e);
            std::process::exit(1);
        }
    }

    // [2] Split into independent halves — this is what a proxy needs in order
    //     to pump both directions concurrently. `into_inner()` reaches the raw
    //     `tokio_util::codec::Framed`; the wrapped crate-owned `KafkaFramed`
    //     type does not itself implement `Sink`/`Stream` so that its API stays
    //     stable regardless of the underlying codec.
    println!("\n[2] Splitting into read/write halves...");
    let (mut sink, mut stream) = framed.into_inner().split();

    // [3] Encode a frame ourselves. In a real proxy these bytes would arrive
    //     from a downstream client and be forwarded without inspection.
    //
    //     NOTE: because we own the whole connection, we also own the
    //     correlation ID space — no reactor is competing for it.
    let correlation_id = 1;
    let request_bytes: Bytes = request
        .encode_frame(
            metadata_version,
            correlation_id,
            Some("framed-relay-example".to_string()),
        )
        .expect("failed to encode request");

    println!(
        "\n[3] Sending frame ({} bytes, cid={})...",
        request_bytes.len(),
        correlation_id
    );
    if let Err(e) = sink.send(KafkaFrame::new(request_bytes)).await {
        eprintln!("ERROR: send failed: {}", e);
        std::process::exit(1);
    }

    // [4] Read the raw response frame. The length prefix is already stripped;
    //     a proxy would forward `frame.data` downstream verbatim.
    println!("\n[4] Awaiting response frame...");
    match stream.next().await {
        Some(Ok(frame)) => {
            println!("Received {} bytes (undecoded)", frame.data.len());
            let echoed = i32::from_be_bytes(frame.data[..4].try_into().unwrap());
            println!("  correlation_id in response: {}", echoed);
            assert_eq!(echoed, correlation_id, "correlation_id must round-trip 1:1");
            println!("  correlation_id round-tripped 1:1");
        }
        Some(Err(e)) => {
            eprintln!("ERROR: read failed: {}", e);
            std::process::exit(1);
        }
        None => {
            eprintln!("ERROR: connection closed by broker");
            std::process::exit(1);
        }
    }

    println!("\nDone.");
}
