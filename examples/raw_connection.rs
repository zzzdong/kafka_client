//! Raw connection example - demonstrates low-level Connection API
//!
//! This example shows how to use the Connection layer directly,
//! bypassing the higher-level KafkaClient/ClusterClient abstractions.
//!
//! **Use this only when you need:**
//! - Direct protocol-level access
//! - Custom request/response handling
//! - Debugging protocol issues
//!
//! For normal usage, prefer `KafkaClient` instead.
//!
//! # Usage
//!
//! ```bash
//! # Default: connects to localhost:9092
//! cargo run --example raw_connection
//!
//! # Custom bootstrap server
//! KAFKA_BOOTSTRAP=192.168.1.100:9092 cargo run --example raw_connection
//! ```

use kafka_client::connection::Builder as ConnectionBuilder;
use kafka_client::protocol::MetadataRequest;
use kafka_client::transport::SecurityProtocol;
use std::net::SocketAddr;

fn get_bootstrap_addr() -> SocketAddr {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP")
        .unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    bootstrap.parse().expect("Invalid bootstrap address format. Expected: host:port")
}

#[tokio::main]
async fn main() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let addr = get_bootstrap_addr();
    println!("=== Raw Connection Example ===");
    println!("Target: {}", addr);
    println!("Protocol: Plaintext (no TLS/SASL)");
    println!();

    // Build connection manually
    println!("[1] Building connection...");
    let builder = ConnectionBuilder::new(
        addr,
        SecurityProtocol::Plaintext,
        "kafka-client".to_string(),
        "0.1.0".to_string(),
    )
    .with_client_id("raw-connection-example".to_string());

    let mut conn = match builder.build().await {
        Ok(c) => {
            println!("Connection established");
            c
        }
        Err(e) => {
            eprintln!("ERROR: Failed to connect: {}", e);
            std::process::exit(1);
        }
    };

    // Send raw Metadata request
    println!("\n[2] Sending MetadataRequest...");
    let meta_req = MetadataRequest {
        topics: None,  // Query all topics
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
    };

    let resp = match conn
        .send_request::<_, kafka_client::protocol::MetadataResponse>(&meta_req)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("ERROR: Failed to send request: {}", e);
            std::process::exit(1);
        }
    };

    // Display response
    println!("\n[3] Metadata response:");
    println!("  Cluster ID: {:?}", resp.cluster_id);
    println!("  Controller ID: {}", resp.controller_id);
    println!("  Brokers: {}", resp.brokers.len());
    for b in &resp.brokers {
        println!("    Broker {}: {}:{}", b.node_id, b.host, b.port);
    }
    println!("  Topics: {}", resp.topics.len());
    for t in &resp.topics {
        if let Some(name) = &t.name {
            println!("    Topic '{}': {} partitions", name, t.partitions.len());
        }
    }

    println!("\nDone. Connection will be closed on exit.");
}