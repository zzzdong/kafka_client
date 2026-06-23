//! Basic connection example
//!
//! Demonstrates the simplest way to connect to a Kafka cluster
//! and discover broker metadata.
//!
//! # Usage
//!
//! ```bash
//! # Default: connects to localhost:9092
//! cargo run --example basic_connect
//!
//! # Custom bootstrap server
//! KAFKA_BOOTSTRAP=192.168.1.100:9092 cargo run --example basic_connect
//! ```

use kafka_client::KafkaClient;
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
    // Initialize logging (optional, but helpful for debugging)
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let addr = get_bootstrap_addr();
    println!("=== Connecting to Kafka at {} ===", addr);

    // Build and connect the client
    let client = match KafkaClient::builder(vec![addr])
        .with_client_id("basic-connect-example")
        .build()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: Failed to connect to Kafka: {}", e);
            std::process::exit(1);
        }
    };

    println!("SUCCESS: Connected!");

    // Query cluster metadata
    let brokers = client.metadata().get_all_brokers().await;
    println!("Discovered {} brokers:", brokers.len());
    for b in &brokers {
        println!("  Broker {}: {}:{}", b.node_id, b.host, b.port);
    }

    // Clean shutdown
    if let Err(e) = client.close().await {
        eprintln!("WARNING: Error during shutdown: {}", e);
    }
    println!("Connection closed.");
}
