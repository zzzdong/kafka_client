//! SASL authentication example
//!
//! Demonstrates how to connect to a Kafka cluster with SASL authentication.
//! Supports PLAIN and SCRAM-SHA-256/512 mechanisms.
//!
//! # Usage
//!
//! ```bash
//! # PLAIN authentication
//! KAFKA_BOOTSTRAP=localhost:9092 \
//! SASL_MECHANISM=PLAIN \
//! SASL_USERNAME=user \
//! SASL_PASSWORD=pass \
//! cargo run --example sasl_auth
//!
//! # SCRAM-SHA-256 authentication
//! KAFKA_BOOTSTRAP=localhost:9092 \
//! SASL_MECHANISM=SCRAM-SHA-256 \
//! SASL_USERNAME=user \
//! SASL_PASSWORD=pass \
//! cargo run --example sasl_auth
//! ```

use kafka_client::{KafkaClient, SaslMechanismType};
use std::net::SocketAddr;

fn get_bootstrap_addr() -> SocketAddr {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP")
        .unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    bootstrap.parse().expect("Invalid bootstrap address format. Expected: host:port")
}

fn get_sasl_config() -> (SaslMechanismType, String, String) {
    let mechanism = std::env::var("SASL_MECHANISM")
        .unwrap_or_else(|_| "PLAIN".to_string());
    
    let mechanism_type = match mechanism.to_uppercase().as_str() {
        "PLAIN" => SaslMechanismType::Plain,
        "SCRAM-SHA-256" => SaslMechanismType::ScramSha256,
        "SCRAM-SHA-512" => SaslMechanismType::ScramSha512,
        _ => {
            eprintln!("ERROR: Unsupported SASL mechanism: {}", mechanism);
            eprintln!("Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512");
            std::process::exit(1);
        }
    };

    let username = std::env::var("SASL_USERNAME")
        .expect("SASL_USERNAME environment variable required");
    let password = std::env::var("SASL_PASSWORD")
        .expect("SASL_PASSWORD environment variable required");

    (mechanism_type, username, password)
}

#[tokio::main]
async fn main() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let addr = get_bootstrap_addr();
    let (mechanism, username, password) = get_sasl_config();

    println!("=== SASL Authentication Example ===");
    println!("Bootstrap: {}", addr);
    println!("Mechanism: {:?}", mechanism);
    println!("Username: {}", username);
    println!();

    // Build client with SASL authentication
    println!("[1] Connecting with SASL authentication...");
    let client = match KafkaClient::builder(vec![addr])
        .with_client_id("sasl-auth-example")
        .with_sasl(mechanism, username, password)
        .build()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: Failed to connect: {}", e);
            std::process::exit(1);
        }
    };

    println!("Connected successfully!");

    // Query metadata to verify connection
    println!("\n[2] Querying cluster metadata...");
    let brokers = client.metadata().get_all_brokers().await;
    println!("Discovered {} brokers:", brokers.len());
    for b in &brokers {
        println!("  Broker {}: {}:{}", b.node_id, b.host, b.port);
    }

    // Clean shutdown
    println!("\n[3] Shutting down...");
    if let Err(e) = client.close().await {
        eprintln!("WARNING: Shutdown error: {}", e);
    }
    println!("Done.");
}