//! TLS connection example
//!
//! Demonstrates how to connect to a Kafka cluster with TLS encryption.
//! Shows both TLS-only (no auth) and TLS+SASL configurations.
//!
//! # Usage
//!
//! ```bash
//! # TLS only (no SASL authentication)
//! KAFKA_BOOTSTRAP=localhost:9093 \
//! KAFKA_DOMAIN=kafka.example.com \
//! cargo run --example tls_connect
//!
//! # TLS with SASL authentication
//! KAFKA_BOOTSTRAP=localhost:9093 \
//! KAFKA_DOMAIN=kafka.example.com \
//! SASL_MECHANISM=SCRAM-SHA-256 \
//! SASL_USERNAME=user \
//! SASL_PASSWORD=pass \
//! cargo run --example tls_connect
//! ```

use kafka_client::{KafkaClient, SaslMechanismType, TlsConfig};
use std::net::SocketAddr;

fn get_bootstrap_addr() -> SocketAddr {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP")
        .unwrap_or_else(|_| "127.0.0.1:9093".to_string());
    bootstrap.parse().expect("Invalid bootstrap address format. Expected: host:port")
}

fn get_tls_domain() -> String {
    std::env::var("KAFKA_DOMAIN")
        .unwrap_or_else(|_| "localhost".to_string())
}

fn get_sasl_config() -> Option<(SaslMechanismType, String, String)> {
    let mechanism = std::env::var("SASL_MECHANISM").ok()?;
    let username = std::env::var("SASL_USERNAME").ok()?;
    let password = std::env::var("SASL_PASSWORD").ok()?;

    let mechanism_type = match mechanism.to_uppercase().as_str() {
        "PLAIN" => SaslMechanismType::Plain,
        "SCRAM-SHA-256" => SaslMechanismType::ScramSha256,
        "SCRAM-SHA-512" => SaslMechanismType::ScramSha512,
        _ => {
            eprintln!("WARNING: Unsupported SASL mechanism: {}", mechanism);
            return None;
        }
    };

    Some((mechanism_type, username, password))
}

#[tokio::main]
async fn main() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let addr = get_bootstrap_addr();
    let domain = get_tls_domain();
    let sasl_config = get_sasl_config();

    println!("=== TLS Connection Example ===");
    println!("Bootstrap: {}", addr);
    println!("TLS Domain: {}", domain);

    // Build TLS config
    let tls_config = TlsConfig {
        domain,
        verify_certificate: true,
        ..Default::default()
    };

    // Build client with TLS
    println!();
    println!("[1] Connecting with TLS...");
    let builder = match sasl_config {
        Some((mechanism, username, password)) => {
            println!("SASL: {:?} (user: {})", mechanism, username);
            KafkaClient::builder(vec![addr])
                .with_client_id("tls-connect-example")
                .with_sasl_tls(tls_config.clone(), mechanism, username, password)
        }
        None => {
            println!("SASL: None");
            KafkaClient::builder(vec![addr])
                .with_client_id("tls-connect-example")
                .with_tls_config(tls_config)
        }
    };

    let client = match builder.build().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: Failed to connect: {}", e);
            std::process::exit(1);
        }
    };

    println!("Connected successfully!");

    // Query metadata
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