//! Admin API example
//!
//! Demonstrates administrative operations using the high-level Admin client:
//! - List topics and describe clusters
//! - Create topics with custom partition counts
//! - Describe topic details (partitions, leaders)
//! - Delete topics
//!
//! # Usage
//!
//! ```bash
//! # Default: connects to localhost:9092
//! cargo run --example admin_operations
//!
//! # Custom bootstrap server
//! KAFKA_BOOTSTRAP=192.168.1.100:9092 cargo run --example admin_operations
//! ```

use kafka_client::{Client, admin::NewTopic};
use std::net::SocketAddr;

fn get_bootstrap_addrs() -> Vec<SocketAddr> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP")
        .unwrap_or_else(|_| "127.0.0.1:29093,127.0.0.1:29095,127.0.0.1:29097".to_string());
    bootstrap
        .split(',')
        .map(|s| {
            s.trim()
                .parse()
                .expect("Invalid bootstrap address format. Expected: host:port")
        })
        .collect()
}

#[tokio::main]
async fn main() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let addrs = get_bootstrap_addrs();
    println!("=== Admin Operations Example ===");
    println!("Bootstrap: {:?}", addrs);
    println!();

    // ── Connect ──
    println!("[1] Connecting to Kafka...");
    let client = match Client::builder(addrs)
        .with_client_id("admin-example")
        .build()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: Failed to connect: {}", e);
            std::process::exit(1);
        }
    };
    println!("Connected!");

    let admin = client.admin();

    // ── Describe cluster ──
    println!("\n[2] Cluster info...");
    let info = admin
        .describe_cluster()
        .await
        .expect("Failed to describe cluster");
    println!("  Brokers: {}", info.brokers.len());
    for b in &info.brokers {
        println!("    Broker {}: {}:{}", b.id, b.host, b.port);
    }

    // ── List existing topics ──
    println!("\n[3] Listing topics...");
    let topics = admin.list_topics().await.expect("Failed to list topics");
    println!("  {} user topics:", topics.len());
    for t in &topics {
        println!("    '{}': {} partitions", t.name, t.partitions);
    }

    // ── Create a topic ──
    let topic_name = "admin-example-topic";
    println!(
        "\n[4] Creating topic '{}' (3 partitions, rf=1)...",
        topic_name
    );
    let result = admin
        .create_topic(&NewTopic::new(topic_name, 3, 1))
        .await
        .expect("Failed to create topic");

    match result.error_code {
        0 => println!("  Created successfully"),
        36 => println!("  Already exists"),
        code => eprintln!("  Failed with error code {}", code),
    }

    // ── Describe the topic ──
    println!("\n[5] Describing topic '{}'...", topic_name);
    let desc = admin
        .describe_topics(&[topic_name])
        .await
        .expect("Failed to describe topic");

    if let Some(t) = desc.first() {
        println!("  Partitions: {}", t.partitions.len());
        for p in &t.partitions {
            println!(
                "    Partition {}: leader={}, replicas={:?}",
                p.partition, p.leader_id, p.replicas
            );
        }
    } else {
        println!("  Topic not found in metadata");
    }

    // ── Delete the topic (cleanup) ──
    println!("\n[6] Deleting topic '{}'...", topic_name);
    let result = admin
        .delete_topic(topic_name)
        .await
        .expect("Failed to delete topic");

    if result.is_success() {
        println!("  Deleted successfully");
    } else {
        eprintln!("  Delete failed: error_code={}", result.error_code);
    }

    // ── Clean shutdown ──
    println!("\n[7] Shutting down...");
    if let Err(e) = client.close().await {
        eprintln!("WARNING: Shutdown error: {}", e);
    }
    println!("Done.");
}
