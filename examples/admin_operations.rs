//! Admin API example
//!
//! Demonstrates administrative operations on Kafka cluster:
//! - Create topics
//! - Delete topics
//! - List topics
//! - Describe topic configurations
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

use kafka_client::KafkaClient;
use kafka_client::protocol::create_topics_request::CreatableTopic;
use kafka_client::protocol::{CreateTopicsRequest, CreateTopicsResponse};
use kafka_client::protocol::delete_topics_request::{DeleteTopicsRequest, DeleteTopicState};
use std::net::SocketAddr;
use std::time::Duration;

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
    println!("=== Admin Operations Example ===");
    println!("Bootstrap: {}", addr);
    println!();

    // Connect to Kafka
    println!("[1] Connecting to Kafka...");
    let client = match KafkaClient::builder(vec![addr])
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

    // List existing topics
    println!("\n[2] Listing existing topics...");
    client.cluster().refresh_metadata().await.expect("Failed to refresh metadata");
    let topics = client.metadata().get_all_topics().await;
    println!("Found {} topics:", topics.len());
    for t in &topics {
        if let Some(name) = &t.name {
            println!("  '{}': {} partitions", name, t.partitions.len());
        }
    }

    // Create a new topic
    let topic_name = "admin-example-topic";
    println!("\n[3] Creating topic '{}'...", topic_name);

    let create_req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic_name.to_string(),
            num_partitions: 3,
            replication_factor: 1,
            assignments: vec![],
            configs: vec![],
        }],
        timeout_ms: 10000,
        validate_only: false,
    };

    let resp: CreateTopicsResponse = match client.cluster().send_to_any_broker(&create_req).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("ERROR: Failed to create topic: {}", e);
            std::process::exit(1);
        }
    };

    for t in &resp.topics {
        match t.error_code {
            0 => println!("  Topic '{}' created successfully", t.name),
            36 => println!("  Topic '{}' already exists", t.name),
            code => eprintln!("  ERROR: Topic '{}' failed with error code {}", t.name, code),
        }
    }

    // Wait for topic to be ready
    println!("\n[4] Waiting for topic metadata...");
    tokio::time::sleep(Duration::from_secs(2)).await;
    client.cluster().refresh_metadata().await.expect("Failed to refresh metadata");

    if let Some(tm) = client.metadata().get_topic(topic_name).await {
        println!("Topic '{}' is ready:", topic_name);
        for p in &tm.partitions {
            println!("  Partition {} → Leader {}", p.partition_index, p.leader_id);
        }
    } else {
        eprintln!("WARNING: Topic '{}' not found in metadata", topic_name);
    }

    // Delete the topic (cleanup)
    println!("\n[5] Deleting topic '{}'...", topic_name);
    let delete_req = DeleteTopicsRequest {
        topics: vec![DeleteTopicState {
            name: Some(topic_name.to_string()),
            topic_id: uuid::Uuid::nil(),
        }],
        topic_names: Some(vec![topic_name.to_string()]),  // For older versions
        timeout_ms: 10000,
    };

    match client.cluster().send_to_any_broker::<_, kafka_client::protocol::DeleteTopicsResponse>(&delete_req).await {
        Ok(resp) => {
            for t in &resp.responses {
                match t.error_code {
                    0 => println!("  Topic '{}' deleted successfully", t.name.as_deref().unwrap_or_default()),
                    code => eprintln!("  ERROR: Delete failed with error code {}", code),
                }
            }
        }
        Err(e) => {
            eprintln!("ERROR: Failed to delete topic: {}", e);
        }
    }

    // Clean shutdown
    println!("\n[6] Shutting down...");
    if let Err(e) = client.close().await {
        eprintln!("WARNING: Shutdown error: {}", e);
    }
    println!("Done.");
}