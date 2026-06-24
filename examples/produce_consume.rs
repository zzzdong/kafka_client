//! Produce and consume example
//!
//! Demonstrates a complete workflow: create topic → produce → consume.
//! Shows how to use Producer and Consumer with proper configuration.
//!
//! # Usage
//!
//! ```bash
//! # Default: connects to localhost:9092
//! cargo run --example produce_consume
//!
//! # Custom bootstrap server
//! KAFKA_BOOTSTRAP=192.168.1.100:9092 cargo run --example produce_consume
//!
//! # Custom topic name
//! KAFKA_TOPIC=my-topic cargo run --example produce_consume
//! ```

use bytes::Bytes;
use kafka_client::protocol::create_topics_request::CreatableTopic;
use kafka_client::protocol::{CreateTopicsRequest, CreateTopicsResponse};
use kafka_client::{
    AutoOffsetReset, ConsumerConfig, KafkaClient, PartitionAssignmentStrategy, ProducerConfig,
    ProducerRecord,
};
use std::net::SocketAddr;
use std::time::Duration;

fn get_bootstrap_addr() -> SocketAddr {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    bootstrap
        .parse()
        .expect("Invalid bootstrap address format. Expected: host:port")
}

fn get_topic_name() -> String {
    std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "example-topic".to_string())
}

#[tokio::main]
async fn main() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let addr = get_bootstrap_addr();
    let topic = get_topic_name();
    println!("=== Produce-Consume Example ===");
    println!("Bootstrap: {}", addr);
    println!("Topic: {}", topic);

    // Connect to Kafka
    println!("\n[1] Connecting to Kafka...");
    let client = match KafkaClient::builder(vec![addr])
        .with_client_id("produce-consume-example")
        .with_metadata_ttl(Duration::from_secs(10))
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

    // Create topic (if not exists)
    println!("\n[2] Creating topic '{}'...", topic);
    let create_req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.clone(),
            num_partitions: 3,
            replication_factor: 1,
            assignments: vec![],
            configs: vec![],
        }],
        timeout_ms: 5000,
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
        // error_code 36 = TOPIC_ALREADY_EXISTS (acceptable)
        if t.error_code != 0 && t.error_code != 36 {
            eprintln!(
                "ERROR: Topic creation failed with error code {}",
                t.error_code
            );
            std::process::exit(1);
        }
        println!("Topic '{}' created (error_code={})", t.name, t.error_code);
    }

    // Wait for metadata to propagate
    println!("\n[3] Waiting for topic metadata...");
    tokio::time::sleep(Duration::from_secs(2)).await;
    client
        .cluster()
        .refresh_metadata()
        .await
        .expect("Failed to refresh metadata");

    if let Some(tm) = client.metadata().get_topic(&topic).await {
        println!("Topic '{}' has {} partitions:", topic, tm.partitions.len());
        for p in &tm.partitions {
            println!("  Partition {} → Leader {}", p.partition_index, p.leader_id);
        }
    } else {
        eprintln!("WARNING: Topic '{}' not found in metadata", topic);
    }

    // Create producer config (will be used later)
    let producer_config = ProducerConfig {
        acks: 1,
        timeout_ms: 5000,
        retries: 3,
        batch_size: 16384,
        linger_ms: 50,
        ..Default::default()
    };

    let producer = match client.producer(producer_config).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ERROR: Failed to create producer: {}", e);
            std::process::exit(1);
        }
    };

    // Consume messages — start before producing
    println!("\n[4] Consuming messages...");
    let consumer_config = ConsumerConfig {
        group_id: "example-consumer-group".to_string(),
        auto_commit: true,
        auto_commit_interval_ms: 1000,
        auto_offset_reset: AutoOffsetReset::Earliest,
        min_bytes: 1,
        max_bytes: 1048576,
        partition_max_bytes: 1048576,
        max_wait_ms: 5000,
        session_timeout_ms: 45000,
        rebalance_timeout_ms: 60000,
        heartbeat_interval_ms: 3000,
        partition_assignment_strategy: PartitionAssignmentStrategy::Range,
    };

    let mut consumer = client.consumer(consumer_config);

    match consumer.subscribe(vec![topic.clone()]).await {
        Ok(_) => println!("Subscribed to topic '{}'", topic),
        Err(e) => {
            eprintln!("ERROR: Failed to subscribe: {}", e);
            std::process::exit(1);
        }
    }

    // Start polling in background before producing
    let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let poll_handle = tokio::spawn(async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while std::time::Instant::now() < deadline {
            match consumer.poll(2000).await {
                Ok(recs) => {
                    let mut guard = received_clone.lock().unwrap();
                    for r in recs {
                        println!(
                            "  Received: partition={}, offset={}, key={:?}, value={}",
                            r.partition,
                            r.offset,
                            r.key
                                .as_ref()
                                .map(|k| String::from_utf8_lossy(k).to_string()),
                            String::from_utf8_lossy(&r.value)
                        );
                        guard.push(r);
                    }
                    if guard.len() >= 3 {
                        break;
                    }
                }
                Err(e) => eprintln!("  WARNING: Poll error: {}", e),
            }
        }
    });

    // Wait for consumer group assignment
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Now produce — consumer is already listening
    println!("\n[5] Producing messages...");
    for i in 0..3 {
        let record = ProducerRecord::new(&topic, Bytes::from(format!("message-{}", i)))
            .with_key(Bytes::from(format!("key-{}", i)));

        match producer.send(record).await {
            Ok(meta) => println!(
                "  Sent to partition {} at offset {}",
                meta.partition, meta.offset
            ),
            Err(e) => eprintln!("  ERROR: Failed to send message {}: {}", i, e),
        }
    }

    producer.flush().await.expect("Failed to flush producer");
    println!("All messages flushed.");

    // Wait for consumer to finish
    poll_handle.await.ok();
    let records = received.lock().unwrap();
    println!("\nConsumed {} messages total", records.len());

    // Clean shutdown
    println!("\n[6] Shutting down...");
    if let Err(e) = client.close().await {
        eprintln!("WARNING: Shutdown error: {}", e);
    }
    println!("Done.");
}
