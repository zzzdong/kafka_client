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
use tokio::sync::mpsc;

fn get_bootstrap_addrs() -> Vec<SocketAddr> {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:29093,127.0.0.1:29095,127.0.0.1:29097".to_string());
    bootstrap
        .split(',')
        .map(|s| s.trim().parse().expect("Invalid bootstrap address format. Expected: host:port"))
        .collect()
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

    let addrs = get_bootstrap_addrs();
    let topic = get_topic_name();
    println!("=== Produce-Consume Example ===");
    println!("Bootstrap: {:?}", addrs);
    println!("Topic: {}", topic);

    // Connect to Kafka
    println!("\n[1] Connecting to Kafka...");
    let client = match KafkaClient::builder(addrs)
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
            replication_factor: 3,
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
        auto_commit_interval: Duration::from_secs(1),
        auto_offset_reset: AutoOffsetReset::Earliest,
        min_bytes: 1,
        max_bytes: 1048576,
        partition_max_bytes: 1048576,
        max_wait: Duration::from_secs(5),
        session_timeout: Duration::from_secs(10),
        rebalance_timeout: Duration::from_secs(30),
        heartbeat_interval: Duration::from_secs(3),
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
    let (records_tx, mut records_rx) = mpsc::channel::<kafka_client::ConsumerRecord>(32);

    let poll_handle = tokio::spawn(async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut count = 0usize;
        while std::time::Instant::now() < deadline && count < 3 {
            match consumer.poll(2000).await {
                Ok(recs) => {
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
                        if records_tx.send(r).await.is_err() {
                            return;
                        }
                        count += 1;
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

    // Wait for consumer to finish and drain remaining records from channel
    poll_handle.await.ok();
    // sender 已被 task 退出时 drop，channel 会在 buffer 排空后返回 None
    let mut records = vec![];
    while let Some(r) = records_rx.recv().await {
        records.push(r);
    }
    println!("\nConsumed {} messages total", records.len());

    // Clean shutdown
    println!("\n[6] Shutting down...");
    if let Err(e) = client.close().await {
        eprintln!("WARNING: Shutdown error: {}", e);
    }
    println!("Done.");
}
