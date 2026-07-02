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
use kafka_client::{Client, ConsumerConfig, ProducerConfig, ProducerRecord, admin::NewTopic};
use std::time::Duration;

fn get_bootstrap_addrs() -> Vec<String> {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    bootstrap.split(',').map(|s| s.trim().to_string()).collect()
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
    let client = match Client::builder(addrs)
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
    let cluster_info = client.admin().describe_cluster().await.unwrap();
    let rf = (3).min(cluster_info.brokers.len()).max(1) as i16;
    println!("\n[2] Creating topic '{}' (rf={})...", topic, rf);
    let result = client
        .admin()
        .create_topic(&NewTopic::new(&topic, 3, rf))
        .await
        .unwrap();
    use kafka_client::KafkaErrorCode;
    match result.error_code {
        KafkaErrorCode::NONE => println!("Topic '{}' created", topic),
        KafkaErrorCode::TOPIC_ALREADY_EXISTS => println!("Topic '{}' already exists", topic),
        code => {
            eprintln!("ERROR: Topic creation failed: {}", code);
            std::process::exit(1);
        }
    }

    // Wait for metadata to propagate
    println!("\n[3] Waiting for topic metadata...");
    tokio::time::sleep(Duration::from_secs(2)).await;
    client
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
    let producer_config = ProducerConfig::new();

    let producer = client.producer(producer_config).await;

    // Consume messages — start before producing
    println!("\n[4] Consuming messages...");
    let consumer_config = ConsumerConfig::new().with_group_id("example-consumer-group");

    let mut consumer = client.consumer(consumer_config);

    match consumer.subscribe(vec![topic.clone()]).await {
        Ok(_) => println!("Subscribed to topic '{}'", topic),
        Err(e) => {
            eprintln!("ERROR: Failed to subscribe: {}", e);
            std::process::exit(1);
        }
    }

    // Start consumer stream — background polling automatically
    let mut stream = consumer.into_stream();

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

    // Drain remaining records from stream with a timeout
    println!("\nConsuming remaining records...");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut count = 0usize;
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(2000), stream.recv()).await {
            Ok(Some(r)) => {
                println!(
                    "  Received: partition={}, offset={}, key={:?}, value={}",
                    r.partition,
                    r.offset,
                    r.key
                        .as_ref()
                        .map(|k| String::from_utf8_lossy(k).to_string()),
                    String::from_utf8_lossy(&r.value)
                );
                count += 1;
            }
            Ok(None) => break,
            Err(_) => {
                if count >= 3 {
                    break; // received enough
                }
            }
        }
    }
    println!("Consumed {} messages", count);

    // Clean shutdown
    println!("\n[6] Shutting down...");
    if let Err(e) = client.close().await {
        eprintln!("WARNING: Shutdown error: {}", e);
    }
    println!("Done.");
}
