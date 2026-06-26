//! 批量消息测试示例
//!
//! 验证 100 条消息的批量生产和消费。
//!
//! # Usage
//!
//! ```bash
//! # 需要先启动 Kafka，然后运行：
//! cargo run --example large_batch
//!
//! # 自定义 bootstrap 地址
//! KAFKA_BOOTSTRAP=127.0.0.1:29092 cargo run --example large_batch
//! ```

use bytes::Bytes;
use kafka_client::protocol::create_topics_request::CreatableTopic;
use kafka_client::protocol::{CreateTopicsRequest, CreateTopicsResponse};
use kafka_client::{ConsumerConfig, KafkaClient, ProducerConfig, ProducerRecord};
use std::net::SocketAddr;
use std::time::Duration;

fn get_bootstrap_addrs() -> Vec<SocketAddr> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP")
        .unwrap_or_else(|_| "127.0.0.1:29093,127.0.0.1:29095,127.0.0.1:29097".to_string());
    bootstrap
        .split(',')
        .map(|s| s.trim().parse().expect("Invalid bootstrap address"))
        .collect()
}

fn get_topic_name() -> String {
    std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "tc-large".to_string())
}

fn get_message_count() -> i32 {
    std::env::var("KAFKA_MESSAGE_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let addrs = get_bootstrap_addrs();
    let topic = get_topic_name();
    let msg_count = get_message_count();
    println!("=== Large Batch Example ===");
    println!("Bootstrap: {:?}", addrs);
    println!("Topic: {}", topic);
    println!("Message count: {}", msg_count);

    // 1. Connect
    println!("\n[1] Connecting to Kafka...");
    let client = match KafkaClient::builder(addrs)
        .with_client_id("large-batch-example")
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

    // 2. Create topic
    println!("\n[2] Creating topic '{}' (3 partitions)...", topic);
    let create_req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.clone(),
            num_partitions: 3,
            replication_factor: 3,
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
        if t.error_code != 0 && t.error_code != 36 {
            eprintln!(
                "ERROR: Topic creation failed: error_code={}, message={:?}",
                t.error_code, t.error_message
            );
            std::process::exit(1);
        }
        println!("  Topic '{}' ready (error_code={})", t.name, t.error_code);
    }

    // Wait for metadata
    tokio::time::sleep(Duration::from_secs(2)).await;
    client
        .cluster()
        .refresh_metadata()
        .await
        .expect("Failed to refresh metadata");

    // 3. Produce messages
    println!("\n[3] Producing {} messages to '{}'...", msg_count, topic);
    let producer_config = ProducerConfig::new().with_retries(5); // more retries for large batch resilience

    let producer = match client.producer(producer_config).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ERROR: Failed to create producer: {}", e);
            std::process::exit(1);
        }
    };

    // Batch all messages to avoid per-message network round-trips
    let records: Vec<ProducerRecord> = (0..msg_count)
        .map(|i| ProducerRecord::new(&topic, Bytes::from(format!("msg-{}", i))))
        .collect();
    let sent = producer
        .send_batch(records)
        .await
        .expect("Failed to buffer batch");
    if sent as i32 != msg_count {
        eprintln!(
            "ERROR: send_batch returned {}, expected {}",
            sent, msg_count
        );
        std::process::exit(1);
    }
    producer.flush().await.expect("Failed to flush producer");
    println!("  Produced {} messages", msg_count);

    // 4. Consume messages
    println!("\n[4] Consuming messages...");
    let consumer_config = ConsumerConfig::new("cg-large-example")
        .with_auto_commit_interval(Duration::from_secs(1))
        .with_earliest()
        .with_min_bytes(0)
        .with_max_bytes(1048576)
        .with_max_wait(Duration::from_secs(5));

    let mut consumer = client.consumer(consumer_config);
    match consumer.subscribe(vec![topic.clone()]).await {
        Ok(_) => println!("  Subscribed to '{}'", topic),
        Err(e) => {
            eprintln!("ERROR: Failed to subscribe: {}", e);
            std::process::exit(1);
        }
    }

    // Poll all messages (consumer auto-joins group on first poll)
    let mut all_records = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while all_records.len() < msg_count as usize && std::time::Instant::now() < deadline {
        match consumer.poll_timeout(Duration::from_millis(3000)).await {
            Ok(records) => {
                for r in &records {
                    println!(
                        "  Received: partition={}, offset={}, value={}",
                        r.partition,
                        r.offset,
                        String::from_utf8_lossy(&r.value)
                    );
                }
                all_records.extend(records);
            }
            Err(e) => eprintln!("  WARNING: Poll error: {}", e),
        }
    }

    println!(
        "\n[5] Result: consumed {} / {} messages",
        all_records.len(),
        msg_count
    );

    if all_records.len() as i32 >= msg_count {
        println!("SUCCESS: All messages consumed!");
    } else {
        eprintln!(
            "FAILURE: Expected at least {}, got {}",
            msg_count,
            all_records.len()
        );
    }

    // Clean shutdown
    println!("\n[6] Shutting down...");
    if let Err(e) = consumer.close().await {
        eprintln!("WARNING: Consumer close error: {}", e);
    }
    if let Err(e) = client.close().await {
        eprintln!("WARNING: Client close error: {}", e);
    }
    println!("Done.");
}
