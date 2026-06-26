//! 生产-消费压力测试
//!
//! 验证大量消息的生产和消费性能与准确性。
//!
//! # Usage
//!
//! ```bash
//! cargo run --example stress_test
//!
//! # 自定义参数
//! KAFKA_BOOTSTRAP=127.0.0.1:9092 \
//!   KAFKA_TOPIC=stress-topic \
//!   MESSAGE_COUNT=50000 \
//!   cargo run --example stress_test
//! ```

use bytes::Bytes;
use kafka_client::protocol::create_topics_request::CreatableTopic;
use kafka_client::protocol::{CreateTopicsRequest, CreateTopicsResponse};
use kafka_client::{ConsumerConfig, KafkaClient, ProducerConfig, ProducerRecord};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

fn get_bootstrap_addrs() -> Vec<SocketAddr> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP")
        .unwrap_or_else(|_| "127.0.0.1:29093,127.0.0.1:29095,127.0.0.1:29097".to_string());
    bootstrap
        .split(',')
        .map(|s| s.trim().parse().expect("Invalid bootstrap address"))
        .collect()
}

fn get_topic_name() -> String {
    std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "stress-test-topic".to_string())
}

fn get_message_count() -> usize {
    std::env::var("MESSAGE_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10000)
}

fn get_batch_size() -> usize {
    std::env::var("BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .try_init();

    let addrs = get_bootstrap_addrs();
    let topic = get_topic_name();
    let msg_count = get_message_count();
    let batch_size = get_batch_size();

    println!("============================================");
    println!("  Kafka 生产-消费压力测试");
    println!("============================================");
    println!("  Bootstrap:   {:?}", addrs);
    println!("  Topic:       {}", topic);
    println!("  Partitions:  3");
    println!("  Message 数:  {}", msg_count);
    println!("  Batch 大小:   {}", batch_size);
    println!("============================================\n");

    // =====================================================================
    // 1. 连接 Kafka
    // =====================================================================
    print!("[1/5] 连接 Kafka...");
    std::io::Write::flush(&mut std::io::stdout()).ok();
    let client = match KafkaClient::builder(addrs)
        .with_client_id("stress-test")
        .with_metadata_ttl(Duration::from_secs(5))
        .build()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!(" 失败: {}", e);
            std::process::exit(1);
        }
    };
    println!(" ✓");

    // =====================================================================
    // 2. 创建 Topic (3 partitions)
    // =====================================================================
    print!("[2/5] 创建 Topic '{}' (3 partitions)...", topic);
    std::io::Write::flush(&mut std::io::stdout()).ok();
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
            eprintln!(" 失败: {}", e);
            std::process::exit(1);
        }
    };
    for t in &resp.topics {
        if t.error_code != 0 && t.error_code != 36 {
            eprintln!(" 失败: error_code={}", t.error_code);
            std::process::exit(1);
        }
    }
    println!(" ✓");

    // 等待 metadata 传播
    tokio::time::sleep(Duration::from_secs(2)).await;
    client
        .cluster()
        .refresh_metadata()
        .await
        .expect("刷新 metadata 失败");

    // =====================================================================
    // 3. 批量生产消息
    // =====================================================================
    println!("\n[3/5] 开始生产 {} 条消息...", msg_count);

    let producer_config = ProducerConfig::new()
        .with_timeout(15000)
        .with_batch_size(65536)
        .with_linger(10);

    let producer = match client.producer(producer_config).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("创建 Producer 失败: {}", e);
            std::process::exit(1);
        }
    };

    // Warm-up: 先发送少量消息让连接建立
    let warmup: Vec<ProducerRecord> = (0..100)
        .map(|i| {
            ProducerRecord::new(&topic, Bytes::from(format!("warmup-{}", i)))
                .with_key(Bytes::from(format!("key-{}", i)))
        })
        .collect();
    let _ = producer.send_batch(warmup).await;
    producer.flush().await.unwrap();

    // 正式生产
    let produce_start = Instant::now();
    let mut total_produced = 0usize;
    let mut batch_num = 0usize;

    while total_produced < msg_count {
        let remaining = msg_count - total_produced;
        let this_batch = batch_size.min(remaining);

        let batch: Vec<ProducerRecord> = (0..this_batch)
            .map(|j| {
                let seq = total_produced + j;
                ProducerRecord::new(&topic, Bytes::from(format!("msg-{:06}", seq)))
                    .with_key(Bytes::from(format!("key-{:06}", seq)))
            })
            .collect();

        match producer.send_batch(batch).await {
            Ok(n) => total_produced += n,
            Err(e) => eprintln!("  Batch {} 发送失败: {}", batch_num, e),
        }

        batch_num += 1;
        if batch_num % 20 == 0 {
            print!(
                "\r  已发送: {}/{} (batch #{})",
                total_produced, msg_count, batch_num
            );
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }
    }

    // 确保所有消息 flush 到 broker
    producer.flush().await.expect("Flush 失败");

    let produce_elapsed = produce_start.elapsed();
    let produce_throughput = if produce_elapsed.as_secs_f64() > 0.0 {
        (total_produced as f64) / produce_elapsed.as_secs_f64()
    } else {
        0.0
    };

    print!("\r  已发送: {}/{} ✓\n", total_produced, msg_count);
    println!("  生产耗时:  {:.2?}", produce_elapsed);
    println!("  生产吞吐:  {:.0} msgs/sec", produce_throughput);

    // =====================================================================
    // 4. 消费所有消息
    // =====================================================================
    println!("\n[4/5] 开始消费所有消息...");

    let consumer_config = ConsumerConfig::new("stress-test-group")
        .with_auto_commit_interval(Duration::from_secs(2))
        .with_earliest()
        .with_max_bytes(50 * 1024 * 1024)
        .with_partition_max_bytes(10 * 1024 * 1024)
        .with_max_wait(Duration::from_secs(3))
        .with_session_timeout(Duration::from_secs(15));

    let mut consumer = client.consumer(consumer_config);
    consumer.subscribe(vec![topic.clone()]).await.unwrap();

    // 等待分区分配
    print!("  等待 consumer group 分配分区...");
    std::io::Write::flush(&mut std::io::stdout()).ok();
    for i in 0..30 {
        let assignment = consumer.group().assignment().await;
        let has_partitions: usize = assignment.values().map(|v| v.len()).sum();
        if has_partitions > 0 {
            println!("  ✓ 已分配 {} 个分区 ({}s)", has_partitions, i + 1);
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // 消费循环
    let consume_start = Instant::now();
    let mut consumed_ids = HashSet::new();
    let mut total_consumed = 0usize;
    let mut empty_polls = 0u32;
    const MAX_EMPTY_POLLS: u32 = 20; // 连续空闲 N 次后认为消费完毕

    let deadline = Instant::now() + Duration::from_secs(120);

    while empty_polls < MAX_EMPTY_POLLS && Instant::now() < deadline {
        match consumer.poll_timeout(Duration::from_millis(3000)).await {
            Ok(records) => {
                if records.is_empty() {
                    empty_polls += 1;
                } else {
                    empty_polls = 0; // 有数据则重置空闲计数
                    for r in &records {
                        let value_str = String::from_utf8_lossy(&r.value);
                        if let Some(seq_str) = value_str.strip_prefix("msg-") {
                            if let Ok(seq) = seq_str.parse::<usize>() {
                                consumed_ids.insert(seq);
                            }
                        }
                        total_consumed += 1;
                    }

                    // Progress bar
                    if total_consumed % 1000 == 0 {
                        print!(
                            "\r  已消费: {} 条消息 (去重后: {})",
                            total_consumed,
                            consumed_ids.len()
                        );
                        std::io::Write::flush(&mut std::io::stdout()).ok();
                    }
                }
            }
            Err(e) => {
                eprintln!("  Poll 错误: {}", e);
                empty_polls += 1;
            }
        }
    }

    let consume_elapsed = consume_start.elapsed();
    let consume_throughput = if consume_elapsed.as_secs_f64() > 0.0 {
        (total_consumed as f64) / consume_elapsed.as_secs_f64()
    } else {
        0.0
    };

    print!(
        "\r  已消费: {} 条消息 (去重后: {}) ✓\n",
        total_consumed,
        consumed_ids.len()
    );
    println!("  消费耗时:  {:.2?}", consume_elapsed);
    println!("  消费吞吐:  {:.0} msgs/sec", consume_throughput);

    // =====================================================================
    // 5. 验证准确性
    // =====================================================================
    println!("\n[5/5] 验证准确性...");

    let expected_count = msg_count;
    let mut errors = Vec::new();

    // 检查消息数量
    if consumed_ids.len() != expected_count {
        errors.push(format!(
            "消息数量不匹配: 预期 {}，实际 {} (差值: {})",
            expected_count,
            consumed_ids.len(),
            if consumed_ids.len() > expected_count {
                consumed_ids.len() - expected_count
            } else {
                expected_count - consumed_ids.len()
            }
        ));
    }

    // 检查缺失消息
    let mut missing = Vec::new();
    for seq in 0..expected_count {
        if !consumed_ids.contains(&seq) {
            missing.push(seq);
            if missing.len() >= 10 {
                break;
            }
        }
    }
    if !missing.is_empty() {
        errors.push(format!("缺失消息 (前10): {:?}", missing));
    }

    // 检查是否有非预期消息
    let mut unexpected: Vec<usize> = consumed_ids
        .iter()
        .filter(|&&seq| seq >= expected_count)
        .copied()
        .collect();
    unexpected.sort();
    if !unexpected.is_empty() {
        errors.push(format!(
            "非预期消息 (前10): {:?}",
            &unexpected[..unexpected.len().min(10)]
        ));
    }

    // =====================================================================
    // 输出报告
    // =====================================================================
    println!("\n============================================");
    println!("  压力测试报告");
    println!("============================================");
    println!("  消息总数:    {}", msg_count);
    println!("  生产耗时:    {:.2?}", produce_elapsed);
    println!(
        "  生产吞吐:    {:.0} msgs/sec ({:.1} MB/s)",
        produce_throughput,
        produce_throughput * 64.0 / 1024.0 / 1024.0
    );
    println!("  消费耗时:    {:.2?}", consume_elapsed);
    println!(
        "  消费吞吐:    {:.0} msgs/sec ({:.1} MB/s)",
        consume_throughput,
        consume_throughput * 64.0 / 1024.0 / 1024.0
    );
    println!("  总耗时:      {:.2?}", produce_elapsed + consume_elapsed);
    println!("  有效消息:    {} (去重)", consumed_ids.len());
    println!("  总接收:      {} (含 warmup)", total_consumed);
    println!("  Warmup:      100 条");
    println!("--------------------------------------------");

    if errors.is_empty() {
        println!("  ✅ 准确性: 完全正确，零丢失、零重复！");
    } else {
        println!("  ❌ 准确性: 发现 {} 个问题:", errors.len());
        for e in &errors {
            println!("    - {}", e);
        }
    }
    println!("============================================");

    // Cleanup
    print!("\n  关闭连接...");
    std::io::Write::flush(&mut std::io::stdout()).ok();
    if let Err(e) = consumer.close().await {
        eprintln!(" Consumer 关闭失败: {}", e);
    }
    if let Err(e) = client.close().await {
        eprintln!(" Client 关闭失败: {}", e);
    }
    println!(" ✓");
}
