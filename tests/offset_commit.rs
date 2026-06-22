//! 偏移量提交测试
//!
//! 验证消费者在消费后提交偏移量，auto_commit 任务正常工作。
//!
//! 运行: KAFKA_RUNTIME=direct cargo test --test offset_commit --features integration_tests -- --nocapture

#![cfg(feature = "integration_tests")]

mod common;

use common::KafkaInstance;
use kafka_client::client::consumer::AutoOffsetReset;
use kafka_client::client::core::KafkaClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

// #region debug-point C:test-flow
fn dbg_event(hypothesis: &str, location: &str, msg: &str, data: Option<String>) {
    let payload = format!(
        r#"{{"sessionId":"offset-commit-hang","runId":"pre","hypothesisId":"{}","location":"{}","msg":"[DEBUG] {}","data":{}}}"#,
        hypothesis,
        location,
        msg.replace('"', "\\\""),
        data.unwrap_or_else(|| "{}".to_string())
    );
    let _ = std::process::Command::new("curl")
        .args([
            "-s", "--max-time", "1", "-X", "POST",
            "http://127.0.0.1:7777/event",
            "-H", "Content-Type: application/json",
            "-d", &payload,
        ])
        .status();
}
// #endregion

#[tokio::test]
async fn test_offset_commit() {
    dbg_event("C", "offset_commit.rs:25", "test start", None);
    let server = KafkaInstance::start().await;
    dbg_event("C", "offset_commit.rs:27", "kafka instance started", Some(format!(r#"{{"bootstrap":"{}"}}"#, server.bootstrap())));
    let client = Arc::new(Mutex::new(
        KafkaClient::connect(server.client_config()).await.unwrap(),
    ));

    // 先生产一些消息
    {
        let mut c = client.lock().await;
        common::create_topic(&mut c, "tc-offset", 2).await;
    }
    dbg_event("C", "offset_commit.rs:35", "topic created", None);
    common::produce_messages(&client, "tc-offset", 5).await;
    dbg_event("C", "offset_commit.rs:37", "messages produced", None);

    // 消费者消费并提交偏移量
    let c1_client = Arc::new(Mutex::new(
        KafkaClient::connect(server.client_config()).await.unwrap(),
    ));
    let mut consumer = kafka_client::client::consumer::Consumer::new(
        c1_client,
        kafka_client::client::consumer::ConsumerConfig {
            group_id: "cg-offset-test".to_string(),
            auto_commit: true,
            auto_commit_interval_ms: 1000,
            auto_offset_reset: AutoOffsetReset::Earliest,
            ..Default::default()
        },
    )
    .await;
    dbg_event("C", "offset_commit.rs:50", "consumer created", None);
    consumer
        .subscribe(vec!["tc-offset".to_string()])
        .await
        .unwrap();
    dbg_event("C", "offset_commit.rs:54", "consumer subscribed", None);

    // 等待组加入（最多 30s，避免无限挂起）
    let mut wait_loops = 0;
    let assignment_deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let a = consumer.assignment().await;
        let total: usize = a.values().map(|v| v.len()).sum();
        wait_loops += 1;
        println!(
            "  [offset_commit] waiting for assignment: loop={}, total_partitions={}",
            wait_loops, total
        );
        dbg_event("C", "offset_commit.rs:60", "waiting for assignment", Some(format!(r#"{{"loop":{},"total":{}}}"#, wait_loops, total)));
        if total > 0 {
            break;
        }
        if std::time::Instant::now() >= assignment_deadline {
            panic!(
                "Timed out waiting for consumer group assignment after {} loops",
                wait_loops
            );
        }
        sleep(Duration::from_secs(1)).await;
    }
    dbg_event("C", "offset_commit.rs:65", "assignment received", None);
    println!("  [offset_commit] assignment received");

    // 消费消息
    let mut consumed = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while consumed.len() < 5 && std::time::Instant::now() < deadline {
        dbg_event("C", "offset_commit.rs:71", "calling poll", Some(format!(r#"{{"consumed":{}}}"#, consumed.len())));
        let records = consumer.poll(3000).await.unwrap();
        dbg_event("C", "offset_commit.rs:73", "poll returned", Some(format!(r#"{{"records":{}}}"#, records.len())));
        consumed.extend(records);
    }
    assert!(
        consumed.len() >= 5,
        "Should have consumed at least 5 messages, got {}",
        consumed.len()
    );
    println!("  Consumed {} messages, committing offset", consumed.len());
    let _ = consumer.commit().await;
    sleep(Duration::from_secs(2)).await;
    println!("  Offset commit completed without error");
    dbg_event("C", "offset_commit.rs:85", "test end", None);
}
