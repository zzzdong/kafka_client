//! 消费者组分配与 Rebalance 测试
//!
//! 验证消费者组的分区分配和多消费者 Rebalance 行为。
//! 需要 3-broker 集群（consumer group 需要 coordinator）。
//!
//! 单独运行:
//!   cargo test --test consumer_group --features integration_tests -- --nocapture
//!
//! 通过 run-all-tests.sh 批量运行:
//!   KAFKA_BOOTSTRAP=... ./run-all-tests.sh

#![cfg(feature = "integration_tests")]

mod common;

use common::compose;
use common::{build_test_client, consumer_config};
use kafka_client::AutoOffsetReset;
use std::time::Duration;
use tokio::time::sleep;

/// 确保集群已就绪
async fn setup() {
    compose::ensure(&compose::clusters::THREE_BROKER).await;
}

/// Wait for a consumer to receive partition assignments.
async fn wait_for_assignment(
    consumer: &kafka_client::Consumer,
    timeout: Duration,
) -> std::collections::HashMap<String, Vec<i32>> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let a = consumer.group().assignment().await;
        let total: usize = a.values().map(|v| v.len()).sum();
        if total > 0 {
            return a;
        }
        if std::time::Instant::now() > deadline {
            panic!("Consumer failed to join group within {:?}", timeout);
        }
        sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::test]
async fn test_consumer_group_assignment() {
    setup().await;
    let client = build_test_client().await;

    common::create_topic(&client, "tc-group", 3).await;

    // 消费者加入组，验证获得分区分配
    let c1_client = build_test_client().await;
    let mut c1 = c1_client.consumer(consumer_config("cg-group-test", AutoOffsetReset::Earliest));
    c1.subscribe(vec!["tc-group".to_string()]).await.unwrap();

    // 等待组加入
    let assignment = wait_for_assignment(&c1, Duration::from_secs(30)).await;

    println!("  Consumer assignment: {:?}", assignment);
    let total: usize = assignment.values().map(|v| v.len()).sum();
    assert!(total > 0, "Consumer should have partitions assigned");
    assert_eq!(total, 3, "Should have all 3 partitions");
}

#[tokio::test]
async fn test_consumer_group_rebalance() {
    setup().await;
    let client = build_test_client().await;

    // 创建 6 分区主题，便于两个消费者平均分配
    common::create_topic(&client, "tc-rebalance", 6).await;

    let group_id = "cg-rebalance-test";

    // 消费者 1 加入组
    let c1_client = build_test_client().await;
    let mut c1 = c1_client.consumer(consumer_config(group_id, AutoOffsetReset::Earliest));
    c1.subscribe(vec!["tc-rebalance".to_string()])
        .await
        .unwrap();

    let a1 = wait_for_assignment(&c1, Duration::from_secs(30)).await;
    let total1: usize = a1.values().map(|v| v.len()).sum();
    println!("  Consumer 1 alone: {} partitions", total1);
    assert_eq!(total1, 6, "Single consumer should get all 6 partitions");

    // 消费者 2 加入同一个组 → 触发 Rebalance
    let c2_client = build_test_client().await;
    let mut c2 = c2_client.consumer(consumer_config(group_id, AutoOffsetReset::Earliest));
    c2.subscribe(vec!["tc-rebalance".to_string()])
        .await
        .unwrap();

    let a2 = wait_for_assignment(&c2, Duration::from_secs(30)).await;
    let total2: usize = a2.values().map(|v| v.len()).sum();
    println!("  Consumer 2 joined: {} partitions", total2);
    assert!(total2 > 0, "Consumer 2 should get at least 1 partition");

    // 消费者 1 的分配应发生变化（rebalance 后减少）
    sleep(Duration::from_secs(3)).await;
    let a1_after = c1.group().assignment().await;
    let total1_after: usize = a1_after.values().map(|v| v.len()).sum();
    println!("  Consumer 1 after rebalance: {} partitions", total1_after);

    // 两个消费者的分区总和应为 6
    assert_eq!(
        total1_after + total2,
        6,
        "Partition total after rebalance should be 6 (c1={} + c2={})",
        total1_after,
        total2
    );

    // 验证两个消费者的分区没有重叠
    let partitions_c1: std::collections::HashSet<i32> =
        a1_after.values().flat_map(|v| v.iter()).copied().collect();
    let partitions_c2: std::collections::HashSet<i32> =
        a2.values().flat_map(|v| v.iter()).copied().collect();
    let intersection: Vec<_> = partitions_c1.intersection(&partitions_c2).collect();
    assert!(
        intersection.is_empty(),
        "Partitions should not overlap after rebalance: {:?}",
        intersection
    );

    println!(
        "  Rebalance test PASSED: c1={} partitions, c2={} partitions, no overlap",
        total1_after, total2
    );
}
