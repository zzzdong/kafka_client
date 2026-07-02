//! Producer benchmark — all send modes
//!
//! Tests all three send semantics:
//!   send       (buffer + wait for ack)
//!   send_direct(direct + wait for ack)
//!   send_batch (batch buffer, no wait, explicit flush)
//!
//! # Usage
//!
//! ```bash
//! # Quick smoke test (100 msgs, batch mode)
//! cargo run --example producer_bench
//!
//! # Single-message send (buffer + ack)
//! SEND_MODE=send MESSAGE_COUNT=1000 cargo run --example producer_bench
//!
//! # Direct send (no buffer, each msg one RTT)
//! SEND_MODE=direct MESSAGE_COUNT=100 cargo run --example producer_bench
//!
//! # Batch send (buffer only, explicit flush)
//! SEND_MODE=batch MESSAGE_COUNT=100000 cargo run --example producer_bench
//! ```

use bytes::Bytes;
use kafka_client::KafkaErrorCode;
use kafka_client::{Client, ProducerConfig, ProducerRecord, admin::NewTopic};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendMode {
    /// buffer + wait for ack per message
    Send,
    /// direct (no buffer) + wait for ack per message
    Direct,
    /// batch buffer, no wait, outer flush
    Batch,
}

impl std::str::FromStr for SendMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "send" => Ok(SendMode::Send),
            "direct" => Ok(SendMode::Direct),
            "batch" => Ok(SendMode::Batch),
            _ => Err(format!("Unknown mode: {}. Use send/direct/batch", s)),
        }
    }
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_bootstrap(addrs: &str) -> Vec<String> {
    addrs.split(',').map(|s| s.trim().to_string()).collect()
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .try_init();

    let bootstrap = env_or("KAFKA_BOOTSTRAP", "127.0.0.1:9092");
    let topic = env_or("KAFKA_TOPIC", "benchmark-topic");
    let message_count: usize = env_or("MESSAGE_COUNT", "100")
        .parse()
        .expect("MESSAGE_COUNT");
    let message_size: usize = env_or("MESSAGE_SIZE", "100").parse().expect("MESSAGE_SIZE");
    let threads: usize = env_or("THREADS", "2").parse().expect("THREADS");
    let partition_count: i32 = env_or("PARTITIONS", "3").parse().expect("PARTITIONS");
    let batch_size: usize = env_or("BATCH_SIZE", "16384").parse().expect("BATCH_SIZE");
    let linger_ms: u64 = env_or("LINGER_MS", "10").parse().expect("LINGER_MS");
    let acks: i16 = env_or("ACKS", "1").parse().expect("ACKS");
    let mode: SendMode = env_or("SEND_MODE", "batch").parse().expect("SEND_MODE");

    println!("=== Producer Benchmark ===");
    println!("  Mode:          {:?}", mode);
    println!("  Bootstrap:     {}", bootstrap);
    println!("  Topic:         {}", topic);
    println!("  Partitions:    {}", partition_count);
    println!("  Messages:      {}", message_count);
    println!("  Message size:  {} B", message_size);
    println!("  Threads:       {}", threads);
    println!("  Batch size:    {} B", batch_size);
    println!("  Linger:        {} ms", linger_ms);
    println!("  Acks:          {}", acks);

    // ── Connect ──
    println!("\n[1] Connecting...");
    let addrs = parse_bootstrap(&bootstrap);
    let client = Client::builder(addrs)
        .with_client_id("producer-bench")
        .build()
        .await
        .expect("connect");

    // ── Topic ──
    println!("\n[2] Topic '{}'...", topic);
    let admin = client.admin();
    let cluster_info = admin.describe_cluster().await.unwrap();
    let rf = (3).min(cluster_info.brokers.len()).max(1) as i16;
    let _ = admin.delete_topic(&topic).await;
    let r = admin
        .create_topic(&NewTopic::new(&topic, partition_count, rf))
        .await
        .expect("create_topic");
    match r.error_code {
        KafkaErrorCode::NONE => println!("  Created"),
        KafkaErrorCode::TOPIC_ALREADY_EXISTS => println!("  Exists"),
        code => panic!("create topic: {}", code),
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    client.refresh_metadata().await.expect("refresh");

    // ── Producer ──
    println!("\n[3] Producer (mode={:?})...", mode);
    let producer = Arc::new(
        client
            .producer(
                ProducerConfig::new()
                    .with_acks(acks)
                    .with_batch_size(batch_size)
                    .with_linger(linger_ms),
            )
            .await,
    );

    // ── Payload ──
    let payload = Bytes::from(vec![b'x'; message_size]);

    // ── Send ──
    println!(
        "\n[4] Sending {} msgs, {} threads...",
        message_count, threads
    );
    let start = Instant::now();
    let fail = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(threads + 1));

    let per = message_count / threads;
    let rem = message_count % threads;

    // Progress
    let prog = tokio::spawn(async move {
        for s in 1..=300 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            print!("\r  ... {}s ...", s);
            use std::io::Write;
            std::io::stdout().flush().ok();
        }
    });

    let mut handles = Vec::with_capacity(threads);
    for t in 0..threads {
        let n = if t == 0 { per + rem } else { per };
        let p = Arc::clone(&producer);
        let pl = payload.clone();
        let tp = topic.clone();
        let f = Arc::clone(&fail);
        let b = Arc::clone(&barrier);
        let m = mode;

        handles.push(tokio::spawn(async move {
            b.wait().await;
            match m {
                SendMode::Send => {
                    for i in 0..n {
                        let rec = ProducerRecord::new(&tp, pl.clone())
                            .with_partition((i as i32) % partition_count);
                        if let Err(e) = p.send(rec).await {
                            f.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if f.load(std::sync::atomic::Ordering::Relaxed) <= 3 {
                                eprintln!("\n  send error[{}]: {}", i, e);
                            }
                        }
                    }
                }
                SendMode::Direct => {
                    for i in 0..n {
                        let rec = ProducerRecord::new(&tp, pl.clone())
                            .with_partition((i as i32) % partition_count);
                        if let Err(e) = p.send_direct(rec).await {
                            f.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if f.load(std::sync::atomic::Ordering::Relaxed) <= 3 {
                                eprintln!("\n  direct error[{}]: {}", i, e);
                            }
                        }
                    }
                }
                SendMode::Batch => {
                    let mut batch = Vec::with_capacity(n);
                    for i in 0..n {
                        batch.push(
                            ProducerRecord::new(&tp, pl.clone())
                                .with_partition((i as i32) % partition_count),
                        );
                    }
                    match p.send_batch(batch).await {
                        Ok(c) => {
                            if c != n {
                                f.fetch_add((n - c) as u64, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            f.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
                            eprintln!("\n  batch error: {}", e);
                        }
                    }
                }
            }
        }));
    }

    barrier.wait().await;
    for h in handles {
        h.await.expect("task");
    }
    prog.abort();

    // Only batch mode needs explicit flush
    if mode == SendMode::Batch {
        print!("  Flushing...");
        use std::io::Write;
        std::io::stdout().flush().ok();
        tokio::time::timeout(Duration::from_secs(30), producer.flush())
            .await
            .expect("flush timeout")
            .expect("flush");
        println!(" done");
    }

    let elapsed = start.elapsed();
    let failures = fail.load(std::sync::atomic::Ordering::Relaxed);
    let success = message_count - failures as usize;
    let tput = if elapsed.as_secs_f64() > 0.0 {
        success as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("\n=== Results ({:?}) ===", mode);
    println!("  Duration:  {:.2?}", elapsed);
    println!("  Success:   {} / {}", success, message_count);
    println!("  Failures:  {}", failures);
    println!("  Throughput: {:.0} msgs/sec", tput);
    println!(
        "  Throughput: {:.1} MB/s",
        tput * message_size as f64 / 1024.0 / 1024.0
    );

    println!("\n[5] Shutdown...");
    client.close().await.expect("close");
    println!("Done.");
}
