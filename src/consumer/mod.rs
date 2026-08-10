//! Consumer — high-level Kafka message consumer
//!
//! Architecture:
//!
//! ```text
//! Consumer (unified facade)
//!   └── cmd_tx → ConsumerTask (single tokio background task)
//!         ├── [group_id == ""] Direct mode: fetch all partitions from metadata
//!         ├── [group_id != ""] Group mode: join/sync/heartbeat/commit
//!         └── Shared FetchEngine:
//!               ├── by-broker parallel fetch requests
//!               ├── per-partition RecordBatchCursor buffer
//!               └── record_tx → Consumer::poll()
//! ```
//!
//! Both modes use the same fetch pipeline. The difference is only how
//! partition assignments and offsets are managed.

mod config;
mod group_coordinator;
mod handles;
mod reactor;
mod types;
mod util;

// Re-export public API types
pub use config::{AutoOffsetReset, ConsumerConfig, PartitionAssignmentStrategy};
pub use handles::{GroupHandle, OffsetHandle};
pub use types::ConsumerRecord;

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::cluster::ClusterClient;
use crate::consumer::reactor::spawn_consumer_task;
use crate::error::Result;

// ===========================================================================
// ConsumerStream — auto-closing streaming wrapper
// ===========================================================================

/// A streaming wrapper that yields individual [`ConsumerRecord`] values.
///
/// The consumer's background task is automatically shut down when dropped.
pub struct ConsumerStream {
    rx: mpsc::Receiver<Vec<ConsumerRecord>>,
    cmd_tx: mpsc::UnboundedSender<types::ConsumerCommand>,
    buffer: VecDeque<ConsumerRecord>,
}

impl ConsumerStream {
    /// Receive the next record.
    pub async fn recv(&mut self) -> Option<ConsumerRecord> {
        if let Some(record) = self.buffer.pop_front() {
            return Some(record);
        }
        let batch = self.rx.recv().await?;
        self.buffer.extend(batch);
        self.buffer.pop_front()
    }
}

impl Drop for ConsumerStream {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(types::ConsumerCommand::Shutdown);
    }
}

// ===========================================================================
// Consumer (unified facade)
// ===========================================================================
pub struct Consumer {
    cmd_tx: mpsc::UnboundedSender<types::ConsumerCommand>,
    record_rx: mpsc::Receiver<Vec<ConsumerRecord>>,
    offset_handle: handles::OffsetHandle,
    group_handle: handles::GroupHandle,
    is_group: bool,
    started: bool,
}

impl Consumer {
    pub(crate) fn new(cluster: Arc<ClusterClient>, config: ConsumerConfig) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (record_tx, record_rx) = mpsc::channel(64);

        let is_group = config.group_id.is_some();

        spawn_consumer_task(cluster, config, cmd_rx, record_tx);

        let cmd_tx_clone = cmd_tx.clone();
        Self {
            cmd_tx,
            record_rx,
            offset_handle: handles::OffsetHandle {
                cmd_tx: cmd_tx_clone.clone(),
            },
            group_handle: handles::GroupHandle {
                cmd_tx: cmd_tx_clone,
            },
            is_group,
            started: false,
        }
    }

    /// Subscribe to topics.
    ///
    /// In **group mode** (non-empty `group_id`), this triggers a consumer group
    /// join to receive partition assignments from the group coordinator.
    ///
    /// In **direct mode** (empty `group_id`), this fetches all partitions of
    /// the given topics from the cluster metadata and starts consuming them.
    ///
    /// This method blocks until partition assignment and offset initialization
    /// are complete. In group mode, this may take up to the rebalance timeout.
    pub async fn subscribe(&mut self, topics: Vec<String>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(types::ConsumerCommand::Subscribe {
                topics,
                reply: Some(tx),
            })
            .map_err(|_| crate::error::KafkaError::ConnectionClosed)?;
        rx.await
            .map_err(|_| crate::error::KafkaError::ConnectionClosed)?
    }

    /// Manually assign specific partitions to consume (direct mode only).
    ///
    /// This is the direct-mode equivalent of `subscribe()`, giving you
    /// fine-grained control over which topic-partitions to consume.
    ///
    /// This method blocks until offset initialization completes.
    pub async fn assign(&mut self, topic: impl Into<String>, partitions: Vec<i32>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(types::ConsumerCommand::Assign {
                topic: topic.into(),
                partitions,
                reply: Some(tx),
            })
            .map_err(|_| crate::error::KafkaError::ConnectionClosed)?;
        rx.await
            .map_err(|_| crate::error::KafkaError::ConnectionClosed)?
    }

    /// Seek to a specific offset for a partition (direct mode only).
    pub async fn seek(&self, topic: impl Into<String>, partition: i32, offset: i64) -> Result<()> {
        self.cmd_tx
            .send(types::ConsumerCommand::Seek {
                topic: topic.into(),
                partition,
                offset,
            })
            .map_err(|_| crate::error::KafkaError::ConnectionClosed)?;
        Ok(())
    }

    /// Unsubscribe from all topics.
    ///
    /// In **direct mode**, this clears all partition assignments and offsets.
    /// In **group mode**, this also sends a LeaveGroup request and stops the
    /// heartbeat loop.
    ///
    /// After `unsubscribe()`, you can call `subscribe()` or `assign()` again
    /// to start consuming different topics.
    pub async fn unsubscribe(&mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(types::ConsumerCommand::Unsubscribe { reply: tx })
            .map_err(|_| crate::error::KafkaError::ConnectionClosed)?;
        rx.await
            .map_err(|_| crate::error::KafkaError::ConnectionClosed)?
    }

    /// Block until records are available or the channel is closed.
    ///
    /// The first call signals the background task to start fetching (if not
    /// already started by `into_stream`). Subsequent calls continue reading
    /// from the prefetched buffer.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::KafkaError::ConnectionClosed`] if the background
    /// consumer task has terminated (e.g. due to a fatal error).
    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        if !self.started {
            self.cmd_tx
                .send(types::ConsumerCommand::StartPolling)
                .map_err(|_| crate::error::KafkaError::ConnectionClosed)?;
            self.started = true;
        }
        let batch = self
            .record_rx
            .recv()
            .await
            .ok_or(crate::error::KafkaError::ConnectionClosed)?;
        let mut records = batch;
        while let Ok(batch) = self.record_rx.try_recv() {
            records.extend(batch);
        }
        Ok(records)
    }

    /// Poll with a timeout.
    ///
    /// Returns an empty `Vec` on timeout, or
    /// [`crate::error::KafkaError::ConnectionClosed`] if the background consumer
    /// task has terminated.
    pub async fn poll_timeout(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        if !self.started {
            self.cmd_tx
                .send(types::ConsumerCommand::StartPolling)
                .map_err(|_| crate::error::KafkaError::ConnectionClosed)?;
            self.started = true;
        }
        let mut records = Vec::new();
        while let Ok(batch) = self.record_rx.try_recv() {
            records.extend(batch);
        }
        if !records.is_empty() {
            return Ok(records);
        }
        match tokio::time::timeout(timeout, self.record_rx.recv()).await {
            Ok(Some(batch)) => {
                records = batch;
                while let Ok(batch) = self.record_rx.try_recv() {
                    records.extend(batch);
                }
                Ok(records)
            }
            Ok(None) => Err(crate::error::KafkaError::ConnectionClosed),
            Err(_) => Ok(Vec::new()),
        }
    }

    /// Non-blocking poll: returns immediately with whatever is buffered.
    ///
    /// Returns an empty `Vec` if no data is currently buffered. Does NOT
    /// indicate a closed connection — use [`poll`](Self::poll) to
    /// distinguish between "no data" and "consumer terminated".
    pub async fn try_poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        if !self.started {
            self.cmd_tx
                .send(types::ConsumerCommand::StartPolling)
                .map_err(|_| crate::error::KafkaError::ConnectionClosed)?;
            self.started = true;
        }
        let mut records = Vec::new();
        while let Ok(batch) = self.record_rx.try_recv() {
            records.extend(batch);
        }
        Ok(records)
    }

    /// Close the consumer, shutting down the background task.
    pub async fn close(&self) -> Result<()> {
        let _ = self.cmd_tx.send(types::ConsumerCommand::Shutdown);
        Ok(())
    }

    /// Access offset management handle.
    pub fn offsets(&self) -> &OffsetHandle {
        &self.offset_handle
    }

    /// Access group coordination handle.
    pub fn group(&self) -> &GroupHandle {
        &self.group_handle
    }

    /// Whether this consumer is in group mode.
    pub fn is_group(&self) -> bool {
        self.is_group
    }

    /// Convert this consumer into a streaming interface that yields individual
    /// [`ConsumerRecord`] values, one at a time.
    ///
    /// The returned [`ConsumerStream`] shuts down the background task
    /// automatically when dropped.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut stream = consumer.into_stream();
    /// while let Some(record) = stream.recv().await {
    ///     println!("Got offset: {}", record.offset);
    /// }
    /// ```
    pub fn into_stream(mut self) -> ConsumerStream {
        if !self.started {
            let _ = self.cmd_tx.send(types::ConsumerCommand::StartPolling);
            self.started = true;
        }

        ConsumerStream {
            rx: self.record_rx,
            cmd_tx: self.cmd_tx.clone(),
            buffer: VecDeque::new(),
        }
    }
}
