use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::cluster::ClusterClient;
use crate::consumer::config::ConsumerConfig;
use crate::consumer::reactor::spawn_reactor;
use crate::consumer::types::ConsumerCommand;
use crate::consumer::types::ConsumerRecord;
use crate::error::{KafkaError, Result};

// ===========================================================================
// OffsetHandle — offset management
// ===========================================================================
pub struct OffsetHandle {
    pub(crate) cmd_tx: mpsc::UnboundedSender<ConsumerCommand>,
}

impl OffsetHandle {
    pub async fn commit(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ConsumerCommand::Commit { reply: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }
    pub async fn get(&self, topic: &str, partition: i32) -> Option<i64> {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(ConsumerCommand::GetOffset {
            topic: topic.to_string(),
            partition,
            reply: tx,
        });
        rx.await.ok().flatten()
    }
    pub async fn set(&self, topic: &str, partition: i32, offset: i64) {
        let _ = self.cmd_tx.send(ConsumerCommand::SetOffset {
            topic: topic.to_string(),
            partition,
            offset,
        });
    }
}

// ===========================================================================
// GroupHandle — group coordination
// ===========================================================================
pub struct GroupHandle {
    pub(crate) cmd_tx: mpsc::UnboundedSender<ConsumerCommand>,
}

impl GroupHandle {
    pub async fn heartbeat(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ConsumerCommand::Heartbeat { reply: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }
    pub async fn leave(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ConsumerCommand::Leave { reply: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }
    pub async fn assignment(&self) -> HashMap<String, Vec<i32>> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(ConsumerCommand::GetAssignment { reply: tx });
        rx.await.unwrap_or_default()
    }
    pub async fn commit(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ConsumerCommand::Commit { reply: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }
}

// ===========================================================================
// GroupConsumer — group coordination + reactor
// ===========================================================================
pub struct GroupConsumer {
    pub(crate) cmd_tx: mpsc::UnboundedSender<ConsumerCommand>,
    record_rx: mpsc::Receiver<Vec<ConsumerRecord>>,
    running: Arc<AtomicBool>,
    offset_handle: OffsetHandle,
    group_handle: GroupHandle,
}

impl GroupConsumer {
    pub(crate) fn new(cluster: Arc<ClusterClient>, config: ConsumerConfig) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (record_tx, record_rx) = mpsc::channel(64);
        let running = Arc::new(AtomicBool::new(true));

        spawn_reactor(cluster, config, cmd_rx, record_tx, running.clone());

        let cmd_tx_clone = cmd_tx.clone();
        Self {
            cmd_tx,
            record_rx,
            running,
            offset_handle: OffsetHandle {
                cmd_tx: cmd_tx_clone.clone(),
            },
            group_handle: GroupHandle {
                cmd_tx: cmd_tx_clone,
            },
        }
    }

    pub async fn subscribe(&self, topics: Vec<String>) -> Result<()> {
        self.cmd_tx
            .send(ConsumerCommand::Subscribe { topics })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        Ok(())
    }

    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        let batch = self.record_rx.recv().await.unwrap_or_default();
        let mut records = batch;
        while let Ok(batch) = self.record_rx.try_recv() {
            records.extend(batch);
        }
        let _ = self.cmd_tx.send(ConsumerCommand::TryFetch);
        Ok(records)
    }

    pub async fn poll_timeout(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        let mut records = Vec::new();
        while let Ok(batch) = self.record_rx.try_recv() {
            records.extend(batch);
        }
        if !records.is_empty() {
            let _ = self.cmd_tx.send(ConsumerCommand::TryFetch);
            return Ok(records);
        }
        match tokio::time::timeout(timeout, self.record_rx.recv()).await {
            Ok(Some(batch)) => {
                records = batch;
                while let Ok(batch) = self.record_rx.try_recv() {
                    records.extend(batch);
                }
                let _ = self.cmd_tx.send(ConsumerCommand::TryFetch);
                Ok(records)
            }
            Ok(None) | Err(_) => Ok(Vec::new()),
        }
    }

    pub async fn try_poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        let mut records = Vec::new();
        while let Ok(batch) = self.record_rx.try_recv() {
            records.extend(batch);
        }
        let _ = self.cmd_tx.send(ConsumerCommand::TryFetch);
        Ok(records)
    }

    pub async fn close(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        Ok(())
    }

    pub fn offsets(&self) -> &OffsetHandle {
        &self.offset_handle
    }

    pub fn group(&self) -> &GroupHandle {
        &self.group_handle
    }
}

impl Drop for GroupConsumer {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}
