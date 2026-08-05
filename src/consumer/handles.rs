use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

use crate::consumer::types::ConsumerCommand;
use crate::error::{KafkaError, Result};

// ===========================================================================
// OffsetHandle — offset management (shared between Direct and Group modes)
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
// GroupHandle — group coordination (only meaningful in Group mode)
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
    /// Current group generation (for transactional offset commits).
    pub async fn generation(&self) -> i32 {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(ConsumerCommand::GetGroupMetadata { reply: tx });
        rx.await.map(|(generation, _)| generation).unwrap_or(0)
    }
    /// Current member id (for transactional offset commits).
    pub async fn member_id(&self) -> String {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(ConsumerCommand::GetGroupMetadata { reply: tx });
        rx.await.map(|(_, member_id)| member_id).unwrap_or_default()
    }
    pub async fn commit(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ConsumerCommand::Commit { reply: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }
}
