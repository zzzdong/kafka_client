//! Auto-generated from Kafka protocol
//! Message: BrokerHeartbeatRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 63,
    msg_type = "request",
    valid_versions = "0-2",
    flexible_versions = "0+"
)]
pub struct BrokerHeartbeatRequest {
    /// The broker ID.
    #[kafka(versions = "0+")]
    pub broker_id: i32,
    /// The broker epoch.
    #[kafka(versions = "0+", default = -1)]
    pub broker_epoch: i64,
    /// The highest metadata offset which the broker has reached.
    #[kafka(versions = "0+")]
    pub current_metadata_offset: i64,
    /// True if the broker wants to be fenced, false otherwise.
    #[kafka(versions = "0+")]
    pub want_fence: bool,
    /// True if the broker wants to be shut down, false otherwise.
    #[kafka(versions = "0+")]
    pub want_shut_down: bool,
    /// Log directories that failed and went offline.
    #[kafka(versions = "1+", tag = 0, tagged_versions = "1+")]
    pub offline_log_dirs: Vec<Uuid>,
    /// List of log directories that are cordoned. This is null before the broker reaches the RECOVERY state.
    #[kafka(versions = "2+", nullable_versions = "2+", tag = 1, tagged_versions = "2+", default = None)]
    pub cordoned_log_dirs: Option<Vec<Uuid>>,
}
