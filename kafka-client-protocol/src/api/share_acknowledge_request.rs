//! Auto-generated from Kafka protocol
//! Message: ShareAcknowledgeRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AcknowledgementBatch {
    /// First offset of batch of records to acknowledge.
    #[kafka(versions = "0+")]
    pub first_offset: i64,
    /// Last offset (inclusive) of batch of records to acknowledge.
    #[kafka(versions = "0+")]
    pub last_offset: i64,
    /// Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
    #[kafka(versions = "0+")]
    pub acknowledge_types: Vec<i8>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AcknowledgePartition {
    /// The partition index.
    #[kafka(versions = "0+", map_key)]
    pub partition_index: i32,
    /// Record batches to acknowledge.
    #[kafka(versions = "0+")]
    pub acknowledgement_batches: Vec<AcknowledgementBatch>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AcknowledgeTopic {
    /// The unique topic ID.
    #[kafka(versions = "0+", map_key)]
    pub topic_id: Uuid,
    /// The partitions containing records to acknowledge.
    #[kafka(versions = "0+")]
    pub partitions: Vec<AcknowledgePartition>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 79,
    msg_type = "request",
    valid_versions = "1-2",
    flexible_versions = "0+"
)]
pub struct ShareAcknowledgeRequest {
    /// The group identifier.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub group_id: Option<String>,
    /// The member ID.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub member_id: Option<String>,
    /// The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
    #[kafka(versions = "0+")]
    pub share_session_epoch: i32,
    /// Whether Renew type acknowledgements present in AcknowledgementBatches.
    #[kafka(versions = "2+", default = false)]
    pub is_renew_ack: bool,
    /// The topics containing records to acknowledge.
    #[kafka(versions = "0+")]
    pub topics: Vec<AcknowledgeTopic>,
}
