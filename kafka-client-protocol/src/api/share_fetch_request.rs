//! Auto-generated from Kafka protocol
//! Message: ShareFetchRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
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
pub struct FetchPartition {
    /// The partition index.
    #[kafka(versions = "0+", map_key)]
    pub partition_index: i32,
    /// The maximum bytes to fetch from this partition. 0 when only acknowledgement with no fetching is required. See KIP-74 for cases where this limit may not be honored.
    #[kafka(versions = "0")]
    pub partition_max_bytes: i32,
    /// Record batches to acknowledge.
    #[kafka(versions = "0+")]
    pub acknowledgement_batches: Vec<AcknowledgementBatch>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct FetchTopic {
    /// The unique topic ID.
    #[kafka(versions = "0+", map_key)]
    pub topic_id: Uuid,
    /// The partitions to fetch.
    #[kafka(versions = "0+")]
    pub partitions: Vec<FetchPartition>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ForgottenTopic {
    /// The unique topic ID.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The partitions indexes to forget.
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 78, msg_type = "request", valid_versions = "1-2", flexible_versions = "0+")]
pub struct ShareFetchRequest {
    /// The group identifier.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub group_id: Option<String>,
    /// The member ID.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub member_id: Option<String>,
    /// The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
    #[kafka(versions = "0+")]
    pub share_session_epoch: i32,
    /// The maximum time in milliseconds to wait for the response.
    #[kafka(versions = "0+")]
    pub max_wait_ms: i32,
    /// The minimum bytes to accumulate in the response.
    #[kafka(versions = "0+")]
    pub min_bytes: i32,
    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    #[kafka(versions = "0+", default = 0x7fffffff)]
    pub max_bytes: i32,
    /// The maximum number of records to fetch. This limit can be exceeded for alignment of batch boundaries.
    #[kafka(versions = "1+")]
    pub max_records: i32,
    /// The optimal number of records for batches of acquired records and acknowledgements.
    #[kafka(versions = "1+")]
    pub batch_size: i32,
    /// The acquire mode to control the fetch behavior - 0:batch-optimized,1:record-limit.
    #[kafka(versions = "2+", nullable_versions = "2+", default = 0)]
    pub share_acquire_mode: i8,
    /// Whether Renew type acknowledgements present in AcknowledgementBatches.
    #[kafka(versions = "2+", default = false)]
    pub is_renew_ack: bool,
    /// The topics to fetch.
    #[kafka(versions = "0+")]
    pub topics: Vec<FetchTopic>,
    /// The partitions to remove from this share session.
    #[kafka(versions = "0+")]
    pub forgotten_topics_data: Vec<ForgottenTopic>,
}

