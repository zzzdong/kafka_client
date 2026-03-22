//! ShareFetch API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 78

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// ShareFetchRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 78, valid_versions = "1-2", flexible_versions = "0+")]
pub struct ShareFetchRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "0+")]
    pub share_session_epoch: i32,
    #[kafka(versions = "0+")]
    pub max_wait_ms: i32,
    #[kafka(versions = "0+")]
    pub min_bytes: i32,
    #[kafka(versions = "0+")]
    pub max_bytes: i32,
    #[kafka(versions = "1+")]
    pub max_records: i32,
    #[kafka(versions = "1+")]
    pub batch_size: i32,
    #[kafka(versions = "2+")]
    pub share_acquire_mode: i8,
    #[kafka(versions = "2+")]
    pub is_renew_ack: bool,
    #[kafka(versions = "0+")]
    pub topics: Vec<ShareFetchRequestFetchTopic>,
    #[kafka(versions = "0+")]
    pub forgotten_topics_data: Vec<ShareFetchRequestForgottenTopic>,
}


/// ShareFetchRequestFetchTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareFetchRequestFetchTopic {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<ShareFetchRequestFetchPartition>,
}

/// ShareFetchRequestFetchPartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareFetchRequestFetchPartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0")]
    pub partition_max_bytes: i32,
    #[kafka(versions = "0+")]
    pub acknowledgement_batches: Vec<ShareFetchRequestAcknowledgementBatch>,
}

/// ShareFetchRequestAcknowledgementBatch
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareFetchRequestAcknowledgementBatch {
    #[kafka(versions = "0+")]
    pub first_offset: i64,
    #[kafka(versions = "0+")]
    pub last_offset: i64,
    #[kafka(versions = "0+")]
    pub acknowledge_types: Vec<i8>,
}

/// ShareFetchRequestForgottenTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareFetchRequestForgottenTopic {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}
/// ShareFetchResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 78, valid_versions = "1-2", flexible_versions = "0+")]
pub struct ShareFetchResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "1+")]
    pub acquisition_lock_timeout_ms: i32,
    #[kafka(versions = "0+")]
    pub responses: Vec<ShareFetchResponseShareFetchableTopicResponse>,
    #[kafka(versions = "0+")]
    pub node_endpoints: Vec<ShareFetchResponseNodeEndpoint>,
}


/// ShareFetchResponseShareFetchableTopicResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareFetchResponseShareFetchableTopicResponse {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<ShareFetchResponsePartitionData>,
}

/// ShareFetchResponsePartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareFetchResponsePartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub acknowledge_error_code: i16,
    #[kafka(versions = "0+")]
    pub acknowledge_error_message: String,
    #[kafka(versions = "0+")]
    pub current_leader: ShareFetchResponseLeaderIdAndEpoch,
    #[kafka(versions = "0+")]
    pub records: Vec<u8>,
    #[kafka(versions = "0+")]
    pub acquired_records: Vec<ShareFetchResponseAcquiredRecords>,
}

/// ShareFetchResponseLeaderIdAndEpoch
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareFetchResponseLeaderIdAndEpoch {
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}

/// ShareFetchResponseAcquiredRecords
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareFetchResponseAcquiredRecords {
    #[kafka(versions = "0+")]
    pub first_offset: i64,
    #[kafka(versions = "0+")]
    pub last_offset: i64,
    #[kafka(versions = "0+")]
    pub delivery_count: i16,
}

/// ShareFetchResponseNodeEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareFetchResponseNodeEndpoint {
    #[kafka(versions = "0+")]
    pub node_id: i32,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: i32,
    #[kafka(versions = "0+")]
    pub rack: String,
}
