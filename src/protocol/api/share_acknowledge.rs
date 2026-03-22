//! ShareAcknowledge API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 79

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// ShareAcknowledgeRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 79, valid_versions = "1-2", flexible_versions = "0+")]
pub struct ShareAcknowledgeRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "0+")]
    pub share_session_epoch: i32,
    #[kafka(versions = "2+")]
    pub is_renew_ack: bool,
    #[kafka(versions = "0+")]
    pub topics: Vec<ShareAcknowledgeRequestAcknowledgeTopic>,
}


/// ShareAcknowledgeRequestAcknowledgeTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareAcknowledgeRequestAcknowledgeTopic {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<ShareAcknowledgeRequestAcknowledgePartition>,
}

/// ShareAcknowledgeRequestAcknowledgePartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareAcknowledgeRequestAcknowledgePartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub acknowledgement_batches: Vec<ShareAcknowledgeRequestAcknowledgementBatch>,
}

/// ShareAcknowledgeRequestAcknowledgementBatch
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareAcknowledgeRequestAcknowledgementBatch {
    #[kafka(versions = "0+")]
    pub first_offset: i64,
    #[kafka(versions = "0+")]
    pub last_offset: i64,
    #[kafka(versions = "0+")]
    pub acknowledge_types: Vec<i8>,
}
/// ShareAcknowledgeResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 79, valid_versions = "1-2", flexible_versions = "0+")]
pub struct ShareAcknowledgeResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "2+")]
    pub acquisition_lock_timeout_ms: i32,
    #[kafka(versions = "0+")]
    pub responses: Vec<ShareAcknowledgeResponseShareAcknowledgeTopicResponse>,
    #[kafka(versions = "0+")]
    pub node_endpoints: Vec<ShareAcknowledgeResponseNodeEndpoint>,
}


/// ShareAcknowledgeResponseShareAcknowledgeTopicResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareAcknowledgeResponseShareAcknowledgeTopicResponse {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<ShareAcknowledgeResponsePartitionData>,
}

/// ShareAcknowledgeResponsePartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareAcknowledgeResponsePartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub current_leader: ShareAcknowledgeResponseLeaderIdAndEpoch,
}

/// ShareAcknowledgeResponseLeaderIdAndEpoch
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareAcknowledgeResponseLeaderIdAndEpoch {
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}

/// ShareAcknowledgeResponseNodeEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareAcknowledgeResponseNodeEndpoint {
    #[kafka(versions = "0+")]
    pub node_id: i32,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: i32,
    #[kafka(versions = "0+")]
    pub rack: String,
}
