//! ReadShareGroupState API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 84

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// ReadShareGroupStateRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 84, valid_versions = "0", flexible_versions = "0+")]
pub struct ReadShareGroupStateRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<ReadShareGroupStateRequestReadStateData>,
}


/// ReadShareGroupStateRequestReadStateData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ReadShareGroupStateRequestReadStateData {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<ReadShareGroupStateRequestPartitionData>,
}

/// ReadShareGroupStateRequestPartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ReadShareGroupStateRequestPartitionData {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}
/// ReadShareGroupStateResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 84, valid_versions = "0", flexible_versions = "0+")]
pub struct ReadShareGroupStateResponse {
    #[kafka(versions = "0+")]
    pub results: Vec<ReadShareGroupStateResponseReadStateResult>,
}


/// ReadShareGroupStateResponseReadStateResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ReadShareGroupStateResponseReadStateResult {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<ReadShareGroupStateResponsePartitionResult>,
}

/// ReadShareGroupStateResponsePartitionResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ReadShareGroupStateResponsePartitionResult {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub state_epoch: i32,
    #[kafka(versions = "0+")]
    pub start_offset: i64,
    #[kafka(versions = "0+")]
    pub state_batches: Vec<ReadShareGroupStateResponseStateBatch>,
}

/// ReadShareGroupStateResponseStateBatch
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ReadShareGroupStateResponseStateBatch {
    #[kafka(versions = "0+")]
    pub first_offset: i64,
    #[kafka(versions = "0+")]
    pub last_offset: i64,
    #[kafka(versions = "0+")]
    pub delivery_state: i8,
    #[kafka(versions = "0+")]
    pub delivery_count: i16,
}
