//! WriteShareGroupState API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 85

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// WriteShareGroupStateRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 85, valid_versions = "0-1", flexible_versions = "0+")]
pub struct WriteShareGroupStateRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<WriteShareGroupStateRequestWriteStateData>,
}


/// WriteShareGroupStateRequestWriteStateData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct WriteShareGroupStateRequestWriteStateData {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<WriteShareGroupStateRequestPartitionData>,
}

/// WriteShareGroupStateRequestPartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct WriteShareGroupStateRequestPartitionData {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub state_epoch: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub start_offset: i64,
    #[kafka(versions = "1+")]
    pub delivery_complete_count: i32,
    #[kafka(versions = "0+")]
    pub state_batches: Vec<WriteShareGroupStateRequestStateBatch>,
}

/// WriteShareGroupStateRequestStateBatch
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct WriteShareGroupStateRequestStateBatch {
    #[kafka(versions = "0+")]
    pub first_offset: i64,
    #[kafka(versions = "0+")]
    pub last_offset: i64,
    #[kafka(versions = "0+")]
    pub delivery_state: i8,
    #[kafka(versions = "0+")]
    pub delivery_count: i16,
}
/// WriteShareGroupStateResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 85, valid_versions = "0-1", flexible_versions = "0+")]
pub struct WriteShareGroupStateResponse {
    #[kafka(versions = "0+")]
    pub results: Vec<WriteShareGroupStateResponseWriteStateResult>,
}


/// WriteShareGroupStateResponseWriteStateResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct WriteShareGroupStateResponseWriteStateResult {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<WriteShareGroupStateResponsePartitionResult>,
}

/// WriteShareGroupStateResponsePartitionResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct WriteShareGroupStateResponsePartitionResult {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
