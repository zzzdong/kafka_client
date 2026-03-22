//! InitializeShareGroupState API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 83

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// InitializeShareGroupStateRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 83, valid_versions = "0", flexible_versions = "0+")]
pub struct InitializeShareGroupStateRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<InitializeShareGroupStateRequestInitializeStateData>,
}


/// InitializeShareGroupStateRequestInitializeStateData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct InitializeShareGroupStateRequestInitializeStateData {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<InitializeShareGroupStateRequestPartitionData>,
}

/// InitializeShareGroupStateRequestPartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct InitializeShareGroupStateRequestPartitionData {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub state_epoch: i32,
    #[kafka(versions = "0+")]
    pub start_offset: i64,
}
/// InitializeShareGroupStateResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 83, valid_versions = "0", flexible_versions = "0+")]
pub struct InitializeShareGroupStateResponse {
    #[kafka(versions = "0+")]
    pub results: Vec<InitializeShareGroupStateResponseInitializeStateResult>,
}


/// InitializeShareGroupStateResponseInitializeStateResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct InitializeShareGroupStateResponseInitializeStateResult {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<InitializeShareGroupStateResponsePartitionResult>,
}

/// InitializeShareGroupStateResponsePartitionResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct InitializeShareGroupStateResponsePartitionResult {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
