//! SyncGroup API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 14

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// SyncGroupRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 14, valid_versions = "0-5", flexible_versions = "4+")]
pub struct SyncGroupRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub generation_id: i32,
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "3+")]
    pub group_instance_id: String,
    #[kafka(versions = "5+")]
    pub protocol_type: String,
    #[kafka(versions = "5+")]
    pub protocol_name: String,
    #[kafka(versions = "0+")]
    pub assignments: Vec<SyncGroupRequestSyncGroupRequestAssignment>,
}


/// SyncGroupRequestSyncGroupRequestAssignment
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct SyncGroupRequestSyncGroupRequestAssignment {
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "0+")]
    pub assignment: Vec<u8>,
}
/// SyncGroupResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 14, valid_versions = "0-5", flexible_versions = "4+")]
pub struct SyncGroupResponse {
    #[kafka(versions = "1+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "5+")]
    pub protocol_type: String,
    #[kafka(versions = "5+")]
    pub protocol_name: String,
    #[kafka(versions = "0+")]
    pub assignment: Vec<u8>,
}

