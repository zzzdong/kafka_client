//! JoinGroup API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 11

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// JoinGroupRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 11, valid_versions = "0-9", flexible_versions = "6+")]
pub struct JoinGroupRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub session_timeout_ms: i32,
    #[kafka(versions = "1+")]
    pub rebalance_timeout_ms: i32,
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "5+")]
    pub group_instance_id: String,
    #[kafka(versions = "0+")]
    pub protocol_type: String,
    #[kafka(versions = "0+")]
    pub protocols: Vec<JoinGroupRequestJoinGroupRequestProtocol>,
    #[kafka(versions = "8+")]
    pub reason: String,
}


/// JoinGroupRequestJoinGroupRequestProtocol
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct JoinGroupRequestJoinGroupRequestProtocol {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub metadata: Vec<u8>,
}
/// JoinGroupResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 11, valid_versions = "0-9", flexible_versions = "6+")]
pub struct JoinGroupResponse {
    #[kafka(versions = "2+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub generation_id: i32,
    #[kafka(versions = "7+")]
    pub protocol_type: String,
    #[kafka(versions = "0+")]
    pub protocol_name: String,
    #[kafka(versions = "0+")]
    pub leader: String,
    #[kafka(versions = "9+")]
    pub skip_assignment: bool,
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "0+")]
    pub members: Vec<JoinGroupResponseJoinGroupResponseMember>,
}


/// JoinGroupResponseJoinGroupResponseMember
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct JoinGroupResponseJoinGroupResponseMember {
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "5+")]
    pub group_instance_id: String,
    #[kafka(versions = "0+")]
    pub metadata: Vec<u8>,
}
