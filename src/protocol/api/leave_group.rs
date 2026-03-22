//! LeaveGroup API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 13

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// LeaveGroupRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 13, valid_versions = "0-5", flexible_versions = "4+")]
pub struct LeaveGroupRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0-2")]
    pub member_id: String,
    #[kafka(versions = "3+")]
    pub members: Vec<LeaveGroupRequestMemberIdentity>,
}


/// LeaveGroupRequestMemberIdentity
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct LeaveGroupRequestMemberIdentity {
    #[kafka(versions = "3+")]
    pub member_id: String,
    #[kafka(versions = "3+")]
    pub group_instance_id: String,
    #[kafka(versions = "5+")]
    pub reason: String,
}
/// LeaveGroupResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 13, valid_versions = "0-5", flexible_versions = "4+")]
pub struct LeaveGroupResponse {
    #[kafka(versions = "1+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "3+")]
    pub members: Vec<LeaveGroupResponseMemberResponse>,
}


/// LeaveGroupResponseMemberResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct LeaveGroupResponseMemberResponse {
    #[kafka(versions = "3+")]
    pub member_id: String,
    #[kafka(versions = "3+")]
    pub group_instance_id: String,
    #[kafka(versions = "3+")]
    pub error_code: i16,
}
