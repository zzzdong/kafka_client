//! DescribeGroups API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 15

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DescribeGroupsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 15, valid_versions = "0-6", flexible_versions = "5+")]
pub struct DescribeGroupsRequest {
    #[kafka(versions = "0+")]
    pub groups: Vec<String>,
    #[kafka(versions = "3+")]
    pub include_authorized_operations: bool,
}

/// DescribeGroupsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 15, valid_versions = "0-6", flexible_versions = "5+")]
pub struct DescribeGroupsResponse {
    #[kafka(versions = "1+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub groups: Vec<DescribeGroupsResponseDescribedGroup>,
}


/// DescribeGroupsResponseDescribedGroup
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeGroupsResponseDescribedGroup {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "6+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub group_state: String,
    #[kafka(versions = "0+")]
    pub protocol_type: String,
    #[kafka(versions = "0+")]
    pub protocol_data: String,
    #[kafka(versions = "0+")]
    pub members: Vec<DescribeGroupsResponseDescribedGroupMember>,
    #[kafka(versions = "3+")]
    pub authorized_operations: i32,
}

/// DescribeGroupsResponseDescribedGroupMember
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeGroupsResponseDescribedGroupMember {
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "4+")]
    pub group_instance_id: String,
    #[kafka(versions = "0+")]
    pub client_id: String,
    #[kafka(versions = "0+")]
    pub client_host: String,
    #[kafka(versions = "0+")]
    pub member_metadata: Vec<u8>,
    #[kafka(versions = "0+")]
    pub member_assignment: Vec<u8>,
}
