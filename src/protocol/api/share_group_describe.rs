//! ShareGroupDescribe API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 77

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ShareGroupDescribeRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 77, valid_versions = "1", flexible_versions = "0+")]
pub struct ShareGroupDescribeRequest {
    #[kafka(versions = "0+")]
    pub group_ids: Vec<String>,
    #[kafka(versions = "0+")]
    pub include_authorized_operations: bool,
}

/// ShareGroupDescribeResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 77, valid_versions = "1", flexible_versions = "0+")]
pub struct ShareGroupDescribeResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub groups: Vec<ShareGroupDescribeResponseDescribedGroup>,
}


/// ShareGroupDescribeResponseDescribedGroup
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareGroupDescribeResponseDescribedGroup {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub group_state: String,
    #[kafka(versions = "0+")]
    pub group_epoch: i32,
    #[kafka(versions = "0+")]
    pub assignment_epoch: i32,
    #[kafka(versions = "0+")]
    pub assignor_name: String,
    #[kafka(versions = "0+")]
    pub members: Vec<ShareGroupDescribeResponseMember>,
    #[kafka(versions = "0+")]
    pub authorized_operations: i32,
}

/// ShareGroupDescribeResponseMember
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareGroupDescribeResponseMember {
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "0+")]
    pub rack_id: String,
    #[kafka(versions = "0+")]
    pub member_epoch: i32,
    #[kafka(versions = "0+")]
    pub client_id: String,
    #[kafka(versions = "0+")]
    pub client_host: String,
    #[kafka(versions = "0+")]
    pub subscribed_topic_names: Vec<String>,
    #[kafka(versions = "0+")]
    pub assignment: Assignment,
}

/// ShareGroupDescribeResponseTopicPartitions
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareGroupDescribeResponseTopicPartitions {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}

/// ShareGroupDescribeResponseAssignment
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareGroupDescribeResponseAssignment {
    #[kafka(versions = "0+")]
    pub topic_partitions: Vec<ShareGroupDescribeResponseTopicPartitions>,
}
