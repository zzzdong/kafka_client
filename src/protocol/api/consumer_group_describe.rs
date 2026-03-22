//! ConsumerGroupDescribe API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 69

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ConsumerGroupDescribeRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 69, valid_versions = "0-1", flexible_versions = "0+")]
pub struct ConsumerGroupDescribeRequest {
    #[kafka(versions = "0+")]
    pub group_ids: Vec<String>,
    #[kafka(versions = "0+")]
    pub include_authorized_operations: bool,
}

/// ConsumerGroupDescribeResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 69, valid_versions = "0-1", flexible_versions = "0+")]
pub struct ConsumerGroupDescribeResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub groups: Vec<ConsumerGroupDescribeResponseDescribedGroup>,
}


/// ConsumerGroupDescribeResponseDescribedGroup
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ConsumerGroupDescribeResponseDescribedGroup {
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
    pub members: Vec<ConsumerGroupDescribeResponseMember>,
    #[kafka(versions = "0+")]
    pub authorized_operations: i32,
}

/// ConsumerGroupDescribeResponseMember
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ConsumerGroupDescribeResponseMember {
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "0+")]
    pub instance_id: String,
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
    pub subscribed_topic_regex: String,
    #[kafka(versions = "0+")]
    pub assignment: Assignment,
    #[kafka(versions = "0+")]
    pub target_assignment: Assignment,
    #[kafka(versions = "1+")]
    pub member_type: i8,
}

/// ConsumerGroupDescribeResponseTopicPartitions
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ConsumerGroupDescribeResponseTopicPartitions {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}

/// ConsumerGroupDescribeResponseAssignment
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ConsumerGroupDescribeResponseAssignment {
    #[kafka(versions = "0+")]
    pub topic_partitions: Vec<ConsumerGroupDescribeResponseTopicPartitions>,
}
