//! StreamsGroupDescribe API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 89

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// StreamsGroupDescribeRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 89, valid_versions = "0", flexible_versions = "0+")]
pub struct StreamsGroupDescribeRequest {
    #[kafka(versions = "0+")]
    pub group_ids: Vec<String>,
    #[kafka(versions = "0+")]
    pub include_authorized_operations: bool,
}

/// StreamsGroupDescribeResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 89, valid_versions = "0", flexible_versions = "0+")]
pub struct StreamsGroupDescribeResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub groups: Vec<StreamsGroupDescribeResponseDescribedGroup>,
}


/// StreamsGroupDescribeResponseDescribedGroup
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupDescribeResponseDescribedGroup {
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
    pub topology: StreamsGroupDescribeResponseTopology,
    #[kafka(versions = "0+")]
    pub members: Vec<StreamsGroupDescribeResponseMember>,
    #[kafka(versions = "0+")]
    pub authorized_operations: i32,
}

/// StreamsGroupDescribeResponseTopology
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupDescribeResponseTopology {
    #[kafka(versions = "0+")]
    pub epoch: i32,
    #[kafka(versions = "0+")]
    pub subtopologies: Vec<StreamsGroupDescribeResponseSubtopology>,
}

/// StreamsGroupDescribeResponseSubtopology
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupDescribeResponseSubtopology {
    #[kafka(versions = "0+")]
    pub subtopology_id: String,
    #[kafka(versions = "0+")]
    pub source_topics: Vec<String>,
    #[kafka(versions = "0+")]
    pub repartition_sink_topics: Vec<String>,
    #[kafka(versions = "0+")]
    pub state_changelog_topics: Vec<TopicInfo>,
    #[kafka(versions = "0+")]
    pub repartition_source_topics: Vec<TopicInfo>,
}

/// StreamsGroupDescribeResponseMember
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupDescribeResponseMember {
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "0+")]
    pub member_epoch: i32,
    #[kafka(versions = "0+")]
    pub instance_id: String,
    #[kafka(versions = "0+")]
    pub rack_id: String,
    #[kafka(versions = "0+")]
    pub client_id: String,
    #[kafka(versions = "0+")]
    pub client_host: String,
    #[kafka(versions = "0+")]
    pub topology_epoch: i32,
    #[kafka(versions = "0+")]
    pub process_id: String,
    #[kafka(versions = "0+")]
    pub user_endpoint: Endpoint,
    #[kafka(versions = "0+")]
    pub client_tags: Vec<KeyValue>,
    #[kafka(versions = "0+")]
    pub task_offsets: Vec<TaskOffset>,
    #[kafka(versions = "0+")]
    pub task_end_offsets: Vec<TaskOffset>,
    #[kafka(versions = "0+")]
    pub assignment: Assignment,
    #[kafka(versions = "0+")]
    pub target_assignment: Assignment,
    #[kafka(versions = "0+")]
    pub is_classic: bool,
}

/// StreamsGroupDescribeResponseEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupDescribeResponseEndpoint {
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: StreamsGroupDescribeResponseuint16,
}

/// StreamsGroupDescribeResponseTaskOffset
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupDescribeResponseTaskOffset {
    #[kafka(versions = "0+")]
    pub subtopology_id: String,
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub offset: i64,
}

/// StreamsGroupDescribeResponseAssignment
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupDescribeResponseAssignment {
    #[kafka(versions = "0+")]
    pub active_tasks: Vec<StreamsGroupDescribeResponseTaskIds>,
    #[kafka(versions = "0+")]
    pub standby_tasks: Vec<StreamsGroupDescribeResponseTaskIds>,
    #[kafka(versions = "0+")]
    pub warmup_tasks: Vec<StreamsGroupDescribeResponseTaskIds>,
}

/// StreamsGroupDescribeResponseTaskIds
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupDescribeResponseTaskIds {
    #[kafka(versions = "0+")]
    pub subtopology_id: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}

/// StreamsGroupDescribeResponseKeyValue
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupDescribeResponseKeyValue {
    #[kafka(versions = "0+")]
    pub key: String,
    #[kafka(versions = "0+")]
    pub value: String,
}

/// StreamsGroupDescribeResponseTopicInfo
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupDescribeResponseTopicInfo {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: i32,
    #[kafka(versions = "0+")]
    pub replication_factor: i16,
    #[kafka(versions = "0+")]
    pub topic_configs: Vec<StreamsGroupDescribeResponseKeyValue>,
}
