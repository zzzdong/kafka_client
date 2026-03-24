//! Auto-generated from Kafka protocol
//! Message: ProduceResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct BatchIndexAndErrorMessage {
    /// The batch index of the record that caused the batch to be dropped.
    #[kafka(versions = "8+")]
    pub batch_index: i32,
    /// The error message of the record that caused the batch to be dropped.
    #[kafka(versions = "8+", nullable_versions = "8+", default = None)]
    pub batch_index_error_message: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    #[kafka(versions = "10+", default = -1)]
    pub leader_id: i32,
    /// The latest known leader epoch.
    #[kafka(versions = "10+", default = -1)]
    pub leader_epoch: i32,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionProduceResponse {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub index: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The base offset.
    #[kafka(versions = "0+")]
    pub base_offset: i64,
    /// The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
    #[kafka(versions = "2+", nullable_versions = "2+", default = -1)]
    pub log_append_time_ms: i64,
    /// The log start offset.
    #[kafka(versions = "5+", nullable_versions = "5+", default = -1)]
    pub log_start_offset: i64,
    /// The batch indices of records that caused the batch to be dropped.
    #[kafka(versions = "8+", nullable_versions = "8+")]
    pub record_errors: Option<Vec<BatchIndexAndErrorMessage>>,
    /// The global error message summarizing the common root cause of the records that caused the batch to be dropped.
    #[kafka(versions = "8+", nullable_versions = "8+", default = None)]
    pub error_message: Option<String>,
    /// The leader broker that the producer should use for future requests.
    #[kafka(versions = "10+", tag = 0, tagged_versions = "10+")]
    pub current_leader: LeaderIdAndEpoch,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicProduceResponse {
    /// The topic name.
    #[kafka(versions = "0-12", nullable_versions = "0-12", map_key)]
    pub name: Option<String>,
    /// The unique topic ID
    #[kafka(versions = "13+", nullable_versions = "13+", map_key)]
    pub topic_id: Option<Uuid>,
    /// Each partition that we produced to within the topic.
    #[kafka(versions = "0+")]
    pub partition_responses: Vec<PartitionProduceResponse>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    #[kafka(versions = "10+", map_key)]
    pub node_id: i32,
    /// The node's hostname.
    #[kafka(versions = "10+")]
    pub host: String,
    /// The node's port.
    #[kafka(versions = "10+")]
    pub port: i32,
    /// The rack of the node, or null if it has not been assigned to a rack.
    #[kafka(versions = "10+", nullable_versions = "10+", default = None)]
    pub rack: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 0, msg_type = "response", valid_versions = "3-13", flexible_versions = "9+")]
pub struct ProduceResponse {
    /// Each produce response.
    #[kafka(versions = "0+")]
    pub responses: Vec<TopicProduceResponse>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "1+", nullable_versions = "1+", default = 0)]
    pub throttle_time_ms: i32,
    /// Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
    #[kafka(versions = "10+", tag = 0, tagged_versions = "10+")]
    pub node_endpoints: Vec<NodeEndpoint>,
}

