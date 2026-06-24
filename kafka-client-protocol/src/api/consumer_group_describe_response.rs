//! Auto-generated from Kafka protocol
//! Message: ConsumerGroupDescribeResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicPartitions {
    /// The topic ID.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The topic name.
    #[kafka(versions = "0+")]
    pub topic_name: String,
    /// The partitions.
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Assignment {
    /// The assigned topic-partitions to the member.
    #[kafka(versions = "0+")]
    pub topic_partitions: Vec<TopicPartitions>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Member {
    /// The member ID.
    #[kafka(versions = "0+")]
    pub member_id: String,
    /// The member instance ID.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub instance_id: Option<String>,
    /// The member rack ID.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub rack_id: Option<String>,
    /// The current member epoch.
    #[kafka(versions = "0+")]
    pub member_epoch: i32,
    /// The client ID.
    #[kafka(versions = "0+")]
    pub client_id: String,
    /// The client host.
    #[kafka(versions = "0+")]
    pub client_host: String,
    /// The subscribed topic names.
    #[kafka(versions = "0+")]
    pub subscribed_topic_names: Vec<String>,
    /// the subscribed topic regex otherwise or null of not provided.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub subscribed_topic_regex: Option<String>,
    /// The current assignment.
    #[kafka(versions = "0+")]
    pub assignment: Assignment,
    /// The target assignment.
    #[kafka(versions = "0+")]
    pub target_assignment: Assignment,
    /// -1 for unknown. 0 for classic member. +1 for consumer member.
    #[kafka(versions = "1+", default = -1)]
    pub member_type: i8,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribedGroup {
    /// The describe error, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
    /// The group ID string.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The group state string, or the empty string.
    #[kafka(versions = "0+")]
    pub group_state: String,
    /// The group epoch.
    #[kafka(versions = "0+")]
    pub group_epoch: i32,
    /// The assignment epoch.
    #[kafka(versions = "0+")]
    pub assignment_epoch: i32,
    /// The selected assignor.
    #[kafka(versions = "0+")]
    pub assignor_name: String,
    /// The members.
    #[kafka(versions = "0+")]
    pub members: Vec<Member>,
    /// 32-bit bitfield to represent authorized operations for this group.
    #[kafka(versions = "0+", default = -2147483648)]
    pub authorized_operations: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 69,
    msg_type = "response",
    valid_versions = "0-1",
    flexible_versions = "0+"
)]
pub struct ConsumerGroupDescribeResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// Each described group.
    #[kafka(versions = "0+")]
    pub groups: Vec<DescribedGroup>,
}
