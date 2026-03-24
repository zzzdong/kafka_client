//! Auto-generated from Kafka protocol
//! Message: AlterPartitionRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct BrokerState {
    /// The ID of the broker.
    #[kafka(versions = "3+")]
    pub broker_id: i32,
    /// The epoch of the broker. It will be -1 if the epoch check is not supported.
    #[kafka(versions = "3+", default = -1)]
    pub broker_epoch: i64,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The leader epoch of this partition.
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    /// The ISR for this partition. Deprecated since version 3.
    #[kafka(versions = "0-2")]
    pub new_isr: Vec<i32>,
    /// The ISR for this partition.
    #[kafka(versions = "3+")]
    pub new_isr_with_epochs: Vec<BrokerState>,
    /// 1 if the partition is recovering from an unclean leader election; 0 otherwise.
    #[kafka(versions = "1+", default = 0)]
    pub leader_recovery_state: i8,
    /// The expected epoch of the partition which is being updated.
    #[kafka(versions = "0+")]
    pub partition_epoch: i32,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicData {
    /// The ID of the topic to alter ISRs for.
    #[kafka(versions = "2+", nullable_versions = "2+")]
    pub topic_id: Option<Uuid>,
    /// The partitions to alter ISRs for.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 56, msg_type = "request", valid_versions = "2-3", flexible_versions = "0+")]
pub struct AlterPartitionRequest {
    /// The ID of the requesting broker.
    #[kafka(versions = "0+")]
    pub broker_id: i32,
    /// The epoch of the requesting broker.
    #[kafka(versions = "0+", default = -1)]
    pub broker_epoch: i64,
    /// The topics to alter ISRs for.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicData>,
}

