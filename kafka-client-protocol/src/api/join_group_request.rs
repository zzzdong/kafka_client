//! Auto-generated from Kafka protocol
//! Message: JoinGroupRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct JoinGroupRequestProtocol {
    /// The protocol name.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The protocol metadata.
    #[kafka(versions = "0+")]
    pub metadata: Bytes,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 11, msg_type = "request", valid_versions = "0-9", flexible_versions = "6+")]
pub struct JoinGroupRequest {
    /// The group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
    #[kafka(versions = "0+")]
    pub session_timeout_ms: i32,
    /// The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
    #[kafka(versions = "1+", nullable_versions = "1+", default = -1)]
    pub rebalance_timeout_ms: i32,
    /// The member id assigned by the group coordinator.
    #[kafka(versions = "0+")]
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    #[kafka(versions = "5+", nullable_versions = "5+", default = None)]
    pub group_instance_id: Option<String>,
    /// The unique name the for class of protocols implemented by the group we want to join.
    #[kafka(versions = "0+")]
    pub protocol_type: String,
    /// The list of protocols that the member supports.
    #[kafka(versions = "0+")]
    pub protocols: Vec<JoinGroupRequestProtocol>,
    /// The reason why the member (re-)joins the group.
    #[kafka(versions = "8+", nullable_versions = "8+", default = None)]
    pub reason: Option<String>,
}

