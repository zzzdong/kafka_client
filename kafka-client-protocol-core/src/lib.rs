//! Kafka 协议基础设施
//!
//! 提供与具体协议无关的通用工具箱，可被其他项目复用

pub mod codec;
pub mod error;
pub mod frame;
pub mod header;
pub mod message;
pub mod message_impls;
pub mod record_batch;

// 重新导出核心类型
pub use codec::*;
pub use error::{KafkaErrorCode, ProtocolError, ProtocolResult};
pub use kafka_client_protocol_derive::KafkaMessage;
pub use message::{Message, Request, Response};
pub use record_batch::{CompressionType, Header, Record, RecordBatch};
pub use uuid::Uuid;
