//! Kafka 协议定义
//!
//! 包含 Kafka 协议的具体定义，由 codegen 生成，随 Kafka 版本更新

pub mod api;
pub mod version;

// 重新导出生成的 API 结构体
pub use api::*;

// 重新导出 version
pub use version::{VersionRange, versions};

// 重新导出 core 中的核心类型（方便用户使用）
pub use kafka_client_protocol_core::{KafkaMessage, Message, Request, Response, ProtocolError};
