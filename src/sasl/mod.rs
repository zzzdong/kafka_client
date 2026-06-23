use crate::error::SaslError;
use async_trait::async_trait;
use bytes::Bytes;

pub mod plain;
pub mod scram;

pub use plain::PlainMechanism;
pub use scram::AsyncScramMechanism;

/// SASL 凭证
#[derive(Debug, Clone)]
pub struct SaslCredentials {
    pub mechanism: SaslMechanismType,
    pub username: String,
    pub password: String,
    pub authzid: Option<String>, // 授权身份（PLAIN 使用）
}

/// SASL 机制类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanismType {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl SaslMechanismType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// SASL 机制 trait（异步版本）
/// 预留的抽象设计，用于未来扩展
#[async_trait]
#[allow(dead_code)]
pub trait SaslMechanism: Send + Sync {
    /// 机制名称
    fn name(&self) -> &'static str;

    /// 是否为 client-first 机制
    fn is_client_first(&self) -> bool;

    /// 生成初始响应（client-first 机制使用）
    async fn initial_response(
        &mut self,
        credentials: &SaslCredentials,
    ) -> Result<Option<Bytes>, SaslError>;

    /// 处理服务器挑战
    async fn challenge(&mut self, challenge: &[u8]) -> Result<Option<Bytes>, SaslError>;

    /// 认证是否完成
    fn is_complete(&self) -> bool;

    /// 认证是否成功
    fn is_success(&self) -> bool;

    /// 重置状态（用于重试）
    fn reset(&mut self);
}

/// 创建 SASL 机制实例（异步版本）
/// 预留的工厂函数，用于未来扩展
#[allow(dead_code)]
pub fn create_mechanism(mechanism_type: SaslMechanismType) -> Box<dyn SaslMechanism> {
    match mechanism_type {
        SaslMechanismType::Plain => Box::new(PlainMechanism::new()),
        SaslMechanismType::ScramSha256 => Box::new(AsyncScramMechanism::new_sha256()),
        SaslMechanismType::ScramSha512 => Box::new(AsyncScramMechanism::new_sha512()),
    }
}
