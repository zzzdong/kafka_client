// src/message.rs
use crate::{
    error::ProtocolResult,
    header::{RequestHeader, ResponseHeader},
};
use bytes::{BufMut, Bytes, BytesMut};

// ============================================================================
// 核心 Message trait（所有可编解码的类型）
// ============================================================================

/// 可编解码的基础 trait
///
/// 所有需要序列化/反序列化的类型都实现此 trait
pub trait Message: Sized + Default + PartialEq {
    /// 消息类型名称
    fn type_name() -> &'static str;

    /// 获取消息支持的最高版本
    fn max_version() -> i16;

    /// 获取消息支持的最低版本
    fn min_version() -> i16;

    /// 获取支持灵活格式的最低版本
    fn flexible_version() -> Option<i16> {
        None
    }

    /// 检查版本是否支持灵活格式
    fn is_flexible_version(version: i16) -> bool {
        Self::flexible_version()
            .map(|v| version >= v)
            .unwrap_or(false)
    }

    /// 编码消息（不包括长度前缀和头）
    fn encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()>;

    /// 解码消息（不包括长度前缀和头）
    fn decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self>;

    /// 计算编码后的大小
    fn size(&self, version: i16) -> usize;

    /// 默认版本
    fn default_version() -> i16 {
        Self::min_version()
    }

    /// 检查当前值是否为默认值
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

// ============================================================================
// Request trait（所有请求消息）
// ============================================================================

/// Kafka 请求消息
///
/// 所有请求消息（有 api_key 的顶层消息）都实现此 trait
pub trait Request: Message {
    /// 获取 API Key
    fn api_key(&self) -> i16;

    /// 编码为请求数据（包含 header + body，不包含长度前缀）
    ///
    /// 长度前缀由上层 Tokio Codec 负责添加
    fn encode_frame(
        &self,
        version: i16,
        correlation_id: i32,
        client_id: Option<String>,
    ) -> ProtocolResult<Bytes> {
        let use_flexible = Self::is_flexible_version(version);

        let mut buf = BytesMut::new();

        // 1. 编码请求头
        if use_flexible {
            RequestHeader::new_v2(self.api_key(), version, correlation_id, client_id)
                .encode(&mut buf);
        } else {
            RequestHeader::new_v1(self.api_key(), version, correlation_id, client_id)
                .encode(&mut buf);
        }

        // 2. 编码请求体
        self.encode(&mut buf, version)?;

        Ok(buf.freeze())
    }
}

/// Kafka 响应消息
///
/// 所有响应消息（有 api_key 的顶层消息）都实现此 trait
pub trait Response: Message {
    /// 获取 API Key（响应的 api_key 与请求相同）
    fn api_key(&self) -> i16;

    /// 从响应数据解码（包含 header + body，不包含长度前缀）
    ///
    /// 返回 (响应体, 响应头)，连接层需要 correlation_id 进行请求匹配
    fn decode_frame(data: Bytes, version: i16) -> ProtocolResult<(ResponseHeader, Self)> {
        let use_flexible = Self::is_flexible_version(version);
        let mut buf = data;

        let header = ResponseHeader::decode(&mut buf, use_flexible)?;
        let body = Self::decode(&mut buf, version)?;

        Ok((header, body))
    }
}
