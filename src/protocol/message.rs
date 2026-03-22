use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::protocol::error::ProtocolResult;
use crate::protocol::header::RequestHeader;

/// Kafka 消息核心 Trait
///
/// 类似于 prost::Message，提供统一的编解码接口
pub trait Message: Sized {
    /// 消息类型名称
    fn type_name() -> &'static str;

    /// API Key（请求/响应类型）
    /// - 对于请求，返回对应的 API Key
    /// - 对于响应，返回 None（响应通过 correlation_id 匹配）
    fn api_key(&self) -> Option<i16> {
        None
    }

    /// 获取消息的默认版本
    fn default_version() -> i16 {
        0
    }

    /// 编码消息到缓冲区
    ///
    /// # Parameters
    /// - `buf`: 目标缓冲区
    /// - `version`: API 版本，决定编码格式
    fn encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()>;

    /// 从缓冲区解码消息
    ///
    /// # Parameters
    /// - `buf`: 源缓冲区（会消耗数据）
    /// - `version`: API 版本，决定解码格式
    fn decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self>;

    /// 计算编码后的长度（可选优化）
    fn encoded_len(&self, version: i16) -> usize {
        // 默认实现：编码到临时缓冲区计算长度
        let mut buf = BytesMut::new();
        self.encode(&mut buf, version).unwrap();
        buf.len()
    }

    /// 编码到新的 BytesMut
    fn encode_to_bytes(&self, version: i16) -> ProtocolResult<BytesMut> {
        let mut buf = BytesMut::with_capacity(self.encoded_len(version));
        self.encode(&mut buf, version)?;
        Ok(buf)
    }
}

/// 带请求头的消息（用于发送请求）
pub trait RequestMessage: Message {
    /// 获取请求头
    fn request_header(&self, version: i16, correlation_id: i32, client_id: &str) -> RequestHeader;

    /// 编码为完整的请求帧（包含长度前缀和请求头）
    fn encode_frame(
        &self,
        version: i16,
        correlation_id: i32,
        client_id: &str,
    ) -> ProtocolResult<BytesMut> {
        let header = self.request_header(version, correlation_id, client_id);

        let mut buf = BytesMut::new();

        // 编码请求头
        header.encode(&mut buf);

        // 编码请求体
        self.encode(&mut buf, version)?;

        // 添加长度前缀
        let mut frame = BytesMut::with_capacity(4 + buf.len());
        frame.put_i32(buf.len() as i32);
        frame.extend_from_slice(&buf);

        Ok(frame)
    }
}

/// 带响应头的消息（用于接收响应）
pub trait ResponseMessage: Message {
    /// 从完整响应帧解码（跳过长度前缀和响应头）
    fn decode_frame(data: Bytes, version: i16) -> ProtocolResult<Self> {
        let mut buf = data;

        // 读取 correlation_id（响应头）
        if buf.remaining() < 4 {
            return Err(crate::protocol::error::ProtocolError::InvalidData("Frame too short".to_string()));
        }
        let _correlation_id = buf.get_i32();

        // 如果是 flexible format，跳过 tagged_fields
        if version >= 9 && buf.has_remaining() {
            crate::protocol::codec::skip_tagged_fields(&mut buf)?;
        }

        let message = Self::decode(&mut buf, version)?;

        Ok(message)
    }
}

/// 版本感知的编解码（用于内部实现）
pub trait VersionedEncode {
    fn encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()>;
}

pub trait VersionedDecode: Sized {
    fn decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self>;
}
