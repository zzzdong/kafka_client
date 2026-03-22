//! UUID 类型定义
//!
//! 简单的 UUID wrapper，用于 Kafka 协议中的 UUID 类型

use bytes::{Buf, BufMut, Bytes};
use crate::protocol::{ProtocolError, ProtocolResult};

/// UUID 包装类型
///
/// 使用 16 字节存储 UUID 数据
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct Uuid([u8; 16]);

impl Uuid {
    /// 创建一个新的 UUID
    pub fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// 获取 UUID 的字节表示
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// 将 UUID 编码到缓冲区
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_slice(&self.0);
    }

    /// 从缓冲区解码 UUID
    pub fn decode(buf: &mut impl Buf) -> ProtocolResult<Self> {
        if buf.remaining() < 16 {
            return Err(ProtocolError::InsufficientData {
                expected: 16,
                actual: buf.remaining(),
            });
        }
        let mut bytes = [0u8; 16];
        buf.copy_to_slice(&mut bytes);
        Ok(Self(bytes))
    }

    /// 编码后的长度（固定 16 字节）
    pub const fn encoded_len(&self) -> usize {
        16
    }
}

impl From<[u8; 16]> for Uuid {
    fn from(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl From<Uuid> for [u8; 16] {
    fn from(uuid: Uuid) -> Self {
        uuid.0
    }
}

impl AsRef<[u8]> for Uuid {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 标准 UUID 格式: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        let b = self.0;
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3],
            b[4], b[5],
            b[6], b[7],
            b[8], b[9],
            b[10], b[11], b[12], b[13], b[14], b[15]
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_uuid_encode_decode() {
        let uuid = Uuid::new([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                              0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10]);
        
        let mut buf = BytesMut::new();
        uuid.encode(&mut buf);
        
        assert_eq!(buf.len(), 16);
        
        let decoded = Uuid::decode(&mut buf.freeze()).unwrap();
        assert_eq!(uuid, decoded);
    }

    #[test]
    fn test_uuid_display() {
        let uuid = Uuid::new([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                              0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10]);
        
        let s = uuid.to_string();
        assert_eq!(s, "01020304-0506-0708-090a-0b0c0d0e0f10");
    }
}
