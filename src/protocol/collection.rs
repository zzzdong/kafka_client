// src/protocol/collections.rs
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::protocol::{Message, ProtocolResult, ProtocolError};
use crate::protocol::codec::*;

/// 计算 unsigned varint 编码后的长度
pub fn varint_len(mut value: u32) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len
}

// ============ Vec<T> 实现 ============
impl<T: Message> Message for Vec<T> {
    fn type_name() -> &'static str {
        "Vec"
    }

    fn api_key(&self) -> Option<i16> {
        None
    }

    fn default_version() -> i16 {
        T::default_version()
    }

    fn encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()> {
        let use_flexible = version >= 9;
        
        if use_flexible {
            encode_compact_message_array(buf, self, |b, item| {
                item.encode(b, version)?;
                Ok(())
            });
        } else {
            encode_message_array(buf, self, |b, item| {
                item.encode(b, version)?;
                Ok(())
            });
        }
        
        Ok(())
    }

    fn decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self> {
        let use_flexible = version >= 9;
        
        if use_flexible {
            decode_compact_array(buf, |b| T::decode(b, version))
        } else {
            decode_array(buf, |b| T::decode(b, version))
        }
    }

    fn encoded_len(&self, version: i16) -> usize {
        let use_flexible = version >= 9;
        
        let len_size = if use_flexible {
            varint_len(self.len() as u32 + 1)
        } else {
            4
        };
        
        len_size + self.iter()
            .map(|item| item.encoded_len(version))
            .sum::<usize>()
    }
}

// ============ Option<T> 实现 ============
impl<T: Message> Message for Option<T> {
    fn type_name() -> &'static str {
        "Option"
    }

    fn api_key(&self) -> Option<i16> {
        None
    }

    fn default_version() -> i16 {
        T::default_version()
    }

    fn encode(&self, buf: &mut BytesMut, version: i16) -> ProtocolResult<()> {
        let use_flexible = version >= 9;
        
        match self {
            Some(v) => {
                if use_flexible {
                    // 先编码长度
                    let len = v.encoded_len(version);
                    encode_unsigned_varint(buf, len as u32 + 1);
                    v.encode(buf, version)?;
                } else {
                    v.encode(buf, version)?;
                }
            }
            None => {
                if use_flexible {
                    encode_unsigned_varint(buf, 0);
                } else {
                    buf.put_i32(-1);
                }
            }
        }
        
        Ok(())
    }

    fn decode(buf: &mut Bytes, version: i16) -> ProtocolResult<Self> {
        let use_flexible = version >= 9;
        
        if use_flexible {
            let len = decode_unsigned_varint(buf) as usize;
            if len == 0 {
                return Ok(None);
            }
            // 实际数据长度是 len - 1
            // 这里需要根据具体类型解码
            let value = T::decode(buf, version)?;
            Ok(Some(value))
        } else {
            // 传统格式：检查是否为 null
            if buf.remaining() >= 4 {
                let peek = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
                if peek == -1 {
                    buf.advance(4);
                    return Ok(None);
                }
            }
            let value = T::decode(buf, version)?;
            Ok(Some(value))
        }
    }

    fn encoded_len(&self, version: i16) -> usize {
        let use_flexible = version >= 9;
        
        match self {
            Some(v) => {
                let inner_len = v.encoded_len(version);
                if use_flexible {
                    varint_len(inner_len as u32 + 1) + inner_len
                } else {
                    inner_len
                }
            }
            None => {
                if use_flexible {
                    1
                } else {
                    4
                }
            }
        }
    }
}