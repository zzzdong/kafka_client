// src/frame.rs
//! 通用帧处理（仅长度前缀）
//!
//! 提供与协议无关的通用帧编码/解码功能

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::{ProtocolResult, ProtocolError};

/// 编码原始帧（仅长度前缀）
///
/// # 参数
/// - `data`: 要编码的数据
///
/// # 返回
/// 编码后的帧（4字节长度 + 数据）
pub fn encode_raw_frame(data: &[u8]) -> BytesMut {
    let mut frame = BytesMut::with_capacity(4 + data.len());
    frame.put_i32(data.len() as i32);
    frame.extend_from_slice(data);
    frame
}

/// 解码原始帧（仅长度前缀）
///
/// # 参数
/// - `buf`: 包含帧数据的缓冲区
///
/// # 返回
/// 解码后的数据（不包含长度前缀）
pub fn decode_raw_frame(buf: &mut Bytes) -> ProtocolResult<Bytes> {
    if buf.remaining() < 4 {
        return Err(ProtocolError::insufficient_data(4, buf.remaining()));
    }
    
    let len = buf.get_i32();
    if len < 0 {
        return Err(ProtocolError::invalid_data("Negative frame size".to_string()));
    }
    
    if buf.remaining() < len as usize {
        return Err(ProtocolError::insufficient_data(len as usize, buf.remaining()));
    }
    
    Ok(buf.copy_to_bytes(len as usize))
}

/// 计算帧的总大小（长度前缀 + 数据）
pub fn frame_size(data_len: usize) -> usize {
    4 + data_len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_raw_frame() {
        let data = b"hello world";
        let mut frame = encode_raw_frame(data);
        
        let decoded = decode_raw_frame(&mut frame.freeze()).unwrap();
        assert_eq!(&decoded[..], data);
    }

    #[test]
    fn test_frame_size() {
        assert_eq!(frame_size(10), 14);
        assert_eq!(frame_size(0), 4);
    }
}
