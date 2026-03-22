use bytes::{Bytes, BytesMut, Buf, BufMut};
use tokio_util::codec::{Decoder, Encoder};
use std::io;

/// 原始帧（握手阶段使用，只包含长度前缀和数据）
#[derive(Debug, Clone)]
pub struct RawFrame {
    pub size: i32,
    pub data: Bytes,
}

/// Kafka 帧（业务阶段使用，包含 correlation_id）
#[derive(Debug, Clone)]
pub struct KafkaFrame {
    pub correlation_id: i32,
    pub data: Bytes,
}

/// 握手阶段编解码器
pub struct ApiVersionsCodec {
    max_frame_size: usize,
}

impl ApiVersionsCodec {
    pub fn new() -> Self {
        Self { max_frame_size: 100 * 1024 * 1024 }  // 100MB
    }
}

impl Default for ApiVersionsCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for ApiVersionsCodec {
    type Item = RawFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 1. 检查是否有足够的字节读取长度前缀（4字节）
        if src.len() < 4 {
            return Ok(None);
        }

        // 2. 读取长度
        let size = i32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // 3. 检查大小限制
        if size > self.max_frame_size {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Frame too large"));
        }

        // 4. 检查是否有完整帧
        if src.len() < 4 + size {
            // 预留足够的空间
            src.reserve(4 + size - src.len());
            return Ok(None);
        }

        // 5. 取出帧数据
        src.advance(4);
        let data = src.split_to(size).freeze();

        Ok(Some(RawFrame { size: size as i32, data }))
    }
}

impl Encoder<RawFrame> for ApiVersionsCodec {
    type Error = io::Error;

    fn encode(&mut self, item: RawFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.put_i32(item.size);
        dst.extend_from_slice(&item.data);
        Ok(())
    }
}

/// 业务阶段编解码器
pub struct KafkaCodec {
    max_frame_size: usize,
}

impl KafkaCodec {
    pub fn new() -> Self {
        Self { max_frame_size: 100 * 1024 * 1024 }
    }
}

impl Default for KafkaCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for KafkaCodec {
    type Item = KafkaFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 1. 检查是否有足够的字节读取长度前缀（4字节）
        if src.len() < 4 {
            return Ok(None);
        }

        // 2. 读取长度
        let size = i32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // 3. 检查大小限制
        if size > self.max_frame_size {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Frame too large"));
        }

        // 4. 检查是否有完整帧
        if src.len() < 4 + size {
            // 预留足够的空间
            src.reserve(4 + size - src.len());
            return Ok(None);
        }

        // 5. 取出帧数据
        src.advance(4);
        let data = src.split_to(size).freeze();

        // 6. 从数据前4字节提取 correlation_id
        if data.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Frame too short for correlation_id"));
        }
        let correlation_id = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);

        Ok(Some(KafkaFrame { correlation_id, data }))
    }
}

impl Encoder<KafkaFrame> for KafkaCodec {
    type Error = io::Error;

    fn encode(&mut self, item: KafkaFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.put_i32(item.data.len() as i32);
        dst.extend_from_slice(&item.data);
        Ok(())
    }
}
