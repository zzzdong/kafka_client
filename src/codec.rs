use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Kafka 帧
#[derive(Debug, Clone)]
pub struct KafkaFrame {
    pub data: Bytes,
}

impl KafkaFrame {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }
}

/// 业务阶段编解码器
pub struct KafkaCodec {
    max_frame_size: usize,
}

impl KafkaCodec {
    pub fn new() -> Self {
        Self {
            max_frame_size: 100 * 1024 * 1024,
        }
    }

    pub fn new_with_max_frame_size(max_frame_size: usize) -> Self {
        Self { max_frame_size }
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Frame too large",
            ));
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

        Ok(Some(KafkaFrame { data }))
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
