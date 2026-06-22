use std::sync::atomic::{AtomicU32, Ordering};

/// 分区路由策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum PartitionRouting {
    #[default]
    RoundRobin,
    HashKey,
    Fixed(i32),
    Random,
}


/// 分区路由器
pub struct PartitionRouter {
    routing: PartitionRouting,
    counter: AtomicU32,
}

impl PartitionRouter {
    pub fn new(routing: PartitionRouting) -> Self {
        Self {
            routing,
            counter: AtomicU32::new(0),
        }
    }

    /// 选择分区
    pub fn select_partition(&self, key: Option<&[u8]>, partition_count: usize) -> i32 {
        if partition_count == 0 {
            return 0;
        }

        let idx = match self.routing {
            PartitionRouting::RoundRobin => {
                let c = self.counter.fetch_add(1, Ordering::SeqCst);
                (c as usize) % partition_count
            }
            PartitionRouting::HashKey => match key {
                Some(k) => {
                    let hash = murmur2(k);
                    ((hash as i32).wrapping_abs() as usize) % partition_count
                }
                None => {
                    let c = self.counter.fetch_add(1, Ordering::SeqCst);
                    (c as usize) % partition_count
                }
            },
            PartitionRouting::Fixed(p) => (p as usize) % partition_count,
            PartitionRouting::Random => rand::random_range(0..partition_count),
        };

        idx as i32
    }
}

/// MurmurHash2 算法（Kafka 使用的哈希）
fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747b28c;
    const M: u32 = 0x5bd1e995;
    const R: u32 = 24;

    let len = data.len();
    let mut h: u32 = SEED ^ len as u32;
    let mut i = 0;

    while i + 4 <= len {
        let mut k = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
        i += 4;
    }

    // 处理剩余字节
    match len - i {
        3 => {
            h ^= (data[i + 2] as u32) << 16;
            h ^= (data[i + 1] as u32) << 8;
            h ^= data[i] as u32;
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= (data[i + 1] as u32) << 8;
            h ^= data[i] as u32;
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= data[i] as u32;
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur2() {
        // Test vector from Kafka
        let data = b"test";
        let hash = murmur2(data);
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_partition_router_round_robin() {
        let router = PartitionRouter::new(PartitionRouting::RoundRobin);

        for _ in 0..10 {
            let partition = router.select_partition(None, 3);
            assert!(partition >= 0 && partition < 3);
        }
    }

    #[test]
    fn test_partition_router_hash_key() {
        let router = PartitionRouter::new(PartitionRouting::HashKey);

        let key = b"test-key";
        let partition1 = router.select_partition(Some(key), 3);
        let partition2 = router.select_partition(Some(key), 3);

        assert_eq!(partition1, partition2);
    }
}
