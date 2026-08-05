//! Partition router - message partitioning strategy

use std::sync::atomic::{AtomicU32, Ordering};

/// Partition routing strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionRouting {
    #[default]
    RoundRobin,
    HashKey,
    Fixed(i32),
    Random,
}

/// Partition router (cloneable for concurrent access)
#[derive(Clone)]
pub struct PartitionRouter {
    routing: PartitionRouting,
    counter: std::sync::Arc<AtomicU32>,
}

impl PartitionRouter {
    pub fn new(routing: PartitionRouting) -> Self {
        Self {
            routing,
            counter: std::sync::Arc::new(AtomicU32::new(0)),
        }
    }

    /// Select partition
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
                    // Java's DefaultPartitioner uses
                    // `toPositive(murmur2(key)) % numPartitions`, where
                    // `toPositive(i) = i & 0x7fffffff`. Masking (rather than
                    // `abs`) keeps the partition mapping identical to Java
                    // clients, which matters when both write to the same topic.
                    ((hash & 0x7fffffff) as usize) % partition_count
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

/// MurmurHash2 algorithm (used by Kafka)
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
        let data = b"test";
        let hash = murmur2(data);
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_partition_router_round_robin() {
        let router = PartitionRouter::new(PartitionRouting::RoundRobin);

        for _ in 0..10 {
            let partition = router.select_partition(None, 3);
            assert!((0..3).contains(&partition));
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

    #[test]
    fn test_hash_key_matches_java_positive_masking() {
        // murmur2("partition-me") == 0xf5408e20, which is negative when
        // interpreted as i32. Java's DefaultPartitioner computes
        // (murmur2 & 0x7fffffff) % numPartitions. With 0xf5408e20:
        //   0xf5408e20 & 0x7fffffff == 0x75408e20 == 1_967_164_960
        //   1_967_164_960 % 5 == 0, % 7 == 5
        // A simple abs() would yield 180_318_688, mapping to different
        // partitions (3 and 4 respectively).
        let router = PartitionRouter::new(PartitionRouting::HashKey);
        assert_eq!(router.select_partition(Some(b"partition-me"), 5), 0);
        assert_eq!(router.select_partition(Some(b"partition-me"), 7), 5);
    }
}
