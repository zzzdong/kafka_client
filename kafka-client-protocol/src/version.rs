use std::fmt;
use std::ops::{RangeFrom, RangeInclusive};

/// 版本范围（支持 Kafka JSON 格式）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionRange {
    /// 精确版本，如 "0" 或 "3"
    Exact(i16),
    /// 范围，如 "0-12"（包含两端）
    Range(RangeInclusive<i16>),
    /// 从某个版本开始，如 "4+"
    From(RangeFrom<i16>),
    /// 所有版本
    All,
}

impl VersionRange {
    /// 从 Kafka JSON 格式字符串解析
    pub fn parse(s: &str) -> Self {
        let s = s.trim();

        if s == "0+" || s == "none" || s == "all" {
            return VersionRange::All;
        }

        if s.contains('-') {
            let parts: Vec<&str> = s.split('-').collect();
            if parts.len() == 2 {
                let start = parts[0].parse::<i16>().unwrap();
                let end = parts[1].parse::<i16>().unwrap();
                return VersionRange::Range(start..=end);
            }
        }

        if s.ends_with('+') {
            let start = s.trim_end_matches('+').parse::<i16>().unwrap();
            return VersionRange::From(start..);
        }

        VersionRange::Exact(s.parse().unwrap())
    }

    /// 检查版本是否在范围内
    pub fn contains(&self, version: i16) -> bool {
        match self {
            VersionRange::Exact(v) => version == *v,
            VersionRange::Range(range) => range.contains(&version),
            VersionRange::From(range) => version >= range.start,
            VersionRange::All => true,
        }
    }

    /// 获取最小版本
    pub fn min_version(&self) -> i16 {
        match self {
            VersionRange::Exact(v) => *v,
            VersionRange::Range(range) => *range.start(),
            VersionRange::From(range) => range.start,
            VersionRange::All => 0,
        }
    }

    /// 获取最大版本（如果有）
    pub fn max_version(&self) -> Option<i16> {
        match self {
            VersionRange::Exact(v) => Some(*v),
            VersionRange::Range(range) => Some(*range.end()),
            VersionRange::From(_) => None,
            VersionRange::All => None,
        }
    }

    /// 与另一个版本范围取交集
    pub fn intersect(&self, other: &VersionRange) -> Option<VersionRange> {
        let min = self.min_version().max(other.min_version());

        let max = match (self.max_version(), other.max_version()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        match max {
            Some(max) if max >= min => Some(VersionRange::Range(min..=max)),
            None => Some(VersionRange::From(min..)),
            _ => None,
        }
    }
}

impl fmt::Display for VersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersionRange::Exact(v) => write!(f, "{}", v),
            VersionRange::Range(range) => write!(f, "{}-{}", range.start(), range.end()),
            VersionRange::From(range) => write!(f, "{}+", range.start),
            VersionRange::All => write!(f, "0+"),
        }
    }
}

/// 版本范围常量
pub mod versions {
    use super::VersionRange;

    /// 所有版本
    pub const ALL: VersionRange = VersionRange::All;

    /// 基础版本 (v0-v2)
    pub const BASIC: VersionRange = VersionRange::Range(0..=2);

    /// 扩展版本 (v3-v8)
    pub const EXTENDED: VersionRange = VersionRange::Range(3..=8);

    /// 柔性格式版本 (v9+)
    pub const FLEXIBLE: VersionRange = VersionRange::From(9..);

    /// 创建从指定版本开始的版本范围
    pub fn from(version: i16) -> VersionRange {
        VersionRange::From(version..)
    }

    /// 创建精确版本
    pub fn exact(version: i16) -> VersionRange {
        VersionRange::Exact(version)
    }

    /// 创建版本范围
    pub fn range(start: i16, end: i16) -> VersionRange {
        VersionRange::Range(start..=end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_exact() {
        assert_eq!(VersionRange::parse("3"), VersionRange::Exact(3));
    }

    #[test]
    fn test_parse_range() {
        assert_eq!(VersionRange::parse("0-12"), VersionRange::Range(0..=12));
    }

    #[test]
    fn test_parse_from() {
        assert_eq!(VersionRange::parse("4+"), VersionRange::From(4..));
    }

    #[test]
    fn test_contains() {
        let range = VersionRange::Range(0..=5);
        assert!(range.contains(0));
        assert!(range.contains(5));
        assert!(!range.contains(6));
    }
}
