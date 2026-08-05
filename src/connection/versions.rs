//! Negotiated API versions

use std::collections::HashMap;

/// Negotiated API versions from ApiVersions handshake
#[derive(Debug, Clone)]
pub struct NegotiatedVersions {
    versions: HashMap<i16, i16>,
}

impl NegotiatedVersions {
    pub fn new() -> Self {
        Self {
            versions: HashMap::new(),
        }
    }

    pub fn set_version(&mut self, api_key: i16, version: i16) {
        self.versions.insert(api_key, version);
    }

    pub fn get_version(&self, api_key: i16) -> Option<i16> {
        self.versions.get(&api_key).copied()
    }
}

impl Default for NegotiatedVersions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_unknown_version_returns_none() {
        let versions = NegotiatedVersions::new();
        assert_eq!(versions.get_version(0), None);
    }

    #[test]
    fn set_and_get_version() {
        let mut versions = NegotiatedVersions::new();
        versions.set_version(3, 12);
        versions.set_version(9, 8);
        assert_eq!(versions.get_version(3), Some(12));
        assert_eq!(versions.get_version(9), Some(8));
        assert_eq!(versions.get_version(42), None);
    }

    #[test]
    fn overwrite_version() {
        let mut versions = NegotiatedVersions::new();
        versions.set_version(0, 3);
        versions.set_version(0, 9);
        assert_eq!(versions.get_version(0), Some(9));
    }
}
