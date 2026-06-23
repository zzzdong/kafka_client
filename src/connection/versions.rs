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
