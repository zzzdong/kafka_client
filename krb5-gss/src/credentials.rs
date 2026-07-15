/// Kerberos credentials model (distinct from SASL username/password).
///
/// The authentication subject in Kerberos is a **principal + keytab / credential cache**;
/// there is no password concept (passwords are only used by `kinit` to obtain a TGT).
#[derive(Debug, Clone)]
pub struct KerberosCredentials {
    /// Client principal, e.g. `client@EXAMPLE.COM` or `user@EXAMPLE.COM`.
    pub principal: String,
    /// Service name (the service component of a Kerberos service principal,
    /// e.g. `kafka` / `host` / `HTTP`). Defaults to `kafka`.
    pub service_name: String,
    /// Realm. May be omitted if the principal already includes `@REALM`.
    pub realm: Option<String>,
    /// Keytab file path (contains the long-term key). Mutually exclusive with
    /// `ccache_path`; only keytab is currently supported.
    pub keytab_path: Option<String>,
    /// Credential cache path. Currently unimplemented; reserved for future use.
    pub ccache_path: Option<String>,
    /// Broker hostname (used to construct the Kerberos service principal
    /// `service/host`). If set, `sasl_authenticate_gssapi` will prefer this
    /// value over the connection IP.
    pub broker_hostname: Option<String>,
}

impl KerberosCredentials {
    pub fn new(principal: impl Into<String>) -> Self {
        Self {
            principal: principal.into(),
            service_name: "kafka".to_string(),
            realm: None,
            keytab_path: None,
            ccache_path: None,
            broker_hostname: None,
        }
    }

    pub fn with_service_name(mut self, service_name: impl Into<String>) -> Self {
        self.service_name = service_name.into();
        self
    }

    pub fn with_realm(mut self, realm: impl Into<String>) -> Self {
        self.realm = Some(realm.into());
        self
    }

    pub fn with_keytab(mut self, path: impl Into<String>) -> Self {
        self.keytab_path = Some(path.into());
        self
    }

    pub fn with_ccache(mut self, path: impl Into<String>) -> Self {
        self.ccache_path = Some(path.into());
        self
    }

    /// Set the broker hostname (used for the Kerberos service principal `service/hostname`).
    /// If unset, the connection IP is used as a fallback during GSSAPI authentication.
    pub fn with_broker_hostname(mut self, host: impl Into<String>) -> Self {
        self.broker_hostname = Some(host.into());
        self
    }

    /// Split the principal into (name, realm): prefers an explicit realm,
    /// otherwise extracts from the `@` suffix of the principal.
    pub fn split(&self) -> (String, String) {
        if let Some((name, realm)) = self.principal.rsplit_once('@') {
            (name.to_string(), realm.to_string())
        } else {
            (
                self.principal.clone(),
                self.realm.clone().unwrap_or_default(),
            )
        }
    }

    /// Resolve the realm: prefers an explicit realm, otherwise extracts from
    /// the `@` suffix of the principal.
    pub fn realm(&self) -> Option<String> {
        if let Some(ref r) = self.realm {
            Some(r.clone())
        } else if let Some((_, realm)) = self.principal.rsplit_once('@') {
            Some(realm.to_string())
        } else {
            None
        }
    }
}
