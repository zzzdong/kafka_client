#!/usr/bin/env bash
# Generate TLS certificates for Kafka TLS integration tests.
#
# Two formats are generated from a single CA:
#   PEM format  – for the Rust client (rustls): ca-cert.pem, broker-cert.pem, broker-key.pem
#   JKS format  – for the Kafka Docker broker:  kafka01.keystore.jks, kafka.truststore.jks
#
# Prerequisites:
#   - openssl (always needed for PEM generation)
#   - keytool (from JDK/JRE, needed for JKS generation)
#     If keytool is not available, JKS files will be kept as-is
#     (from the initial Kafka source copy).
#
# Usage:
#   cd tests && ./gen-certs.sh
#
# Environment:
#   KEYSTORE_PASS   Password for keystores/truststores (default: abcdefgh)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS_DIR="${SCRIPT_DIR}/fixtures/tls"
mkdir -p "${CERTS_DIR}"

PASS="${KEYSTORE_PASS:-abcdefgh}"

echo "=== Generating TLS certificates in ${CERTS_DIR} ==="
echo "  Keystore password: ${PASS}"

# ===========================================================================
# Phase 1: CA certificate (PEM)
# ===========================================================================
echo ""
echo "--- Phase 1: CA certificate ---"

if [ -f "${CERTS_DIR}/ca-key.pem" ] && [ -f "${CERTS_DIR}/ca-cert.pem" ]; then
    echo "  [SKIP] CA cert already exists (delete ca-key.pem + ca-cert.pem to regenerate)"
else
    echo "  [1/1] Generating CA key and self-signed certificate..."
    openssl req -new -x509 -days 3650 -nodes \
      -subj "/CN=KafkaTestCA" \
      -keyout "${CERTS_DIR}/ca-key.pem" \
      -out "${CERTS_DIR}/ca-cert.pem" 2>/dev/null
    echo "  Done: ca-key.pem, ca-cert.pem"
fi

# ===========================================================================
# Phase 2: Broker certificate (PEM, signed by CA)
# ===========================================================================
echo ""
echo "--- Phase 2: Broker certificate ---"

if [ -f "${CERTS_DIR}/broker-key.pem" ] && [ -f "${CERTS_DIR}/broker-cert.pem" ]; then
    echo "  [SKIP] Broker cert already exists (delete broker-*.pem to regenerate)"
else
    echo "  [1/4] Generating broker private key..."
    openssl genrsa -out "${CERTS_DIR}/broker-key.pem" 2048 2>/dev/null

    echo "  [2/4] Generating broker CSR..."
    openssl req -new -key "${CERTS_DIR}/broker-key.pem" \
      -subj "/CN=localhost" \
      -out "${CERTS_DIR}/broker.csr" 2>/dev/null

    echo "  [3/4] Creating SAN extension config..."
    cat > "${CERTS_DIR}/broker.ext" <<'SANEOF'
subjectAltName = DNS:localhost,IP:127.0.0.1
SANEOF

    echo "  [4/4] Signing broker certificate with CA..."
    openssl x509 -req -days 3650 \
      -in "${CERTS_DIR}/broker.csr" \
      -CA "${CERTS_DIR}/ca-cert.pem" \
      -CAkey "${CERTS_DIR}/ca-key.pem" \
      -CAcreateserial \
      -extfile "${CERTS_DIR}/broker.ext" \
      -out "${CERTS_DIR}/broker-cert.pem" 2>/dev/null

    rm -f "${CERTS_DIR}/broker.csr" "${CERTS_DIR}/broker.ext"
    echo "  Done: broker-key.pem, broker-cert.pem"
fi

# ===========================================================================
# Phase 3: PEM chain (broker cert + CA cert, for PKCS12/JKS import)
# ===========================================================================
echo ""
echo "--- Phase 3: PEM chain ---"
echo "  [1/1] Creating chain.pem (broker-cert + ca-cert)..."
cat "${CERTS_DIR}/broker-cert.pem" "${CERTS_DIR}/ca-cert.pem" > "${CERTS_DIR}/chain.pem"

# ===========================================================================
# Phase 4: PKCS12 keystore (intermediate format for JKS conversion)
# ===========================================================================
echo ""
echo "--- Phase 4: PKCS12 keystore ---"
echo "  [1/1] Creating broker.keystore.p12..."
openssl pkcs12 -export \
  -in "${CERTS_DIR}/chain.pem" \
  -inkey "${CERTS_DIR}/broker-key.pem" \
  -name broker \
  -password "pass:${PASS}" \
  -out "${CERTS_DIR}/broker.keystore.p12" 2>/dev/null
echo "  Done: broker.keystore.p12"

# ===========================================================================
# Phase 5: JKS files (for Kafka Docker broker) — requires keytool
# ===========================================================================
echo ""
echo "--- Phase 5: JKS keystore/truststore (for Kafka broker) ---"

if command -v keytool &>/dev/null; then
    echo "  keytool found at $(command -v keytool)"

    # 5a. JKS keystore (kafka01.keystore.jks)
    if [ -f "${CERTS_DIR}/kafka01.keystore.jks" ]; then
        echo "  [SKIP] kafka01.keystore.jks already exists (delete to regenerate)"
    else
        echo "  [5a] Creating kafka01.keystore.jks from PKCS12..."
        keytool -importkeystore \
          -srckeystore "${CERTS_DIR}/broker.keystore.p12" \
          -srcstoretype PKCS12 \
          -srcstorepass "${PASS}" \
          -destkeystore "${CERTS_DIR}/kafka01.keystore.jks" \
          -deststoretype JKS \
          -deststorepass "${PASS}" \
          -noprompt 2>/dev/null
        echo "  Done: kafka01.keystore.jks"
    fi

    # 5b. Truststore (kafka.truststore.jks)
    if [ -f "${CERTS_DIR}/kafka.truststore.jks" ]; then
        echo "  [SKIP] kafka.truststore.jks already exists (delete to regenerate)"
    else
        echo "  [5b] Creating kafka.truststore.jks with CA cert..."
        keytool -keystore "${CERTS_DIR}/kafka.truststore.jks" \
          -storetype JKS \
          -alias CARoot \
          -import \
          -file "${CERTS_DIR}/ca-cert.pem" \
          -storepass "${PASS}" \
          -noprompt 2>/dev/null
        echo "  Done: kafka.truststore.jks"
    fi

    # 5c. Client keystore (client.keystore.jks)
    if [ -f "${CERTS_DIR}/client.keystore.jks" ]; then
        echo "  [SKIP] client.keystore.jks already exists (delete to regenerate)"
    else
        echo "  [5c] Creating client.keystore.jks from broker cert..."
        # The client keystore needs the broker cert + client key for mTLS
        # For simplicity in testing, use the broker cert as the client cert too
        keytool -importkeystore \
          -srckeystore "${CERTS_DIR}/broker.keystore.p12" \
          -srcstoretype PKCS12 \
          -srcstorepass "${PASS}" \
          -destkeystore "${CERTS_DIR}/client.keystore.jks" \
          -deststoretype JKS \
          -deststorepass "${PASS}" \
          -noprompt 2>/dev/null
        echo "  Done: client.keystore.jks"
    fi
else
    echo "  [WARN] keytool not found — JKS files will NOT be regenerated."
    echo "         Existing JKS files from Kafka source will be preserved."
    echo "         Install JDK (java) to enable JKS generation."
    for f in kafka01.keystore.jks kafka.truststore.jks client.keystore.jks; do
        if [ ! -f "${CERTS_DIR}/${f}" ]; then
            echo "  [ERROR] Missing JKS file: ${f}"
            echo "          Please copy from Kafka source or install JDK."
            exit 1
        fi
    done
    echo "  All JKS files exist (from initial setup). Continuing..."
fi

# ===========================================================================
# Phase 6: Credential files
# ===========================================================================
echo ""
echo "--- Phase 6: Credential files ---"

# Only overwrite if they don't exist (to preserve existing if password differs)
for cr in kafka_keystore_creds kafka_ssl_key_creds kafka_truststore_creds; do
    if [ ! -f "${CERTS_DIR}/${cr}" ]; then
        echo "  Creating ${cr}..."
        echo -n "${PASS}" > "${CERTS_DIR}/${cr}"
    else
        echo "  [SKIP] ${cr} already exists"
    fi
done

# ===========================================================================
# Phase 7: Client SSL properties (for Java-based clients)
# ===========================================================================
echo ""
echo "--- Phase 7: Client SSL properties ---"

if [ -f "${CERTS_DIR}/client-ssl.properties" ]; then
    echo "  [SKIP] client-ssl.properties already exists"
else
    echo "  Writing client-ssl.properties..."
    cat > "${CERTS_DIR}/client-ssl.properties" <<PROPEOF
security.protocol=SSL
ssl.truststore.location=${CERTS_DIR}/kafka.truststore.jks
ssl.truststore.password=${PASS}
ssl.keystore.location=${CERTS_DIR}/client.keystore.jks
ssl.keystore.password=${PASS}
ssl.key.password=${PASS}
ssl.client.auth=required
ssl.endpoint.identification.algorithm=
PROPEOF
fi

# ===========================================================================
# Cleanup
# ===========================================================================
echo ""
echo "--- Cleanup ---"
rm -f "${CERTS_DIR}/chain.pem" "${CERTS_DIR}/broker.keystore.p12"
rm -f "${CERTS_DIR}/ca-cert.srl" 2>/dev/null || true
echo "  Removed temporary files"

# Ensure all files are world-readable (container needs access via volume mount)
chmod 644 "${CERTS_DIR}"/* 2>/dev/null || true

# ===========================================================================
# Summary
# ===========================================================================
echo ""
echo "=== Done ==="
echo ""
echo "Generated files in ${CERTS_DIR}:"
ls -la "${CERTS_DIR}/"
echo ""
echo "=== Client TlsConfig (Rust test) ==="
echo "  // Without certificate verification (self-signed):"
echo '  TlsConfig {'
echo '    verify_certificate: false,'
echo '    domain: "localhost".to_string(),'
echo '    ..Default::default()'
echo '  }'
echo ""
echo "  // With CA certificate verification:"
echo '  TlsConfig {'
echo '    verify_certificate: true,'
echo '    domain: "localhost".to_string(),'
echo '    ca_cert_path: Some("tests/fixtures/tls/ca-cert.pem".to_string()),'
echo '    ..Default::default()'
echo '  }'
