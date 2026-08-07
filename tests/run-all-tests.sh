#!/usr/bin/env bash
# 一站式集成测试启动脚本。
#
# 启动 3-broker 集群 (docker-compose.yml)、
# SASL 单节点 (docker-compose.sasl.yml) 和
# TLS 单节点 (docker-compose.tls.yml)，
# 等待所有服务就绪后运行全部集成测试。
#
# 环境变量：
#   KAFKA_CLI           容器 CLI：podman | docker（默认：auto-detect）
#   KAFKA_IMAGE         容器镜像（默认 apache/kafka:4.3.0）
#   RUST_TEST_THREADS   测试并发数（默认 1）
#   SASL_MECHANISM      SASL 认证机制（默认 PLAIN）
#   SASL_USERNAME       SASL 用户名（默认 admin）
#   SASL_PASSWORD       SASL 密码（默认 admin-secret）
#   SKIP_CLEANUP        设为非空值可跳过集群关闭
#   TEST_FILTER         只运行特定测试文件（如 "produce_consume"）

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.0}"
RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"

# 所有测试列表，按依赖的集群分组
THREE_BROKER_TESTS=(
    "produce_consume"
    "produce_with_keys"
    "producer_acks"
    "large_batch"
    "multi_topic"
    "offset_commit"
    "offset_reset"
    "consumer_group"
    "consumer_seek"
    "consumer_api"
    "producer_api"
    "admin"
    "transactions"
    "cluster"
)

SASL_TESTS=("auth")
TLS_TESTS=("tls")
ACL_TESTS=("acl")
KERBEROS_TESTS=("kerberos" "kerberos_service_ticket" "reproduce_0x7b")
KERBEROS_MULTI_TESTS=("kerberos_multi")

# 如果未设置 KAFKA_BOOTSTRAP，设为默认的 3-broker 地址
DEFAULT_BOOTSTRAP="127.0.0.1:29093,127.0.0.1:29095,127.0.0.1:29097"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-${DEFAULT_BOOTSTRAP}}"

cd "${PROJECT_ROOT}"
MUSL_TARGET="${MUSL_TARGET:-x86_64-unknown-linux-musl}"
SKIP_MUSL_BUILD=""
# Multi-broker Kerberos tests run inside a debian:trixie-slim test-runner
# container, so the test binary must be statically linked (musl) to avoid
# depending on the host's glibc. If musl is unavailable or the build fails,
# mark the run as FAILED rather than silently skipping kerberos_multi, so its
# integration coverage stays mandatory.
MUSL_BUILD_FAILED=""
if ! rustup target list --installed | grep -q "${MUSL_TARGET}"; then
    rustup target add "${MUSL_TARGET}" || {
        echo "  ERROR: cannot install musl target — multi-broker Kerberos integration tests cannot run"
        SKIP_MUSL_BUILD=1
        MUSL_BUILD_FAILED=1
    }
fi
if [ -z "${SKIP_MUSL_BUILD:-}" ] \
    && cargo test --no-run --target "${MUSL_TARGET}" \
        --features integration_tests --test kerberos_multi 2>/dev/null; then
    mkdir -pv target/test-bin
    BIN="$(ls target/${MUSL_TARGET}/debug/deps/kerberos_multi-* 2>/dev/null | grep -v '\.d$' | head -1)"
    if [ -n "${BIN}" ]; then
        cp -v "${BIN}" target/test-bin/kerberos_multi
        echo "  musl test binary ready: target/test-bin/kerberos_multi"
    else
        echo "  ERROR: musl test binary not found — multi-broker Kerberos integration tests cannot run"
        SKIP_MUSL_BUILD=1
        MUSL_BUILD_FAILED=1
    fi
else
    echo "  ERROR: musl build failed — multi-broker Kerberos integration tests cannot run"
    SKIP_MUSL_BUILD=1
    MUSL_BUILD_FAILED=1
fi

# Auto-detect container CLI
detect_cli() {
    if command -v podman &>/dev/null; then
        echo "podman"
    elif command -v docker &>/dev/null; then
        echo "docker"
    else
        echo "docker"
    fi
}
CLI="${KAFKA_CLI:-$(detect_cli)}"
COMPOSE_CMD="${CLI} compose"

cd "${SCRIPT_DIR}"

echo "  Container CLI: ${CLI}"

# ---------------------------------------------------------------------------
# 1. Start compose stacks
# ---------------------------------------------------------------------------
echo "=== Stopping any leftover Kafka test containers ==="
${COMPOSE_CMD} -f docker-compose.yml down -v 2>/dev/null || podman rm -f kafka-1 kafka-2 kafka-3 2>/dev/null || true
${COMPOSE_CMD} -f docker-compose.sasl.yml down -v 2>/dev/null || podman rm -f kafka-sasl-broker 2>/dev/null || true
${COMPOSE_CMD} -f docker-compose.tls.yml down -v 2>/dev/null || podman rm -f kafka-tls-broker 2>/dev/null || true
${COMPOSE_CMD} -f docker-compose.acl.yml down -v 2>/dev/null || podman rm -f kafka-acl-broker 2>/dev/null || true
# Clean up kerberos keytabs before starting fresh
rm -rf "${SCRIPT_DIR}/fixtures/kerberos/keytabs"
${COMPOSE_CMD} -f docker-compose.kerberos.yml down -v 2>/dev/null || true
${COMPOSE_CMD} -f docker-compose.kerberos-multi.yml down -v 2>/dev/null || true
rm -rf "${SCRIPT_DIR}/fixtures/kerberos-multi/keytabs"

echo "=== Starting 3-broker cluster (docker-compose.yml) ==="
echo "    CLI: ${CLI}, Image: ${KAFKA_IMAGE}"
KAFKA_IMAGE="${KAFKA_IMAGE}" ${COMPOSE_CMD} -f docker-compose.yml up -d

echo "=== Starting SASL broker (docker-compose.sasl.yml) ==="
KAFKA_IMAGE="${KAFKA_IMAGE}" ${COMPOSE_CMD} -f docker-compose.sasl.yml up -d

echo "=== Starting TLS broker (docker-compose.tls.yml) ==="
KAFKA_IMAGE="${KAFKA_IMAGE}" ${COMPOSE_CMD} -f docker-compose.tls.yml up -d

echo "=== Starting ACL broker (docker-compose.acl.yml) ==="
KAFKA_IMAGE="${KAFKA_IMAGE}" ${COMPOSE_CMD} -f docker-compose.acl.yml up -d

echo "=== Starting KDC + Kerberos Kafka (docker-compose.kerberos.yml) ==="
KAFKA_IMAGE="${KAFKA_IMAGE}" ${COMPOSE_CMD} -f docker-compose.kerberos.yml up -d --build
# KDC 初始化后生成 keytabs, Kafka 通过 depends_on:condition:service_healthy 自动启动

echo "=== Starting KDC + Kerberos multi-broker (docker-compose.kerberos-multi.yml) ==="
# 该 stack 使用独立 realm MULTI.EXAMPLE.COM 与独立 KDC 别名
# kdc-multi.example.com, 因此可与上面的单节点 stack 并行运行。
# 陈旧 keytab 对应已删除的 KDC 数据库, 必须清理后由 KDC 重新导出。
rm -rf "${SCRIPT_DIR}/fixtures/kerberos-multi/keytabs"
KAFKA_IMAGE="${KAFKA_IMAGE}" ${COMPOSE_CMD} -f docker-compose.kerberos-multi.yml up -d --build

# ---------------------------------------------------------------------------
# 2. Wait for brokers to be ready
# ---------------------------------------------------------------------------
wait_broker() {
    local container="$1" internal_port="$2" host_port="$3"
    local max_retries="${4:-60}"
    echo -n "  ${container} (port ${host_port})... "
    for i in $(seq 1 "${max_retries}"); do
        if ${CLI} exec "${container}" \
            kafka-broker-api-versions.sh \
            --bootstrap-server "127.0.0.1:${internal_port}" &>/dev/null; then
            echo "ready (~${i}s)"
            return 0
        fi
        if ${CLI} exec "${container}" \
            bash -c "echo > /dev/tcp/127.0.0.1/${internal_port}" 2>/dev/null; then
            sleep 3
            echo "ready (~${i}s, port)"
            return 0
        fi
        # 容器已退出时立即失败, 不必空等到超时。启动崩溃 (如 Kerberos
        # "Checksum failed") 会让容器直接 Exited, 再等下去也不会就绪。
        local state
        state="$(${CLI} inspect -f '{{.State.Status}}' "${container}" 2>/dev/null || echo "missing")"
        if [ "${state}" != "running" ]; then
            echo "container ${state} (crashed after ~${i}s)"
            return 1
        fi
        sleep 1
    done
    echo "timeout"
    return 1
}

# 打印容器启动失败的致命错误, 便于定位崩溃原因
diagnose_broker() {
    local container="$1"
    echo "  --- ${container} fatal errors ---"
    ${CLI} logs "${container}" 2>&1 \
        | grep -iE "ERROR|FATAL|Caused by" \
        | head -10 \
        | sed 's/^/    /' || true
}

echo "=== Waiting for 3-broker cluster to be ready ==="
wait_broker "kafka-1" 9092 29093 || {
    echo "ERROR: kafka-1 not ready"
    ${COMPOSE_CMD} -f docker-compose.yml logs --tail=20 kafka-1
    ${COMPOSE_CMD} -f docker-compose.yml down -v
    ${COMPOSE_CMD} -f docker-compose.sasl.yml down -v
    exit 1
}
wait_broker "kafka-2" 9092 29095 || {
    echo "ERROR: kafka-2 not ready"
    ${COMPOSE_CMD} -f docker-compose.yml logs --tail=20 kafka-2
    ${COMPOSE_CMD} -f docker-compose.yml down -v
    ${COMPOSE_CMD} -f docker-compose.sasl.yml down -v
    exit 1
}
wait_broker "kafka-3" 9092 29097 || {
    echo "ERROR: kafka-3 not ready"
    ${COMPOSE_CMD} -f docker-compose.yml logs --tail=20 kafka-3
    ${COMPOSE_CMD} -f docker-compose.yml down -v
    ${COMPOSE_CMD} -f docker-compose.sasl.yml down -v
    exit 1
}

echo "=== Waiting for SASL broker to be ready ==="
wait_broker "kafka-sasl-broker" 9094 9094 || {
    echo "WARNING: SASL broker not ready — SASL tests may be skipped"
}

echo "=== Waiting for TLS broker to be ready ==="
wait_broker "kafka-tls-broker" 9093 9093 || {
    echo "WARNING: TLS broker not ready — TLS tests may be skipped"
}

echo "=== Waiting for ACL broker to be ready ==="
wait_broker "kafka-acl-broker" 9098 9098 120 || {
    echo "WARNING: ACL broker not ready — ACL tests may be skipped"
}

echo "=== Waiting for Kerberos Kafka broker to be ready ==="
wait_broker "kafka-kerberos-broker" 9096 9096 120 || {
    echo "WARNING: Kerberos broker not ready — Kerberos tests may be skipped"
    diagnose_broker "kafka-kerberos-broker"
}

echo "=== Waiting for Kerberos multi-broker cluster to be ready ==="
MULTI_READY=1
for i in 1 2 3; do
    # 每台 broker 监听各自的端口: 19096 / 19097 / 19098
    # (见 docker-compose.kerberos-multi.yml 的 KAFKA_LISTENERS)
    port=$((19095 + i))
    if ! wait_broker "kafka-kerberos-${i}" "${port}" "${port}" 120; then
        echo "WARNING: kafka-kerberos-${i} not ready — multi-broker Kerberos tests skipped"
        diagnose_broker "kafka-kerberos-${i}"
        MULTI_READY=""
    fi
done

# ---------------------------------------------------------------------------
# 3. Run all integration tests
# ---------------------------------------------------------------------------
cd "${PROJECT_ROOT}"

echo "=== Running integration tests ==="

run_tests() {
    local test_name="$1"
    if [ -n "${TEST_FILTER:-}" ] && [[ "${test_name}" != *"${TEST_FILTER}"* ]]; then
        echo "  [SKIP] ${test_name} (filter: ${TEST_FILTER})"
        return 0
    fi
    echo "  [RUN] ${test_name}"
    KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP}" \
    KAFKA_BOOTSTRAP_SASL="${KAFKA_BOOTSTRAP_SASL:-127.0.0.1:9094}" \
    KAFKA_BOOTSTRAP_TLS="${KAFKA_BOOTSTRAP_TLS:-127.0.0.1:9093}" \
    KAFKA_BOOTSTRAP_KERBEROS="${KAFKA_BOOTSTRAP_KERBEROS:-127.0.0.1:9096}" \
    KERBEROS_KEYTAB="${KERBEROS_KEYTAB:-${SCRIPT_DIR}/fixtures/kerberos/keytabs/client.keytab}" \
    KERBEROS_KDC_HOST="${KERBEROS_KDC_HOST:-localhost}" \
    KERBEROS_KDC_PORT="${KERBEROS_KDC_PORT:-8888}" \
    KAFKA_CLUSTER_SIZE="${KAFKA_CLUSTER_SIZE:-3}" \
    SASL_MECHANISM="${SASL_MECHANISM:-PLAIN}" \
    SASL_USERNAME="${SASL_USERNAME:-admin}" \
    SASL_PASSWORD="${SASL_PASSWORD:-admin-secret}" \
    RUST_TEST_THREADS=1 \
    cargo test --test "${test_name}" --features integration_tests -- --nocapture 2>&1
}

TEST_EXIT_CODE=0

echo ""
echo "--- 3-broker cluster tests ---"
for test in "${THREE_BROKER_TESTS[@]}"; do
    run_tests "${test}" || TEST_EXIT_CODE=$?
done

echo ""
echo "--- SASL auth tests ---"
for test in "${SASL_TESTS[@]}"; do
    run_tests "${test}" || TEST_EXIT_CODE=$?
done

echo ""
echo "--- TLS tests ---"
for test in "${TLS_TESTS[@]}"; do
    run_tests "${test}" || TEST_EXIT_CODE=$?
done

echo ""
echo "--- ACL tests ---"
for test in "${ACL_TESTS[@]}"; do
    KAFKA_BOOTSTRAP_ACL="127.0.0.1:9098" \
    KAFKA_CLUSTER_SIZE=1 \
    run_tests "${test}" || TEST_EXIT_CODE=$?
done

echo ""
echo "--- Kerberos tests ---"
for test in "${KERBEROS_TESTS[@]}"; do
    # Kerberos 连接的是单节点 broker (port 9096)，而非 3-broker 集群
    KAFKA_CLUSTER_SIZE=1 \
    KAFKA_BOOTSTRAP_KERBEROS="127.0.0.1:9096" \
    KERBEROS_KEYTAB="${SCRIPT_DIR}/fixtures/kerberos/keytabs/client.keytab" \
    KERBEROS_KDC_HOST="localhost" \
    KERBEROS_KDC_PORT="8888" \
    run_tests "${test}" || TEST_EXIT_CODE=$?
done

echo ""
echo "--- Kerberos multi-broker tests ---"
cd "${SCRIPT_DIR}"

if [ -z "${SKIP_MUSL_BUILD:-}" ]; then
    if [ -n "${MULTI_READY:-}" ]; then
        for test in "${KERBEROS_MULTI_TESTS[@]}"; do
            echo "  [RUN] ${test} (in-container)"
            # 测试在 compose 网络内的 test-runner 容器中运行, 容器网络解析
            # broker1/2/3.example.com 与 kdc-multi.example.com, 无需宿主机 /etc/hosts。
            ${COMPOSE_CMD} -f docker-compose.kerberos-multi.yml run --rm test-runner \
                || TEST_EXIT_CODE=$?
        done
    else
        echo "  SKIPPED: multi-broker Kerberos cluster not ready"
        TEST_EXIT_CODE=1
    fi
fi

# ---------------------------------------------------------------------------
# 4. Cleanup
# ---------------------------------------------------------------------------
echo ""
echo "=== Cleaning up ==="
if [ -z "${SKIP_CLEANUP:-}" ]; then
    cd "${SCRIPT_DIR}"
    ${COMPOSE_CMD} -f docker-compose.yml down -v 2>/dev/null || podman rm -f kafka-1 kafka-2 kafka-3 2>/dev/null || true
    ${COMPOSE_CMD} -f docker-compose.sasl.yml down -v 2>/dev/null || podman rm -f kafka-sasl-broker 2>/dev/null || true
    ${COMPOSE_CMD} -f docker-compose.tls.yml down -v 2>/dev/null || podman rm -f kafka-tls-broker 2>/dev/null || true
    ${COMPOSE_CMD} -f docker-compose.acl.yml down -v 2>/dev/null || podman rm -f kafka-acl-broker 2>/dev/null || true
    rm -rf "${SCRIPT_DIR}/fixtures/kerberos/keytabs"
    ${COMPOSE_CMD} -f docker-compose.kerberos.yml down -v 2>/dev/null || true
    rm -rf "${SCRIPT_DIR}/fixtures/kerberos-multi/keytabs"
    ${COMPOSE_CMD} -f docker-compose.kerberos-multi.yml down -v 2>/dev/null || true
else
    echo "  SKIP_CLEANUP set — leaving clusters running"
fi

echo ""
if [ -n "${MUSL_BUILD_FAILED:-}" ] && [ "${TEST_EXIT_CODE}" -eq 0 ]; then
    echo "=== ERROR: musl build for multi-broker Kerberos tests failed (not just skipped) ==="
    TEST_EXIT_CODE=1
fi
if [ "${TEST_EXIT_CODE}" -eq 0 ]; then
    echo "=== All tests PASSED ==="
else
    echo "=== Some tests FAILED (exit code: ${TEST_EXIT_CODE}) ==="
fi
exit "${TEST_EXIT_CODE}"
