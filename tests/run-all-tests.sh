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
    "cluster"
)

SASL_TESTS=("auth")
TLS_TESTS=("tls")

# 如果未设置 KAFKA_BOOTSTRAP，设为默认的 3-broker 地址
DEFAULT_BOOTSTRAP="127.0.0.1:29093,127.0.0.1:29095,127.0.0.1:29097"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-${DEFAULT_BOOTSTRAP}}"

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

echo "=== Starting 3-broker cluster (docker-compose.yml) ==="
echo "    CLI: ${CLI}, Image: ${KAFKA_IMAGE}"
KAFKA_IMAGE="${KAFKA_IMAGE}" ${COMPOSE_CMD} -f docker-compose.yml up -d

echo "=== Starting SASL broker (docker-compose.sasl.yml) ==="
KAFKA_IMAGE="${KAFKA_IMAGE}" ${COMPOSE_CMD} -f docker-compose.sasl.yml up -d

echo "=== Starting TLS broker (docker-compose.tls.yml) ==="
KAFKA_IMAGE="${KAFKA_IMAGE}" ${COMPOSE_CMD} -f docker-compose.tls.yml up -d

# ---------------------------------------------------------------------------
# 2. Wait for brokers to be ready
# ---------------------------------------------------------------------------
wait_broker() {
    local container="$1" internal_port="$2" host_port="$3"
    echo -n "  ${container} (port ${host_port})... "
    for i in $(seq 1 60); do
        if ${CLI} exec "${container}" \
            kafka-broker-api-versions.sh \
            --bootstrap-server "127.0.0.1:${internal_port}" 2>/dev/null; then
            echo "ready (~${i}s)"
            return 0
        fi
        if ${CLI} exec "${container}" \
            bash -c "echo > /dev/tcp/127.0.0.1/${internal_port}" 2>/dev/null; then
            sleep 3
            echo "ready (~${i}s, port)"
            return 0
        fi
        sleep 1
    done
    echo "timeout"
    return 1
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
    KAFKA_BOOTSTRAP_SASL="127.0.0.1:9094" \
    KAFKA_BOOTSTRAP_TLS="127.0.0.1:9093" \
    KAFKA_CLUSTER_SIZE="3" \
    SASL_MECHANISM="${SASL_MECHANISM:-PLAIN}" \
    SASL_USERNAME="${SASL_USERNAME:-admin}" \
    SASL_PASSWORD="${SASL_PASSWORD:-admin-secret}" \
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
else
    echo "  SKIP_CLEANUP set — leaving clusters running"
fi

echo ""
if [ "${TEST_EXIT_CODE}" -eq 0 ]; then
    echo "=== All tests PASSED ==="
else
    echo "=== Some tests FAILED (exit code: ${TEST_EXIT_CODE}) ==="
fi
exit "${TEST_EXIT_CODE}"
