#!/usr/bin/env bash
# =============================================================================
# download-kafka.sh — 下载并安装 Kafka 用于集成测试
#
# 用法:
#   ./tests/download-kafka.sh                # 下载默认版本 4.3.0
#   ./tests/download-kafka.sh 3.9.0          # 下载指定版本
#   KAFKA_MIRROR=https://... ./tests/download-kafka.sh  # 自定义镜像
#
# 环境变量:
#   KAFKA_VERSION    Kafka 版本（默认 4.3.0）
#   KAFKA_MIRROR     下载镜像基址（默认自动选择）
#   KAFKA_HOME       安装目标目录（默认 tests/kafka）
#   KAFKA_CLEAN      设为 true 则先删除已有安装
# =============================================================================

set -euo pipefail

# ── 配置 ──────────────────────────────────────────────────────────────────

KAFKA_VERSION="${KAFKA_VERSION:-${1:-4.3.0}}"
KAFKA_HOME="${KAFKA_HOME:-$(cd "$(dirname "$0")" && pwd)/kafka}"
SCALA_VERSION="2.13"

# 镜像列表（按优先级排序），通过 KAFKA_MIRROR 覆盖
MIRRORS=(
    "${KAFKA_MIRROR:-}"
    "https://dlcdn.apache.org/kafka"
    "https://mirrors.tuna.tsinghua.edu.cn/apache/kafka"
    "https://mirrors.ustc.edu.cn/apache/kafka"
    "https://archive.apache.org/dist/kafka"
)

# ── 颜色输出 ──────────────────────────────────────────────────────────────

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── 清理 ──────────────────────────────────────────────────────────────────

if [[ "${KAFKA_CLEAN:-false}" == "true" && -d "$KAFKA_HOME" ]]; then
    info "Cleaning existing installation at $KAFKA_HOME"
    rm -rf "$KAFKA_HOME"
fi

# ── 下载 ──────────────────────────────────────────────────────────────────

ARCHIVE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
TARGET_DIR="$KAFKA_HOME"

if [ -f "$TARGET_DIR/libs/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.jar" ]; then
    info "Kafka ${KAFKA_VERSION} already installed at ${TARGET_DIR}"
    exit 0
fi

mkdir -p "$KAFKA_HOME"
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

DOWNLOADED=false
for mirror in "${MIRRORS[@]}"; do
    [ -z "$mirror" ] && continue
    URL="${mirror}/${KAFKA_VERSION}/${ARCHIVE}"
    info "Trying ${URL} ..."

    if command -v curl &>/dev/null; then
        HTTP_CODE=$(curl -fLSs --connect-timeout 10 --max-time 300 -o "${WORK_DIR}/${ARCHIVE}" -w "%{http_code}" "$URL" 2>/dev/null || echo "000")
    elif command -v wget &>/dev/null; then
        HTTP_CODE=$(wget --timeout=10 --tries=2 -q -O "${WORK_DIR}/${ARCHIVE}" "$URL" 2>/dev/null && echo "200" || echo "000")
    else
        error "Neither curl nor wget found"; exit 1
    fi

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "302" ]; then
        DOWNLOADED=true
        info "Downloaded ${ARCHIVE} (HTTP ${HTTP_CODE})"
        break
    fi
    warn "HTTP ${HTTP_CODE}, trying next mirror..."
done

if [ "$DOWNLOADED" = false ]; then
    error "Failed to download Kafka ${KAFKA_VERSION} from any mirror"
    echo ""
    echo "  You can manually download from:"
    echo "    https://kafka.apache.org/downloads"
    echo "    https://downloads.apache.org/kafka/${KAFKA_VERSION}/${ARCHIVE}"
    echo ""
    echo "  Then extract to: ${TARGET_DIR}"
    echo "    tar xzf ${ARCHIVE} -C $(dirname "$TARGET_DIR")"
    echo "    mv $(dirname "$TARGET_DIR")/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${TARGET_DIR}"
    exit 1
fi

# ── 校验（gpg 签名检查，可选） ──────────────────────────────────────────

if command -v gpg &>/dev/null && [ -n "${KAFKA_VERIFY:-}" ]; then
    info "Verifying signature ..."
    KEYS_URL="https://downloads.apache.org/kafka/KEYS"
    ASC_URL="${URL}.asc"
    curl -fLSs "$KEYS_URL" 2>/dev/null | gpg --import 2>/dev/null || true
    curl -fLSs "$ASC_URL" -o "${WORK_DIR}/${ARCHIVE}.asc" 2>/dev/null && \
        gpg --verify "${WORK_DIR}/${ARCHIVE}.asc" "${WORK_DIR}/${ARCHIVE}" && \
        info "Signature verified" || warn "Signature verification skipped"
fi

# ── 解压 ──────────────────────────────────────────────────────────────────

[ -d "$TARGET_DIR" ] && rm -rf "$TARGET_DIR"
mkdir -p "$(dirname "$TARGET_DIR")"

info "Extracting to ${TARGET_DIR} ..."
tar xzf "${WORK_DIR}/${ARCHIVE}" -C "$(dirname "$TARGET_DIR")"
mv "$(dirname "$TARGET_DIR")/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "$TARGET_DIR"

# 验证安装
if [ -f "$TARGET_DIR/bin/kafka-storage.sh" ]; then
    info "Kafka ${KAFKA_VERSION} installed successfully at ${TARGET_DIR}"
    # 清理不用保留的 docs / site-docs
    rm -rf "$TARGET_DIR/site-docs" "$TARGET_DIR/licenses" 2>/dev/null || true
    info "Size: $(du -sh "$TARGET_DIR" 2>/dev/null | cut -f1)"
else
    error "Installation appears incomplete — bin/kafka-storage.sh not found"
    exit 1
fi
