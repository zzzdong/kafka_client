#!/usr/bin/env bash
#
# Publish all crates in this workspace to crates.io in dependency order.
#
# Usage:
#   ./scripts/publish.sh              # dry-run (check only)
#   ./scripts/publish.sh --execute    # actually publish
#
set -euo pipefail
IFS=$'\n\t'

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ------------------------------------------------------------------
# Ordered list of crates to publish (directories relative to root).
# kafka-client-protocol-codegen is excluded (publish = false).
# ------------------------------------------------------------------
CRATES=(
    "kafka-client-protocol-derive"
    "kafka-client-protocol-core"
    "kafka-client-protocol"
)

MAIN_CRATE="."  # kafka_client (root package)

# ------------------------------------------------------------------
# Determine mode: dry-run or actual publish
# ------------------------------------------------------------------
MODE="${1:---dry-run}"
if [ "$MODE" = "--execute" ]; then
    DRY_RUN=""
    echo ">>> Publishing crates for real <<<"
else
    DRY_RUN="--dry-run"
    echo ">>> Dry-run mode (pass --execute to actually publish) <<<"
fi
echo ""

# ------------------------------------------------------------------
# 1. Check cargo login status
# ------------------------------------------------------------------
echo "==> Checking crates.io authentication..."
if ! cargo token list &>/dev/null; then
    echo "ERROR: Not logged in to crates.io. Run 'cargo login' first."
    exit 1
fi
echo "    OK - authenticated to crates.io"
echo ""

# ------------------------------------------------------------------
# 2. Publish sub-crates first (dependency order)
# ------------------------------------------------------------------
for crate in "${CRATES[@]}"; do
    echo "==> Publishing $crate (sub-crate) ..."
    (
        cd "$ROOT/$crate"
        cargo publish $DRY_RUN
    )
    echo "    done"
    echo ""
done

# ------------------------------------------------------------------
# 3. Publish main crate last
# ------------------------------------------------------------------
echo "==> Publishing main crate (kafka_client) ..."
cargo publish $DRY_RUN
echo "    done"
echo ""

# ------------------------------------------------------------------
# 4. Summary
# ------------------------------------------------------------------
if [ -n "$DRY_RUN" ]; then
    echo "================================================================="
    echo "  Dry-run completed successfully!"
    echo "  To actually publish, re-run with:"
    echo "      ./scripts/publish.sh --execute"
    echo "================================================================="
else
    echo "================================================================="
    echo "  All crates published successfully!"
    echo "  Versions published:"
    echo "    - kafka-client-protocol-derive  (0.1.0)"
    echo "    - kafka-client-protocol-core    (0.1.0)"
    echo "    - kafka-client-protocol         (0.1.0)"
    echo "    - kafka_client                  (0.1.0)"
    echo "================================================================="
fi

