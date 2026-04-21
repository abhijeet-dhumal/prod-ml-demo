#!/usr/bin/env bash
# wait-and-materialize.sh
#
# Polls until all three Spark ETL jobs COMPLETE, then runs feast materialize-incremental.
# Run this in a terminal and leave it in the background while you work on other phases.
#
# USAGE:
#   source .env
#   bash scripts/wait-and-materialize.sh
#
# What it does:
#   1. Polls oc get sparkapplication every 30s until all RUNNING jobs reach COMPLETED
#   2. Prints [METRIC] lines from each driver pod
#   3. Runs `feast materialize-incremental` via the feast pod
#   4. Verifies Redis has feature keys
#   5. Calls collect-run-metrics.sh for rapids and cpu runs
set -euo pipefail

NAMESPACE="${NAMESPACE:-smartshop}"
APPS=(
  "smartshop-feature-engineering-rapids"
  "smartshop-feature-engineering-cpu-baseline"
  "smartshop-text-preprocessing"
)

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── 1. Wait for all SparkApps to complete ─────────────────────────────────────

log "Waiting for SparkApps to complete..."
while true; do
  all_done=true
  for app in "${APPS[@]}"; do
    status=$(oc get sparkapplication "$app" -n "$NAMESPACE" \
      -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "UNKNOWN")
    log "  $app → $status"
    if [[ "$status" != "COMPLETED" && "$status" != "FAILED" ]]; then
      all_done=false
    fi
  done

  if $all_done; then
    log "All SparkApps finished."
    break
  fi
  sleep 30
done

# ── 2. Print key metrics from each driver ─────────────────────────────────────

log ""
log "══ SparkApp Metrics Summary ═══════════════════════════════"
for app in "${APPS[@]}"; do
  status=$(oc get sparkapplication "$app" -n "$NAMESPACE" \
    -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "UNKNOWN")
  log "  [$status] $app"
  DRIVER_POD="${app}-driver"
  if oc get pod "$DRIVER_POD" -n "$NAMESPACE" &>/dev/null; then
    oc logs "$DRIVER_POD" -n "$NAMESPACE" 2>/dev/null | \
      grep "\[METRIC\]" | sed 's/^/      /' || true
  fi
done
log "══════════════════════════════════════════════════════════"

# Exit early if any job failed
for app in "${APPS[@]}"; do
  status=$(oc get sparkapplication "$app" -n "$NAMESPACE" \
    -o jsonpath='{.status.appState.state}' 2>/dev/null)
  if [[ "$status" == "FAILED" ]]; then
    log "ERROR: $app FAILED — check logs before materializing."
    log "  oc logs -n $NAMESPACE ${app}-driver | tail -50"
    exit 1
  fi
done

# ── 3. Feast materialize-incremental ──────────────────────────────────────────

FEAST_POD=$(oc get pod -n "$NAMESPACE" -l "feast.dev/name=smartshop-feast" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [[ -z "$FEAST_POD" ]]; then
  log "ERROR: feast pod not found in namespace $NAMESPACE"
  exit 1
fi

log "Running feast materialize-incremental via $FEAST_POD ..."
MATL_START=$(date +%s)

oc exec -n "$NAMESPACE" "$FEAST_POD" -c online -- \
  feast -c /feast-data/smartshop/feast/feature_repo materialize-incremental \
    "$(date -u +%Y-%m-%dT%H:%M:%S)" 2>&1 | tee /tmp/feast-materialize.log

MATL_ELAPSED=$(( $(date +%s) - MATL_START ))
log "feast materialize-incremental done in ${MATL_ELAPSED}s"

# ── 4. Verify Redis has feature keys ──────────────────────────────────────────

log "Verifying Redis feature keys..."
REDIS_HOST="${REDIS_HOST:-redis.smartshop.svc.cluster.local}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_PASS="${REDIS_PASSWORD:-}"

KEY_COUNT=$(oc exec -n "$NAMESPACE" "$FEAST_POD" -c online -- python3 -c "
import redis
r = redis.Redis(host='$REDIS_HOST', port=$REDIS_PORT, password='$REDIS_PASS', decode_responses=True)
keys = r.keys('*')
print(len(keys))
" 2>/dev/null || echo "0")

log "Redis feature keys: $KEY_COUNT"
if [[ "$KEY_COUNT" -gt 0 ]]; then
  log "✅ Feast materialization verified — $KEY_COUNT keys in Redis"
else
  log "⚠️  Redis appears empty — check feast materialize logs at /tmp/feast-materialize.log"
fi

# ── 5. Collect run metrics bundles ────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log "Collecting RAPIDS metrics bundle..."
RUN_TYPE=rapids APP_NAME=smartshop-feature-engineering-rapids \
  bash "${SCRIPT_DIR}/collect-run-metrics.sh" || true

log "Collecting CPU baseline metrics bundle..."
RUN_TYPE=cpu APP_NAME=smartshop-feature-engineering-cpu-baseline \
  bash "${SCRIPT_DIR}/collect-run-metrics.sh" || true

log "Collecting feast metrics bundle..."
RUN_TYPE=feast bash "${SCRIPT_DIR}/collect-run-metrics.sh" || true

log ""
log "══ All done. Next steps: ══════════════════════════════════"
log "  Phase 5: Submit TrainJob"
log "    oc apply -f /tmp/rec-trainjob.yaml"
log "  Phase 7: Run metrics notebook"
log "    jupyter notebook notebooks/metrics_analysis.ipynb"
log "═══════════════════════════════════════════════════════════"
