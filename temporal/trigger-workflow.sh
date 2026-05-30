#!/usr/bin/env bash
# Trigger a PullCompanies workflow via the Temporal CLI inside the running container.
# Usage: ./trigger-workflow.sh [source] [country]
# Example: ./trigger-workflow.sh companies_house GB

SOURCE=${1:-companies_house}
COUNTRY=${2:-GB}

set -euo pipefail

TEMPORAL_NAMESPACE=${TEMPORAL_NAMESPACE:-corpscout}
TEMPORAL_NAMESPACE_RETENTION=${TEMPORAL_NAMESPACE_RETENTION:-168h}
TASK_QUEUE=${TEMPORAL_TASK_QUEUE:-corpscout-pipelines}

# Map source name to workflow type.
case "$SOURCE" in
  companies_house) WORKFLOW_TYPE="PullCompaniesHouse" ;;
  brreg)           WORKFLOW_TYPE="PullBrreg" ;;
  *)               echo "Unknown source: $SOURCE"; exit 1 ;;
esac

WORKFLOW_ID="manual-${SOURCE}-${COUNTRY}-$(date +%Y%m%d-%H%M%S)"

echo "Starting workflow:"
echo "  ID:         $WORKFLOW_ID"
echo "  Type:       $WORKFLOW_TYPE"
echo "  Namespace:  $TEMPORAL_NAMESPACE"
echo "  Task queue: $TASK_QUEUE"
echo "  Input:      {\"country\":\"$COUNTRY\"}"
echo ""

if ! docker compose exec -T -e TEMPORAL_ADDRESS=temporal:7233 temporal \
  temporal operator namespace describe -n "$TEMPORAL_NAMESPACE" >/dev/null 2>&1; then
  echo "Namespace $TEMPORAL_NAMESPACE not found. Creating it..."
  docker compose exec -T -e TEMPORAL_ADDRESS=temporal:7233 temporal \
    temporal operator namespace create \
    --namespace "$TEMPORAL_NAMESPACE" \
    --retention "$TEMPORAL_NAMESPACE_RETENTION"
fi

docker compose exec -T -e TEMPORAL_ADDRESS=temporal:7233 temporal \
  temporal workflow start \
  --namespace "$TEMPORAL_NAMESPACE" \
  --task-queue "$TASK_QUEUE" \
  --type "$WORKFLOW_TYPE" \
  --workflow-id "$WORKFLOW_ID" \
  --input "{\"country\":\"$COUNTRY\"}"
