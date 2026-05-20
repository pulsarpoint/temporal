#!/usr/bin/env bash
# Trigger a PullCompanies workflow via the Temporal CLI inside the running container.
# Usage: ./trigger-workflow.sh [source] [country]
# Example: ./trigger-workflow.sh companies_house GB

SOURCE=${1:-companies_house}
COUNTRY=${2:-GB}
WORKFLOW_ID="manual-${SOURCE}-${COUNTRY}-$(date +%Y%m%d-%H%M%S)"

echo "Starting workflow:"
echo "  ID:         $WORKFLOW_ID"
echo "  Type:       PullCompanies"
echo "  Task queue: corpscout-pipelines"
echo "  Input:      {\"source\":\"$SOURCE\",\"country\":\"$COUNTRY\"}"
echo ""

docker compose exec -e TEMPORAL_ADDRESS=temporal:7233 temporal \
  temporal workflow start \
  --namespace corpscout \
  --task-queue corpscout-pipelines \
  --type PullCompanies \
  --workflow-id "$WORKFLOW_ID" \
  --input "{\"source\":\"$SOURCE\",\"country\":\"$COUNTRY\"}"
