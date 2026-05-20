#!/usr/bin/env bash
# Trigger a PullCompanies workflow via the Temporal CLI inside the running container.
# Usage: ./trigger-workflow.sh [source] [country]
# Example: ./trigger-workflow.sh companies_house GB

SOURCE=${1:-companies_house}
COUNTRY=${2:-GB}

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
echo "  Task queue: corpscout-pipelines"
echo "  Input:      {\"country\":\"$COUNTRY\"}"
echo ""

docker compose exec -e TEMPORAL_ADDRESS=temporal:7233 temporal \
  temporal workflow start \
  --namespace corpscout \
  --task-queue corpscout-pipelines \
  --type "$WORKFLOW_TYPE" \
  --workflow-id "$WORKFLOW_ID" \
  --input "{\"country\":\"$COUNTRY\"}"
