#!/usr/bin/env bash
# Runs Scenario Lab scenarios against a running stack and prints each report.
# usage: scripts/run-scenarios.sh [S01 S02 ...]      (default: all except S04, which needs the broker stopped)
set -euo pipefail
WEB=${MINISHOP_WEB_URL:-http://localhost:5100}
TOKEN=${MINISHOP_DEMO_TOKEN:-minishop-dev}
CODES=${*:-S01 S02 S03 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15 S16}
failed=0
for code in $CODES; do
  run=$(curl -sS -X POST -H "X-Demo-Token: $TOKEN" "$WEB/demo/scenarios/$code/run")
  id=$(echo "$run" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  status=Running
  while [ "$status" = "Running" ]; do
    sleep 2
    report=$(curl -sS -H "X-Demo-Token: $TOKEN" "$WEB/demo/scenarios/runs/$id")
    status=$(echo "$report" | sed -n 's/.*"status":"\([A-Za-z]*\)".*/\1/p' | head -1)
  done
  summary=$(echo "$report" | sed -n 's/.*"summary":"\([^"]*\)".*/\1/p')
  echo "$code  $status  — $summary   ($WEB/developer/scenarios/$id)"
  [ "$status" = "PASSED" ] || failed=1
done
exit $failed
