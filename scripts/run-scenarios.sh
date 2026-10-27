#!/usr/bin/env bash
# ==================================================================================================
# scripts/run-scenarios.sh
# Runs Scenario Lab scenarios against a running stack and prints each report.
# usage: scripts/run-scenarios.sh [S01 S02 ...]      (default: all except S04, which needs the broker stopped)
#
# What it does: for each scenario code it asks the Web to start a run (POST /demo/scenarios/{code}/run),
# polls the run report until it is no longer "Running", prints one summary line, and finally exits
# non-zero if any scenario did not PASS. That makes it usable as a smoke test in CI.
# Needs: curl and sed only (no jq), and a stack started with run-local.sh or compose (Development mode).
#
# Read next: src/MiniShop.Web/Scenarios/ScenarioCatalog.cs (what each scenario does and checks).
# ==================================================================================================

# Strict mode: stop on errors, undefined variables and failed pipelines.
set -euo pipefail
# Base URL of the Web; override with MINISHOP_WEB_URL=... if it runs elsewhere. ${VAR:-default} = default if unset.
WEB=${MINISHOP_WEB_URL:-http://localhost:5100}
# Demo token sent in the X-Demo-Token header (must match Demo:Token of the services).
TOKEN=${MINISHOP_DEMO_TOKEN:-minishop-dev}
# Scenario codes: the script arguments ($*) or, if none were given, every scenario except S04
# (S04 stops RabbitMQ and needs the optional runner from the "faults" compose profile).
CODES=${*:-S01 S02 S03 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15 S16}
# Becomes 1 as soon as one scenario does not pass; used as the exit code at the end.
failed=0
for code in $CODES; do
  # Start the scenario. The response is JSON describing the new run; -sS = silent but show errors.
  run=$(curl -sS -X POST -H "X-Demo-Token: $TOKEN" "$WEB/demo/scenarios/$code/run")
  # Extract the run id from the JSON with sed (keeps the script free of a jq dependency):
  # \( ... \) captures the characters after "id":" up to the next quote, and \1 prints only them.
  id=$(echo "$run" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  # Poll until the run finishes. Scenarios run asynchronously (sagas, timeouts), some take ~30-60 s.
  status=Running
  while [ "$status" = "Running" ]; do
    # Do not hammer the server: one poll every 2 seconds.
    sleep 2
    # Fetch the current report of this run.
    report=$(curl -sS -H "X-Demo-Token: $TOKEN" "$WEB/demo/scenarios/runs/$id")
    # Extract the run's status (Running / PASSED / FAILED ...) the same way; head -1 guarantees a single value.
    status=$(echo "$report" | sed -n 's/.*"status":"\([A-Za-z]*\)".*/\1/p' | head -1)
  done
  # One-line human summary from the report.
  summary=$(echo "$report" | sed -n 's/.*"summary":"\([^"]*\)".*/\1/p')
  # Print code, verdict, summary and a link to the full report in the developer console.
  echo "$code  $status  — $summary   ($WEB/developer/scenarios/$id)"
  # Any status other than PASSED marks the whole run as failed (but we keep running the others).
  [ "$status" = "PASSED" ] || failed=1
done
# 0 = all passed, 1 = at least one did not.
exit $failed
