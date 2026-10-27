#!/usr/bin/env bash
# ==================================================================================================
# scripts/run-local.sh
# Starts RabbitMQ in Docker and the four projects with `dotnet run` (Development settings, ports 5100-5103).
# Stop everything with Ctrl+C.
#
# Why this mode? It is the best mode for DEBUGGING: you can attach a debugger to any service and edit
# code without rebuilding images. Only the broker runs in a container. In this mode the Scenario Lab
# simulates S09 with /demo/restart-bus (there is no container to kill) and S04 needs the "faults"
# runner or a manual `docker stop minishop-rabbitmq`.
#
# Read next: infra/compose.yaml (the all-in-Docker alternative), scripts/run-scenarios.sh.
# ==================================================================================================

# Strict mode: -e exit on the first failing command, -u error on undefined variables,
# -o pipefail a pipeline fails if ANY part fails (not only the last one).
set -euo pipefail
# Go to the repository root (the parent of this script's folder), so the script works from any directory.
cd "$(dirname "$0")/.."
# Start only the rabbitmq service from the compose file, detached (-d) in the background.
docker compose -f infra/compose.yaml up -d rabbitmq
# Load appsettings.Development.json in every service: enables the demo control plane and diagnostics.
export ASPNETCORE_ENVIRONMENT=Development
# When this script exits (Ctrl+C or an error), kill every process in its process group, i.e. all four
# `dotnet run` children below. Without this, Ctrl+C would leave orphaned services holding the ports.
trap 'kill 0' EXIT
# Start each service in the background (&). --no-launch-profile ignores Properties/launchSettings.json so
# the --urls given here are the only source of truth for the ports. Ports match infra/compose.yaml.
dotnet run --project src/MiniShop.Orders    --no-launch-profile --urls http://localhost:5101 &
dotnet run --project src/MiniShop.Inventory --no-launch-profile --urls http://localhost:5102 &
dotnet run --project src/MiniShop.Payments  --no-launch-profile --urls http://localhost:5103 &
dotnet run --project src/MiniShop.Web       --no-launch-profile --urls http://localhost:5100 &
# Tell the student where to go.
echo "MiniShop: http://localhost:5100  (RabbitMQ UI: http://localhost:15672 guest/guest)"
# Block until the background jobs end (normally: until Ctrl+C), then the EXIT trap cleans up.
wait
