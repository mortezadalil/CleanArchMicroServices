#!/usr/bin/env bash
# Starts RabbitMQ in Docker and the four projects with `dotnet run` (Development settings, ports 5100-5103).
# Stop everything with Ctrl+C.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f infra/compose.yaml up -d rabbitmq
export ASPNETCORE_ENVIRONMENT=Development
trap 'kill 0' EXIT
dotnet run --project src/MiniShop.Orders    --no-launch-profile --urls http://localhost:5101 &
dotnet run --project src/MiniShop.Inventory --no-launch-profile --urls http://localhost:5102 &
dotnet run --project src/MiniShop.Payments  --no-launch-profile --urls http://localhost:5103 &
dotnet run --project src/MiniShop.Web       --no-launch-profile --urls http://localhost:5100 &
echo "MiniShop: http://localhost:5100  (RabbitMQ UI: http://localhost:15672 guest/guest)"
wait
