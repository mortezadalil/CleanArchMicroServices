# ==================================================================================================
# src/MiniShop.Orders/Dockerfile - container image of the Orders service.
#
# WHAT THIS FILE IS
#   Recipe used by `docker compose -f infra/compose.yaml up --build` to build the Orders image.
#   It is a MULTI-STAGE build:
#     stage 1 ("build") uses the big .NET SDK image (compilers, NuGet) to compile and publish;
#     stage 2 uses the small ASP.NET runtime image and receives ONLY the published output.
#   The final image therefore contains no SDK and no source code: smaller, faster to pull, less to attack.
#
# WHY THE REPOSITORY ROOT IS THE BUILD CONTEXT
#   compose.yaml builds with `context: ..` (the repo root) and `dockerfile: src/MiniShop.Orders/Dockerfile`.
#   The .csproj compiles linked source from ../../contracts and ../../shared, and the build needs the
#   root-level Directory.*.props and global.json. Docker can only COPY files inside the build context,
#   so the context must be the root, not this folder.
#
# Read next: infra/compose.yaml (the "orders" service that uses this image).
# ==================================================================================================

# Build context is the repository root (contracts/ and shared/ are linked source).
# Stage 1: the .NET 10 SDK image, named "build" so stage 2 can copy from it.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
# All following paths are relative to /src inside the build container.
WORKDIR /src
# Root build settings: shared MSBuild properties, central package versions and the pinned SDK version.
COPY Directory.Build.props Directory.Packages.props global.json ./
# The message contracts and shared building blocks that MiniShop.Orders.csproj links in.
COPY contracts/ contracts/
COPY shared/ shared/
# The Orders project itself. Other services are not copied: they are not needed to build this one.
COPY src/MiniShop.Orders/ src/MiniShop.Orders/
# Restore packages, compile in Release mode and put the deployable files in /app.
RUN dotnet publish src/MiniShop.Orders/MiniShop.Orders.csproj -c Release -o /app

# Stage 2: the runtime-only ASP.NET Core image (no SDK). This is the image that actually runs.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
# Take only the published output from stage 1.
COPY --from=build /app .
# curl is needed by the compose healthcheck (`curl -fsS http://localhost:8080/health/live`): the runtime
# image does not ship it. Removing the apt lists afterwards keeps the image layer small.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
# Kestrel listens on port 8080 on all interfaces ("+") inside the container; compose maps it to 5101.
ENV ASPNETCORE_URLS=http://+:8080
# Documentation for humans and tools: this container serves on 8080 (does not publish the port by itself).
EXPOSE 8080
# Start the service. Exec form (JSON array) makes dotnet PID 1, so it receives SIGTERM for a clean shutdown
# and its exit code (e.g. 1 from /demo/crash, S09) is what Docker's restart policy sees.
ENTRYPOINT ["dotnet", "MiniShop.Orders.dll"]
