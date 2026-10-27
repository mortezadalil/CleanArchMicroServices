# Dockerfile of the MiniShop Payments service (used by infra/compose.yaml).
#
# It is a MULTI-STAGE build: stage 1 uses the big .NET SDK image to compile and publish the app,
# stage 2 copies only the published output into the much smaller ASP.NET runtime image.
# The final image therefore contains no source code and no compiler.
#
# Build context is the repository root (contracts/ and shared/ are linked source).
# That is why every COPY below uses paths relative to the repository root, and why compose builds
# this file with "context: .." instead of "context: src/MiniShop.Payments": the .csproj compiles
# ../../contracts and ../../shared directly, so those folders must be inside the build context.
# Read next: MiniShop.Payments.csproj

# Stage 1 ("build"): the full .NET 10 SDK (compiler, NuGet, dotnet publish).
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
# All following commands run inside /src in the build container.
WORKDIR /src
# Solution-wide build settings: target framework and language options (Directory.Build.props), central
# NuGet package versions (Directory.Packages.props, the .csproj lists packages WITHOUT versions) and the
# pinned SDK version (global.json).  Without them the project would not restore or compile.
COPY Directory.Build.props Directory.Packages.props global.json ./
# The message contracts, compiled INTO this service as linked source (see the .csproj).
COPY contracts/ contracts/
# The shared building blocks (Outbox, Inbox, IdempotentConsumer, faults, telemetry), also linked source.
COPY shared/ shared/
# The Payments project itself.  Only this service is copied, so a change in another service does not
# invalidate this image's build cache.
COPY src/MiniShop.Payments/ src/MiniShop.Payments/
# Restore NuGet packages, compile in Release mode and write the ready-to-run output to /app.
RUN dotnet publish src/MiniShop.Payments/MiniShop.Payments.csproj -c Release -o /app

# Stage 2 (final image): only the ASP.NET Core runtime, no SDK.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
# The application lives in /app.  The relative database path "data/payments.db" would resolve under it,
# but compose sets Database__Path=/data/payments.db (a volume), so data survives container restarts.
WORKDIR /app
# Take only the published output from stage 1; everything else from the build stage is thrown away.
COPY --from=build /app .
# Install curl so compose's healthcheck can call http://localhost:8080/health/live inside the container.
# --no-install-recommends and removing the apt lists keep the image small.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
# Kestrel listens on port 8080 on all network interfaces ("+"), so other containers can reach it.
ENV ASPNETCORE_URLS=http://+:8080
# Documentation for humans and tools: the container serves on 8080 (compose decides the host port mapping).
EXPOSE 8080
# Start the service.  The exec (JSON array) form makes dotnet PID 1, so it receives Docker's stop signal
# directly and can shut down gracefully (finish the current message, stop the bus).
ENTRYPOINT ["dotnet", "MiniShop.Payments.dll"]
