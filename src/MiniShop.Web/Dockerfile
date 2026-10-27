# ==================================================================================================
# FILE: src/MiniShop.Web/Dockerfile
#
# WHAT IT IS
#   The container image of MiniShop.Web (storefront + Developer Console, CQRS read side).
#   It is a MULTI-STAGE build: stage 1 uses the big .NET SDK image to compile, stage 2 copies only the
#   compiled output into the small ASP.NET runtime image.  The final image contains no compiler and
#   no source code, which makes it smaller and safer.
#
# HOW IT IS USED
#   docker-compose builds it with the REPOSITORY ROOT as build context, for example:
#     docker build -f src/MiniShop.Web/Dockerfile .
#
# Read next: MiniShop.Web.csproj (why contracts/ and shared/ must be copied), then Program.cs.
# ==================================================================================================

# Build context is the repository root (contracts/ and shared/ are linked source).
# ---- Stage 1: build ------------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
# Solution-wide build settings: target framework, nullable, central package versions, SDK version.
# Without them the .csproj would not know its TargetFramework or its package versions.
COPY Directory.Build.props Directory.Packages.props global.json ./
# The .csproj compiles these folders as linked source, so they must be inside the image too.
COPY contracts/ contracts/
COPY shared/ shared/
# Only THIS service's source: the image does not depend on the other services' code.
COPY src/MiniShop.Web/ src/MiniShop.Web/
# Restore, compile in Release and write the ready-to-run app into /app.
RUN dotnet publish src/MiniShop.Web/MiniShop.Web.csproj -c Release -o /app

# ---- Stage 2: runtime ----------------------------------------------------------------------------
# The ASP.NET Core runtime only (no SDK): much smaller.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
# Take just the published output from the build stage.
COPY --from=build /app .
# curl is installed for the docker-compose healthcheck (infra/compose.yaml calls /health/live with it).
# Cleaning the apt lists afterwards keeps the layer small.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
# Listen on port 8080 on all network interfaces inside the container ("+" = any host name).
ENV ASPNETCORE_URLS=http://+:8080
# Documentation for humans/tools: the container serves on 8080 (compose maps it to a host port).
EXPOSE 8080
# Start the Web.  readmodel.db is created at startup (see Program.cs, EnsureCreated).
ENTRYPOINT ["dotnet", "MiniShop.Web.dll"]
