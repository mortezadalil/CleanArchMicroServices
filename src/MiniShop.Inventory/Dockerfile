# Build context is the repository root (contracts/ and shared/ are linked source).
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props Directory.Packages.props global.json ./
COPY contracts/ contracts/
COPY shared/ shared/
COPY src/MiniShop.Inventory/ src/MiniShop.Inventory/
RUN dotnet publish src/MiniShop.Inventory/MiniShop.Inventory.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "MiniShop.Inventory.dll"]
