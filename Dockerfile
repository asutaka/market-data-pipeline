# Stage 1: Build source code
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /app

# Copy csproj and restore
COPY src/MarketDataEngine/MarketDataEngine.csproj ./src/MarketDataEngine/
RUN dotnet restore ./src/MarketDataEngine/MarketDataEngine.csproj

# Copy the rest and publish
COPY src/MarketDataEngine/ ./src/MarketDataEngine/
WORKDIR /app/src/MarketDataEngine
RUN dotnet publish -c Release -o /out

# Stage 2: Runtime image (Alpine based for minimal attack surface and size)
FROM mcr.microsoft.com/dotnet/runtime:8.0-alpine
WORKDIR /app
COPY --from=build /out ./
ENTRYPOINT ["dotnet", "MarketDataEngine.dll"]
