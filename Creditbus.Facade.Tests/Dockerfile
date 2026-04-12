FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Creditbus.Facade.Tests/Creditbus.Facade.Tests.csproj", "Creditbus.Facade.Tests/"]
COPY ["Creditbus.Facade/Creditbus.Facade.csproj", "Creditbus.Facade/"]
RUN dotnet restore "Creditbus.Facade.Tests/Creditbus.Facade.Tests.csproj"
COPY . .
WORKDIR "/src/Creditbus.Facade.Tests"
RUN dotnet build "./Creditbus.Facade.Tests.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./Creditbus.Facade.Tests.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Creditbus.Facade.Tests.dll"]
