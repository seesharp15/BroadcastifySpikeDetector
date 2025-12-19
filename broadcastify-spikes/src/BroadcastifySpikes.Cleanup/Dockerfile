FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .
RUN dotnet restore BroadcastifySpikes.sln
RUN dotnet publish BroadcastifySpikes.Cleanup/BroadcastifySpikes.Cleanup.csproj -c Release -o /out

FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /out .
ENTRYPOINT ["dotnet", "BroadcastifySpikes.Cleanup.dll"]
