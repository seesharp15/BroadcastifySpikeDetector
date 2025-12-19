Initial Setup steps:

#0.1 Create a new folder + subfolders
mkdir -p broadcastify-spikes/{src,infra}
cd broadcastify-spikes

#0.2 Create a .env for local secrets/config
cat > infra/.env <<'EOF'
# Postgres
POSTGRES_DB=broadcastify
POSTGRES_USER=broadcastify
POSTGRES_PASSWORD=broadcastify_local_pw

# App config (shared)
APP_ENV=local
LOG_LEVEL=Information

# Ingest
INGEST_POLL_SECONDS=300

# Detect
DETECT_POLL_SECONDS=120
LOOKBACK_DAYS=3
ROBUST_Z=3.5
RECOVERY_Z=1.0
MIN_SAMPLES=40
PERSIST_SAMPLES=3

# Alert
ALERT_SUPPRESS_HOURS=8
EOF

#0.3 Create a Docker network + persistent volumes via compose (we’ll define next)
cat > infra/docker-compose.yml <<'EOF'
services:
  postgres:
    image: postgres:16
    container_name: broadcastify-postgres
    env_file: .env
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB"]
      interval: 5s
      timeout: 3s
      retries: 20

  redis:
    image: redis:7
    container_name: broadcastify-redis
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: ["redis-server", "--appendonly", "yes"]
    healthcheck:
      test: ["CMD", "redis-cli", "PING"]
      interval: 5s
      timeout: 3s
      retries: 20

  # These are placeholders for now; we’ll add builds once code exists.
  ingestor:
    image: broadcastify/ingestor:dev
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    env_file: .env
    environment:
      - SERVICE_NAME=ingestor
      - POSTGRES_HOST=postgres
      - REDIS_HOST=redis
    restart: unless-stopped

  detector:
    image: broadcastify/detector:dev
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    env_file: .env
    environment:
      - SERVICE_NAME=detector
      - POSTGRES_HOST=postgres
      - REDIS_HOST=redis
    restart: unless-stopped

  alerter:
    image: broadcastify/alerter:dev
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    env_file: .env
    environment:
      - SERVICE_NAME=alerter
      - POSTGRES_HOST=postgres
      - REDIS_HOST=redis
    restart: unless-stopped

volumes:
  pg_data:
  redis_data:
EOF



#Initial setup (Step 2: bring up just Postgres + Redis and verify)
cd infra
docker compose up -d postgres redis
docker compose ps

#verify health
docker logs -f broadcastify-postgres

#postgres
docker exec -it broadcastify-postgres psql -U broadcastify -d broadcastify -c "select now();"
#redis 
docker exec -it broadcastify-redis redis-cli PING


#Initial setup (Step 3: repo hygiene)
#Add a .gitignore at repo root:
cd ..
cat > .gitignore <<'EOF'
**/bin/
**/obj/
**/*.user
**/*.suo
**/.vs/
infra/.env
EOF




#Step 4.1 — Create the .NET solution + projects
cd src
dotnet new sln -n BroadcastifySpikes

dotnet new classlib -n BroadcastifySpikes.Core
dotnet new console  -n BroadcastifySpikes.Ingestor
dotnet new console  -n BroadcastifySpikes.Detector
dotnet new console  -n BroadcastifySpikes.Alerter

dotnet sln BroadcastifySpikes.sln add BroadcastifySpikes.Core/BroadcastifySpikes.Core.csproj
dotnet sln BroadcastifySpikes.sln add BroadcastifySpikes.Ingestor/BroadcastifySpikes.Ingestor.csproj
dotnet sln BroadcastifySpikes.sln add BroadcastifySpikes.Detector/BroadcastifySpikes.Detector.csproj 
dotnet sln BroadcastifySpikes.sln add BroadcastifySpikes.Alerter/BroadcastifySpikes.Alerter.csproj

dotnet add BroadcastifySpikes.Ingestor/BroadcastifySpikes.Ingestor.csproj reference BroadcastifySpikes.Core/BroadcastifySpikes.Core.csproj
dotnet add BroadcastifySpikes.Detector/BroadcastifySpikes.Detector.csproj reference BroadcastifySpikes.Core/BroadcastifySpikes.Core.csproj
dotnet add BroadcastifySpikes.Alerter/BroadcastifySpikes.Alerter.csproj reference BroadcastifySpikes.Core/BroadcastifySpikes.Core.csproj

#Step 4.2 — Add NuGet packages (Keeping packages minimal on purpose.)
dotnet add BroadcastifySpikes.Core/BroadcastifySpikes.Core.csproj package Npgsql --version 8.0.5
dotnet add BroadcastifySpikes.Core/BroadcastifySpikes.Core.csproj package StackExchange.Redis --version 2.8.16

dotnet add BroadcastifySpikes.Ingestor/BroadcastifySpikes.Ingestor.csproj package HtmlAgilityPack --version 1.11.70





