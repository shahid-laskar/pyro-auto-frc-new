# Auto FRC — Production Deployment Guide & Checklist

This runbook details the exact procedure for building the immutable Docker container image on the Windows developer workstation, transferring artifacts securely to the SDC production server (`10.201.222.67`), deploying the service, and performing post-deployment smoke tests.

---

## 1. System Topology & Coordinates

| Component | Host / Environment | Coordinates / Value |
|---|---|---|
| **Build Workstation** | Windows Dev Machine | `D:\pyro\pyro_auto_frc` |
| **Production Server** | SDC SM Server (RHEL / Linux x86_64) | `10.201.222.67` |
| **SSH User / Deploy Path** | Remote Linux User | `m01400120u1@10.201.222.67:/home/m01400120u1/autofrc/` |
| **Container Architecture** | Linux AMD64 | Multi-stage `python:3.12-slim` (non-root `appuser`) |
| **Internal Service Port** | Uvicorn ASGI | `8000/tcp` (Mapped to `127.0.0.1:8000`) |
| **External Gateway / Proxy** | Nginx Reverse Proxy / SSL | `https://smpyrogateway.bsnl.co.in/api` |
| **PostgreSQL DB** | Primary (Write: 5432) / Standby (Read: 5433) | `10.201.222.77` |
| **Oracle DB** | BCD Master Table | SDC Oracle Production Instance |
| **Payment Gateway** | Pyro API Gateway | `https://bsnlapigateway.pyrogroup.com` |

---

## 2. Pre-Build Verification Checklist (Windows PC)

Run these checks in PowerShell inside `D:\pyro\pyro_auto_frc` before building:

- [ ] **Working Tree Clean**:
  ```powershell
  git status
  ```
  Ensure there are no uncommitted modifications. Note the short git commit hash:
  ```powershell
  git rev-parse --short HEAD
  ```

- [ ] **Automated Test Suite (260/260 PASS)**:
  Execute the full regression suite to ensure zero regressions:
  ```powershell
  .venv\Scripts\pytest -q
  ```
  *Requirement: All 260 tests must pass.*

- [ ] **Docker Engine Running**:
  Verify Docker Desktop is active on Windows:
  ```powershell
  docker version
  ```

- [ ] **`.dockerignore` Cleanliness Check**:
  Ensure `.dockerignore` exists and ignores local secrets and virtual environments:
  ```powershell
  Get-Content .dockerignore
  ```
  *(Must exclude `.env`, `.env.*`, `.git`, `__pycache__`, and `.venv`)*

- [ ] **Prepare Production `.env.production`**:
  Ensure your server `.env.production` file has the correct production values configured:
  ```ini
  # Pyro API Gateway
  PYRO_BASE_URL=https://bsnlapigateway.pyrogroup.com
  PYRO_API_KEY=<PROD_PYRO_API_KEY>
  PYRO_LOGIN_ID=<PROD_PYRO_LOGIN_ID>
  PYRO_PASSWORD=<PROD_PYRO_PASSWORD>
  PYRO_SECRET_KEY=<PROD_3DES_SECRET_KEY>
  ADMIN_API_KEY=bf6f3acd83ab427b4e31b7c2b4ee07a52bde6ee594b3cb1665c35ad6e8ad0356

  # Oracle DB (BCD Only)
  ORACLE_USER=CAF_ADMIN
  ORACLE_PASSWORD=<PROD_ORACLE_PASSWORD>
  ORACLE_DSN=<PROD_ORACLE_HOST>:1521/<SERVICE_NAME>

  # Postgres DB (Write Endpoint: 5432)
  PG_WRITE_HOST=10.201.222.77
  PG_WRITE_PORT=5432
  PG_WRITE_DATABASE=sanchaarmitra
  PG_WRITE_USER=m01400120u1
  PG_WRITE_PASSWORD=<PROD_PG_PASSWORD>

  # Postgres DB (Read Endpoint: 5433 or 5432)
  PG_READ_HOST=10.201.222.77
  PG_READ_PORT=5432
  PG_READ_DATABASE=sanchaarmitra
  PG_READ_USER=m01400120u1
  PG_READ_PASSWORD=<PROD_PG_PASSWORD>

  PG_MIN_CONN=2
  PG_MAX_CONN=10

  # Zone Configuration (Start with NZ for controlled rollout, or ALL)
  ENABLED_ZONES=NZ

  # Concurrency & Scaling Parameters
  RECHARGE_CONCURRENCY=4
  RECHARGE_BATCH_SIZE=250
  STATUS_CHECK_BATCH_SIZE=100
  RECHARGE_MAX_RETRIES=3

  # Initial Deploy: Keep scheduler disabled for manual smoke test
  ENABLE_SCHEDULER=false
  CALLBACK_BASE_URL=https://smpyrogateway.bsnl.co.in/api
  ```

---

## 3. Build & Packaging Commands (Windows PowerShell)

Run the following commands in PowerShell from `D:\pyro\pyro_auto_frc`:

### Step 3.1: Define Version Tag
```powershell
$TAG = "v7"   # or "prod-$(Get-Date -Format 'yyyyMMdd')"
$IMAGE_NAME = "frc-recharge:$TAG"
$TAR_NAME = "frc_recharge_$TAG.tar.gz"
Write-Host "Target Image: $IMAGE_NAME -> $TAR_NAME"
```

### Step 3.2: Cross-Platform Build for Linux AMD64
Build the container using `docker buildx` targeting the server's CPU architecture:
```powershell
docker buildx build --platform linux/amd64 -t $IMAGE_NAME .
```

### Step 3.3: Export and Compress Docker Image
Save the immutable container image and compress it with gzip:
```powershell
docker save $IMAGE_NAME | gzip > $TAR_NAME
```

### Step 3.4: Verify Archive Size & Generate SHA256 Checksum
```powershell
Get-Item $TAR_NAME | Select-Object Name, @{Name="Size (MB)"; Expression={[math]::Round($_.Length / 1MB, 2)}}
Get-FileHash $TAR_NAME -Algorithm SHA256
```
*(Typical compressed archive size: ~80–90 MB)*

---

## 4. Secure Artifact Transfer (Windows to Server)

Transfer the image, compose file, and environment settings to the SDC server:

```powershell
$SERVER = "m01400120u1@10.201.222.67"
$REMOTE_DIR = "/home/m01400120u1/autofrc"

# 1. Transfer Docker Image Archive
scp $TAR_NAME "${SERVER}:${REMOTE_DIR}/"

# 2. Transfer Production Docker Compose file
scp docker-compose-prod.yml "${SERVER}:${REMOTE_DIR}/docker-compose.yml"

# 3. Transfer Production .env file
scp .env.production "${SERVER}:${REMOTE_DIR}/.env"
```

---

## 5. Server Deployment Commands (Linux Server `10.201.222.67`)

Log in to the server and deploy the new container:

### Step 5.1: SSH into Server
```bash
ssh m01400120u1@10.201.222.67
cd /home/m01400120u1/autofrc/
```

### Step 5.2: Restrict File Permissions
Ensure the `.env` secret file is restricted to the current user:
```bash
chmod 600 .env
```

### Step 5.3: Load the Docker Image
```bash
# Decompress and load image
gzip -d -c frc_recharge_v7.tar.gz | docker load

# Verify image is loaded in local docker registry
docker images | grep frc-recharge
```

### Step 5.4: Verify `docker-compose.yml` Image Tag
Ensure the image tag in `docker-compose.yml` matches what was loaded (e.g. `frc-recharge:v7`):
```bash
sed -i 's|image: frc.*|image: frc-recharge:v7|g' docker-compose.yml
cat docker-compose.yml | grep "image:"
```

### Step 5.5: Launch the New Service Container
```bash
# Gracefully stop existing container
docker compose down

# Start the new container in detached background mode
docker compose up -d
```

---

## 6. Post-Deployment Smoke Test & Verification Checklist (Server)

Execute these operational checks immediately after launching:

### Step 6.1: Container Status & Health Check
```bash
docker compose ps
```
- [ ] Container `frc_recharge` is in state `Up` and `(healthy)`.

### Step 6.2: Inspect Startup Logs
```bash
docker compose logs --tail=100 -f frc-recharge
```
Look for the following log milestones:
- [ ] `Postgres write pool initialised (min=2 max=10)`
- [ ] `Postgres read pool initialised (min=2 max=10)`
- [ ] `Oracle pool initialised (BCD, min=1 max=5)`
- [ ] `Shared httpx client initialised (keepalive=20 max=50)`
- [ ] `Pyro authentication successful - user: <USER>`
- [ ] `FRC Pyro Recharge Service -- ready`

### Step 6.3: HTTP Diagnostic Endpoints
Execute non-mutating diagnostics:

```bash
# 1. Health check
curl -s http://localhost:8000/api/health
# Expected: {"status":"ok"}

# 2. Database Read & Write Pools Health
curl -s http://localhost:8000/api/health/database
# Expected: {"read":{"status":"ok",...},"write":{"status":"ok",...}}

# 3. Pyro Token Status
curl -s http://localhost:8000/api/token-status
# Expected: {"session_token_present":true,"access_token_present":true,"access_expires_in_s":890+}

# 4. Admin Zones Configuration
ADMIN_KEY="bf6f3acd83ab427b4e31b7c2b4ee07a52bde6ee594b3cb1665c35ad6e8ad0356"
curl -s -H "X-Admin-Api-Key: $ADMIN_KEY" http://localhost:8000/api/admin/zones
# Expected: {"status":"HEALTHY","configured_zones":"NZ",...}
```

### Step 6.4: Controlled Manual Pilot Run (Single Batch)
Trigger one manual population and recharge batch to verify live connectivity:

```bash
# Trigger Batch Population (NZ Circles)
curl -s -X POST -H "X-Admin-Api-Key: $ADMIN_KEY" \
  "http://localhost:8000/api/admin/trigger-batch-population?zones=NZ"

# Trigger Recharge Dispatch (NZ Circles)
curl -s -X POST -H "X-Admin-Api-Key: $ADMIN_KEY" \
  "http://localhost:8000/api/admin/trigger-recharge?zones=NZ"
```

Inspect the dispatch logs:
```bash
docker compose logs --tail=50 frc-recharge | grep "RECHARGE"
```
Verify:
- [ ] Requests execute concurrently (bounded by `asyncio.Semaphore(4)`).
- [ ] Zero re-authentications occur during dispatch (access token reused).
- [ ] Oracle BCD rows remain `RQ` (zero post-recharge writebacks).

### Step 6.5: Enable Automated Background Scheduler
Once manual smoke tests confirm healthy operation, enable the automated background scheduler:
```bash
# 1. Set ENABLE_SCHEDULER=true in .env
sed -i 's/ENABLE_SCHEDULER=false/ENABLE_SCHEDULER=true/' .env

# 2. Restart container to pick up scheduler activation
docker compose up -d

# 3. Verify scheduler started in logs
docker compose logs --tail=30 frc-recharge | grep -i "scheduler"
# Expected: "Scheduler started"
```

---

## 7. Emergency Rollback Procedure

If critical issues or unexpected failures occur during deployment:

```bash
# Step 1: Stop the current faulty container
docker compose down

# Step 2: Edit docker-compose.yml to revert to previous image tag (e.g. v6)
sed -i 's|image: frc-recharge:v7|image: frc-recharge:v6|g' docker-compose.yml

# Step 3: Re-launch previous certified container
docker compose up -d

# Step 4: Verify health of rolled-back container
docker compose ps
docker compose logs -f --tail=50 frc-recharge
```

---

## 8. Summary Command Cheat Sheet

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           WINDOWS DEV MACHINE                            │
├──────────────────────────────────────────────────────────────────────────┤
│ cd D:\pyro\pyro_auto_frc                                                 │
│ .venv\Scripts\pytest -q                                                  │
│ docker buildx build --platform linux/amd64 -t frc-recharge:v7 .          │
│ docker save frc-recharge:v7 | gzip > frc_recharge_v7.tar.gz              │
│ scp frc_recharge_v7.tar.gz m01400120u1@10.201.222.67:~/autofrc/         │
│ scp docker-compose-prod.yml m01400120u1@10.201.222.67:~/autofrc/         │
│ scp .env.production m01400120u1@10.201.222.67:~/autofrc/.env            │
└──────────────────────────────────────────────────────────────────────────┘
                                     │
                                    SCP
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                         SDC SERVER (10.201.222.67)                       │
├──────────────────────────────────────────────────────────────────────────┤
│ ssh m01400120u1@10.201.222.67                                            │
│ cd /home/m01400120u1/autofrc/                                            │
│ chmod 600 .env                                                           │
│ gzip -d -c frc_recharge_v7.tar.gz | docker load                          │
│ docker compose down                                                      │
│ docker compose up -d                                                     │
│ docker compose ps                                                        │
│ docker compose logs -f --tail=100 frc-recharge                           │
│ curl http://localhost:8000/api/health                                    │
└──────────────────────────────────────────────────────────────────────────┘
```
