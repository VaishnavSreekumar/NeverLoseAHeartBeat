# MayaCardio Cloud Receiver — Render Deployment & PostgreSQL Storage Guide

This document details the configuration, deployment, and verification procedures for hosting the **MayaCardio Cloud Ingestion & Analytics Receiver** on **Render.com** (or any Docker-compatible PaaS) with durable PostgreSQL storage.

---

## 1. Cloud Architecture & Storage Feasibility

### Ephemeral Filesystem vs. Durable Cloud Storage
- **Render Web Services run on an ephemeral container filesystem.** Any local file written inside the container (such as a local SQLite database file `/app/cloud_server.db`) is completely destroyed when the container idles out after 15 minutes of inactivity or when a new deployment/restart occurs.
- **Durable Storage Engine Requirement:** To comply with the MayaCardio assessment's requirement for a crash-resilient cloud copy with state preservation across restarts, the cloud receiver uses a hybrid storage engine:
  * When `DATABASE_URL` or `CLOUD_DATABASE_URL` is set to a `postgresql://` (or legacy `postgres://`) connection string, `pipeline.cloud.storage.CloudStorageEngine` operates against PostgreSQL with ACID transactions, strict idempotency, duplicate conflict detection, and crash recovery.
  * When no PostgreSQL URL is provided, the engine defaults to a local SQLite WAL database for local developer convenience and offline edge tests.
- **Edge Device Storage Unchanged:** Edge device acquisition and storage (`pipeline.storage.StorageEngine`) remains 100% local SQLite WAL with synchronous disk commits, unaffected by the cloud engine.

---

## 2. Free-Tier Provider Analysis & Honest Limitations

Before deploying for assessment evaluation, review these critical free-tier constraints:

| Provider & Service | Free-Tier Quotas | Idle / Spin-Down Behavior | Expiration & Data Retention Policy | Assessment Judging Suitability |
| :--- | :--- | :--- | :--- | :--- |
| **Render Web Service (Free)** | • 512 MB RAM, 0.1 CPU<br>• 750 instance hours/month | **Spins down after 15 min idle.** Cold start takes **50–60 seconds** on the first request. | Ephemeral container disk. Files reset on restart. | **Suitable for API gateway**, but client health checks must allow for cold start. |
| **Render Managed PostgreSQL (Free)** | • 1 GB storage limit<br>• 1 free database per account<br>• Max 97 concurrent connections | Always online within the 30-day window. | **Strict 30-Day Expiration:** Render free databases expire and are **permanently deleted after 30 days**. No automated backups. | **High Risk for Extended Judging:** If the evaluation period exceeds 30 days, all persisted cloud data is wiped. |
| **Neon Serverless PostgreSQL (Free Alternative)** | • 0.5 GB storage limit<br>• Unlimited compute hours<br>• Standard PostgreSQL 16 | Scales compute to zero when idle; resumes in $< 500\text{ ms}$. | **No 30-day expiration timer.** Data persists indefinitely on active free accounts. SSL enforced (`sslmode=require`). | **Recommended Alternative:** Best suited for judging as there is no 30-day automatic deletion timer. |
| **Supabase PostgreSQL (Free Alternative)** | • 500 MB database storage<br>• Standard PostgreSQL 15 | Always online during active use; pauses after 7 days of zero activity. | Can be unpaused with one click in the web console. | **Viable Alternative:** Generous quotas, easily unpaused if needed. |

> [!WARNING]
> **Render Free PostgreSQL Expiration Notice:**  
> If you create a Render Managed PostgreSQL instance on the free tier, note that Render will automatically revoke and drop the database after 30 days. For longer evaluations, connect a free PostgreSQL database from **Neon (`neon.tech`)** or **Supabase (`supabase.com`)** by setting the `DATABASE_URL` environment variable.

---

## 3. Environment Variables Configuration

Configure the following environment variables in the Render dashboard (or `.env` file for container runs):

| Variable Name | Required? | Default (if unset) | Description |
| :--- | :---: | :---: | :--- |
| `DATABASE_URL` | **Yes** (for cloud) | `None` (falls back to local SQLite) | Connection string for PostgreSQL (e.g. `postgresql://user:pass@ep-host.region.neon.tech/dbname?sslmode=require`). Legacy `postgres://` prefixes are automatically normalized to `postgresql://`. |
| `CLOUD_DATABASE_URL` | Optional | `None` | Alternative environment variable name for PostgreSQL connection string. Takes precedence if set. |
| `CLOUD_API_KEY` | Recommended | `None` (authentication disabled) | 64-character hex secret string. When configured, incoming HTTP requests must provide this key in the `X-API-Key` header. |
| `PORT` | Auto-set by Render | `8000` | Port on which the Uvicorn web server listens. Render automatically injects this variable (e.g., `10000`). |
| `HOST` | Optional | `0.0.0.0` | IP interface binding. Set to `0.0.0.0` so container routes traffic from external proxies. |

> [!IMPORTANT]
> **Do not hardcode or commit database credentials or API keys.** Set them securely in the Render Environment Secrets dashboard. Never check credentials into Git.

---

## 4. Schema Auto-Initialization & Migrations

**No manual database migration step is required.**

When `pipeline.cloud.server:app` boots up, `CloudStorageEngine` automatically initializes all required tables and indexes idempotently using `CREATE TABLE IF NOT EXISTS`:

1. `sessions`: Session metadata, sensor configurations, and active status.
2. `samples`: Microsecond-indexed channel values with composite primary key `(session_id, sensor, sample_index)`.
3. `loss_ledger`: Authoritative dropped intervals with dual partial unique indexes preventing interval overlaps.
4. `checkpoints`: Dual contiguous persisted and resolved indices per session and sensor.
5. `window_results`: 1-second Section 3 aggregates (`mean`, `std`, `min`, `max`, `missing_count`, `r_peaks_detected`, `heart_rate_bpm`).

All column types and dialect differences (e.g. `DOUBLE PRECISION`, `BIGSERIAL`, `BOOLEAN`, quoted `"window"` identifiers) are handled natively by the engine.

---

## 5. Health Check Configuration

Render automatically monitors container health and routes traffic once the health check returns HTTP 200.

- **Platform Liveness Probe Endpoint:** `GET /healthz` (or root `GET /`)
- **Render Dashboard Setting:**  
  In your Render Web Service settings, set **Health Check Path** to:
  ```
  /healthz
  ```
- **Response Format (HTTP 200):**
  ```json
  {"status": "healthy"}
  ```
- **Authentication Bypass on Health Probes:** Public platform liveness probes (`/healthz` and `/`) are unauthenticated to allow Render's internal health check runner to verify container readiness without an API key.
- **Authenticated Telemetry Endpoint:** `GET /api/v1/status` provides detailed session telemetry, sample counts, and checkpoints, and requires `X-API-Key` when `CLOUD_API_KEY` is set.

---

## 6. Step-by-Step Render Deployment Guide

### Step 1: Provision a Free PostgreSQL Database
1. Create a database on [Neon](https://neon.tech) (recommended, no 30-day limit) or [Render](https://render.com).
2. Copy the external connection string:
   ```
   postgresql://maya_user:<PASSWORD>@<HOST>/mayacardio?sslmode=require
   ```

### Step 2: Create a New Web Service on Render
1. Log into your Render Dashboard.
2. Click **New +** -> **Web Service**.
3. Connect your GitHub repository:
   ```
   https://github.com/VaishnavSreekumar/NeverLoseAHeartBeat
   ```
4. Configure service parameters:
   - **Name:** `mayacardio-cloud-receiver` (or your choice)
   - **Region:** Closest to your edge device or test runner (e.g., Singapore, Frankfurt, Oregon)
   - **Branch:** `main`
   - **Runtime:** **Docker** (Render will automatically detect the root `Dockerfile`)
   - **Plan:** **Free**

### Step 3: Add Environment Variables
In the **Environment Variables** section, add:
1. `DATABASE_URL`: Your full PostgreSQL connection string.
2. `CLOUD_API_KEY`: A secure random token (e.g., generate via `python -c "import secrets; print(secrets.token_hex(32))"`).

### Step 4: Configure Health Check Path
Under **Advanced Settings**:
- **Health Check Path:** `/healthz`
- **Auto-Deploy:** Yes (deploys on push to `main`)

### Step 5: Deploy
Click **Create Web Service**. Render will:
1. Build the Docker image from [`Dockerfile`](../Dockerfile).
2. Install Python 3.12 dependencies (`fastapi`, `uvicorn`, `psycopg2-binary`, `httpx`).
3. Start `uvicorn pipeline.cloud.server:app --host 0.0.0.0 --port $PORT`.
4. Run health check probes against `/healthz`.
5. Mark the service **Live** with a public URL: `https://<your-service-name>.onrender.com`.

---

## 7. Post-Deployment Verification

Execute these verification commands from your local workstation or edge device:

### A. Verify Public Health Check
```bash
curl -i https://<your-service-name>.onrender.com/api/v1/status
```
*Expected response (HTTP 200 OK):*
```json
{"status":"healthy","service":"MayaCardio Cloud Ingestion Receiver","database":"postgresql","samples_saved":0,"active_sessions":[]}
```

### B. Verify Authenticated Ingestion
```bash
curl -X POST https://<your-service-name>.onrender.com/api/v1/ingest/batch \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <YOUR_CLOUD_API_KEY>" \
  -d '{
    "session_id": "render_test_01",
    "sensor": 1,
    "first_index": 0,
    "count": 3,
    "data": [[10, 20, 30], [11, 21, 31], [12, 22, 32]],
    "lost": [],
    "session_end": false
  }'
```
*Expected response (HTTP 200 OK):*
```json
{
  "session_id": "render_test_01",
  "sensor": 1,
  "contiguous_persisted_index": 2,
  "contiguous_resolved_index": 2,
  "samples_inserted": 3,
  "duplicates_ignored": 0,
  "loss_records_inserted": 0,
  "highest_observed_index": 2
}
```

### C. Verify Idempotent Duplicate Re-transmission
Submit the exact same `curl` command again. The response must acknowledge success with:
```json
{
  "samples_inserted": 0,
  "duplicates_ignored": 3,
  "contiguous_persisted_index": 2
}
```

### D. Verify Application Restart Durability
1. In the Render Dashboard, trigger **Manual Deploy -> Restart Service**.
2. Wait for the service to return to **Live**.
3. Query the status endpoint with session filter:
   ```bash
   curl -H "X-API-Key: <YOUR_CLOUD_API_KEY>" "https://<your-service-name>.onrender.com/api/v1/status?session=render_test_01"
   ```
4. Verify that `samples_saved: 3` and the session checkpoint state are retained 100% intact from the durable PostgreSQL database, confirming that restart recovery succeeded across ephemeral container destruction.

---

## 8. Configuring Edge Device to Sync with Render Cloud

On the Raspberry Pi / edge workstation running the ingestion pipeline:

```bash
# 1. Export Render cloud endpoint and authentication credentials
export PIPELINE_CLOUD_URL="https://<your-service-name>.onrender.com"
export PIPELINE_API_KEY="<YOUR_CLOUD_API_KEY>"

# 2. Check live pipeline status against Render cloud
pipeline status --db live_device.db --cloud-url $PIPELINE_CLOUD_URL --api-key $PIPELINE_API_KEY

# 3. Export session data directly from Render cloud
pipeline export --session <SESSION_ID> --from cloud --cloud-url $PIPELINE_CLOUD_URL --api-key $PIPELINE_API_KEY --output cloud_export.ndjson
```
