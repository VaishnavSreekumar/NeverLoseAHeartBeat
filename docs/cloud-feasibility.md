# Free Cloud Service Feasibility & Architecture Evaluation

**Project:** MayaCardio "Never Lose a Heartbeat" Biosignal Pipeline  
**Document Version:** 1.0.0  
**Phase:** Phase 0 — Engineering Foundation  
**Target Deadline:** October 16, 2026, 2:00 PM IST  

---

## 1. Cloud Feasibility Requirements & Constraints

According to Section 1 and Section 4 of the MayaCardio assessment:
- **Zero Cost / Strict Free Tier:** Must run entirely on a real, free cloud provider or free tier. Paid cloud services result in immediate disqualification.
- **5-Second Latency SLA:** Each 1-second window result must be produced and recorded in the cloud within 5 seconds of the window close.
- **Catch-Up Throughput:** Following an outage of up to 2 hours ($5,760,000$ samples), the cloud copy must be complete within 15 minutes ($900$ seconds). Throughput requirement: $\ge 6,400\text{ samples/sec}$.
- **Decoupled Architecture:** Device-side pipeline runs on ARM64 Linux; cloud service runs on the cloud provider's native compute infrastructure (typically x86_64, which is fully compliant with the specification).

---

## 2. Evaluation of Candidate Free Cloud Services

| Candidate Platform | Relevant Free-Tier Quotas | Latency & 5s SLA Feasibility | Catch-Up Feasibility (2h in 15m) | Persistence & Egress Limits | ARM64 Implications | Critical Verifications Required |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Option A: Render.com (Web Service + Managed Postgres)** | • 750 free web instance hrs/mo<br>• 1 GB free Postgres DB<br>• 100 GB/mo bandwidth | **RISK:** Spins down after 15 min idle. Cold-start is 50+ seconds.<br>**FEASIBLE ONLY IF:** Kept continuously awake with regular 500ms streaming pings during tests. | **HIGH:** Batch POSTs (2,000 samples/batch = 3.2 req/s) easily handled by Uvicorn/FastAPI. | • Web service disk is ephemeral.<br>• Managed Postgres is persistent, but free DB expires 30 days after creation. | Runs on x86_64 containers. Zero impact on device ARM64 pipeline. | Verify Postgres expiration date; verify cold-start behavior during judging run. |
| **Option B: Koyeb (Micro/Nano Instance) + Neon/Supabase DB** | • 1 free nano instance (512 MB RAM, 0.1 vCPU)<br>• Always-on (NO automatic idle sleep)<br>• 55 GB egress/mo | **EXCELLENT:** Always-on without spin-down sleep.<br>End-to-end API response time $< 150\text{ ms}$, reliably satisfying the 5s SLA. | **EXCELLENT:** Easily ingests 3.2 batch req/s. Connection pooling to DB handles high concurrency. | • Ephemeral container filesystem.<br>• Relies on external persistent DB (Neon or Supabase). | Runs on x86_64 microVMs. | Confirm container build time and live health check. |
| **Option C: Supabase (Managed Postgres + PostgREST + Edge Functions)** | • 2 active free projects<br>• 500 MB database storage<br>• 5 GB egress/month<br>• Unlimited API requests | **EXCELLENT:** Database is persistent and always available (pauses only after 7 days of inactivity). Latency $< 200\text{ ms}$. | **UNVERIFIED TARGET:** PostgREST batch inserts accept up to 10,000 rows. However, individual sample rows take ~88 bytes each in Postgres $\implies$ 5.76M rows consume **~506.8 MB**, which exceeds the 500 MB free quota! Must use chunked 1-second storage (<25 MB) to avoid quota breach. | • Storage: Strict 500 MB quota requires chunked storage architecture.<br>• Egress: 5 GB quota. | Cloud architecture independent of client architecture. | Must execute representative 5.76M sample benchmark to verify storage consumption before selection. |
| **Option D: Fly.io (Apps + SQLite on LiteFS/Volume)** | • 3 shared-cpu-1x 256MB VMs<br>• 3 GB persistent volume storage<br>• 100 GB outbound bandwidth | **EXCELLENT:** Persistent volumes, sub-50ms latency, no forced sleep. | **EXCELLENT:** Can run FastAPI + persistent volume directly. | • 3 GB volume provides extensive persistent storage. | Supports x86_64. | Requires credit card verification to create account, which may violate "no paid service/no card" preference. |

---

## 3. Recommended Cloud Architecture & Selection

### Primary Recommendation: **Lightweight FastAPI Service on Koyeb backed by Supabase or Neon PostgreSQL with Chunked Storage**
1. **API Tier:** FastAPI app deployed on Koyeb (nano always-on tier, zero cold-start sleep). Exposes:
   - `POST /api/v1/ingest/batch`: Accepts gzip-compressed batches of up to 2,000 samples.
   - `GET /api/v1/health`: Instant health check for device-side mode manager.
   - `GET /api/v1/results`: Fetches online window results.
2. **Database Tier & Storage Schema Optimization:**
   - Provider: Supabase or Neon PostgreSQL free tier.
   - **Critical Architecture Decision:** Instead of storing 5,760,000 individual sample rows (which consumes ~506.8 MB and crashes the 500 MB free quota), store samples as **1-second chunk records** (400 samples/row, 14,400 rows total) or binary bytea blocks.
   - Total database footprint for 2 hours of data drops from $>500\text{ MB}$ to **$< 25\text{ MB}$**, safely complying with free-tier quotas.
3. **Catch-Up Throughput & Unverified Target Status:**
   - 2 hours of data = $5,760,000$ samples.
   - Transmitted in 2,880 HTTP POST requests over 900 seconds ($3.2$ requests/sec).
   - Network bandwidth: $\approx 89\text{ KB/s}$ wire transfer with gzip compression.
   - **Status:** The 15-minute catch-up SLA is an **UNVERIFIED TARGET**. It must be empirically validated via a representative benchmark before finalizing provider selection.

