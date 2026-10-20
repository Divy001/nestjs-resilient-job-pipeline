# ⚡ NestJS Resilient Job Pipeline

A production-grade asynchronous job processing and idempotent webhook pipeline built with **NestJS**, **BullMQ**, **PostgreSQL (Prisma)**, and **Redis**.

Designed for high-throughput B2B SaaS workflows (bulk report generation, third-party webhook ingestion, asynchronous data sync) where failure is not an option and duplicate execution causes financial or data integrity loss.

---

## 🏗️ System Architecture

```
Incoming Webhook / API Request
           │
           │  POST /api/v1/jobs/ingest
           │  Header: X-Idempotency-Key: "order_12345"
           ▼
┌──────────────────────────────────────┐
│       1. Idempotency Guard           │ ───► Duplicate Key? ──► HTTP 409 Conflict
│   (Atomic Redis SETNX with TTL)      │                         (Zero DB Connection Used)
└──────────────────┬───────────────────┘
                   │
                   ▼ Lock Acquired
┌──────────────────────────────────────┐
│       2. Ingestion Service           │
│   (PostgreSQL Single Source of Truth)│ ───► Inserts JobRecord (status: "PENDING")
└──────────────────┬───────────────────┘
                   │
                   ▼ Unified Job UUID
┌──────────────────────────────────────┐
│       3. BullMQ Producer             │
│   (Redis-Backed Job Queue)           │ ───► Returns HTTP 202 ACCEPTED (< 30ms)
└──────────────────┬───────────────────┘
                   │
                   │  Async Decoupled Processing
                   ▼
┌──────────────────────────────────────┐
│       4. BullMQ Worker Pool          │
│   - Controlled Concurrency (5)       │ ───► Success: status = "COMPLETED"
│   - Exponential Backoff (3 retries)  │ ───► Transient Error: Retries in 2s, 4s, 8s
│   - Dead-Letter State Handler        │ ───► Final Exhaustion: status = "FAILED" (DLQ)
└──────────────────────────────────────┘
```

---

## 🛡️ Key Architectural Highlights

### 1. Atomic Idempotency via Redis SETNX
Duplicate webhooks from payment gateways or client retries are blocked in memory before touching PostgreSQL:
- Uses Redis `SET key value EX 60 NX` (atomic check and set in a single command).
- Prevents race conditions where concurrent requests with the identical idempotency key bypass standard `GET -> SET` checks.
- If the server restarts unexpectedly, the 60-second TTL prevents permanent deadlocks.

### 2. Dual-Write Consistency Protection
In distributed systems, writing to a relational database and a message queue can leave "ghost records" if the queue fails after the database commits:
- If `jobQueue.add()` throws an error (e.g. Redis network disconnect), the catch handler catches the failure, marks the PostgreSQL record as `FAILED`, and releases the idempotency lock.
- For zero-data-loss mission-critical environments, this architecture is designed to cleanly evolve into the **Transactional Outbox Pattern** (storing jobs in a dedicated outbox table within the same ACID transaction).

### 3. Resource & Memory Protection
- **Concurrency Capping:** Worker concurrency is strictly pinned (`concurrency: 5`) to prevent Node.js event loop saturation and PostgreSQL connection pool exhaustion.
- **Auto-Purging Redis Metadata:** Queues use `removeOnComplete: 1000` and `removeOnFail: 5000` to prevent unbounded Redis RAM growth over millions of processed events.
- **Exponential Backoff:** Failed jobs retry on an exponential delay (2s, 4s, 8s) to prevent thundering herd spikes on downstream services.

---

## 🛠️ Tech Stack

- **Runtime & Framework:** Node.js, NestJS, TypeScript
- **Job Queue:** BullMQ, Redis 7 (Alpine)
- **Database:** PostgreSQL 16 (Alpine), Prisma ORM
- **Containerization:** Docker & Docker Compose

---

## 🚀 Quickstart (1-Command Run)

### 1. Clone & Configure
```bash
git clone https://github.com/Divy001/nestjs-resilient-job-pipeline.git
cd nestjs-resilient-job-pipeline
cp .env.example .env
```

### 2. Start PostgreSQL & Redis
```bash
docker-compose up -d
```

### 3. Install Dependencies & Run Migrations
```bash
npm install
npx prisma db push
```

### 4. Start the Application
```bash
npm run start:dev
```
API runs on `http://localhost:3000`.

---

## 🧪 Testing the Pipeline with cURL

### 1. Ingest an Async Job (Fast HTTP 202 Accepted)
```bash
curl -i -X POST http://localhost:3000/api/v1/jobs/ingest \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: test-key-001" \
  -H "X-Tenant-Id: tenant-alpha" \
  -d '{
    "jobType": "GENERATE_HEALTHCARE_REPORT",
    "payload": { "patientCount": 150, "reportType": "AUDIT" }
  }'
```

*Response (< 30ms):*
```json
HTTP/1.1 202 Accepted
{
  "jobId": "f7d3a2e1-4b5c-4d8e-9f0a-1b2c3d4e5f6a",
  "status": "PENDING",
  "trackingUrl": "/api/v1/jobs/f7d3a2e1-4b5c-4d8e-9f0a-1b2c3d4e5f6a/status",
  "createdAt": "2026-10-19T10:00:00.000Z"
}
```

### 2. Test Idempotency (Send the Exact Same Request Again)
```bash
curl -i -X POST http://localhost:3000/api/v1/jobs/ingest \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: test-key-001" \
  -H "X-Tenant-Id: tenant-alpha" \
  -d '{
    "jobType": "GENERATE_HEALTHCARE_REPORT",
    "payload": { "patientCount": 150, "reportType": "AUDIT" }
  }'
```

*Response:*
```http
HTTP/1.1 409 Conflict
{
  "statusCode": 409,
  "message": "A request with this idempotency key is already actively processing. Please wait."
}
```

### 3. Poll Job Status
```bash
curl -i http://localhost:3000/api/v1/jobs/f7d3a2e1-4b5c-4d8e-9f0a-1b2c3d4e5f6a/status \
  -H "X-Tenant-Id: tenant-alpha"
```

*Response (once background worker finishes):*
```json
{
  "id": "f7d3a2e1-4b5c-4d8e-9f0a-1b2c3d4e5f6a",
  "tenantId": "tenant-alpha",
  "jobType": "GENERATE_HEALTHCARE_REPORT",
  "status": "COMPLETED",
  "attempts": 1,
  "result": {
    "processedRecords": 250,
    "executionTimestamp": "2026-10-19T10:00:02.150Z"
  },
  "errorMessage": null
}
```

### 4. Test Failure & Exponential Retries
Send a payload with `"simulateFailure": true`:
```bash
curl -i -X POST http://localhost:3000/api/v1/jobs/ingest \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: failure-test-001" \
  -H "X-Tenant-Id: tenant-alpha" \
  -d '{
    "jobType": "GENERATE_HEALTHCARE_REPORT",
    "payload": { "simulateFailure": true }
  }'
```
Watch terminal logs to observe the worker attempt 3 retries at 2-second and 4-second exponential intervals before transitioning the record to `status: "FAILED"`.

---

## 📊 Performance & Safeguards Summary

| Feature | Production Benefit | Implementation |
|---|---|---|
| **Atomic SETNX Lock** | Blocks duplicate jobs without database connection overhead | Redis `NX EX 60` in `IdempotencyGuard` |
| **Concurrency Ceiling** | Prevents DB connection pool exhaustion | BullMQ `@Processor` concurrency: 5 |
| **Exponential Backoff** | Prevents cascading failures on downstream APIs | `attempts: 3`, `delay: 2000`, `type: exponential` |
| **RAM Protection** | Prevents Redis memory leaks over time | `removeOnComplete: 1000`, `removeOnFail: 5000` |
| **Composite Indexes** | Fast dashboard queries on high volume | `@@index([tenantId, status])` |
| **Dead-Letter Logging** | Immediate alerting and post-mortem diagnosis | `@OnWorkerEvent('failed')` |
