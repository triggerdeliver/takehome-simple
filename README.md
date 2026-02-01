# Backend Take-Home: Event Processing Pipeline

## Overview

Build a production-ready event processing system: API receives events → Kafka → Consumer → Redis.

The starter code is intentionally incomplete. Make it pass all tests.

**Expected time: 1.5-2 hours**

---

## What's Included

```
takehome/
├── README.md
├── SOLUTION_TEMPLATE.md
├── docker-compose.yml
├── src/
│   ├── api.ts          # Express API (incomplete)
│   ├── producer.ts     # Kafka producer
│   ├── consumer.ts     # Kafka consumer (incomplete)
│   ├── redis-client.ts # Redis operations (incomplete)
│   └── types.ts
└── tests/
    ├── load.test.ts
    ├── reliability.test.ts
    ├── idempotency.test.ts
    ├── ordering.test.ts
    ├── dlq.test.ts
    ├── rate-limit.test.ts
    └── validation.test.ts
```

---

## Requirements

### 1) Performance
- Process **50,000 events** in 30 seconds
- **≥95%** success rate (verified via Redis)
- Kafka consumer lag returns to **0**

### 2) Reliability
- Consumer can be SIGTERM-killed and restarted **once** during processing
- **0 lost events**
- **0 duplicates** (no second processing, no processedAt rewrite)
- No stale `inflight:*` keys after processing completes

### 3) Idempotency
- Same `eventId` delivered again (including **direct Kafka duplicates**) must not re-run business logic
- `processedAt` must not change
- `attempts` must remain **1** for successfully processed events
- `dedup:{eventId}` must exist for all processed events

### 4) Ordering per userId
- Events have `payload.seq` (1, 2, 3...)
- For each `userId`, events must be processed in **strict order** of `seq`
- **Do not assume arrival order**; events may arrive out of order and must be buffered
- Store order in Redis list `order:{userId}`

### 5) Retry + DLQ
- If `payload.fail = true`, retry **3 times**, then move to DLQ
- Failed events must be finalized with `status=failed` and `attempts >= 3`
- Failed events must not block subsequent seq for the same user
- DLQ stored as `dlq:list` and `dlq:{eventId}` hash

### 6) Rate Limiting
- **100 events/sec per userId**
- Sliding window algorithm
- Return **429** when limit exceeded

### 7) Input Validation
- Validate: `userId` (non-empty string), `type` (string), `payload` (object)
- `payload.seq` must be **positive integer** if present
- Invalid events → **400** error, not sent to Kafka

---

## Redis Schema

Design your own schema. Required keys:

| Key Pattern | Type | Purpose |
|-------------|------|---------|
| `event:{eventId}` | Hash | Final event status |
| `dedup:{eventId}` | String | Idempotency marker |
| `inflight:{eventId}` | String | Processing lock |
| `order:{userId}` | List | Processed seq order |
| `dlq:list` | List | Failed event IDs |
| `dlq:{eventId}` | Hash | Failed event details |
| `rate:{userId}` | ZSet | Sliding window |

### Event Hash Fields

| Field | Type | Description |
|------|------|-------------|
| `status` | string | `processed` \| `failed` |
| `processedAt` | int | Unix ms timestamp |
| `attempts` | int | Total attempts (must be `1` for successful) |
| `userId` | string | User ID |
| `type` | string | Event type |
| `seq` | int | Sequence number (if present) |
| `error` | string | Error text (for failed events) |

---

## Source of Truth

Redis keys are the **only** acceptance source. Metrics are diagnostic only.

---

## How to Run

```bash
npm install

docker-compose up -d

# Terminal 1: API server
npm run dev:api

# Terminal 2: Consumer
npm run dev:consumer

# Run all tests
npm test
```

---

## What to Submit

1. Fixed code in `src/`
2. `SOLUTION.md` answering:
   - How did you ensure ordering while maintaining parallelism?
   - What's your partitioning strategy?
   - How does your idempotency handle race conditions?
   - Trade-offs in your approach?

---

## Constraints

- **Do not modify test files**
- **Do not modify docker-compose.yml**
- **Kafka + Redis only** (no external databases)
- You may add npm packages
