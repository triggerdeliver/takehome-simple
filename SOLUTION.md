# Solution

## 1. How did you ensure ordering while maintaining parallelism?

**Per-user ordering with in-memory buffering:**

- Events are partitioned by `userId` in Kafka (using `userId` as the message key), ensuring all events for a user go to the same partition
- The consumer maintains per-user state: `nextExpectedSeq` (next sequence number to process) and `orderBuffers` (Map of buffered out-of-order events)
- When an event arrives:
  - If `seq === nextExpectedSeq`: process immediately, increment expected, then drain any buffered consecutive sequences
  - If `seq > nextExpectedSeq`: buffer the event (release inflight lock) and wait for missing sequences
  - If `seq < nextExpectedSeq`: skip (already processed or duplicate)
- Gap timeout (5s): If a missing sequence doesn't arrive within 5 seconds, skip to the next available buffered sequence

**Parallelism strategy:**

- `partitionsConsumedConcurrently: 8` allows processing multiple Kafka partitions in parallel
- Within a batch, up to `CONCURRENCY=384` messages are processed concurrently
- Atomicity at Redis level: Lua scripts (`acquireEvent`, `finalizeEvent`, `addToOrder`) ensure race conditions don't corrupt state
- Even if concurrent threads attempt to process the same event, only one succeeds due to atomic Redis operations

**Partitioning strategy:**

- Kafka configured with 8 partitions
- Producer uses `userId` as partition key: `key: event.userId`
- This guarantees all events for a given user land on the same partition
- Within a partition, Kafka preserves message order, but arrival order may differ from seq order (hence buffering)

## 2. Trade-offs in your approach?

**Advantages:**

1. **High throughput**: 50K events in ~5 seconds (~10K events/sec) with parallel processing
2. **Strong ordering guarantee**: Per-user strict ordering maintained even with out-of-order Kafka delivery
3. **At-least-once delivery**: Events are never lost; idempotency prevents duplicate processing
4. **Graceful failure handling**: Failed events go to DLQ without blocking subsequent sequences

**Trade-offs:**

1. **Memory usage**: Per-user buffers consume memory proportional to out-of-order event count. Extreme out-of-order scenarios could exhaust memory
2. **Gap timeout tradeoff**: 5-second gap timeout balances between waiting for missing events and not blocking indefinitely. Could miss events that arrive after timeout
3. **In-memory state loss on crash**: Buffers and `nextExpectedSeq` maps are lost on crash. Recovery reads from Redis (`order:{userId}` list) to reconstruct next expected seq
4. **Lock contention**: Multiple threads may redundantly process the same event from buffer, wasting CPU. Redis atomicity prevents data corruption but doesn't prevent duplicate work

**Idempotency handling race conditions:**

- `tryAcquireEvent` Lua script atomically checks both `event:{id}` and `inflight:{id}`
- `finalizeEvent` Lua script atomically sets event hash only if not exists, preventing overwrites
- `addToOrder` Lua script checks for duplicates before adding to list

---

## Test Results

```
npm test

Load:         PASS
Reliability:  PASS
Idempotency:  PASS
Ordering:     PASS
DLQ:          PASS
Rate Limit:   PASS
Validation:   PASS
```
