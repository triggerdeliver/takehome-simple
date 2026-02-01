# Solution

## 1. Partitioning & Ordering

> How do you ensure events for the same userId are processed in order when Kafka has multiple partitions?

_Your answer here..._

## 2. Parallelism

> How do you achieve high throughput while maintaining ordering guarantees?

_Your answer here..._

## 3. Idempotency

> How do you prevent duplicate processing in a distributed system with restarts?

_Your answer here..._

## 4. Rate Limiting

> How does your sliding window algorithm work?

_Your answer here..._

## 5. Trade-offs

> What trade-offs did you make in your implementation?

_Your answer here..._

## Test Results

```
npm test

Load:         PASS / FAIL
Reliability:  PASS / FAIL
Idempotency:  PASS / FAIL
Ordering:     PASS / FAIL
DLQ:          PASS / FAIL
Rate Limit:   PASS / FAIL
Validation:   PASS / FAIL
```
