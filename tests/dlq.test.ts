import axios from 'axios';
import Redis from 'ioredis';

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

const USERS = 5;
const SEQ_PER_USER = 20;
const FAIL_SEQ = 7;
const MAX_RETRIES = 3;

const redis = new Redis(REDIS_URL);

interface TestResult {
  passed: boolean;
  failureReasons: string[];
}

interface SentEvent {
  eventId: string;
  userId: string;
  seq: number;
  fail: boolean;
}

async function cleanupRedis(): Promise<void> {
  await redis.flushall();
}

async function sendMixedEvents(): Promise<SentEvent[]> {
  const sentEvents: SentEvent[] = [];
  const batchSize = 25;
  const allEvents: Array<{ userId: string; type: string; payload: { seq: number; fail: boolean } }> = [];

  for (let u = 0; u < USERS; u += 1) {
    const userId = `dlq-user-${u}`;
    for (let seq = 1; seq <= SEQ_PER_USER; seq += 1) {
      allEvents.push({
        userId,
        type: 'dlq_test',
        payload: { seq, fail: seq === FAIL_SEQ },
      });
    }
  }

  for (let i = 0; i < allEvents.length; i += batchSize) {
    const batch = allEvents.slice(i, i + batchSize);
    const ip = `10.4.0.${Math.floor(i / batchSize) % 250}`;
    const response = await axios.post(
      `${API_URL}/events/batch`,
      { events: batch },
      { timeout: 10000, headers: { 'x-forwarded-for': ip } }
    );
    const results = response.data.results as Array<{ eventId: string; status: string }>;
    results.forEach((r, idx) => {
      if (r.status !== 'accepted') return;
      const event = batch[idx];
      sentEvents.push({
        eventId: r.eventId,
        userId: event.userId,
        seq: event.payload.seq,
        fail: event.payload.fail,
      });
    });
  }

  return sentEvents;
}

async function waitForFinalization(totalCount: number, dlqCount: number, timeoutMs: number): Promise<void> {
  const startTime = Date.now();
  while (Date.now() - startTime < timeoutMs) {
    const length = await redis.llen('dlq:list');
    const finalized = await redis.keys('event:*').then((keys) => keys.length);
    if (length >= dlqCount && finalized >= totalCount) return;
    await new Promise((r) => setTimeout(r, 300));
  }
}

async function runDlqTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('DLQ TEST V2.1: retries, DLQ, and ordering skips');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  console.log('\nSending mixed events (one failing seq per user)...');
  const sentEvents = await sendMixedEvents();
  const failedEvents = sentEvents.filter((e) => e.fail);
  const successEvents = sentEvents.filter((e) => !e.fail);

  console.log('\nWaiting for DLQ entries...');
  await waitForFinalization(sentEvents.length, failedEvents.length, 30000);

  const dlqList = await redis.lrange('dlq:list', 0, -1);
  if (dlqList.length !== failedEvents.length) {
    failureReasons.push(`dlq:list length ${dlqList.length}/${failedEvents.length}`);
  }

  for (const event of failedEvents) {
    const dlqKey = `dlq:${event.eventId}`;
    const data = await redis.hgetall(dlqKey);
    if (!data || !data.attempts || !data.error || !data.queuedAt) {
      failureReasons.push(`Missing dlq hash for ${event.eventId}`);
      continue;
    }
    if (parseInt(data.attempts, 10) < MAX_RETRIES) {
      failureReasons.push(`Event ${event.eventId} attempts ${data.attempts} < ${MAX_RETRIES}`);
    }

    const failedHash = await redis.hgetall(`event:${event.eventId}`);
    if (!failedHash || failedHash.status !== 'failed') {
      failureReasons.push(`Event ${event.eventId} missing failed status`);
      continue;
    }
    if (!failedHash.attempts || parseInt(failedHash.attempts, 10) < MAX_RETRIES) {
      failureReasons.push(`Event ${event.eventId} attempts < ${MAX_RETRIES} in event hash`);
    }
    if (failedHash.seq !== FAIL_SEQ.toString()) {
      failureReasons.push(`Event ${event.eventId} seq ${failedHash.seq} != ${FAIL_SEQ}`);
    }
  }

  for (const event of successEvents) {
    const data = await redis.hgetall(`event:${event.eventId}`);
    if (!data || data.status !== 'processed') {
      failureReasons.push(`Event ${event.eventId} missing processed status`);
      continue;
    }
    if (!data.attempts || parseInt(data.attempts, 10) !== 1) {
      failureReasons.push(`Event ${event.eventId} attempts != 1`);
    }
  }

  console.log('\nChecking ordering lists skip failed seq...');
  for (let u = 0; u < USERS; u += 1) {
    const userId = `dlq-user-${u}`;
    const list = await redis.lrange(`order:${userId}`, 0, -1);
    if (list.length !== SEQ_PER_USER - 1) {
      failureReasons.push(`User ${userId} has ${list.length}/${SEQ_PER_USER - 1} ordered entries`);
      continue;
    }
    let expected = 1;
    for (let i = 0; i < list.length; i += 1) {
      if (expected === FAIL_SEQ) expected += 1;
      if (list[i] !== expected.toString()) {
        failureReasons.push(`User ${userId} order mismatch at index ${i}: got ${list[i]}, expected ${expected}`);
        break;
      }
      expected += 1;
    }
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('DLQ TEST V2.1 RESULTS');
  console.log('═'.repeat(60));

  if (result.passed) {
    console.log('PASSED');
  } else {
    console.log('FAILED');
    for (const reason of result.failureReasons) {
      console.log(`   - ${reason}`);
    }
  }
  console.log('═'.repeat(60) + '\n');
}

runDlqTest()
  .then(async (result) => {
    printResult(result);
    await redis.quit();
    process.exit(result.passed ? 0 : 1);
  })
  .catch(async (error) => {
    console.error('Test failed with error:', error);
    await redis.quit();
    process.exit(1);
  });
