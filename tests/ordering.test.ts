import axios from 'axios';
import Redis from 'ioredis';

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

const USERS = 10;
const SEQ_PER_USER = 100;
const PHASE_DELAY_MS = 1000;

const redis = new Redis(REDIS_URL);

interface TestResult {
  passed: boolean;
  failureReasons: string[];
}

async function cleanupRedis(): Promise<void> {
  await redis.flushall();
}

async function sendOutOfOrderEvents(): Promise<void> {
  const batchSize = 200;
  const evenEvents: Array<{ userId: string; type: string; payload: object }> = [];
  const oddEvents: Array<{ userId: string; type: string; payload: object }> = [];

  for (let u = 0; u < USERS; u += 1) {
    const userId = `order-user-${u}`;
    for (let seq = 1; seq <= SEQ_PER_USER; seq += 1) {
      const event = { userId, type: 'ordering_test', payload: { seq } };
      if (seq % 2 === 0) evenEvents.push(event);
      else oddEvents.push(event);
    }
  }

  for (let i = 0; i < evenEvents.length; i += batchSize) {
    const batch = evenEvents.slice(i, i + batchSize);
    const ip = `10.3.1.${Math.floor(i / batchSize) % 250}`;
    await axios.post(
      `${API_URL}/events/batch`,
      { events: batch },
      { timeout: 10000, headers: { 'x-forwarded-for': ip } }
    );
  }

  await new Promise((r) => setTimeout(r, PHASE_DELAY_MS));

  for (let i = 0; i < oddEvents.length; i += batchSize) {
    const batch = oddEvents.slice(i, i + batchSize);
    const ip = `10.3.2.${Math.floor(i / batchSize) % 250}`;
    await axios.post(
      `${API_URL}/events/batch`,
      { events: batch },
      { timeout: 10000, headers: { 'x-forwarded-for': ip } }
    );
  }
}

async function waitForProcessing(timeoutMs: number): Promise<void> {
  const startTime = Date.now();

  while (Date.now() - startTime < timeoutMs) {
    const processedCount = await redis.keys('event:*').then((keys) => keys.length);
    if (processedCount >= USERS * SEQ_PER_USER) {
      return;
    }
    await new Promise((r) => setTimeout(r, 300));
  }
}

async function runOrderingTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('ORDERING TEST V2.1: per-user ordering with out-of-order arrival');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  console.log('\nSending out-of-order events (evens then odds)...');
  await sendOutOfOrderEvents();

  console.log('\nWaiting for processing...');
  await waitForProcessing(30000);

  console.log('\nChecking per-user order lists...');
  for (let u = 0; u < USERS; u += 1) {
    const userId = `order-user-${u}`;
    const list = await redis.lrange(`order:${userId}`, 0, -1);
    if (list.length !== SEQ_PER_USER) {
      failureReasons.push(`User ${userId} has ${list.length}/${SEQ_PER_USER} ordered entries`);
      continue;
    }
    for (let i = 0; i < SEQ_PER_USER; i += 1) {
      const expected = (i + 1).toString();
      if (list[i] !== expected) {
        failureReasons.push(`User ${userId} order mismatch at index ${i}: got ${list[i]}, expected ${expected}`);
        break;
      }
    }
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('ORDERING TEST V2.1 RESULTS');
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

runOrderingTest()
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
