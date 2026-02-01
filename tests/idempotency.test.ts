import axios from 'axios';
import Redis from 'ioredis';
import { Kafka } from 'kafkajs';

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

const INITIAL_EVENTS = 100;
const DUPLICATE_PERCENT = 30;
const DUPLICATE_DELAY_MS = 2000;

const redis = new Redis(REDIS_URL);
const kafka = new Kafka({ clientId: 'idempotency-test', brokers: [KAFKA_BROKER] });

interface TestResult {
  passed: boolean;
  failureReasons: string[];
}

interface SentEvent {
  eventId: string;
  userId: string;
  type: string;
  payload: object;
}

async function cleanupRedis(): Promise<void> {
  await redis.flushall();
}

async function sendInitialEvents(count: number): Promise<SentEvent[]> {
  const events: SentEvent[] = [];
  const batchSize = 50;

  for (let i = 0; i < count; i += batchSize) {
    const batch = Array.from({ length: Math.min(batchSize, count - i) }, (_, j) => ({
      userId: `idem-user-${(i + j) % 10}`,
      type: 'idempotency_test',
      payload: { index: i + j },
    }));
    const response = await axios.post(`${API_URL}/events/batch`, { events: batch });
    const results = response.data.results as Array<{ eventId: string; status: string }>;
    results.forEach((r, idx) => {
      if (r.status !== 'accepted') return;
      const event = batch[idx];
      events.push({
        eventId: r.eventId,
        userId: event.userId,
        type: event.type,
        payload: event.payload,
      });
    });
  }

  return events;
}

async function waitForProcessing(eventIds: string[], timeoutMs: number): Promise<void> {
  const startTime = Date.now();

  while (Date.now() - startTime < timeoutMs) {
    let processed = 0;
    for (const eventId of eventIds) {
      const exists = await redis.exists(`event:${eventId}`);
      if (exists) processed++;
    }
    if (processed >= eventIds.length) return;
    await new Promise((r) => setTimeout(r, 200));
  }
}

async function getProcessedAtMap(eventIds: string[]): Promise<Map<string, number>> {
  const map = new Map<string, number>();

  for (const eventId of eventIds) {
    const processedAt = await redis.hget(`event:${eventId}`, 'processedAt');
    if (processedAt) {
      map.set(eventId, parseInt(processedAt));
    }
  }

  return map;
}

async function sendDuplicatesToKafka(events: SentEvent[]): Promise<void> {
  const producer = kafka.producer();
  await producer.connect();

  const messages = events.map((event) => ({
    key: event.userId,
    value: JSON.stringify({
      id: event.eventId,
      userId: event.userId,
      type: event.type,
      payload: event.payload,
      timestamp: Date.now(),
    }),
  }));

  await producer.send({ topic: 'events', messages });
  await producer.disconnect();
}

async function runIdempotencyTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('IDEMPOTENCY TEST: Duplicate eventId detection');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  console.log(`\nSending ${INITIAL_EVENTS} initial events...`);
  const sentEvents = await sendInitialEvents(INITIAL_EVENTS);
  const eventIds = sentEvents.map((e) => e.eventId);

  console.log('\nWaiting for processing...');
  await waitForProcessing(eventIds, 30000);

  console.log('\nRecording initial processedAt values...');
  const initialProcessedAt = await getProcessedAtMap(eventIds);

  console.log('\nSimulating duplicate delivery via Kafka...');
  const duplicateCount = Math.floor(sentEvents.length * (DUPLICATE_PERCENT / 100));
  const duplicates = sentEvents.slice(0, duplicateCount);
  await sendDuplicatesToKafka(duplicates);

  await new Promise((r) => setTimeout(r, DUPLICATE_DELAY_MS));

  console.log('\nChecking processedAt values...');
  const finalProcessedAt = await getProcessedAtMap(eventIds);

  let processedAtChangedCount = 0;
  for (const [eventId, initial] of initialProcessedAt) {
    const final = finalProcessedAt.get(eventId);
    if (final && final !== initial) {
      processedAtChangedCount++;
    }
  }

  if (processedAtChangedCount > 0) {
    failureReasons.push(`${processedAtChangedCount} events had processedAt changed`);
  }

  let dedupKeysExist = 0;
  for (const eventId of eventIds) {
    const exists = await redis.exists(`dedup:${eventId}`);
    if (exists) dedupKeysExist++;
  }

  if (dedupKeysExist < eventIds.length) {
    failureReasons.push(`Only ${dedupKeysExist}/${eventIds.length} dedup keys exist`);
  }

  let attemptsOverOne = 0;
  for (const eventId of eventIds) {
    const attempts = await redis.hget(`event:${eventId}`, 'attempts');
    if (attempts && parseInt(attempts, 10) > 1) {
      attemptsOverOne++;
    }
  }
  if (attemptsOverOne > 0) {
    failureReasons.push(`${attemptsOverOne} events have attempts > 1`);
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('IDEMPOTENCY TEST RESULTS');
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

runIdempotencyTest()
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
