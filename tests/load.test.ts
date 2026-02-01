import axios from 'axios';
import Redis from 'ioredis';
import { Kafka } from 'kafkajs';

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

const TOTAL_EVENTS = 50000;
const TIME_LIMIT_MS = 30000;
const MIN_SUCCESS_RATE = 0.95;
const BATCH_SIZE = 500;
const CONCURRENT_SENDERS = 20;
const SAMPLE_SIZE = 100;

const redis = new Redis(REDIS_URL);
const kafka = new Kafka({ clientId: 'load-test', brokers: [KAFKA_BROKER] });

interface TestResult {
  totalSent: number;
  totalProcessedByRedis: number;
  durationMs: number;
  throughput: number;
  successRate: number;
  kafkaLag: number;
  passed: boolean;
  failureReasons: string[];
}

async function cleanupRedis(): Promise<void> {
  await redis.flushall();
}

async function countProcessedInRedis(): Promise<number> {
  const keys = await redis.keys('event:*');
  return keys.length;
}

async function getKafkaLag(): Promise<number> {
  const admin = kafka.admin();
  await admin.connect();

  try {
    const offsets = await admin.fetchOffsets({ groupId: 'event-processors', topics: ['events'] });
    const topicOffsets = await admin.fetchTopicOffsets('events');

    let totalLag = 0;
    for (const partition of offsets) {
      for (const p of partition.partitions) {
        const latestOffset = topicOffsets.find((t) => t.partition === p.partition);
        if (latestOffset) {
          const lag = parseInt(latestOffset.offset) - parseInt(p.offset);
          totalLag += Math.max(0, lag);
        }
      }
    }
    return totalLag;
  } finally {
    await admin.disconnect();
  }
}

async function sendBatch(
  events: Array<{ userId: string; type: string; payload: object }>
): Promise<{ sent: number; eventIds: string[] }> {
  try {
    const response = await axios.post(
      `${API_URL}/events/batch`,
      { events },
      { timeout: 15000 }
    );
    const results = response.data.results as Array<{ eventId: string; status: string }>;
    const accepted = results.filter((r) => r.status === 'accepted');
    return {
      sent: accepted.length,
      eventIds: accepted.map((r) => r.eventId),
    };
  } catch {
    return { sent: 0, eventIds: [] };
  }
}

async function sendAllEvents(): Promise<{ totalSent: number; allEventIds: string[] }> {
  const userIds = Array.from({ length: 5000 }, (_, i) => `user-${i}`);
  const batches: Array<Array<{ userId: string; type: string; payload: object }>> = [];

  for (let i = 0; i < TOTAL_EVENTS; i += BATCH_SIZE) {
    const batch = Array.from({ length: Math.min(BATCH_SIZE, TOTAL_EVENTS - i) }, (_, j) => ({
      userId: userIds[(i + j) % userIds.length],
      type: 'load_test',
      payload: { idx: i + j },
    }));
    batches.push(batch);
  }

  const allEventIds: string[] = [];
  let totalSent = 0;

  const chunkSize = Math.ceil(batches.length / CONCURRENT_SENDERS);
  const promises: Promise<{ sent: number; eventIds: string[] }>[] = [];

  for (let i = 0; i < batches.length; i += chunkSize) {
    const chunk = batches.slice(i, i + chunkSize);
    promises.push(
      (async () => {
        let chunkSent = 0;
        const chunkEventIds: string[] = [];
        for (const batch of chunk) {
          const result = await sendBatch(batch);
          chunkSent += result.sent;
          chunkEventIds.push(...result.eventIds);
        }
        return { sent: chunkSent, eventIds: chunkEventIds };
      })()
    );
  }

  const results = await Promise.all(promises);
  for (const result of results) {
    totalSent += result.sent;
    allEventIds.push(...result.eventIds);
  }

  return { totalSent, allEventIds };
}

async function waitForProcessing(targetCount: number, timeoutMs: number): Promise<void> {
  const startTime = Date.now();
  const targetProcessed = Math.floor(targetCount * MIN_SUCCESS_RATE);

  while (Date.now() - startTime < timeoutMs) {
    const processed = await countProcessedInRedis();
    if (processed >= targetProcessed) {
      return;
    }
    await new Promise((r) => setTimeout(r, 500));
  }
}

async function runLoadTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('LOAD TEST: 50K events in 30 seconds');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  console.log('\nSending events...');
  const startTime = Date.now();
  const { totalSent, allEventIds } = await sendAllEvents();
  console.log(`   Sent: ${totalSent.toLocaleString()} events`);

  console.log('\nWaiting for processing...');
  await waitForProcessing(totalSent, TIME_LIMIT_MS);

  const durationMs = Date.now() - startTime;

  console.log('\nVerifying results in Redis...');
  const totalProcessedByRedis = await countProcessedInRedis();
  const throughput = Math.round((totalProcessedByRedis / durationMs) * 1000);
  const successRate = totalSent > 0 ? totalProcessedByRedis / totalSent : 0;

  let kafkaLag = 0;
  try {
    kafkaLag = await getKafkaLag();
  } catch {
    console.log('   Warning: Could not check Kafka lag');
  }

  if (totalProcessedByRedis < TOTAL_EVENTS * MIN_SUCCESS_RATE) {
    failureReasons.push(
      `Processed ${totalProcessedByRedis.toLocaleString()} events, need ${(TOTAL_EVENTS * MIN_SUCCESS_RATE).toLocaleString()}`
    );
  }
  if (durationMs > TIME_LIMIT_MS) {
    failureReasons.push(`Duration ${durationMs}ms exceeds limit ${TIME_LIMIT_MS}ms`);
  }
  if (kafkaLag > 0) {
    failureReasons.push(`Kafka lag is ${kafkaLag}, should be 0`);
  }

  console.log('\nValidating sampled Redis hashes...');
  const sampleIds = allEventIds.slice(0, Math.min(SAMPLE_SIZE, allEventIds.length));
  if (sampleIds.length > 0) {
    const pipeline = redis.pipeline();
    for (const eventId of sampleIds) {
      pipeline.hgetall(`event:${eventId}`);
      pipeline.exists(`dedup:${eventId}`);
    }
    const responses = await pipeline.exec();
    if (responses) {
      for (let i = 0; i < responses.length; i += 2) {
        const hash = responses[i]?.[1] as Record<string, string>;
        const dedupExists = responses[i + 1]?.[1] as number;
        if (!hash || !hash.status || !hash.processedAt || !hash.attempts) {
          failureReasons.push('Sample event missing required fields');
          break;
        }
        if (!hash.userId || !hash.type) {
          failureReasons.push('Sample event missing userId/type');
          break;
        }
        if (hash.status !== 'processed') {
          failureReasons.push('Sample event status not processed');
          break;
        }
        if (parseInt(hash.attempts, 10) !== 1) {
          failureReasons.push('Sample event attempts not 1');
          break;
        }
        if (!dedupExists) {
          failureReasons.push('Sample event missing dedup key');
          break;
        }
      }
    }
  }

  const passed = failureReasons.length === 0;

  return {
    totalSent,
    totalProcessedByRedis,
    durationMs,
    throughput,
    successRate,
    kafkaLag,
    passed,
    failureReasons,
  };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('LOAD TEST RESULTS');
  console.log('═'.repeat(60));
  console.log(`  Events sent:           ${result.totalSent.toLocaleString()}`);
  console.log(`  Events in Redis:       ${result.totalProcessedByRedis.toLocaleString()}`);
  console.log(`  Duration:              ${result.durationMs.toLocaleString()}ms`);
  console.log(`  Throughput:            ${result.throughput.toLocaleString()} events/sec`);
  console.log(`  Success rate:          ${(result.successRate * 100).toFixed(2)}%`);
  console.log(`  Kafka lag:             ${result.kafkaLag}`);
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

runLoadTest()
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
