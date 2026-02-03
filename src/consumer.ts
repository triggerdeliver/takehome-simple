import { Kafka, Consumer } from 'kafkajs';
import { Event, ProcessingResult } from './types';
import {
  tryAcquireEvent,
  saveProcessingResult,
  addToOrderList,
  addToDlq,
  releaseInflight,
  cleanupAllInflight,
  disconnect as disconnectRedis,
  redis,
} from './redis-client';

const kafka = new Kafka({
  clientId: 'event-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

let consumer: Consumer;
let isShuttingDown = false;
const inFlightTasks = new Set<Promise<unknown>>();
const inFlightEventIds = new Set<string>();

// Per-user ordering buffers: userId -> Map<seq, Event>
const orderBuffers = new Map<string, Map<number, Event>>();
// Per-user next expected seq
const nextExpectedSeq = new Map<string, number>();
// Per-user gap wait start time (when we started waiting for a missing seq)
const gapWaitStart = new Map<string, number>();

const CONCURRENCY = parseInt(process.env.CONSUMER_CONCURRENCY || '384', 10);
const GAP_TIMEOUT_MS = 5000; // Skip missing seq after 5 seconds
const PARTITIONS_CONCURRENCY = parseInt(process.env.CONSUMER_PARTITIONS || '8', 10);
const MAX_PROCESSING_RETRIES = 3;
const INFLIGHT_RETRY_ATTEMPTS = parseInt(process.env.INFLIGHT_RETRY_ATTEMPTS || '6', 10);
const INFLIGHT_RETRY_DELAY_MS = parseInt(process.env.INFLIGHT_RETRY_DELAY_MS || '30', 10);
const PROCESSING_DELAY_MS = parseInt(process.env.PROCESSING_DELAY_MS || '0', 10);

export async function initConsumer(): Promise<void> {
  consumer = kafka.consumer({
    groupId: 'event-processors',
  });

  await consumer.connect();
  await consumer.subscribe({ topic: 'events', fromBeginning: false });

  console.log('Consumer connected and subscribed');
}

async function processEvent(event: Event): Promise<void> {
  // Check if payload.fail is set - simulate failure
  if (event.payload?.fail === true) {
    throw new Error('Simulated failure for DLQ test');
  }

  if (PROCESSING_DELAY_MS > 0) {
    await new Promise((resolve) => setTimeout(resolve, Math.random() * PROCESSING_DELAY_MS));
  }
}

async function delay(ms: number): Promise<void> {
  if (ms <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForAcquire(eventId: string): Promise<'acquired' | 'processed' | 'inflight'> {
  let status = await tryAcquireEvent(eventId);
  if (status !== 'inflight') {
    return status;
  }

  for (let i = 0; i < INFLIGHT_RETRY_ATTEMPTS; i += 1) {
    await delay(INFLIGHT_RETRY_DELAY_MS);
    status = await tryAcquireEvent(eventId);
    if (status !== 'inflight') {
      return status;
    }
  }

  return 'inflight';
}

async function getNextExpectedSeqFromRedis(userId: string): Promise<number> {
  const orderList = await redis.lrange(`order:${userId}`, 0, -1);
  if (orderList.length === 0) {
    return 1;
  }
  const maxSeq = Math.max(...orderList.map(s => parseInt(s, 10)));
  return maxSeq + 1;
}

async function tryProcessBufferedEvents(userId: string): Promise<void> {
  const buffer = orderBuffers.get(userId);
  if (!buffer || buffer.size === 0) return;

  let nextSeq = nextExpectedSeq.get(userId);
  if (nextSeq === undefined) {
    nextSeq = await getNextExpectedSeqFromRedis(userId);
    nextExpectedSeq.set(userId, nextSeq);
  }

  while (true) {
    if (buffer.has(nextSeq)) {
      // Process the next expected seq
      const event = buffer.get(nextSeq)!;
      buffer.delete(nextSeq);
      gapWaitStart.delete(userId); // Reset gap timer

      await processEventWithRetry(event);

      nextSeq += 1;
      nextExpectedSeq.set(userId, nextSeq);
    } else if (buffer.size > 0) {
      // We have buffered events but not the next expected seq - check gap timeout
      const waitStart = gapWaitStart.get(userId);
      if (!waitStart) {
        // Start waiting for this gap
        gapWaitStart.set(userId, Date.now());
        break;
      } else if (Date.now() - waitStart > GAP_TIMEOUT_MS) {
        // Gap timeout exceeded - skip to next available seq
        const bufferedSeqs = Array.from(buffer.keys()).sort((a, b) => a - b);
        const nextAvailable = bufferedSeqs.find(s => s > nextSeq);
        if (nextAvailable) {
          console.log(`Gap timeout: skipping seq ${nextSeq} to ${nextAvailable - 1} for user ${userId}`);
          nextSeq = nextAvailable;
          nextExpectedSeq.set(userId, nextSeq);
          gapWaitStart.delete(userId);
          // Continue processing from next available
        } else {
          break;
        }
      } else {
        // Still waiting for gap
        break;
      }
    } else {
      // No more buffered events
      gapWaitStart.delete(userId);
      break;
    }
  }
}

async function processEventWithRetry(event: Event): Promise<void> {
  const seq = event.payload?.seq as number | undefined;
  let lastError = '';

  for (let attempt = 1; attempt <= MAX_PROCESSING_RETRIES; attempt += 1) {
    try {
      await processEvent(event);

      // Success - save result
      const result: ProcessingResult = {
        eventId: event.id,
        status: 'processed',
        processedAt: Date.now(),
        attempts: 1,
        userId: event.userId,
        type: event.type,
        seq: seq,
      };
      await saveProcessingResult(result);

      // Add to ordering list if seq is present
      if (seq !== undefined) {
        await addToOrderList(event.userId, seq);
      }

      return;
    } catch (error) {
      lastError = error instanceof Error ? error.message : 'Unknown error';

      if (attempt < MAX_PROCESSING_RETRIES) {
        await delay(5 * attempt);
        continue;
      }

      // Max retries exceeded - send to DLQ
      const failedResult: ProcessingResult = {
        eventId: event.id,
        status: 'failed',
        processedAt: Date.now(),
        attempts: attempt,
        userId: event.userId,
        type: event.type,
        seq: seq,
        error: lastError,
      };
      await saveProcessingResult(failedResult);
      await addToDlq(event.id, attempt, lastError);

      return;
    }
  }
}

async function handleMessage(
  message: { value: Buffer | null; offset: string },
  resolveOffset: (offset: string) => void
): Promise<boolean> {
  if (isShuttingDown) return false;
  if (!message.value) {
    resolveOffset(message.offset);
    return true;
  }

  let event: Event;
  try {
    event = JSON.parse(message.value.toString());
  } catch (error) {
    console.error('Failed to parse message', error);
    resolveOffset(message.offset);
    return true;
  }

  inFlightEventIds.add(event.id);

  try {
    const acquireStatus = await waitForAcquire(event.id);
    if (acquireStatus === 'processed') {
      resolveOffset(message.offset);
      return true;
    }

    if (acquireStatus === 'inflight') {
      return false;
    }

    const seq = event.payload?.seq as number | undefined;

    if (seq !== undefined) {
      let buffer = orderBuffers.get(event.userId);
      if (!buffer) {
        buffer = new Map();
        orderBuffers.set(event.userId, buffer);
      }

      let nextSeq = nextExpectedSeq.get(event.userId);
      if (nextSeq === undefined) {
        nextSeq = await getNextExpectedSeqFromRedis(event.userId);
        nextExpectedSeq.set(event.userId, nextSeq);
      }

      if (seq === nextSeq) {
        await processEventWithRetry(event);
        nextExpectedSeq.set(event.userId, nextSeq + 1);
        await tryProcessBufferedEvents(event.userId);
      } else if (seq > nextSeq) {
        buffer.set(seq, event);
        await releaseInflight(event.id);
      } else {
        await releaseInflight(event.id);
      }
    } else {
      await processEventWithRetry(event);
    }

    resolveOffset(message.offset);
    return true;
  } finally {
    inFlightEventIds.delete(event.id);
  }
}

export async function startConsuming(): Promise<void> {
  await consumer.run({
    autoCommit: true,
    autoCommitInterval: 500,
    autoCommitThreshold: 500,
    partitionsConsumedConcurrently: PARTITIONS_CONCURRENCY,
    eachBatchAutoResolve: false,
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
      commitOffsetsIfNecessary,
    }) => {
      const localInFlight = new Set<Promise<boolean>>();
      let scheduled = 0;
      let allResolved = true;

      for (const message of batch.messages) {
        if (!isRunning() || isStale() || isShuttingDown) {
          allResolved = false;
          break;
        }

        const task = handleMessage(message, resolveOffset).catch((error) => {
          console.error('Error handling message:', error);
          return false;
        });

        inFlightTasks.add(task);
        localInFlight.add(task);
        task.finally(() => {
          inFlightTasks.delete(task);
          localInFlight.delete(task);
        });

        scheduled += 1;
        if (localInFlight.size >= CONCURRENCY) {
          const result = await Promise.race(localInFlight);
          if (!result) {
            allResolved = false;
          }
        }
        if (scheduled % 200 === 0) {
          await heartbeat();
        }
      }

      const results = await Promise.allSettled(localInFlight);
      for (const result of results) {
        if (result.status === 'fulfilled' && result.value === false) {
          allResolved = false;
        }
      }

      if (allResolved) {
        await consumer.commitOffsets([
          {
            topic: batch.topic,
            partition: batch.partition,
            offset: batch.highWatermark,
          },
        ]);
      } else {
        await commitOffsetsIfNecessary();
      }

      await heartbeat();
    },
  });

  console.log('Consumer started');
}

export async function shutdown(): Promise<void> {
  console.log('Shutting down consumer...');
  isShuttingDown = true;

  if (consumer) {
    try {
      await consumer.stop();
    } catch (error) {
      console.error('Error stopping consumer:', error);
    }
  }

  if (inFlightTasks.size > 0) {
    await Promise.race([
      Promise.allSettled(Array.from(inFlightTasks)),
      delay(2000),
    ]);
  }

  for (const eventId of inFlightEventIds) {
    try {
      await releaseInflight(eventId);
    } catch {
      // ignore
    }
  }

  try {
    await cleanupAllInflight();
  } catch {
    // ignore
  }

  if (consumer) {
    await consumer.disconnect();
  }
  await disconnectRedis();

  console.log('Consumer shutdown complete');
}

process.on('SIGTERM', async () => {
  await shutdown();
  process.exit(0);
});

process.on('SIGINT', async () => {
  await shutdown();
  process.exit(0);
});

async function start(): Promise<void> {
  await initConsumer();
  await startConsuming();
}

start().catch((error) => {
  console.error('Failed to start consumer:', error);
  process.exit(1);
});
