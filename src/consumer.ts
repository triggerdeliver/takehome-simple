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

const CONCURRENCY = parseInt(process.env.CONSUMER_CONCURRENCY || '384', 10);
const GAP_TIMEOUT_MS = 5000; // Skip missing seq after 5 seconds
const MAX_PROCESSING_RETRIES = 3;

export async function initConsumer(): Promise<void> {
  consumer = kafka.consumer({
    groupId: 'event-processors',
  });

  await consumer.connect();
  await consumer.subscribe({ topic: 'events', fromBeginning: false });

  console.log('Consumer connected and subscribed');
}

/**
 * TODO: Implement event processing
 * - Check if payload.fail is set (simulate failure for DLQ test)
 * - Add processing delay if PROCESSING_DELAY_MS > 0
 */
async function processEvent(event: Event): Promise<void> {
  // TODO: Implement processing logic
  throw new Error('Not implemented');
}

/**
 * TODO: Implement idempotency check
 * - Use tryAcquireEvent to check if event is already being processed
 * - Handle 'acquired', 'processed', 'inflight' statuses
 */
async function handleIdempotency(eventId: string): Promise<'acquired' | 'processed' | 'inflight'> {
  // TODO: Implement idempotency check
  throw new Error('Not implemented');
}

/**
 * TODO: Implement event processing with retry and DLQ
 * - Retry up to MAX_PROCESSING_RETRIES times
 * - On final failure, send to DLQ
 * - Save processing result to Redis
 * - Handle ordering (payload.seq) if present
 */
async function processEventWithRetry(event: Event): Promise<void> {
  // TODO: Implement retry logic with DLQ
  throw new Error('Not implemented');
}

/**
 * TODO: Implement ordering logic
 * - Buffer out-of-order events per user
 * - Process events in seq order
 * - Handle gap timeout (skip missing seq after GAP_TIMEOUT_MS)
 */
async function handleOrdering(event: Event): Promise<void> {
  // TODO: Implement ordering logic
  throw new Error('Not implemented');
}

/**
 * TODO: Implement message handling
 * - Parse message from Kafka
 * - Check idempotency
 * - Handle ordering if seq present
 * - Process event with retry
 */
async function handleMessage(
  message: { value: Buffer | null; offset: string },
  resolveOffset: (offset: string) => void
): Promise<boolean> {
  // TODO: Implement message handling
  if (!message.value) {
    resolveOffset(message.offset);
    return true;
  }

  throw new Error('Not implemented');
}

export async function startConsuming(): Promise<void> {
  await consumer.run({
    autoCommit: true,
    autoCommitInterval: 500,
    autoCommitThreshold: 500,
    partitionsConsumedConcurrently: 8,
    eachBatchAutoResolve: false,
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
      commitOffsetsIfNecessary,
    }) => {
      // TODO: Implement batch processing with concurrency control
      // - Process messages concurrently up to CONCURRENCY limit
      // - Handle shutdown gracefully
      // - Commit offsets appropriately

      for (const message of batch.messages) {
        if (!isRunning() || isStale() || isShuttingDown) {
          break;
        }

        // TODO: Process message
        resolveOffset(message.offset);
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
