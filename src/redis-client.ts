import Redis from 'ioredis';
import { ProcessingResult, RateLimitResult } from './types';

const redis = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  maxRetriesPerRequest: 3,
});

// TTL constants
const EVENT_TTL_SEC = 86400; // 24 hours
const DEDUP_TTL_MS = 86400000; // 24 hours
const INFLIGHT_TTL_MS = 30000; // 30 seconds
const DLQ_TTL_SEC = 86400; // 24 hours

// Rate limit constants
const USER_RATE_LIMIT = 100;
const RATE_WINDOW_MS = 1000;
const RATE_KEY_TTL_SEC = 2;

export type AcquireStatus = 'acquired' | 'inflight' | 'processed';

/**
 * TODO: Implement atomic event acquisition using Lua script
 *
 * Requirements:
 * - Check if event is already processed (event:{eventId} exists) -> return 'processed'
 * - Check if event is inflight (inflight:{eventId} exists) -> return 'inflight'
 * - Otherwise, set inflight lock and return 'acquired'
 *
 * Hint: Use redis.defineCommand() to create a Lua script for atomicity
 */
export async function tryAcquireEvent(eventId: string): Promise<AcquireStatus> {
  // TODO: Implement atomic acquire with Lua script
  throw new Error('Not implemented');
}

/**
 * TODO: Implement processing result saving
 *
 * Requirements:
 * - Save result to event:{eventId} hash with fields: status, processedAt, attempts, userId, type, seq, error
 * - Set dedup:{eventId} key for deduplication
 * - Delete inflight:{eventId} lock
 * - Use atomic Lua script to prevent race conditions
 */
export async function saveProcessingResult(result: ProcessingResult): Promise<boolean> {
  // TODO: Implement atomic save with Lua script
  throw new Error('Not implemented');
}

/**
 * TODO: Implement ordering list management
 *
 * Requirements:
 * - Add seq to order:{userId} list
 * - Check for duplicates before adding
 * - Use atomic Lua script
 */
export async function addToOrderList(userId: string, seq: number): Promise<boolean> {
  // TODO: Implement atomic add to order list
  throw new Error('Not implemented');
}

/**
 * TODO: Implement DLQ (Dead Letter Queue) addition
 *
 * Requirements:
 * - Add eventId to dlq:list
 * - Save error details to dlq:{eventId} hash
 * - Set TTL on DLQ entry
 */
export async function addToDlq(
  eventId: string,
  attempts: number,
  error: string
): Promise<void> {
  // TODO: Implement DLQ addition
  throw new Error('Not implemented');
}

/**
 * TODO: Implement sliding window rate limiting
 *
 * Requirements:
 * - Use sorted set with timestamp as score
 * - Remove entries outside the window
 * - Check if count exceeds limit
 * - Add new entry if under limit
 * - Return allowed, remaining, resetAt
 *
 * Hint: Use Lua script for atomicity
 */
export async function checkRateLimit(userId: string, member?: string): Promise<RateLimitResult> {
  // TODO: Implement sliding window rate limit with Lua script
  throw new Error('Not implemented');
}

/**
 * Release inflight lock for an event
 */
export async function releaseInflight(eventId: string): Promise<void> {
  await redis.del(`inflight:${eventId}`);
}

/**
 * Cleanup all inflight locks (for shutdown)
 */
export async function cleanupAllInflight(): Promise<number> {
  const keys = await redis.keys('inflight:*');
  if (keys.length === 0) return 0;
  return await redis.del(...keys);
}

/**
 * Check if event was already processed
 */
export async function isEventProcessed(eventId: string): Promise<boolean> {
  const exists = await redis.exists(`event:${eventId}`);
  return exists === 1;
}

/**
 * Check for duplicate event
 */
export async function isDuplicate(eventId: string): Promise<boolean> {
  const exists = await redis.exists(`dedup:${eventId}`);
  return exists === 1;
}

/**
 * Get event data by ID
 */
export async function getEventData(eventId: string): Promise<Record<string, string> | null> {
  const data = await redis.hgetall(`event:${eventId}`);
  if (Object.keys(data).length === 0) return null;
  return data;
}

// ============ Metrics (diagnostic only) ============

export async function getMetrics(): Promise<{
  processed: number;
  failed: number;
  rateLimited: number;
  duplicate: number;
}> {
  const [processed, failed, rateLimited, duplicate] = await Promise.all([
    redis.get('metrics:processed'),
    redis.get('metrics:failed'),
    redis.get('metrics:rate_limited'),
    redis.get('metrics:duplicate'),
  ]);

  return {
    processed: parseInt(processed || '0'),
    failed: parseInt(failed || '0'),
    rateLimited: parseInt(rateLimited || '0'),
    duplicate: parseInt(duplicate || '0'),
  };
}

export async function incrementMetric(
  metric: 'processed' | 'failed' | 'rate_limited' | 'duplicate'
): Promise<void> {
  await redis.incr(`metrics:${metric}`);
}

export async function resetMetrics(): Promise<void> {
  await redis.del(
    'metrics:processed',
    'metrics:failed',
    'metrics:rate_limited',
    'metrics:duplicate'
  );
}

// ============ Test utilities ============

export async function countProcessedEvents(): Promise<number> {
  const keys = await redis.keys('event:*');
  return keys.length;
}

export async function getAllProcessedEventIds(): Promise<string[]> {
  const keys = await redis.keys('event:*');
  return keys.map((k) => k.replace('event:', ''));
}

export async function flushAll(): Promise<void> {
  await redis.flushall();
}

export async function disconnect(): Promise<void> {
  await redis.quit();
}

export { redis };
