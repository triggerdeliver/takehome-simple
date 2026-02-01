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

// Lua script: atomic acquire for event processing
(redis as any).defineCommand('acquireEvent', {
  numberOfKeys: 2,
  lua: `
    -- KEYS[1] = event:{eventId}
    -- KEYS[2] = inflight:{eventId}
    -- ARGV[1] = inflight TTL ms
    if redis.call('EXISTS', KEYS[1]) == 1 then
      return 2  -- already processed
    end
    if redis.call('EXISTS', KEYS[2]) == 1 then
      return 0  -- inflight by another worker
    end
    redis.call('SET', KEYS[2], '1', 'PX', ARGV[1])
    return 1  -- acquired
  `,
});

// Lua script: finalize event with all required fields
(redis as any).defineCommand('finalizeEvent', {
  numberOfKeys: 3,
  lua: `
    -- KEYS[1] = event:{eventId}
    -- KEYS[2] = dedup:{eventId}
    -- KEYS[3] = inflight:{eventId}
    -- ARGV[1] = EVENT_TTL_SEC
    -- ARGV[2..n-1] = field/value pairs
    -- ARGV[n] = DEDUP_TTL_MS
    if redis.call('EXISTS', KEYS[1]) == 1 then
      redis.call('DEL', KEYS[3])
      return 0  -- already finalized
    end
    local ttl = tonumber(ARGV[1])
    local dedupTtl = tonumber(ARGV[#ARGV])
    for i = 2, (#ARGV - 1), 2 do
      redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
    end
    redis.call('EXPIRE', KEYS[1], ttl)
    redis.call('SET', KEYS[2], '1', 'PX', dedupTtl)
    redis.call('DEL', KEYS[3])
    return 1
  `,
});

// Lua script: sliding window rate limit
(redis as any).defineCommand('rateLimitCheck', {
  numberOfKeys: 1,
  lua: `
    local now = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local limit = tonumber(ARGV[3])
    local member = ARGV[4]
    local ttl = tonumber(ARGV[5])

    redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
    local count = redis.call('ZCARD', KEYS[1])

    if count >= limit then
      local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
      local resetAt = now + window
      if oldest[2] ~= nil then
        resetAt = tonumber(oldest[2]) + window
      end
      return {0, 0, resetAt}
    end

    redis.call('ZADD', KEYS[1], now, member)
    redis.call('EXPIRE', KEYS[1], ttl)

    local remaining = limit - count - 1
    local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
    local resetAt = now + window
    if oldest[2] ~= nil then
      resetAt = tonumber(oldest[2]) + window
    end
    return {1, remaining, resetAt}
  `,
});

// Lua script: add to order list atomically (check for duplicates)
(redis as any).defineCommand('addToOrder', {
  numberOfKeys: 1,
  lua: `
    -- KEYS[1] = order:{userId}
    -- ARGV[1] = seq value
    local seq = ARGV[1]
    -- Check if seq already exists in list
    local existing = redis.call('LRANGE', KEYS[1], 0, -1)
    for _, v in ipairs(existing) do
      if v == seq then
        return 0  -- already exists
      end
    end
    redis.call('RPUSH', KEYS[1], seq)
    return 1
  `,
});

// Lua script: add to DLQ
(redis as any).defineCommand('addToDlq', {
  numberOfKeys: 2,
  lua: `
    -- KEYS[1] = dlq:list
    -- KEYS[2] = dlq:{eventId}
    -- ARGV[1] = eventId
    -- ARGV[2] = attempts
    -- ARGV[3] = error
    -- ARGV[4] = queuedAt
    -- ARGV[5] = ttl seconds
    redis.call('RPUSH', KEYS[1], ARGV[1])
    redis.call('HSET', KEYS[2], 'eventId', ARGV[1], 'attempts', ARGV[2], 'error', ARGV[3], 'queuedAt', ARGV[4])
    redis.call('EXPIRE', KEYS[2], ARGV[5])
    return 1
  `,
});

export async function tryAcquireEvent(eventId: string): Promise<AcquireStatus> {
  const result = await (redis as any).acquireEvent(
    `event:${eventId}`,
    `inflight:${eventId}`,
    INFLIGHT_TTL_MS
  );

  const code = typeof result === 'number' ? result : parseInt(result, 10);
  if (code === 1) return 'acquired';
  if (code === 2) return 'processed';
  return 'inflight';
}

export async function saveProcessingResult(result: ProcessingResult): Promise<boolean> {
  const key = `event:${result.eventId}`;
  const dedupKey = `dedup:${result.eventId}`;
  const inflightKey = `inflight:${result.eventId}`;

  const args: string[] = [
    EVENT_TTL_SEC.toString(),
    'status', result.status,
    'processedAt', result.processedAt.toString(),
    'attempts', (result.attempts || 1).toString(),
  ];

  if (result.userId) {
    args.push('userId', result.userId);
  }
  if (result.type) {
    args.push('type', result.type);
  }
  if (result.seq !== undefined) {
    args.push('seq', result.seq.toString());
  }
  if (result.error) {
    args.push('error', result.error);
  }

  args.push(DEDUP_TTL_MS.toString());

  const saved = await (redis as any).finalizeEvent(key, dedupKey, inflightKey, ...args);
  return Number(saved) === 1;
}

export async function addToOrderList(userId: string, seq: number): Promise<boolean> {
  const result = await (redis as any).addToOrder(`order:${userId}`, seq.toString());
  return Number(result) === 1;
}

export async function addToDlq(
  eventId: string,
  attempts: number,
  error: string
): Promise<void> {
  await (redis as any).addToDlq(
    'dlq:list',
    `dlq:${eventId}`,
    eventId,
    attempts.toString(),
    error,
    Date.now().toString(),
    DLQ_TTL_SEC.toString()
  );
}

export async function checkRateLimit(userId: string, member?: string): Promise<RateLimitResult> {
  const now = Date.now();
  const uniqueMember = member || `${now}:${Math.random().toString(36).slice(2, 10)}`;

  const result = await (redis as any).rateLimitCheck(
    `rate:${userId}`,
    now,
    RATE_WINDOW_MS,
    USER_RATE_LIMIT,
    uniqueMember,
    RATE_KEY_TTL_SEC
  );

  const allowed = result[0] === 1 || result[0] === '1';
  const remaining = parseInt(result[1], 10) || 0;
  const resetAt = parseInt(result[2], 10) || now + RATE_WINDOW_MS;

  return { allowed, remaining, resetAt };
}

export async function releaseInflight(eventId: string): Promise<void> {
  await redis.del(`inflight:${eventId}`);
}

export async function cleanupAllInflight(): Promise<number> {
  const keys = await redis.keys('inflight:*');
  if (keys.length === 0) return 0;
  return await redis.del(...keys);
}

export async function isEventProcessed(eventId: string): Promise<boolean> {
  const exists = await redis.exists(`event:${eventId}`);
  return exists === 1;
}

export async function isDuplicate(eventId: string): Promise<boolean> {
  const exists = await redis.exists(`dedup:${eventId}`);
  return exists === 1;
}

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
