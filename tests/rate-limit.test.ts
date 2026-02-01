import axios from 'axios';
import Redis from 'ioredis';

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

const USER_RATE_LIMIT = 100;
const ACCURACY_THRESHOLD = 0.95;
const MAX_DRIFT = 5;

const redis = new Redis(REDIS_URL);

interface TestResult {
  passed: boolean;
  failureReasons: string[];
}

async function cleanupRedis(): Promise<void> {
  const keys = await redis.keys('rate:*');
  if (keys.length > 0) {
    await redis.del(...keys);
  }
}

async function testUserLimit(): Promise<{ allowed: number; rejected: number; accuracy: number; userId: string }> {
  const userId = `rate-user-${Date.now()}`;
  let allowed = 0;
  let rejected = 0;

  const requests = Array.from({ length: 150 }, async () => {
    try {
      await axios.post(
        `${API_URL}/events`,
        { userId, type: 'rate_user', payload: {} },
        { timeout: 5000 }
      );
      allowed++;
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 429) {
        rejected++;
      }
    }
  });

  await Promise.all(requests);

  const accuracy = 1 - Math.abs(allowed - USER_RATE_LIMIT) / 150;
  return { allowed, rejected, accuracy, userId };
}

async function testBatchLimit(): Promise<{ allowed: number; rejected: number; accuracy: number }> {
  const userId = `rate-batch-${Date.now()}`;
  const events = Array.from({ length: 150 }, (_, i) => ({
    userId,
    type: 'rate_batch',
    payload: { index: i },
  }));

  let allowed = 0;
  let rejected = 0;

  const response = await axios.post(
    `${API_URL}/events/batch`,
    { events },
    { timeout: 10000 }
  );

  const results = response.data.results as Array<{ eventId: string; status: string }>;
  for (const r of results) {
    if (r.status === 'accepted') allowed++;
    if (r.status === 'rate_limited') rejected++;
  }

  const accuracy = 1 - Math.abs(allowed - USER_RATE_LIMIT) / 150;
  return { allowed, rejected, accuracy };
}

async function expectRateKey(userId: string): Promise<boolean> {
  const exists = await redis.exists(`rate:${userId}`);
  return exists === 1;
}

async function testSlidingWindow(): Promise<boolean> {
  const userId = `rate-sliding-${Date.now()}`;

  let firstBatchAllowed = 0;
  const firstBatch = Array.from({ length: 80 }, async () => {
    try {
      await axios.post(`${API_URL}/events`, {
        userId,
        type: 'rate_sliding',
        payload: {},
      });
      firstBatchAllowed++;
    } catch {}
  });
  await Promise.all(firstBatch);

  await new Promise((r) => setTimeout(r, 500));

  let secondBatchAllowed = 0;
  const secondBatch = Array.from({ length: 40 }, async () => {
    try {
      await axios.post(`${API_URL}/events`, {
        userId,
        type: 'rate_sliding',
        payload: {},
      });
      secondBatchAllowed++;
    } catch {}
  });
  await Promise.all(secondBatch);

  // Sliding window should allow some events from second batch
  return secondBatchAllowed > 0 && secondBatchAllowed < 40;
}

async function runRateLimitTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('RATE LIMIT TEST: 100 events/sec per userId');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up rate limit keys...');
  await cleanupRedis();

  console.log('\nTesting per-user limit...');
  const userResult = await testUserLimit();
  console.log(`   Allowed: ${userResult.allowed}, Rejected: ${userResult.rejected}`);

  if (userResult.rejected === 0 || userResult.accuracy < ACCURACY_THRESHOLD) {
    failureReasons.push(`User limit accuracy ${(userResult.accuracy * 100).toFixed(1)}%`);
  }
  if (
    userResult.allowed < USER_RATE_LIMIT - MAX_DRIFT ||
    userResult.allowed > USER_RATE_LIMIT + MAX_DRIFT
  ) {
    failureReasons.push(`User limit allowed ${userResult.allowed} out of bounds [${USER_RATE_LIMIT - MAX_DRIFT}, ${USER_RATE_LIMIT + MAX_DRIFT}]`);
  }
  const userKeyOk = await expectRateKey(userResult.userId);
  if (!userKeyOk) {
    failureReasons.push('Rate key not created for per-user test');
  }

  await new Promise((r) => setTimeout(r, 1500));
  await cleanupRedis();

  console.log('\nTesting batch limit...');
  const batchResult = await testBatchLimit();
  console.log(`   Allowed: ${batchResult.allowed}, Rejected: ${batchResult.rejected}`);

  if (batchResult.rejected === 0 || batchResult.accuracy < ACCURACY_THRESHOLD) {
    failureReasons.push(`Batch limit accuracy ${(batchResult.accuracy * 100).toFixed(1)}%`);
  }
  if (
    batchResult.allowed < USER_RATE_LIMIT - MAX_DRIFT ||
    batchResult.allowed > USER_RATE_LIMIT + MAX_DRIFT
  ) {
    failureReasons.push(`Batch allowed ${batchResult.allowed} out of bounds`);
  }

  console.log('\nTesting sliding window behavior...');
  const isSliding = await testSlidingWindow();
  if (!isSliding) {
    failureReasons.push('Sliding window not detected');
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('RATE LIMIT TEST RESULTS');
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

runRateLimitTest()
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
