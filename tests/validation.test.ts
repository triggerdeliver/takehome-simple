import axios from 'axios';
import Redis from 'ioredis';

const API_URL = process.env.API_URL || 'http://localhost:3000';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

const redis = new Redis(REDIS_URL);

interface TestResult {
  passed: boolean;
  failureReasons: string[];
}

async function cleanupRedis(): Promise<void> {
  await redis.flushall();
}

async function runValidationTest(): Promise<TestResult> {
  console.log('\n' + '═'.repeat(60));
  console.log('VALIDATION TEST: Input validation');
  console.log('═'.repeat(60));

  const failureReasons: string[] = [];

  console.log('\nCleaning up Redis...');
  await cleanupRedis();

  console.log('\nTesting missing userId...');
  try {
    await axios.post(`${API_URL}/events`, {
      type: 'invalid_event',
      payload: {},
    });
    failureReasons.push('Missing userId did not return error');
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status !== 400) {
      failureReasons.push(`Missing userId returned ${error.response?.status}, expected 400`);
    }
  }

  console.log('\nTesting empty userId...');
  try {
    await axios.post(`${API_URL}/events`, {
      userId: '',
      type: 'invalid_event',
      payload: {},
    });
    failureReasons.push('Empty userId did not return error');
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status !== 400) {
      failureReasons.push(`Empty userId returned ${error.response?.status}, expected 400`);
    }
  }

  console.log('\nTesting missing type...');
  try {
    await axios.post(`${API_URL}/events`, {
      userId: 'val-user',
      payload: {},
    });
    failureReasons.push('Missing type did not return error');
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status !== 400) {
      failureReasons.push(`Missing type returned ${error.response?.status}, expected 400`);
    }
  }

  console.log('\nTesting invalid seq (zero)...');
  try {
    await axios.post(`${API_URL}/events`, {
      userId: 'val-user',
      type: 'invalid_seq',
      payload: { seq: 0 },
    });
    failureReasons.push('Zero seq did not return error');
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status !== 400) {
      failureReasons.push(`Zero seq returned ${error.response?.status}, expected 400`);
    }
  }

  console.log('\nTesting invalid seq (negative)...');
  try {
    await axios.post(`${API_URL}/events`, {
      userId: 'val-user',
      type: 'invalid_seq',
      payload: { seq: -1 },
    });
    failureReasons.push('Negative seq did not return error');
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status !== 400) {
      failureReasons.push(`Negative seq returned ${error.response?.status}, expected 400`);
    }
  }

  console.log('\nTesting valid event...');
  try {
    const response = await axios.post(`${API_URL}/events`, {
      userId: 'val-user',
      type: 'valid_event',
      payload: { seq: 1 },
    });
    if (response.status !== 202) {
      failureReasons.push(`Valid event returned ${response.status}, expected 202`);
    }
    if (!response.data.eventId) {
      failureReasons.push('Valid event missing eventId');
    }
  } catch (error) {
    failureReasons.push('Valid event threw error');
  }

  console.log('\nTesting batch with mixed valid/invalid...');
  await cleanupRedis();
  const batchResponse = await axios.post(`${API_URL}/events/batch`, {
    events: [
      { userId: 'val-batch-user', type: 'valid', payload: {} },
      { type: 'invalid_missing_user', payload: {} },
      { userId: 'val-batch-user', type: 'invalid_seq', payload: { seq: -1 } },
    ],
  });

  const results = batchResponse.data.results as Array<{ eventId: string; status: string; error?: string }>;
  if (results.length !== 3) {
    failureReasons.push(`Batch results length ${results.length}, expected 3`);
  } else {
    if (results[0].status !== 'accepted') {
      failureReasons.push(`Batch valid event status ${results[0].status}, expected accepted`);
    }
    if (results[1].status !== 'invalid') {
      failureReasons.push(`Batch missing userId status ${results[1].status}, expected invalid`);
    }
    if (results[2].status !== 'invalid') {
      failureReasons.push(`Batch invalid seq status ${results[2].status}, expected invalid`);
    }
  }

  const passed = failureReasons.length === 0;
  return { passed, failureReasons };
}

function printResult(result: TestResult): void {
  console.log('\n' + '═'.repeat(60));
  console.log('VALIDATION TEST RESULTS');
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

runValidationTest()
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
