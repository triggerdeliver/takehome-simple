import { execSync } from 'child_process';

interface TestResult {
  name: string;
  points: number;
  maxPoints: number;
  passed: boolean;
}

const TESTS: { name: string; script: string; points: number }[] = [
  { name: 'Load', script: 'test:load', points: 20 },
  { name: 'Reliability', script: 'test:reliability', points: 20 },
  { name: 'Idempotency', script: 'test:idempotency', points: 15 },
  { name: 'Ordering', script: 'test:ordering', points: 15 },
  { name: 'DLQ', script: 'test:dlq', points: 15 },
  { name: 'Rate Limit', script: 'test:rate-limit', points: 10 },
  { name: 'Validation', script: 'test:validation', points: 5 },
];

function runTest(script: string): boolean {
  try {
    execSync(`npm run ${script}`, { stdio: 'pipe', timeout: 180000 });
    return true;
  } catch {
    return false;
  }
}

function getGrade(score: number): { grade: string; decision: string } {
  if (score >= 90) return { grade: 'A', decision: 'Strong Hire' };
  if (score >= 75) return { grade: 'B', decision: 'Hire' };
  if (score >= 60) return { grade: 'C', decision: 'Maybe' };
  return { grade: 'D', decision: 'No Hire' };
}

async function main(): Promise<void> {
  console.log('\n' + '='.repeat(60));
  console.log('       SUBMISSION TEST - SIMPLE v1.0');
  console.log('='.repeat(60) + '\n');

  const results: TestResult[] = [];
  let totalScore = 0;
  let totalMax = 0;
  let passedCount = 0;

  for (const test of TESTS) {
    process.stdout.write(`Running ${test.name} Test... `);
    const passed = runTest(test.script);
    const earned = passed ? test.points : 0;

    if (passed) {
      console.log(`\x1b[32m✅ PASS (+${test.points})\x1b[0m`);
      passedCount++;
    } else {
      console.log(`\x1b[31m❌ FAIL (0/${test.points})\x1b[0m`);
    }

    results.push({
      name: test.name,
      points: earned,
      maxPoints: test.points,
      passed,
    });

    totalScore += earned;
    totalMax += test.points;
  }

  const { grade, decision } = getGrade(totalScore);

  console.log('\n' + '-'.repeat(60));
  console.log(`  Tests Passed: ${passedCount}/${TESTS.length}`);
  console.log(`  Final Score:  ${totalScore}/${totalMax}`);
  console.log('');
  console.log(`  Grade: ${grade} ${totalScore >= 60 ? '✅' : '❌'}`);
  console.log(`  Decision: ${decision}`);
  console.log('='.repeat(60) + '\n');

  process.exit(totalScore >= 60 ? 0 : 1);
}

main().catch((error) => {
  console.error('Submission test failed:', error);
  process.exit(1);
});
