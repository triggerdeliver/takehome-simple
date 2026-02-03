import express, { Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { Event } from './types';
import { initProducer, sendEvent, sendEventsBatch, disconnectProducer } from './producer';
import { checkRateLimit, getMetrics, resetMetrics } from './redis-client';

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

/**
 * TODO: Implement event validation
 * - userId: required, non-empty string
 * - type: required, string
 * - payload: optional, must be object if present
 * - payload.seq: if present, must be positive integer
 */
function validateEvent(data: { userId?: string; type?: string; payload?: unknown }): string | null {
  // TODO: Implement validation logic
  return null;
}

/**
 * POST /events
 * TODO: Implement single event ingestion
 * - Validate input
 * - Check rate limit (100 req/sec per user)
 * - Send to Kafka
 * - Return 202 with eventId on success
 * - Return 400 on validation error
 * - Return 429 on rate limit exceeded
 */
app.post('/events', async (req: Request, res: Response) => {
  // TODO: Implement event ingestion
  res.status(501).json({ error: 'Not implemented' });
});

/**
 * POST /events/batch
 * TODO: Implement batch event ingestion
 * - Validate each event
 * - Check rate limits
 * - Send valid events to Kafka
 * - Return 202 with results array
 */
app.post('/events/batch', async (req: Request, res: Response) => {
  // TODO: Implement batch ingestion
  res.status(501).json({ error: 'Not implemented' });
});

/**
 * GET /metrics
 */
app.get('/metrics', async (_req: Request, res: Response) => {
  try {
    const metrics = await getMetrics();
    res.json(metrics);
  } catch (error) {
    console.error('Error getting metrics:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * POST /metrics/reset
 */
app.post('/metrics/reset', async (_req: Request, res: Response) => {
  try {
    await resetMetrics();
    res.json({ status: 'ok' });
  } catch (error) {
    console.error('Error resetting metrics:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * GET /health
 */
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok' });
});

async function start(): Promise<void> {
  await initProducer();

  app.listen(PORT, () => {
    console.log(`API server listening on port ${PORT}`);
  });
}

async function shutdown(): Promise<void> {
  console.log('Shutting down API...');
  await disconnectProducer();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

start().catch((error) => {
  console.error('Failed to start API:', error);
  process.exit(1);
});
