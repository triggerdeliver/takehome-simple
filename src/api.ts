import express, { Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { Event } from './types';
import { initProducer, sendEvent, sendEventsBatch, disconnectProducer } from './producer';
import { checkRateLimit, getMetrics, resetMetrics } from './redis-client';

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

/**
 * Validate event payload
 */
function validateEvent(data: { userId?: string; type?: string; payload?: unknown }): string | null {
  if (!data.userId || typeof data.userId !== 'string' || data.userId.trim() === '') {
    return 'userId is required and must be a non-empty string';
  }
  if (!data.type || typeof data.type !== 'string') {
    return 'type is required and must be a string';
  }
  if (data.payload !== undefined && (typeof data.payload !== 'object' || data.payload === null)) {
    return 'payload must be an object';
  }

  // Validate seq if present
  const payload = data.payload as Record<string, unknown> | undefined;
  if (payload && 'seq' in payload) {
    const seq = payload.seq;
    if (typeof seq !== 'number' || !Number.isInteger(seq) || seq < 1) {
      return 'payload.seq must be a positive integer';
    }
  }

  return null;
}

/**
 * POST /events
 */
app.post('/events', async (req: Request, res: Response) => {
  try {
    const { userId, type, payload } = req.body;

    // Validate input
    const validationError = validateEvent({ userId, type, payload });
    if (validationError) {
      res.status(400).json({ error: validationError });
      return;
    }

    const event: Event = {
      id: uuidv4(),
      userId,
      type,
      payload: payload || {},
      timestamp: Date.now(),
    };

    // Check rate limit (per user only)
    const rateLimitResult = await checkRateLimit(userId, event.id);

    if (!rateLimitResult.allowed) {
      res.status(429).json({
        error: 'Rate limit exceeded',
        remaining: 0,
        resetAt: rateLimitResult.resetAt,
      });
      return;
    }

    await sendEvent(event);

    res.status(202).json({
      eventId: event.id,
      status: 'accepted',
    });
  } catch (error) {
    console.error('Error handling event:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

/**
 * POST /events/batch
 */
app.post('/events/batch', async (req: Request, res: Response) => {
  try {
    const { events } = req.body;

    if (!Array.isArray(events) || events.length === 0) {
      res.status(400).json({ error: 'events array is required' });
      return;
    }

    if (events.length > 1000) {
      res.status(400).json({ error: 'Maximum 1000 events per batch' });
      return;
    }

    const results: Array<{ eventId: string; status: string; error?: string }> = new Array(events.length);
    const acceptedEvents: Event[] = [];

    const CONCURRENCY = parseInt(process.env.RATE_CHECK_CONCURRENCY || '200', 10);

    for (let i = 0; i < events.length; i += CONCURRENCY) {
      const chunk = events.slice(i, i + CONCURRENCY);
      await Promise.all(
        chunk.map(async (eventData, idx) => {
          const { userId, type, payload } = eventData;
          const resultIndex = i + idx;

          // Validate input
          const validationError = validateEvent({ userId, type, payload });
          if (validationError) {
            const eventId = uuidv4();
            results[resultIndex] = { eventId, status: 'invalid', error: validationError };
            return;
          }

          const event: Event = {
            id: uuidv4(),
            userId,
            type,
            payload: payload || {},
            timestamp: Date.now(),
          };

          // Check rate limit (per user only)
          const rateLimitResult = await checkRateLimit(userId, event.id);
          if (!rateLimitResult.allowed) {
            results[resultIndex] = { eventId: event.id, status: 'rate_limited' };
            return;
          }

          acceptedEvents.push(event);
          results[resultIndex] = { eventId: event.id, status: 'accepted' };
        })
      );
    }

    if (acceptedEvents.length > 0) {
      await sendEventsBatch(acceptedEvents);
    }

    res.status(202).json({ results });
  } catch (error) {
    console.error('Error handling batch:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
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
