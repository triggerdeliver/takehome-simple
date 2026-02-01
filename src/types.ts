export interface Event {
  id: string;
  userId: string;
  type: string;
  payload: Record<string, unknown>;
  timestamp: number;
}

export interface ProcessingResult {
  eventId: string;
  status: 'processed' | 'failed' | 'rate_limited';
  processedAt: number;
  error?: string;
  attempts?: number;
  userId?: string;
  type?: string;
  seq?: number;
}


export interface ConsumerMetrics {
  processed: number;
  failed: number;
  rateLimited: number;
  avgProcessingTimeMs: number;
}

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetAt: number;
}
// v2.3
