import { MemoryStore, Options, Store, IncrementResponse } from 'express-rate-limit';
import { createClient } from 'redis';

import { logger } from '../utils/logger';

type RedisClient = ReturnType<typeof createClient>;

/**
 * Fixed-window rate limit store shared across all backend replicas via Redis/Valkey.
 *
 * The default MemoryStore counts per replica, so the effective limit scales with the replica count and resets
 * whenever a new replica starts - exactly when we are under load. If Redis is unavailable we degrade to a local
 * MemoryStore (per-replica limiting) rather than failing requests or disabling rate limiting entirely.
 */
export class RedisRateLimitStore implements Store {
  prefix: string;
  localKeys = false;

  private windowMs = 60000;
  private readonly fallback = new MemoryStore();
  private usingFallback = false;

  constructor(
    private readonly client: RedisClient,
    prefix = 'sw3b:rl:'
  ) {
    this.prefix = prefix;
  }

  init(options: Options): void {
    this.windowMs = options.windowMs;
    this.fallback.init(options);
  }

  async increment(key: string): Promise<IncrementResponse> {
    if (this.client.isReady) {
      try {
        const redisKey = this.prefix + key;
        const [totalHits, , ttl] = await this.client
          .multi()
          .incr(redisKey)
          .pExpire(redisKey, this.windowMs, 'NX')
          .pTTL(redisKey)
          .exec();

        this.setFallback(false);
        const ttlMs = Number(ttl) > 0 ? Number(ttl) : this.windowMs;
        return { totalHits: Number(totalHits), resetTime: new Date(Date.now() + ttlMs) };
      } catch (err) {
        this.setFallback(true, err);
      }
    } else {
      this.setFallback(true);
    }

    return this.fallback.increment(key);
  }

  async decrement(key: string): Promise<void> {
    this.fallback.decrement(key);
    if (this.client.isReady) {
      await this.client.decr(this.prefix + key).catch(() => undefined);
    }
  }

  async resetKey(key: string): Promise<void> {
    this.fallback.resetKey(key);
    if (this.client.isReady) {
      await this.client.del(this.prefix + key).catch(() => undefined);
    }
  }

  shutdown(): void {
    this.fallback.shutdown();
  }

  // only log on state transitions so a Redis outage doesn't log once per request
  private setFallback(active: boolean, err?: unknown): void {
    if (active === this.usingFallback) return;
    this.usingFallback = active;

    if (active) {
      logger.warn(err, 'Rate limit store unavailable, falling back to per-replica in-memory rate limiting');
    } else {
      logger.info('Rate limit store reconnected, resuming shared rate limiting');
    }
  }
}
