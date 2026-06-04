import Redis from 'ioredis';
import { config, logger } from '../config';

export const redis = new Redis({
  host: config.redis.host,
  port: config.redis.port,
  password: config.redis.password,
  maxRetriesPerRequest: 3,
  retryStrategy: (times) => Math.min(times * 100, 3000),
});

redis.on('error', (err) => logger.error(err, 'Redis connection error'));

const KEYS = {
  balance: (accountId: string) => `balance:${accountId}`,
  nonce: (chain: string, address: string) => `nonce:${chain}:${address}`,
  rateLimit: (key: string) => `rl:${key}`,
  blockHeight: (chain: string) => `block:height:${chain}`,
  txStatus: (txHash: string) => `tx:status:${txHash}`,
} as const;

/**
 * Balance cache in Redis (read-through cache backed by Postgres balance_cache table)
 */
export const balanceCache = {
  async get(accountId: string): Promise<string | null> {
    return redis.get(KEYS.balance(accountId));
  },

  async set(accountId: string, balance: bigint, ttlSeconds = 60): Promise<void> {
    await redis.setex(KEYS.balance(accountId), ttlSeconds, balance.toString());
  },

  async invalidate(accountId: string): Promise<void> {
    await redis.del(KEYS.balance(accountId));
  },
};

/**
 * Nonce management for blockchain transactions.
 * Prevents nonce conflicts across concurrent transaction submissions.
 */
export const nonceManager = {
  async getAndIncrement(chain: string, address: string): Promise<number> {
    const val = await redis.incr(KEYS.nonce(chain, address));
    return val - 1;
  },

  async set(chain: string, address: string, nonce: number): Promise<void> {
    await redis.set(KEYS.nonce(chain, address), nonce);
  },

  async get(chain: string, address: string): Promise<number | null> {
    const val = await redis.get(KEYS.nonce(chain, address));
    return val ? parseInt(val, 10) : null;
  },
};

/**
 * Sliding window rate limiter.
 */
export const rateLimiter = {
  async check(key: string, maxRequests: number, windowSeconds: number): Promise<boolean> {
    const redisKey = KEYS.rateLimit(key);
    const now = Date.now();
    const windowStart = now - windowSeconds * 1000;

    const pipeline = redis.pipeline();
    pipeline.zremrangebyscore(redisKey, 0, windowStart);
    pipeline.zadd(redisKey, now, `${now}`);
    pipeline.zcard(redisKey);
    pipeline.expire(redisKey, windowSeconds);
    const results = await pipeline.exec();

    const count = results?.[2]?.[1] as number;
    return count <= maxRequests;
  },
};

/**
 * Block height tracking for indexer progress.
 */
export const blockTracker = {
  async getHeight(chain: string): Promise<number> {
    const val = await redis.get(KEYS.blockHeight(chain));
    return val ? parseInt(val, 10) : 0;
  },

  async setHeight(chain: string, height: number): Promise<void> {
    await redis.set(KEYS.blockHeight(chain), height);
  },
};

/**
 * Transaction status cache for quick lookups.
 */
export const txStatusCache = {
  async get(txHash: string): Promise<string | null> {
    return redis.get(KEYS.txStatus(txHash));
  },

  async set(txHash: string, status: string, ttlSeconds = 300): Promise<void> {
    await redis.setex(KEYS.txStatus(txHash), ttlSeconds, status);
  },
};
