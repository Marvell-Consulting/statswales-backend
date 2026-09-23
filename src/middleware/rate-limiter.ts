import { timingSafeEqual } from 'node:crypto';
import { isIP } from 'node:net';

import { Request, Response, NextFunction } from 'express';
import { ipKeyGenerator, rateLimit, RateLimitRequestHandler, Store } from 'express-rate-limit';
import { createClient } from 'redis';

import { config } from '../config';
import { SessionStore } from '../config/session-store.enum';
import { logger } from '../utils/logger';

import { RedisRateLimitStore } from './rate-limit-store';

// Set by Azure Front Door to the IP of the TCP connection it received. Unlike X-Forwarded-For (and
// X-Azure-ClientIP, which is derived from it) the client cannot influence this value. Backend ingress only accepts
// traffic from Front Door, so we can trust it. Falls back to req.ip when not behind Front Door (local, CI).
const CLIENT_IP_HEADER = 'x-azure-socketip';

export const getClientIp = (req: Request): string => {
  const socketIp = req.get(CLIENT_IP_HEADER)?.trim();
  return socketIp && isIP(socketIp) ? socketIp : req.ip || '';
};

const bypass = (_req: Request, _res: Response, next: NextFunction): void => next();

const createStore = (): Store | undefined => {
  if (config.session.store !== SessionStore.Redis) {
    logger.info('In-memory rate limit store initialized');
    return undefined; // express-rate-limit defaults to MemoryStore
  }

  const client = createClient({
    url: config.session.redisUrl,
    password: config.session.redisPassword,
    disableOfflineQueue: true,
    pingInterval: 1000,
    socket: {
      reconnectStrategy: 1000,
      connectTimeout: 7500,
      family: 4
    }
  });

  client.on('error', (err) => logger.error(err, 'Rate limit redis error'));
  client.connect().catch((err) => logger.error(err, 'Rate limit redis initial connection failed, will retry'));

  return new RedisRateLimitStore(client);
};

const limit = (): RateLimitRequestHandler => {
  return rateLimit({
    windowMs: config.rateLimit.windowMs,
    max: config.rateLimit.maxRequests,
    standardHeaders: true,
    legacyHeaders: false,
    store: createStore(),
    keyGenerator: (req) => ipKeyGenerator(getClientIp(req)),
    handler: (req, res) => {
      logger.warn(`Rate limit exceeded for IP: ${getClientIp(req)}`);
      res.status(429).json({
        message: 'Too many requests, please try again later.'
      });
    }
  });
};

const rateLimitHandler = config.rateLimit.windowMs === -1 ? bypass : limit();

export const rateLimiter = (req: Request, res: Response, next: NextFunction): void => {
  const { bypassToken } = config.rateLimit;
  const headerToken = req.get('x-rate-limit-bypass');

  if (
    bypassToken &&
    headerToken &&
    bypassToken.length === headerToken.length &&
    timingSafeEqual(Buffer.from(bypassToken), Buffer.from(headerToken))
  ) {
    logger.debug('Rate limit bypass token matched, skipping rate limiting for this request.');
    req.rateLimitBypass = true;
    next();
    return;
  }

  rateLimitHandler(req, res, next);
};
