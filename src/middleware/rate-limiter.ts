import { timingSafeEqual } from 'node:crypto';

import { Request, Response, NextFunction } from 'express';
import { rateLimit, RateLimitRequestHandler } from 'express-rate-limit';

import { config } from '../config';
import { logger } from '../utils/logger';

const bypass = (_req: Request, _res: Response, next: NextFunction): void => next();

const limit = (): RateLimitRequestHandler => {
  return rateLimit({
    windowMs: config.rateLimit.windowMs,
    max: config.rateLimit.maxRequests,
    standardHeaders: true,
    legacyHeaders: false,
    handler: (req, res) => {
      logger.warn(`Rate limit exceeded for IP: ${req.ip}`);
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
    next();
    return;
  }

  rateLimitHandler(req, res, next);
};
