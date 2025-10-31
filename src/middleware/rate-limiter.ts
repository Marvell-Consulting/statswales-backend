import { Request, Response, NextFunction } from 'express';
import { rateLimit, RateLimitRequestHandler } from 'express-rate-limit';

import { config } from '../config';

const bypass = (re: Request, res: Response, next: NextFunction): void => next();

const limit = (): RateLimitRequestHandler => {
  return rateLimit({
    windowMs: config.rateLimit.windowMs,
    max: config.rateLimit.maxRequests,
    standardHeaders: true,
    legacyHeaders: false,
    handler: (req, res) => {
      res.status(429).json({
        message: 'Too many requests, please try again later.'
      });
    }
  });
};

export const rateLimiter = config.rateLimit.windowMs === -1 ? bypass : limit();
