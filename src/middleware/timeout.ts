import { Request, Response, NextFunction, RequestHandler } from 'express';

import { config } from '../config';
import { logger } from '../utils/logger';

export function requestTimeout(timeoutMs: number): RequestHandler {
  return (req: Request, res: Response, next: NextFunction): void => {
    if (res.locals._requestTimer) {
      clearTimeout(res.locals._requestTimer);
      res.locals._requestTimer = undefined;
    }

    if (timeoutMs > 0) {
      const timer = setTimeout(() => {
        if (res.headersSent) return;
        logger.warn({ method: req.method, url: req.originalUrl, timeoutMs }, 'Request timed out');
        res.status(504).json({ message: 'Request timed out' });
      }, timeoutMs);

      res.locals._requestTimer = timer;
      res.on('finish', () => clearTimeout(timer));
      res.on('close', () => clearTimeout(timer));
    }

    next();
  };
}

export const defaultTimeout = requestTimeout(config.requestTimeout.defaultMs);
export const longTimeout = requestTimeout(config.requestTimeout.longMs);
export const noTimeout = requestTimeout(0);
