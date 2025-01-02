import { Request, Response, NextFunction } from 'express';
import rateLimit from 'express-rate-limit';

import { appConfig } from '../config';

const config = appConfig();

const bypass = (re: Request, res: Response, next: NextFunction) => next();

const limit = rateLimit({
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

export const rateLimiter = config.rateLimit.windowMs === -1 ? bypass : limit;
