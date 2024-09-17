import rateLimit from 'express-rate-limit';

import { appConfig } from '../config';

const config = appConfig();

export const rateLimiter = rateLimit({
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
