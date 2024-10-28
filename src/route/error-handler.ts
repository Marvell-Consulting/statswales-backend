import { ErrorRequestHandler, NextFunction, Request, Response } from 'express';

import { logger } from '../utils/logger';
import { t } from '../middleware/translation';

export const errorHandler: ErrorRequestHandler = (err: any, req: Request, res: Response, next: NextFunction) => {
    switch (err.status) {
        case 400:
            logger.error(`400 error detected for ${req.originalUrl}, message: ${err.message}`);
            res.status(400);
            break;

        case 401:
            logger.error(`401 error detected for ${req.originalUrl}, message: ${err.message}`);
            res.status(401);
            break;

        case 404:
            logger.error(`404 error detected for ${req.originalUrl}, message: ${err.message}`);
            res.status(404);
            break;

        case 500:
        default:
            logger.error(`unknown error detected for ${req.originalUrl}, message: ${err.message}`);
            res.status(500);
            break;
    }

    res.json({ error: t(err.message, { lng: req.language }) });
};
