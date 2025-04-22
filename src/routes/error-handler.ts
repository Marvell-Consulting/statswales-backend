import { ErrorRequestHandler, NextFunction, Request, Response } from 'express';

import { logger } from '../utils/logger';
import { t } from '../middleware/translation';

export const errorHandler: ErrorRequestHandler = (err: any, req: Request, res: Response, _next: NextFunction) => {
  const message = 'message' in err ? (err as any).message : 'errors.unknown_error';

  switch (err.status) {
    case 400:
      logger.warn(err, `400 error detected for ${req.originalUrl}: ${message}`);
      res.status(400);
      // TODO: flatten the validation errors to make them more friendly
      res.json({
        error: t(message, { lng: req.language }),
        reason: err.validationErrors
      });
      return;

    case 401:
    case 403:
    case 405:
      logger.error(`${err.status} error detected for ${req.originalUrl}: ${message}`);
      res.status(err.status);
      break;

    case 404:
      logger.warn(err, `404 error detected for ${req.originalUrl}: ${message}`);
      res.status(404);
      break;

    case 500:
    default:
      logger.error(err, `unknown error detected for ${req.originalUrl}: ${message}`);
      res.status(500);
      break;
  }

  res.json({ error: t(message, { lng: req.language }) });
};
