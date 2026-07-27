import { ErrorRequestHandler, NextFunction, Request, Response } from 'express';

import { logger } from '../utils/logger';
import { t } from '../middleware/translation';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const errorHandler: ErrorRequestHandler = (err: any, req: Request, res: Response, _next: NextFunction) => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
      logger.warn(`${err.status} error detected for ${req.originalUrl}: ${message}`);
      res.status(err.status);
      break;

    case 404:
      logger.warn(err, `404 error detected for ${req.originalUrl}: ${message}`);
      res.status(404);
      break;

    case 503:
      logger.warn(err, `503 service unavailable for ${req.originalUrl}: ${message}`);
      res.status(503);
      break;

    // A recognised application exception (e.g. UnknownException) explicitly sets status
    // 500 with a deliberate, developer-authored message/translation key, so it's safe to
    // translate and return as-is, same as the other known-status branches above.
    case 500:
      logger.error(err, `500 error detected for ${req.originalUrl}: ${message}`);
      res.status(500);
      break;

    // Anything reaching here has no recognised app-assigned status — e.g. a raw
    // TypeORM/pg or DuckDB driver error that was never wrapped in one of our own
    // exception types. Never echo err.message to the client in this case, as it may
    // contain raw DB/driver error detail (SQL fragments, table/column names, etc).
    // The real error is still logged in full server-side, just above, for diagnosis.
    default:
      logger.error(err, `unknown error detected for ${req.originalUrl}: ${message}`);
      res.status(500);
      res.json({ error: t('errors.unknown_error', { lng: req.language }) });
      return;
  }

  res.json({ error: t(message, { lng: req.language }) });
};
