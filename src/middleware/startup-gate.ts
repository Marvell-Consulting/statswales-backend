import { NextFunction, Request, Response } from 'express';

import { isStartupComplete } from '../utils/startup';

/**
 * Returns 503 until the server has connected to the database and finished starting. The platform shouldn't send
 * traffic before /healthcheck/ready passes, but this keeps any request that arrives early away from routes that
 * need the database.
 */
export const startupGate = (_req: Request, res: Response, next: NextFunction): void => {
  if (isStartupComplete()) {
    next();
    return;
  }
  res.status(503).json({ error: 'starting' });
};
