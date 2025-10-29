import { Request, Response, NextFunction } from 'express';
import { logger } from '../utils/logger';
import { ForbiddenException } from '../exceptions/forbidden.exception';

export const ensureDeveloper = (req: Request, res: Response, next: NextFunction): void => {
  logger.debug(`checking if user is a developer...`);
  if (!res.locals.isDeveloper) {
    next(new ForbiddenException('user is not a developer'));
    return;
  }
  logger.info(`user is a developer`);
  next();
};
