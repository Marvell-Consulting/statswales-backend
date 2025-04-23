import { Router } from 'express';

import { logger } from '../utils/logger';
import { GlobalRole } from '../enums/global-role';
import { ForbiddenException } from '../exceptions/forbidden.exception';
import { listAllDatasets } from '../controllers/dataset';

export const devRouter = Router();

devRouter.use((req, res, next) => {
  logger.debug(`checking if user is a developer...`);
  if (!req.user?.globalRoles?.includes(GlobalRole.Developer)) {
    next(new ForbiddenException('user is not a developer'));
    return;
  }
  logger.info(`user is a developer`);
  next();
});

// GET /developer/dataset
// Returns a list of all datasets
devRouter.get('/dataset', listAllDatasets);
