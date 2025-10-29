import { Router } from 'express';

import { logger } from '../utils/logger';
import { GlobalRole } from '../enums/global-role';
import { ForbiddenException } from '../exceptions/forbidden.exception';
import { listAllDatasets, rebuildAll, rebuildDrafts } from '../controllers/dataset';
import { ensureDeveloper } from '../middleware/ensure-developer';

export const devRouter = Router();

devRouter.use((req, res, next) => {
  logger.debug(`checking if user is a developer...`);
  if (!req.user?.globalRoles?.includes(GlobalRole.Developer)) {
    next(new ForbiddenException('user is not a developer'));
    return;
  }
  logger.info(`user ${req.user?.id} is a developer`);
  next();
});

// GET /developer/dataset
// Returns a list of all datasets
devRouter.get('/dataset', ensureDeveloper, listAllDatasets);

// POST /developer/rebuild/all
// Rebuilds all datasets must be developer or service admin
// Returns 201 only or error
devRouter.post('/rebuild/all', ensureDeveloper, rebuildAll);

// POST /developer/rebuild/published
// Rebuilds all draft (unpublished) revisions must be developer or service admin
// Returns 201 only or error
devRouter.post('/rebuild/published', ensureDeveloper, rebuildDrafts);
