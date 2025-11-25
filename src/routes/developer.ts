import { Router } from 'express';

import { listAllDatasets, rebuildAll, rebuildDrafts } from '../controllers/dataset';
import { ensureDeveloper } from '../middleware/ensure-developer';

export const devRouter = Router();

devRouter.use(ensureDeveloper);

// GET /developer/dataset
// Returns a list of all datasets
devRouter.get('/dataset', listAllDatasets);

// POST /developer/rebuild/all
// Rebuilds all datasets must be developer or service admin
// Returns 201 only or error
devRouter.post('/rebuild/all', rebuildAll);

// POST /developer/rebuild/published
// Rebuilds all draft (unpublished) revisions must be developer or service admin
// Returns 201 only or error
devRouter.post('/rebuild/drafts', rebuildDrafts);
