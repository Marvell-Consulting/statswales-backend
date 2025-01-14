import { Router } from 'express';

import { listPublishedDatasets } from '../controllers/consumer';

export const consumerRouter = Router();

// GET /published/list
// Returns a list of all active datasets e.g. ones with imports
consumerRouter.get('/list', listPublishedDatasets);
