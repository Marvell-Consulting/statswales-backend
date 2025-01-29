import { Router } from 'express';

import {
    listPublishedDatasets,
    getPublishedDatasetById,
    loadPublishedDataset,
    downloadPublishedDataset
} from '../controllers/consumer';

export const consumerRouter = Router();

// GET /published/list
// Returns a list of all active datasets e.g. ones with imports
consumerRouter.get('/list', listPublishedDatasets);

// GET /published/:datasetId
// Returns a published dataset as a json object
consumerRouter.get('/:dataset_id', loadPublishedDataset, getPublishedDatasetById);

// GET /published/:datasetId/revision/:revisionId/download/:format
// Returns a published dataset as a file stream
consumerRouter.get(
    '/:dataset_id/revision/:revision_id/download/:format',
    loadPublishedDataset,
    downloadPublishedDataset
);
