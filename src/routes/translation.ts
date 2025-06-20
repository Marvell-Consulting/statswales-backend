import { Router } from 'express';

import { datasetAuth } from '../middleware/dataset-auth';
import { applyImport, translationPreview, translationExport, validateImport } from '../controllers/translation';
import { fileStreaming } from '../middleware/file-streaming';

export const translationRouter = Router();

// ****** DATASET AUTHORISATION MIDDLEWARE ****** //
// applies auth check for dataset for the current user
translationRouter.use('/:dataset_id', datasetAuth);
// ***** DO NOT REMOVE ***** //

// GET /translation/:dataset_id/preview
// Returns a preview of the translations for the given dataset
translationRouter.get('/:dataset_id/preview', translationPreview);

// GET /translation/:dataset_id/export
// Exports the translations for the given dataset as a CSV file
translationRouter.get('/:dataset_id/export', translationExport);

// POST /translation/:dataset_id/import
// Validates the imported translations from a CSV file
translationRouter.post('/:dataset_id/import', fileStreaming(), validateImport);

// PATCH /translation/:dataset_id/import
// Applies the imported translations to the dataset
translationRouter.patch('/:dataset_id/import', applyImport);
