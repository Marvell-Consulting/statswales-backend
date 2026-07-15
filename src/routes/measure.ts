import 'reflect-metadata';

import express, { Router } from 'express';

import {
  attachLookupTableToMeasure,
  downloadMeasureLookupTable,
  getMeasureInfo,
  getMeasureLookupTableInfo,
  getPreviewOfMeasure,
  resetMeasure,
  updateMeasureMetadata
} from '../controllers/measure';
import { fileStreaming } from '../middleware/file-streaming';
import { longTimeout } from '../middleware/timeout';
import { ensureNoOpenPublishRequest } from '../middleware/ensure-no-open-publish-request';

const jsonParser = express.json();

const router = Router();
export const measureRouter = router;

// GET /dataset/:dataset_id/measure
router.get('/', getMeasureInfo);

// POST /:dataset_id/measure
// Attaches a measure lookup table to a dataset and validates it.
router.post('/', ensureNoOpenPublishRequest, longTimeout, fileStreaming(), attachLookupTableToMeasure);

// DELETE /dataset/:dataset_id/measure/reset
router.delete('/reset', ensureNoOpenPublishRequest, resetMeasure);

// GET /dataset/:dataset_id/dimension/id/:dimension_id/preview
// Returns details of a dimension and a preview of the data
// It should be noted that this returns the raw values in the
// preview as opposed to view which returns interpreted values.
router.get('/preview', getPreviewOfMeasure);

// PATCH /:dataset_id/measure/metadata
// Updates the measure metadata
router.patch('/metadata', ensureNoOpenPublishRequest, jsonParser, updateMeasureMetadata);

router.get('/lookup', getMeasureLookupTableInfo);

router.get('/lookup/raw', downloadMeasureLookupTable);
