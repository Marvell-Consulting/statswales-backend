import 'reflect-metadata';

import { Router } from 'express';
import multer from 'multer';

import { attachLookupTableToMeasure, getPreviewOfMeasure, resetMeasure } from '../controllers/measure-controller';

const upload = multer({ storage: multer.memoryStorage() });

const router = Router();
export const measureRouter = router;

router.delete('/reset', resetMeasure);

// POST /:dataset_id/measure
// Attaches a measure lookup table to a dataset and validates it.
router.post('/', upload.single('csv'), attachLookupTableToMeasure);

// GET /dataset/:dataset_id/dimension/id/:dimension_id/preview
// Returns details of a dimension and a preview of the data
// It should be noted that this returns the raw values in the
// preview as opposed to view which returns interpreted values.
router.get('/preview', getPreviewOfMeasure);
