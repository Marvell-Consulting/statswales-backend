import 'reflect-metadata';

import express, { Router } from 'express';
import multer from 'multer';

import {
  attachLookupTableToMeasure,
  downloadMeasureLookupTable,
  getMeasureInfo,
  getMeasureLookupTableInfo,
  getPreviewOfMeasure,
  resetMeasure,
  updateMeasureMetadata
} from '../controllers/measure-controller';

const jsonParser = express.json();
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, '/tmp/multer-storage');
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
    cb(null, file.fieldname + '-' + uniqueSuffix + '.' + file.originalname.split('.').reverse()[0]);
  }
});

const upload = multer({ storage });

const router = Router();
export const measureRouter = router;

// GET /dataset/:dataset_id/measure
router.get('/', getMeasureInfo);

// POST /:dataset_id/measure
// Attaches a measure lookup table to a dataset and validates it.
router.post('/', upload.single('csv'), attachLookupTableToMeasure);

// DELETE /dataset/:dataset_id/measure/reset
router.delete('/reset', resetMeasure);

// GET /dataset/:dataset_id/dimension/id/:dimension_id/preview
// Returns details of a dimension and a preview of the data
// It should be noted that this returns the raw values in the
// preview as opposed to view which returns interpreted values.
router.get('/preview', getPreviewOfMeasure);

// PATCH /:dataset_id/dimension/by-id/:dimension_id/meta
// Updates the dimension metadata
router.patch('/metadata', jsonParser, updateMeasureMetadata);

router.get('/lookup', getMeasureLookupTableInfo);

router.get('/lookup/raw', downloadMeasureLookupTable);
