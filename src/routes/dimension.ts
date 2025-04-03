import 'reflect-metadata';

import express, { NextFunction, Request, Response, Router } from 'express';
import multer from 'multer';

import { logger } from '../utils/logger';
import {
  attachLookupTableToDimension,
  downloadDimensionLookupTable,
  getDimensionInfo,
  getDimensionLookupTableInfo,
  resetDimension,
  sendDimensionPreview,
  updateDimension,
  updateDimensionMetadata
} from '../controllers/dimension-controller';
import { dimensionIdValidator, hasError } from '../validators';
import { NotFoundException } from '../exceptions/not-found.exception';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionRepository } from '../repositories/dimension';

export const loadDimension = () => {
  return async (req: Request, res: Response, next: NextFunction) => {
    const dimensionIdError = await hasError(dimensionIdValidator(), req);
    if (dimensionIdError) {
      logger.error(dimensionIdError);
      next(new NotFoundException('errors.dimension_id_invalid'));
      return;
    }

    // TODO: include user in query to prevent unauthorized access

    try {
      logger.debug(`Loading dataset ${req.params.dimension_id}...`);
      const dimension = await DimensionRepository.getById(req.params.dimension_id);
      res.locals.dimension_id = dimension.id;
      res.locals.dimension = dimension;
    } catch (err) {
      logger.error(`Failed to load dimension, error: ${err}`);
      next(new NotFoundException('errors.no_dimension'));
      return;
    }

    if (!res.locals.dataset.dimensions.find((dim: Dimension) => dim.id === req.params.dimension_id)) {
      logger.error('Dimension does not belong to dataset');
      next(new NotFoundException('errors.dimension_id_invalid'));
      return;
    }

    next();
  };
};

const jsonParser = express.json();
const upload = multer({ storage: multer.memoryStorage() });

const router = Router();
export const dimensionRouter = router;

// GET /dataset/:dataset_id/dimension/id/:dimension_id
// Returns details of a dimension with its sources and imports
router.get('/by-id/:dimension_id', loadDimension(), getDimensionInfo);

// DELETE /dataset/:dataset_id/dimension/id/:dimension_id/reset
// Resets the dimensions type back to "Raw" and removes the extractor
router.delete('/by-id/:dimension_id/reset', loadDimension(), resetDimension);

// GET /dataset/:dataset_id/dimension/id/:dimension_id/preview
// Returns details of a dimension and a preview of the data
// It should be noted that this returns the raw values in the
// preview as opposed to view which returns interpreted values.
router.get('/by-id/:dimension_id/preview', loadDimension(), sendDimensionPreview);

// POST /:dataset_id/dimension/by-id/:dimension_id/lookup
// Attaches a lookup table to do a dimension and validates
// the lookup table.
router.post('/by-id/:dimension_id/lookup', upload.single('csv'), loadDimension(), attachLookupTableToDimension);

// PATCH /dataset/:dataset_id/dimension/id/:dimension_id/
// Takes a patch request and validates the request against the fact table
// If it fails it sends back an error
router.patch('/by-id/:dimension_id', jsonParser, loadDimension(), updateDimension);

// PATCH /:dataset_id/dimension/by-id/:dimension_id/meta
// Updates the dimension metadata
router.patch('/by-id/:dimension_id/metadata', jsonParser, loadDimension(), updateDimensionMetadata);

router.get('/by-id/:dimension_id/lookup', loadDimension(), getDimensionLookupTableInfo);

router.get('/by-id/:dimension_id/lookup/raw', loadDimension(), downloadDimensionLookupTable);
