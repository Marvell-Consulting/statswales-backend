import { NextFunction, Router, Request, Response } from 'express';
import { FindOptionsRelations } from 'typeorm';

import { logger } from '../utils/logger';
import {
  listPublishedDatasets,
  getPublishedDatasetById,
  downloadPublishedDataset,
  getPublishedDatasetView,
  listPublishedTopics
} from '../controllers/consumer';
import { NotFoundException } from '../exceptions/not-found.exception';
import { PublishedDatasetRepository } from '../repositories/published-dataset';
import { hasError, datasetIdValidator } from '../validators';
import { Dataset } from '../entities/dataset/dataset';

export const consumerRouter = Router();

export const loadPublishedDataset = (relations?: FindOptionsRelations<Dataset>) => {
  return async (req: Request, res: Response, next: NextFunction) => {
    const datasetIdError = await hasError(datasetIdValidator(), req);

    if (datasetIdError) {
      logger.error(datasetIdError);
      next(new NotFoundException('errors.dataset_id_invalid'));
      return;
    }

    try {
      logger.debug(`Loading published dataset ${req.params.dataset_id}...`);
      const dataset = await PublishedDatasetRepository.getById(req.params.dataset_id, relations);
      res.locals.datasetId = dataset.id;
      res.locals.dataset = dataset;
    } catch (err) {
      logger.error(err, `Failed to load dataset`);
      next(new NotFoundException('errors.no_dataset'));
      return;
    }

    next();
  };
};

// GET /published/topics
// Returns a list of all topics with at least one published dataset
consumerRouter.get('/topics', listPublishedTopics);

// GET /published/list
// Returns a list of all active datasets e.g. ones with imports
consumerRouter.get('/list', listPublishedDatasets);

// GET /published/:datasetId
// Returns a published dataset as a json object
consumerRouter.get('/:dataset_id', loadPublishedDataset(), getPublishedDatasetById);

// GET /published/:datasetId/view
// Returns a published dataset as a view of the data
consumerRouter.get('/:dataset_id/view', loadPublishedDataset(), getPublishedDatasetView);

// GET /published/:datasetId/revision/:revisionId/download/:format
// Returns a published dataset as a file stream
consumerRouter.get('/:dataset_id/download/:format', loadPublishedDataset(), downloadPublishedDataset);
