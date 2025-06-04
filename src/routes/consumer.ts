import { NextFunction, Router, Request, Response } from 'express';
import { FindOptionsRelations } from 'typeorm';

import { logger } from '../utils/logger';
import {
  listPublishedDatasets,
  getPublishedDatasetById,
  downloadPublishedDataset,
  getPublishedDatasetView,
  getPublishedDatasetFilters,
  listRootTopics,
  listSubTopics
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

consumerRouter.get('/topic', listRootTopics);
consumerRouter.get('/topic/:topic_id', listSubTopics);

consumerRouter.get('/list', listPublishedDatasets);

consumerRouter.get('/:dataset_id', loadPublishedDataset(), getPublishedDatasetById);

consumerRouter.get('/:dataset_id/view', loadPublishedDataset(), getPublishedDatasetView);
consumerRouter.get('/:dataset_id/view/filters', loadPublishedDataset(), getPublishedDatasetFilters);

consumerRouter.get('/:dataset_id/download/:format', loadPublishedDataset(), downloadPublishedDataset);
