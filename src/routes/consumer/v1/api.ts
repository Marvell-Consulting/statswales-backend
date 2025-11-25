import { NextFunction, Router, Request, Response } from 'express';
import { FindOptionsRelations } from 'typeorm';
import cors from 'cors';

import { logger } from '../../../utils/logger';
import {
  listPublishedDatasets,
  getPublishedDatasetById,
  downloadPublishedDataset,
  getPublishedDatasetView,
  listSubTopics,
  listRootTopics,
  getPublishedDatasetFilters,
  getPostgresPivotTable,
  getDuckDBPivotTable
} from '../../../controllers/consumer';
import { NotFoundException } from '../../../exceptions/not-found.exception';
import { PublishedDatasetRepository } from '../../../repositories/published-dataset';
import { hasError, datasetIdValidator } from '../../../validators';
import { Dataset } from '../../../entities/dataset/dataset';
import { NotAllowedException } from '../../../exceptions/not-allowed.exception';

export const publicApiRouter = Router();

export const loadPublishedDataset = (relations?: FindOptionsRelations<Dataset>) => {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const datasetIdError = await hasError(datasetIdValidator(), req);

    if (datasetIdError) {
      next(new NotFoundException('errors.dataset_id_invalid'));
      return;
    }

    try {
      logger.debug(`Loading published dataset ${req.params.dataset_id}...`);
      const dataset = await PublishedDatasetRepository.getById(req.params.dataset_id, relations);
      res.locals.datasetId = dataset.id;
      res.locals.dataset = dataset;
    } catch (_err) {
      next(new NotFoundException('errors.no_dataset'));
      return;
    }

    next();
  };
};

publicApiRouter.use(cors()); // allow browser XMLHttpRequests from any domain

publicApiRouter.use((req: Request, res: Response, next: NextFunction) => {
  if (req.method !== 'GET') {
    next(new NotAllowedException('errors.method_not_allowed'));
    return;
  }

  res.vary('Accept-Language'); // vary response cache on language header
  next();
});

publicApiRouter.get('/', listPublishedDatasets);

publicApiRouter.get('/topic', listRootTopics);
publicApiRouter.get('/topic/:topic_id', listSubTopics);

publicApiRouter.get('/:dataset_id', loadPublishedDataset(), getPublishedDatasetById);

publicApiRouter.get('/:dataset_id/view', loadPublishedDataset(), getPublishedDatasetView);
publicApiRouter.get('/:dataset_id/view/filters', loadPublishedDataset(), getPublishedDatasetFilters);

publicApiRouter.get('/:dataset_id/download/:format', loadPublishedDataset(), downloadPublishedDataset);

publicApiRouter.get('/:dataset_id/pivot/postgres', loadPublishedDataset(), getPostgresPivotTable);
publicApiRouter.get('/:dataset_id/pivot/duckdb', loadPublishedDataset(), getDuckDBPivotTable);
