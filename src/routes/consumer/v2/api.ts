import { NextFunction, Router, Request, Response } from 'express';
import { FindOptionsRelations } from 'typeorm';
import cors from 'cors';

import { logger } from '../../../utils/logger';
import {
  listPublishedDatasets,
  getPublishedDatasetById,
  getPublishedDatasetView,
  listSubTopics,
  listRootTopics,
  getPublishedDatasetFilters,
  getPostgresPivotTable,
  getPublishedRevisionById
} from '../../../controllers/consumer-v2';
import { NotFoundException } from '../../../exceptions/not-found.exception';
import { PublishedDatasetRepository } from '../../../repositories/published-dataset';
import { hasError, datasetIdValidator, revisionIdValidator } from '../../../validators';
import { Dataset } from '../../../entities/dataset/dataset';
import { NotAllowedException } from '../../../exceptions/not-allowed.exception';
import { Revision } from '../../../entities/dataset/revision';
import { RevisionRepository } from '../../../repositories/revision';

export const publicApiV2Router = Router();

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

export const loadPublishedRevision = (relations?: FindOptionsRelations<Revision>) => {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const revisionIdError = await hasError(revisionIdValidator(), req);

    if (revisionIdError) {
      next(new NotFoundException('errors.revision_id_invalid'));
      return;
    }

    try {
      logger.debug(`Loading published revision ${req.params.dataset_id}...`);
      const revision = await RevisionRepository.getById(req.params.revision_id, relations);
      res.locals.revision_id = revision.id;
      res.locals.revision = revision;
    } catch (_err) {
      next(new NotFoundException('errors.no_revision'));
      return;
    }
    next();
  };
};

publicApiV2Router.use(cors()); // allow browser XMLHttpRequests from any domain

publicApiV2Router.use((req: Request, res: Response, next: NextFunction) => {
  res.vary('Accept-Language'); // vary response cache on language header
  next();
});

publicApiV2Router.get('/', listPublishedDatasets);
publicApiV2Router.get('/topic', listRootTopics);
publicApiV2Router.get('/topic/:topic_id', listSubTopics);

publicApiV2Router.get('/:dataset_id', loadPublishedDataset(), getPublishedDatasetById);
publicApiV2Router.get(
  '/:dataset_id/revision/:revision_id',
  loadPublishedDataset(),
  loadPublishedRevision(),
  getPublishedRevisionById
);

publicApiV2Router.get('/:dataset_id/filters', loadPublishedDataset(), getPublishedDatasetFilters);
publicApiV2Router.get('/:dataset_id/data', loadPublishedDataset(), getPublishedDatasetView);
publicApiV2Router.get('/:dataset_id/data/:filter_id', loadPublishedDataset(), getPublishedDatasetView);

publicApiV2Router.post('/:dataset_id/pivot', loadPublishedDataset(), getPostgresPivotTable);
publicApiV2Router.post('/:dataset_id/data', loadPublishedDataset(), getPostgresPivotTable);

// Hidden route
publicApiV2Router.get('/:dataset_id/postgres_pivot', loadPublishedDataset(), getPostgresPivotTable);
