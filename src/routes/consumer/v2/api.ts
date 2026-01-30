import express, { NextFunction, Router, Request, Response } from 'express';
import cors from 'cors';

import { logger } from '../../../utils/logger';
import {
  listPublishedDatasets,
  getPublishedDatasetById,
  listSubTopics,
  listRootTopics,
  getPublishedRevisionById,
  getPublishedDatasetData,
  getPublishedDatasetFilters,
  generateFilterId,
  getPublishedDatasetPivot,
  generatePivotFilterId,
  getPublishedDatasetPivotFromId,
  searchPublishedDatasets,
  getFilterIdDetails
} from '../../../controllers/consumer-v2';
import { NotFoundException } from '../../../exceptions/not-found.exception';
import { PublishedDatasetRepository } from '../../../repositories/published-dataset';
import { PublishedRevisionRepository } from '../../../repositories/published-revision';
import { hasError, uuidValidator } from '../../../validators';

export const publicApiV2Router = Router();
const jsonParser = express.json();

export const ensurePublishedDataset = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const datasetIdError = await hasError(uuidValidator('dataset_id'), req);

  if (datasetIdError) {
    next(new NotFoundException('errors.dataset_id_invalid'));
    return;
  }

  try {
    logger.debug(`Loading published dataset ${req.params.dataset_id}...`);
    const dataset = await PublishedDatasetRepository.getById(req.params.dataset_id);

    if (!dataset.publishedRevisionId) {
      throw new Error('dataset has no published revision');
    }

    res.locals.datasetId = dataset.id;
    res.locals.dataset = dataset;
  } catch (_err) {
    next(new NotFoundException('errors.no_dataset'));
    return;
  }

  next();
};

export const ensurePublishedRevision = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const revisionIdError = await hasError(uuidValidator('revision_id'), req);

  if (revisionIdError) {
    next(new NotFoundException('errors.revision_id_invalid'));
    return;
  }

  try {
    logger.debug(`Loading published revision ${req.params.dataset_id}...`);
    const revision = await PublishedRevisionRepository.getById(req.params.revision_id);

    if (revision.datasetId !== res.locals.datasetId) {
      throw new Error('revision does not belong to dataset');
    }

    res.locals.revision_id = revision.id;
    res.locals.revision = revision;
  } catch (_err) {
    next(new NotFoundException('errors.no_revision'));
    return;
  }
  next();
};

publicApiV2Router.use(cors()); // allow browser XMLHttpRequests from any domain

publicApiV2Router.use((req: Request, res: Response, next: NextFunction) => {
  res.vary('Accept-Language'); // vary response cache on language header
  next();
});

publicApiV2Router.get('/', listPublishedDatasets);

publicApiV2Router.get('/search', searchPublishedDatasets);

publicApiV2Router.get('/topic', listRootTopics);
publicApiV2Router.get('/topic/:topic_id', listSubTopics);

publicApiV2Router.get('/:dataset_id', ensurePublishedDataset, getPublishedDatasetById);
publicApiV2Router.get(
  '/:dataset_id/revision/:revision_id',
  ensurePublishedDataset,
  ensurePublishedRevision,
  getPublishedRevisionById
);

publicApiV2Router.get('/:dataset_id/filters', ensurePublishedDataset, getPublishedDatasetFilters);
publicApiV2Router.post('/:dataset_id/data', ensurePublishedDataset, jsonParser, generateFilterId);
publicApiV2Router.post('/:dataset_id/pivot', ensurePublishedDataset, jsonParser, generatePivotFilterId);
publicApiV2Router.get('/:dataset_id/data', ensurePublishedDataset, getPublishedDatasetData);
publicApiV2Router.get('/:dataset_id/data/:filter_id', ensurePublishedDataset, getPublishedDatasetData);
publicApiV2Router.get('/:dataset_id/pivot/:filter_id', ensurePublishedDataset, getPublishedDatasetPivotFromId);
publicApiV2Router.get('/:dataset_id/query/', ensurePublishedDataset, getFilterIdDetails);
publicApiV2Router.get('/:dataset_id/query/:filter_id', ensurePublishedDataset, getFilterIdDetails);

// Hidden end point... Not intended for consumers at this stage.  Pivots any existing query and allows for
// more complex multidimensional pivots.
publicApiV2Router.get('/:dataset_id/data/:filter_id/pivot', ensurePublishedDataset, getPublishedDatasetPivot);
