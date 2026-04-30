import { NextFunction, Router, Request, Response } from 'express';
import { EntityNotFoundError, FindOptionsRelations } from 'typeorm';
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
  getPublicationHistory
} from '../../../controllers/consumer';
import { NotFoundException } from '../../../exceptions/not-found.exception';
import { UnknownException } from '../../../exceptions/unknown.exception';
import { longTimeout } from '../../../middleware/timeout';
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
    } catch (err) {
      if (err instanceof EntityNotFoundError) {
        next(new NotFoundException('errors.no_dataset'));
        return;
      }
      logger.error(err, `Failed to load published dataset ${req.params.dataset_id}`);
      next(new UnknownException());
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

publicApiRouter.get(
  '/',
  /*
    #swagger.summary = 'Get a list of all published datasets'
    #swagger.description = 'This endpoint returns a list of all published datasets.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size'
    ]
    #swagger.responses[200] = {
      description: 'A paginated list of all published datasets',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/DatasetsWithCount" }
        }
      }
    }
  */
  listPublishedDatasets
);

publicApiRouter.get(
  '/topic',
  /*
    #swagger.summary = 'Get a list of top-level topics'
    #swagger.description = "Datasets are tagged to topics. There are top-level topics, such as 'Health and social care',
      which can have sub-topics, such as 'Dental services'. This endpoint returns a list of all top-level topics that
      have at least one published dataset tagged to them."
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = ['#/components/parameters/language']
    #swagger.responses[200] = {
      description: 'A list of all top-level topics that have at least one published dataset tagged to them.',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/RootTopics" }
        }
      }
    }
  */
  listRootTopics
);

publicApiRouter.get(
  '/topic/:topic_id',
  /*
    #swagger.summary = 'Get a list of what sits under a given topic'
    #swagger.description = "Datasets are tagged to topics. There are top-level topics, such as 'Health and social
      care', which can have sub-topics, such as 'Dental services'. For a given topic_id, this endpoint returns a
      list of what sits under that topic - either sub-topics or published datasets tagged directly to that topic."
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/topic_id',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/sort_by'
    ]
    #swagger.responses[200] = {
      description: 'A list of what sits under a given topic - either sub-topics or published datasets tagged directly to that topic.',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/PublishedTopics" }
        }
      }
    }
  */
  listSubTopics
);

publicApiRouter.get(
  '/:dataset_id',
  loadPublishedDataset(),
  /*
    #swagger.summary = "Get a published dataset's metadata"
    #swagger.description = 'This endpoint returns all metadata for a published dataset.'
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id'
    ]
    #swagger.responses[200] = {
      description: 'A json object containing all metadata for a published dataset',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/Dataset" }
        }
      }
    }
  */
  getPublishedDatasetById
);

publicApiRouter.get(
  '/:dataset_id/history',
  loadPublishedDataset(),
  // internal frontend use only, don't include in docs. Not secret, just don't want to maintain the contract.
  /* #swagger.ignore = true */
  getPublicationHistory
);

publicApiRouter.get(
  '/:dataset_id/view',
  loadPublishedDataset(),
  /*
    #swagger.summary = 'Get a paginated view of a published dataset'
    #swagger.description = 'This endpoint returns a paginated view of a published dataset, with optional sorting and filtering.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/sort_by',
      '#/components/parameters/filter'
    ]
    #swagger.responses[200] = {
      description: 'A paginated view of a published dataset, with optional sorting and filtering',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/DatasetView" }
        }
      }
    }
  */
  getPublishedDatasetView
);

publicApiRouter.get(
  '/:dataset_id/view/filters',
  loadPublishedDataset(),
  /*
    #swagger.summary = 'Get a list of the filters available for a paginated view of a published dataset'
    #swagger.description = 'This endpoint returns a list of the filters available for a paginated view of a published dataset. These are based on the variables used in the dataset, for example local authorities or financial years.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id'
    ]
    #swagger.responses[200] = {
      description: 'A list of the filters available for a paginated view of a published dataset',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/Filters" }
        }
      }
    }
  */
  getPublishedDatasetFilters
);

publicApiRouter.get(
  '/:dataset_id/download/:format',
  loadPublishedDataset(),
  /*
    #swagger.summary = 'Download a published dataset as a file'
    #swagger.description = 'This endpoint returns a published dataset file in a specified format.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/format',
      '#/components/parameters/sort_by',
      '#/components/parameters/filter',
      '#/components/parameters/view'
    ]
    #swagger.responses[200] = {
      description: 'A published dataset file in a specified format',
      content: {
        'application/octet-stream': {
          schema: { type: 'string', format: 'binary', example: 'data.csv' }
        }
      }
    }
  */
  longTimeout,
  downloadPublishedDataset
);
