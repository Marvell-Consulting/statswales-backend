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

publicApiV2Router.get(
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

publicApiV2Router.get(
  '/search',
  /*
    #swagger.summary = 'Search published datasets'
    #swagger.description = 'This endpoint performs a search across published dataset titles and summaries.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/keywords'
    ]
    #swagger.responses[200] = {
      description: 'A paginated list of matching published datasets',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/DatasetsWithCount" }
        }
      }
    }
  */
  searchPublishedDatasets
);

publicApiV2Router.get(
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
      schema: { $ref: "#/components/schemas/RootTopics" }
    }
  */
  listRootTopics
);

publicApiV2Router.get(
  '/topic/:topic_id',
  /*
    #swagger.summary = 'Get a list of what sits under a given topic'
    #swagger.description = "Datasets are tagged to topics. There are top-level topics, such as 'Health and social
      care', which can have sub-topics, such as 'Dental services'. For a given topic_id, this endpoint returns a
      list of what sits under that topic - either sub-topics or published datasets tagged directly to that topic."
    #swagger.autoQuery = false
    #swagger.parameters['page_size'] = {
      description: 'Number of datasets per page when datasets are returned',
      in: 'query',
      type: 'integer',
      default: 1000
    }
    #swagger.parameters['sort_by'] = {
      description: 'Columns to sort the data by. The value should be a JSON array of objects sent as a URL encoded string.',
      in: 'query',
      required: false,
      content: {
        'application/json': {
          schema: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                columnName: {
                  type: 'string',
                  enum: ['first_published_at', 'last_updated_at', 'title']
                },
                direction: {
                  type: 'string',
                  enum: ['ASC', 'DESC']
                }
              }
            }
          }
        }
      }
    }
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/topic_id',
      '#/components/parameters/page_number'
    ]
    #swagger.responses[200] = {
      description: 'A list of what sits under a given topic - either sub-topics or published datasets tagged directly to that topic.',
      schema: { $ref: "#/components/schemas/PublishedTopics" }
    }
  */
  listSubTopics
);

publicApiV2Router.get(
  '/:dataset_id',
  ensurePublishedDataset,
  /*
    #swagger.summary = "Get a published dataset's metadata"
    #swagger.description = 'This endpoint returns all consumer metadata for a published dataset.'
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id'
    ]
    #swagger.responses[200] = {
      description: 'A json object containing all metadata for a published dataset',
      schema: { $ref: "#/components/schemas/Dataset" }
    }
  */
  getPublishedDatasetById
);

publicApiV2Router.get(
  '/:dataset_id/revision/:revision_id',
  ensurePublishedDataset,
  ensurePublishedRevision,
  /*
    #swagger.summary = 'Get a specific published revision by ID'
    #swagger.description = 'Returns metadata for a specific published revision of a dataset.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id'
    ]
    #swagger.parameters['revision_id'] = {
      in: 'path',
      description: 'The unique identifier of the revision',
      required: true,
      schema: { type: 'string', format: 'uuid' }
    }
    #swagger.responses[200] = {
      description: 'Metadata for the requested revision',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/Revision" }
        }
      }
    }
  */
  getPublishedRevisionById
);

publicApiV2Router.get(
  '/:dataset_id/filters',
  ensurePublishedDataset,
  /*
    #swagger.summary = 'Get the available filters for a dataset'
    #swagger.description = 'Returns all filterable dimensions and their available values for the latest published revision of a dataset.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id'
    ]
    #swagger.responses[200] = {
      description: 'A list of filterable dimensions with their available values',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/Filters" }
        }
      }
    }
  */
  getPublishedDatasetFilters
);

publicApiV2Router.post(
  '/:dataset_id/data',
  ensurePublishedDataset,
  jsonParser,
  /*
    #swagger.summary = 'Generate a filter ID for a dataset query'
    #swagger.description = 'Stores a set of filter and display options as a reusable query, returning a filter ID that can be passed to the data and pivot endpoints.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = ['#/components/parameters/dataset_id']
    #swagger.requestBody = {
      required: true,
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/DataOptions" }
        }
      }
    }
    #swagger.responses[200] = {
      description: 'The generated filter ID',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/FilterId" }
        }
      }
    }
  */
  generateFilterId
);

publicApiV2Router.post(
  '/:dataset_id/pivot',
  ensurePublishedDataset,
  jsonParser,
  /*
    #swagger.summary = 'Generate a filter ID for a pivot query'
    #swagger.description = 'Stores a set of pivot configuration and filter options as a reusable query, returning a filter ID that can be passed to the pivot endpoint.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = ['#/components/parameters/dataset_id']
    #swagger.requestBody = {
      required: true,
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/PivotOptions" }
        }
      }
    }
    #swagger.responses[200] = {
      description: 'The generated filter ID',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/FilterId" }
        }
      }
    }
  */
  generatePivotFilterId
);

publicApiV2Router.get(
  '/:dataset_id/data',
  ensurePublishedDataset,
  /*
    #swagger.summary = 'Get paginated data for a dataset'
    #swagger.description = 'Returns a paginated view of the dataset data.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/sort_by'
    ]
    #swagger.responses[200] = {
      description: 'A paginated view of the dataset data',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/DatasetView" }
        }
      }
    }
  */
  getPublishedDatasetData
);

publicApiV2Router.get(
  '/:dataset_id/data/:filter_id',
  ensurePublishedDataset,
  /*
    #swagger.summary = 'Get paginated data for a dataset using a stored filter'
    #swagger.description = 'Returns a paginated view of the dataset data using the options stored in the given filter ID.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/sort_by'
    ]
    #swagger.parameters['filter_id'] = {
      in: 'path',
      description: 'Filter ID returned by the POST /data endpoint',
      required: true,
      schema: { type: 'string', format: 'uuid' }
    }
    #swagger.responses[200] = {
      description: 'A paginated view of the dataset data',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/DatasetView" }
        }
      }
    }
  */
  getPublishedDatasetData
);

publicApiV2Router.get(
  '/:dataset_id/pivot/:filter_id',
  ensurePublishedDataset,
  /*
    #swagger.summary = 'Get a pivot view of a dataset using a stored filter ID'
    #swagger.description = 'Returns a pivot table view of the dataset data using the configuration stored in a pivot filter ID (created via POST /pivot).'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/sort_by'
    ]
    #swagger.parameters['filter_id'] = {
      in: 'path',
      description: 'Pivot filter ID returned by the POST /pivot endpoint',
      required: true,
      schema: { type: 'string', format: 'uuid' }
    }
    #swagger.responses[200] = {
      description: 'A paginated pivot view of the dataset data',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/DatasetView" }
        }
      }
    }
  */
  getPublishedDatasetPivotFromId
);

publicApiV2Router.get(
  '/:dataset_id/query/',
  ensurePublishedDataset,
  /*
    #swagger.summary = 'Get details of the default query for a dataset'
    #swagger.description = 'Returns the full query configuration for the default (unfiltered) query for a dataset, including the total number of rows and column mappings.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = ['#/components/parameters/dataset_id']
    #swagger.responses[200] = {
      description: 'The stored query configuration',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/QueryStore" }
        }
      }
    }
  */
  getFilterIdDetails
);

publicApiV2Router.get(
  '/:dataset_id/query/:filter_id',
  ensurePublishedDataset,
  /*
    #swagger.summary = 'Get details of a stored filter query'
    #swagger.description = 'Returns the full query configuration stored under a filter ID, including the total number of matching rows and column mappings.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = ['#/components/parameters/dataset_id']
    #swagger.parameters['filter_id'] = {
      in: 'path',
      description: 'Filter ID to retrieve',
      required: true,
      schema: { type: 'string', format: 'uuid' }
    }
    #swagger.responses[200] = {
      description: 'The stored query configuration',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/QueryStore" }
        }
      }
    }
  */
  getFilterIdDetails
);

// Hidden endpoint — not intended for consumers. Pivots any existing query and allows for
// more complex multidimensional pivots.
publicApiV2Router.get(
  '/:dataset_id/data/:filter_id/pivot',
  ensurePublishedDataset,
  /* #swagger.ignore = true */
  getPublishedDatasetPivot
);
