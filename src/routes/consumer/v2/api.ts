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
    #swagger.tags = ['Datasets']
    #swagger.summary = 'Get a list of all published datasets'
    #swagger.description = 'Returns a paginated list of all published datasets, ordered by most recently updated.'
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
    #swagger.tags = ['Datasets']
    #swagger.summary = 'Search published datasets'
    #swagger.description = 'Full-text search across dataset titles and summaries. Returns paginated results ranked by relevance.'
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
    #swagger.tags = ['Topics']
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
    #swagger.tags = ['Topics']
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
      schema: { $ref: "#/components/schemas/PublishedTopics" }
    }
  */
  listSubTopics
);

publicApiV2Router.get(
  '/:dataset_id',
  ensurePublishedDataset,
  /*
    #swagger.tags = ['Datasets']
    #swagger.summary = "Get a published dataset's metadata"
    #swagger.description = 'Returns full metadata for a published dataset including revision details, update frequency, related links, and topics.'
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
    #swagger.tags = ['Datasets']
    #swagger.summary = 'Get a specific published revision by ID'
    #swagger.description = 'Returns metadata for a specific published revision. Use the dataset metadata endpoint to discover available revision IDs.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/revision_id'
    ]
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
    #swagger.tags = ['Data']
    #swagger.summary = 'Get the available filters for a dataset'
    #swagger.description = 'Lists every filterable dimension and its allowed values. Use the column names and reference codes from this response to build a filter object for POST /{dataset_id}/data.'
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
    #swagger.tags = ['Data']
    #swagger.summary = 'Generate a filter ID for a dataset query'
    #swagger.description = 'Stores row filters and display options as a reusable query. Returns a filter ID (UUID) for use with GET /{dataset_id}/data/{filter_id}. Submitting identical filters returns the same ID.'
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
    #swagger.tags = ['Pivot']
    #swagger.summary = 'Generate a filter ID for a pivot query'
    #swagger.description = 'Stores a pivot configuration (x/y axes) with optional filters and display options. Returns a filter ID for GET /{dataset_id}/pivot/{filter_id}.'
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
    #swagger.tags = ['Data']
    #swagger.summary = 'Get paginated data for a dataset'
    #swagger.description = 'Returns all rows for the latest published revision, paginated, with default display options. To apply filters, first create a filter via POST /{dataset_id}/data, then use GET /{dataset_id}/data/{filter_id}.'
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
    #swagger.tags = ['Data']
    #swagger.summary = 'Get paginated data for a dataset using a stored filter'
    #swagger.description = 'Returns paginated data filtered and formatted according to the stored filter ID. Create a filter ID by POSTing to /{dataset_id}/data.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/sort_by',
      '#/components/parameters/filter_id'
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
  '/:dataset_id/pivot/:filter_id',
  ensurePublishedDataset,
  /*
    #swagger.tags = ['Pivot']
    #swagger.summary = 'Get a pivot view of a dataset using a stored filter ID'
    #swagger.description = 'Returns a cross-tabulated pivot view using the configuration stored in the given filter ID.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/sort_by',
      '#/components/parameters/filter_id'
    ]
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
    #swagger.tags = ['Query']
    #swagger.summary = 'Get details of the default query for a dataset'
    #swagger.description = 'Returns the default (unfiltered) query configuration, including total row count and column name mappings.'
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
    #swagger.tags = ['Query']
    #swagger.summary = 'Get details of a stored filter query'
    #swagger.description = 'Returns the full stored query configuration for a filter ID — useful for inspecting what filters and options a filter ID contains.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/dataset_id',
      '#/components/parameters/filter_id'
    ]
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
