import express, { NextFunction, Router, Request, Response } from 'express';
import cors from 'cors';

import { logger } from '../../../utils/logger';
import {
  listPublishedDatasets,
  getPublishedDatasetById,
  listSubTopics,
  listRootTopics,
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
import { longTimeout } from '../../../middleware/timeout';
import { PublishedDatasetRepository } from '../../../repositories/published-dataset';
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

publicApiV2Router.use(cors()); // allow browser XMLHttpRequests from any domain

publicApiV2Router.use((_req: Request, res: Response, next: NextFunction) => {
  res.vary('Accept-Language'); // vary response cache on language header
  next();
});

publicApiV2Router.get(
  '/',
  /*
    #swagger.tags = ['Datasets']
    #swagger.summary = "Get a list of all published datasets"
    #swagger.description = "Returns a paginated list of all published datasets
      and their IDs, ordered by most recently updated."
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size'
    ]
    #swagger.responses[200] = {
      description: 'A paginated list of all published datasets and their IDs',
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
    #swagger.summary = "Search published datasets"
    #swagger.description = "Full-text search across dataset titles and summaries.
      Returns paginated results ranked by relevance."
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/keywords',
      '#/components/parameters/search_mode'
    ]
    #swagger.responses[200] = {
      description: 'A paginated list of matching published datasets and their IDs',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/SearchResultsWithCount" }
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
    #swagger.summary = "Get a list of top-level topics"
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

publicApiV2Router.get(
  '/topic/:topic_id',
  /*
    #swagger.tags = ['Topics']
    #swagger.summary = "Get a list of what sits under a given topic"
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
      description: "A list of what sits under a given topic - either sub-topics or published datasets tagged directly
        to that topic.",
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/PublishedTopics" }
        }
      }
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
    #swagger.description = "Returns current metadata for a published dataset, including dataset summaries,
      topics and related links. You can get dataset IDs from the ‘Lists of datasets and topics’ endpoints."
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

publicApiV2Router.get(
  '/:dataset_id/filters',
  ensurePublishedDataset,
  /*
    #swagger.tags = ['Data']
    #swagger.summary = "Get available filters for a dataset"
    #swagger.description = "<p>Returns a list of variables in a dataset that can be
      filtered, and all filterable values for each variable.</p>
      <p>Variables have a:</p>
      <ul>
        <li>'factTableColumn' name which is used when the dataset is originally created</li>
        <li>human-readable 'columnName'</li>
      </ul>
      <p>Values have a:</p>
      <ul>
        <li>'reference' code</li>
        <li>human-readable 'description'</li>
      </ul>"
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
    #swagger.summary = "Generate a filter ID for a specific query"
    #swagger.description = "<p>Generates a filter ID for a chosen combination of filters and display options. This ID is
      always the same for the same combinations.</p>
      <p>You need to send a JSON body that contains sections for 'filters' and 'options'.</p>
      <p>The 'filters' section should contain the 'columnName' of the variable and the 'reference' codes for the values
      you want to filter in. You can get these from the 'Get available filters for a dataset' endpoint.</p>
      <p>The 'options' section should contain the following:</p>
      <table>
        <thead>
          <tr>
            <th>Option</th>
            <th>Value</th>
            <th>Meaning</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td rowspan='2'>use_raw_column_names</td>
            <td>true [default]</td>
            <td>Variables use ‘factTableColumn’ names, such as ‘AreaCode’</td>
          </tr>
          <tr>
            <td>false</td>
            <td>Variables use human-readable ‘columnName’, such as ‘Area’</td>
          </tr>
          <tr>
            <td rowspan='2'>use_reference_values</td>
            <td>true [default]</td>
            <td>Variable values use ‘reference’ codes, such as ‘K02000001’</td>
          </tr>
          <tr>
            <td>false</td>
            <td>Variable values use human-readable ‘description’, such as ‘United Kingdom’</td>
          </tr>
          <tr>
            <td rowspan='5'>data_value_type</td>
            <td>raw [default]</td>
            <td>Raw data values and dates</td>
          </tr>
          <tr>
            <td>raw_extended</td>
            <td>Raw data values and dates. Plus additional columns added to the table for reference codes, hierarchies and sort codes.</td>
          </tr>
          <tr>
            <td>formatted</td>
            <td>Formatted data values, including rounding to decimal places and commas to separate thousands. Does not include formatted dates.</td>
          </tr>
          <tr>
            <td>formatted_extended</td>
            <td>Formatted data values and dates, including rounding to decimal places and commas to separate thousands. Plus additional columns added to the table for reference codes, hierarchies and sort codes.</td>
          </tr>
          <tr>
            <td>with_note_codes</td>
            <td>Data values annotated with shorthand to provide extra detail</td>
          </tr>
        </tbody>
      </table>"
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
    #swagger.summary = "Generate a filter ID for a specific pivot query"
    #swagger.description = "<p>Generates a filter ID for a chosen combination of pivot configuration, filters and
      display options. This ID is always the same for the same combinations.</p>
      <p>You need to send a JSON body that contains sections for ‘pivot’, ‘filters’ and ‘options’.</p>
      <p>The ‘pivot’ section should contain the variables you want use for the:</p>
      <ul>
        <li>columns of the pivot table, or “x” axis</li>
        <li>rows of the pivot table, or “y” axis</li>
      </ul>
      <p>You can find out what the ‘filters’ and ‘options’ sections should include in the ‘Generate a filter ID for a
      specific query’ endpoint.</p>"
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
  longTimeout,
  ensurePublishedDataset,
  /*
    #swagger.tags = ['Data']
    #swagger.summary = "Get paginated data for a dataset"
    #swagger.description = "Returns rows for the latest published revision as a
      JSON array of objects. Each object has column names as keys. The
      response includes a Content-Disposition header for download. To apply
      filters, first create a filter via POST /{dataset_id}/data, then use
      GET /{dataset_id}/data/{filter_id}."
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/output_format',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/sort_by'
    ]
    #swagger.responses[200] = {
      description: 'A JSON array of data row objects',
      content: {
        'application/json': {
          schema: { type: 'array', items: { $ref: "#/components/schemas/DataRow" } }
        }
      }
    }
  */
  getPublishedDatasetData
);

publicApiV2Router.get(
  '/:dataset_id/data/:filter_id',
  longTimeout,
  ensurePublishedDataset,
  /*
    #swagger.tags = ['Data']
    #swagger.summary = "Get a filtered data table for a dataset"
    #swagger.description = "Returns current data for a published dataset, filtered and displayed according to the
      chosen options for a specific filter ID."
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/output_format',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/sort_by',
      '#/components/parameters/filter_id'
    ]
    #swagger.responses[200] = {
      description: 'A JSON array of filtered data row objects',
      content: {
        'application/json': {
          schema: { type: 'array', items: { $ref: "#/components/schemas/DataRow" } }
        }
      }
    }
  */
  getPublishedDatasetData
);

publicApiV2Router.get(
  '/:dataset_id/pivot/:filter_id',
  longTimeout,
  ensurePublishedDataset,
  /*
    #swagger.tags = ['Pivot']
    #swagger.summary = "Get a pivot table for a dataset"
    #swagger.description = "Returns a pivot table for a published dataset, filtered and displayed according to the
      chosen options for a specific filter ID."
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/output_format',
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

// Debugging-only: returns the default query configuration for the dataset (no filter ID).
publicApiV2Router.get(
  '/:dataset_id/query/',
  ensurePublishedDataset,
  /* #swagger.ignore = true */
  getFilterIdDetails
);

publicApiV2Router.get(
  '/:dataset_id/query/:filter_id',
  ensurePublishedDataset,
  /*
    #swagger.tags = ['Query']
    #swagger.summary = "Get details of a filter query"
    #swagger.description = "Returns the chosen options and configuration for a specific filter ID."
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

// Experimental endpoint — not intended for consumers. Pivots any existing query and allows for
// more complex multidimensional pivots.
publicApiV2Router.get(
  '/:dataset_id/data/:filter_id/pivot',
  longTimeout,
  ensurePublishedDataset,
  /* #swagger.ignore = true */
  getPublishedDatasetPivot
);
