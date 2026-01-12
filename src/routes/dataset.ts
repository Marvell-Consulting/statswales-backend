import 'reflect-metadata';

import express, { Router } from 'express';

import {
  addDataProvider,
  createDataset,
  uploadDataTable,
  getDataProviders,
  getTasklist,
  getTopics,
  getFactTableDefinition,
  listUserDatasets,
  updateMetadata,
  updateDataProviders,
  updateTopics,
  updateSources,
  getDatasetById,
  deleteDraftDatasetById,
  listAllFilesInDataset,
  getAllFilesForDataset,
  updateDatasetGroup,
  getHistory,
  datasetActionRequest,
  generateFilterId,
  datasetPreview
} from '../controllers/dataset';
import { datasetAuth } from '../middleware/dataset-auth';
import { fileStreaming } from '../middleware/file-streaming';

import { revisionRouter } from './revision';
import { dimensionRouter } from './dimension';
import { measureRouter } from './measure';

const jsonParser = express.json();

export const datasetRouter = Router();

// GET /dataset/
// Returns a list of datasets the user can access
datasetRouter.get('/', listUserDatasets);

// POST /dataset
// Creates a new dataset with a title
// Returns a DatasetDTO object
datasetRouter.post('/', jsonParser, createDataset);

// ****** DATASET AUTHORISATION MIDDLEWARE ****** //
// applies auth check for dataset for the current user
datasetRouter.use('/:dataset_id', datasetAuth);
// ***** DO NOT REMOVE ***** //

// DELETE /dataset/:dataset_id
// Deletes the dataset with the given ID
datasetRouter.delete('/:dataset_id', deleteDraftDatasetById);

// GET /dataset/:dataset_id
// Returns the dataset, optionally specify relations to load via 'hydrate' query param
datasetRouter.get('/:dataset_id', getDatasetById);

// PATCH /dataset/:dataset_id/metadata
// Updates the dataset info with the provided data
datasetRouter.patch('/:dataset_id/metadata', jsonParser, updateMetadata);

// POST /dataset/:dataset_id/data
// Upload a data file to a dataset
// Returns a DTO object that includes the draft revision
datasetRouter.post('/:dataset_id/data', fileStreaming(), uploadDataTable);

// Generates a filter ID for the current dataset preview filter selection
datasetRouter.post('/:dataset_id/preview', jsonParser, generateFilterId);

// Returns a preview of the dataset data based on the filter ID if provided
datasetRouter.get('/:dataset_id/preview', datasetPreview);
datasetRouter.get('/:dataset_id/preview/:filter_id', datasetPreview);

datasetRouter.get('/:dataset_id/sources', getFactTableDefinition);

datasetRouter.get('/:dataset_id/fact-table', getFactTableDefinition);

// PATCH /dataset/:dataset_id/sources
// Creates the dimensions and measures from the first import based on user input via JSON
// Body should contain the following structure:
// [
//     {
//         "csvField": "<csv-field>",
//         "sourceType": "data_values || "dimension" || "foot_notes" || "ignore"
//     }
// ]
// Notes: There can only be one object with a type of "dataValue" and one object with a type of "noteCodes"
// and one object with a value of "measure"
// Returns a JSON object with the current state of the dataset including the dimensions created.
datasetRouter.patch('/:dataset_id/sources', jsonParser, updateSources);

// GET /dataset/:dataset_id/tasklist
// Returns a JSON object with info on what parts of the dataset have been created
datasetRouter.get('/:dataset_id/tasklist', getTasklist);

// GET /dataset/:dataset_id/providers
// Returns the data providers for the dataset
datasetRouter.get('/:dataset_id/providers', jsonParser, getDataProviders);

// POST /dataset/:dataset_id/providers
// Adds a new data provider for the dataset
datasetRouter.post('/:dataset_id/providers', jsonParser, addDataProvider);

// PATCH /dataset/:dataset_id/providers
// Updates the data providers for the dataset
datasetRouter.patch('/:dataset_id/providers', jsonParser, updateDataProviders);

// GET /dataset/:dataset_id/topics
// Returns the topics for the dataset
datasetRouter.get('/:dataset_id/topics', jsonParser, getTopics);

// PATCH /dataset/:dataset_id/topics
// Updates the topics for the dataset
datasetRouter.patch('/:dataset_id/topics', jsonParser, updateTopics);

// GET /dataset/:dataset_id/download
// Downloads everything from the datalake relating to this dataset as a zip file
datasetRouter.get('/:dataset_id/download', getAllFilesForDataset);

// GET /dataset/:dataset_id/list-files
// List all the files which are used to build the cube
datasetRouter.get('/:dataset_id/list-files', listAllFilesInDataset);

// PATCH /dataset/:dataset_id/group
// Updates the user group for the dataset
datasetRouter.patch('/:dataset_id/group', jsonParser, updateDatasetGroup);

// GET /dataset/:dataset_id/history
// List the event history for this dataset
datasetRouter.get('/:dataset_id/history', getHistory);

// POST /dataset/:dataset_id/:action
// Request an action (publish, unpublish, archive, unarchive, withdraw) for this dataset
datasetRouter.post('/:dataset_id/:action', jsonParser, datasetActionRequest);

// apply revision child routes
datasetRouter.use('/:dataset_id/revision', revisionRouter);

// apply dimension child routes
datasetRouter.use('/:dataset_id/dimension', dimensionRouter);

// apply measure child routes
datasetRouter.use('/:dataset_id/measure', measureRouter);
