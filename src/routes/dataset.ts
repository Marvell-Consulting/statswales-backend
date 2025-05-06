import { performance } from 'node:perf_hooks';

import 'reflect-metadata';

import express, { NextFunction, Request, Response, Router } from 'express';
import multer from 'multer';
import { FindOptionsRelations } from 'typeorm';
import passport from 'passport';

import { logger } from '../utils/logger';
import {
  withDraftForCube,
  withDraftAndMetadata,
  DatasetRepository,
  withAll,
  withDraftAndProviders,
  withDraftAndTopics,
  withDraftAndMeasure
} from '../repositories/dataset';
import { datasetIdValidator, hasError } from '../validators';
import { NotFoundException } from '../exceptions/not-found.exception';
import { Dataset } from '../entities/dataset/dataset';
import {
  downloadCubeAsCSV,
  downloadCubeAsExcel,
  downloadCubeAsJSON,
  downloadCubeAsParquet,
  downloadCubeFile
} from '../controllers/cube-controller';
import {
  addDataProvider,
  createDataset,
  uploadDataTable,
  cubePreview,
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
  getDatasetOverviewById
} from '../controllers/dataset';
import { rateLimiter } from '../middleware/rate-limiter';

import { revisionRouter } from './revision';
import { dimensionRouter } from './dimension';
import { measureRouter } from './measure';
import { ForbiddenException } from '../exceptions/forbidden.exception';
import { getUserGroupIdsForUser } from '../utils/get-permissions-for-user';

const jsonParser = express.json();
const upload = multer({ storage: multer.memoryStorage() });

const router = Router();
export const datasetRouter = router;

// middleware that loads the dataset (with nested relations) and stores it in res.locals.dataset
// by default, no relations are loaded unless requested in via the relations param
export const loadDataset = (relations?: FindOptionsRelations<Dataset>) => {
  return async (req: Request, res: Response, next: NextFunction) => {
    const datasetIdError = await hasError(datasetIdValidator(), req);
    if (datasetIdError) {
      logger.error(datasetIdError);
      next(new NotFoundException('errors.dataset_id_invalid'));
      return;
    }

    try {
      const start = performance.now();
      const dataset = await DatasetRepository.getById(req.params.dataset_id, relations);
      const end = performance.now();

      // permissions check - dataset must belong to a group the user is a member of
      const userGroupIds = getUserGroupIdsForUser(req.user!);

      if (!dataset.userGroupId || !userGroupIds?.includes(dataset.userGroupId)) {
        logger.warn('User does not have access to this dataset');
        next(new ForbiddenException('errors.dataset_not_in_users_groups'));
        return;
      }

      res.locals.datasetId = dataset.id;
      res.locals.dataset = dataset;

      const size = Math.round(Buffer.byteLength(JSON.stringify(dataset)) / 1024);
      const time = Math.round(end - start);

      logger.debug(`Dataset ${req.params.dataset_id} loaded { size: ${size}kb, time: ${time}ms }`);
    } catch (err) {
      logger.error(err, `Failed to load dataset`);
      next(new NotFoundException('errors.no_dataset'));
      return;
    }

    next();
  };
};

// GET /dataset/
// Returns a list of datasets the user can access
router.get('/', listUserDatasets);

// POST /dataset
// Creates a new dataset with a title
// Returns a DatasetDTO object
router.post('/', jsonParser, createDataset);

// DELETE /dataset/:dataset_id
// Deletes the dataset with the given ID
router.delete('/:dataset_id', loadDataset({}), deleteDraftDatasetById);

// GET /dataset/:dataset_id
// Returns the dataset with no relations hydrated (i.e. validates dataset exists)
router.get('/:dataset_id', loadDataset({}), getDatasetById);

// GET /dataset/:dataset_id/all
// Returns the dataset with all available relations hydrated
router.get('/:dataset_id/all', loadDataset(withAll), getDatasetById);

// GET /dataset/:dataset_id/overview
// Returns the dataset with info required to display the overview page
router.get('/:dataset_id/overview', loadDataset(), getDatasetOverviewById);

// GET /dataset/:dataset_id/metadata
// Returns the dataset with the current draft and metadata
router.get('/:dataset_id/metadata', loadDataset(withDraftAndMetadata), getDatasetById);

// PATCH /dataset/:dataset_id/metadata
// Updates the dataset info with the provided data
router.patch('/:dataset_id/metadata', jsonParser, loadDataset({}), updateMetadata);

// GET /dataset/:dataset_id/data
// Returns the dataset with the current draft revision and data table
router.get('/:dataset_id/data', loadDataset(withDraftForCube), getDatasetById);

// POST /dataset/:dataset_id/data
// Upload a data file to a dataset
// Returns a DTO object that includes the draft revision
router.post('/:dataset_id/data', upload.single('csv'), loadDataset({}), uploadDataTable);

// GET /dataset/:dataset_id/measure
// Returns the dataset with the current draft and measure
router.get('/:dataset_id/measure', loadDataset(withDraftAndMeasure), getDatasetById);

// GET /dataset/:dataset_id/view
// Returns a view of the data file attached to the import
router.get('/:dataset_id/view', loadDataset(withDraftForCube), cubePreview);

// GET /dataset/:dataset_id/cube
// Returns the latest revision of the dataset as a DuckDB File
router.get('/:dataset_id/cube', loadDataset(withDraftForCube), downloadCubeFile);

// GET /dataset/:dataset_id/cube/json
// Returns a JSON file representation of the default view of the cube
router.get('/:dataset_id/cube/json', loadDataset(withDraftForCube), downloadCubeAsJSON);

// GET /dataset/:dataset_id/cube/csv
// Returns a CSV file representation of the default view of the cube
router.get('/:dataset_id/cube/csv', loadDataset(withDraftForCube), downloadCubeAsCSV);

// GET /dataset/:dataset_id/cube/parquet
// Returns a CSV file representation of the default view of the cube
router.get('/:dataset_id/cube/parquet', loadDataset(withDraftForCube), downloadCubeAsParquet);

// GET /dataset/:dataset_id/cube/excel
// Returns a CSV file representation of the default view of the cube
router.get('/:dataset_id/cube/excel', loadDataset(withDraftForCube), downloadCubeAsExcel);

router.get('/:dataset_id/sources', loadDataset({ factTable: true }), getFactTableDefinition);

router.get('/:dataset_id/fact-table', loadDataset({ factTable: true }), getFactTableDefinition);

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
router.patch('/:dataset_id/sources', jsonParser, loadDataset(withDraftForCube), updateSources);

// GET /dataset/:dataset_id/tasklist
// Returns a JSON object with info on what parts of the dataset have been created
router.get('/:dataset_id/tasklist', loadDataset({}), getTasklist);

// GET /dataset/:dataset_id/providers
// Returns the data providers for the dataset
router.get('/:dataset_id/providers', jsonParser, loadDataset(withDraftAndProviders), getDataProviders);

// POST /dataset/:dataset_id/providers
// Adds a new data provider for the dataset
router.post('/:dataset_id/providers', jsonParser, loadDataset({}), addDataProvider);

// PATCH /dataset/:dataset_id/providers
// Updates the data providers for the dataset
router.patch('/:dataset_id/providers', jsonParser, loadDataset({}), updateDataProviders);

// GET /dataset/:dataset_id/topics
// Returns the topics for the dataset
router.get('/:dataset_id/topics', jsonParser, loadDataset(withDraftAndTopics), getTopics);

// PATCH /dataset/:dataset_id/topics
// Updates the topics for the dataset
router.patch('/:dataset_id/topics', jsonParser, loadDataset(withDraftAndTopics), updateTopics);

// GET /dataset/:dataset_id/download
// Downloads everything from the datalake relating to this dataset as a zip file
router.get('/:dataset_id/download', loadDataset(withDraftForCube), getAllFilesForDataset);

// GET /dataset/:dataset_id/list-files
// List all the files which are used to build the cube
router.get('/:dataset_id/list-files', loadDataset(withDraftForCube), listAllFilesInDataset);

// PATCH /dataset/:dataset_id/group
// Updates the user group for the dataset
router.patch('/:dataset_id/group', jsonParser, loadDataset(), updateDatasetGroup);

router.use(
  '/:dataset_id/revision',
  rateLimiter,
  passport.authenticate('jwt', { session: false }),
  loadDataset({
    dimensions: { metadata: true, lookupTable: true },
    factTable: true,
    measure: { measureTable: true, metadata: true },
    revisions: { dataTable: { dataTableDescriptions: true } }
  }),
  revisionRouter
);

router.use(
  '/:dataset_id/dimension',
  rateLimiter,
  passport.authenticate('jwt', { session: false }),
  loadDataset(withDraftForCube),
  dimensionRouter
);

router.use(
  '/:dataset_id/measure',
  rateLimiter,
  passport.authenticate('jwt', { session: false }),
  loadDataset(),
  measureRouter
);
