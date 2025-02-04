import 'reflect-metadata';

import express, { NextFunction, Request, Response, Router } from 'express';
import multer from 'multer';
import { FindOptionsRelations } from 'typeorm';
import passport from 'passport';

import { logger } from '../utils/logger';
import { DatasetRepository } from '../repositories/dataset';
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
    addProvidersToDataset,
    createDataset,
    createFirstRevision,
    cubePreview,
    deleteDatasetById,
    getDatasetById,
    getDatasetTasklist,
    getFactTableDefinition,
    listActiveDatasets,
    listAllDatasets,
    updateDatasetInfo,
    updateDatasetProviders,
    updateDatasetTeam,
    updateDatasetTopics,
    updateSources
} from '../controllers/dataset';
import { rateLimiter } from '../middleware/rate-limiter';

import { revisionRouter } from './revision';
import { dimensionRouter } from './dimension';
import { measureRouter } from './measure';

const jsonParser = express.json();
const upload = multer({ storage: multer.memoryStorage() });

const router = Router();
export const datasetRouter = router;

// middleware that loads the dataset (with nested relations) and stores it in res.locals
// leave relations undefined to load the default relations
// pass an empty object to load no relations
export const loadDataset = (relations?: FindOptionsRelations<Dataset>) => {
    return async (req: Request, res: Response, next: NextFunction) => {
        const datasetIdError = await hasError(datasetIdValidator(), req);
        if (datasetIdError) {
            logger.error(datasetIdError);
            next(new NotFoundException('errors.dataset_id_invalid'));
            return;
        }

        // TODO: include user in query to prevent unauthorized access

        try {
            logger.debug(`Loading dataset ${req.params.dataset_id}...`);
            const dataset = await DatasetRepository.getById(req.params.dataset_id, relations);
            res.locals.datasetId = dataset.id;
            res.locals.dataset = dataset;
        } catch (err) {
            logger.error(`Failed to load dataset, error: ${err}`);
            next(new NotFoundException('errors.no_dataset'));
            return;
        }

        next();
    };
};

router.use(
    '/:dataset_id/revision',
    rateLimiter,
    passport.authenticate('jwt', { session: false }),
    loadDataset(),
    revisionRouter
);
router.use(
    '/:dataset_id/dimension',
    rateLimiter,
    passport.authenticate('jwt', { session: false }),
    loadDataset(),
    dimensionRouter
);
router.use(
    '/:dataset_id/measure',
    rateLimiter,
    passport.authenticate('jwt', { session: false }),
    loadDataset(),
    measureRouter
);

// GET /dataset
// Returns a JSON object with a list of all datasets and their titles
router.get('/', listAllDatasets);

// GET /dataset/active
// Returns a list of all active datasets e.g. ones with imports
router.get('/active', listActiveDatasets);

// GET /dataset/:dataset_id
// Returns the dataset with the given ID with all available relations hydrated
router.get('/:dataset_id', loadDataset(), getDatasetById);

// DELETE /dataset/:dataset_id
// Deletes the dataset with the given ID
router.delete('/:dataset_id', loadDataset({}), deleteDatasetById);

// POST /dataset
// Creates a new dataset with a title
// Returns a DatasetDTO object
router.post('/', jsonParser, createDataset);

// POST /dataset/:dataset_id/data
// Upload a CSV file to a dataset
// Returns a DTO object that includes the revisions and import records
router.post('/:dataset_id/data', upload.single('csv'), loadDataset(), createFirstRevision);

// GET /dataset/:dataset_id/view
// Returns a view of the data file attached to the import
router.get('/:dataset_id/view', loadDataset(), cubePreview);

// GET /dataset/:dataset_id/cube
// Returns the latest revision of the dataset as a DuckDB File
router.get('/:dataset_id/cube', loadDataset(), downloadCubeFile);

// GET /dataset/:dataset_id/cube/json
// Returns a JSON file representation of the default view of the cube
router.get('/:dataset_id/cube/json', loadDataset(), downloadCubeAsJSON);

// GET /dataset/:dataset_id/cube/csv
// Returns a CSV file representation of the default view of the cube
router.get('/:dataset_id/cube/csv', loadDataset(), downloadCubeAsCSV);

// GET /dataset/:dataset_id/cube/parquet
// Returns a CSV file representation of the default view of the cube
router.get('/:dataset_id/cube/parquet', loadDataset(), downloadCubeAsParquet);

// GET /dataset/:dataset_id/cube/excel
// Returns a CSV file representation of the default view of the cube
router.get('/:dataset_id/cube/excel', loadDataset(), downloadCubeAsExcel);

// PATCH /dataset/:dataset_id/info
// Updates the dataset info with the provided data
router.patch('/:dataset_id/info', jsonParser, loadDataset(), updateDatasetInfo);

// PATCH /dataset/:dataset_id/info
// Updates the dataset info with the provided data
router.patch('/:dataset_id/info', jsonParser, loadDataset(), updateDatasetInfo);

router.get('/:dataset/sources', loadDataset(), getFactTableDefinition);

router.get('/:dataset/fact-table', loadDataset(), getFactTableDefinition);

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
router.patch('/:dataset_id/sources', jsonParser, loadDataset(), updateSources);

// GET /dataset/:dataset_id/tasklist
// Returns a JSON object with info on what parts of the dataset have been created
router.get('/:dataset_id/tasklist', loadDataset(), getDatasetTasklist);

// POST /dataset/:dataset_id/providers
// Adds a new data provider for the dataset
router.post('/:dataset_id/providers', jsonParser, loadDataset(), addProvidersToDataset);

// PATCH /dataset/:dataset_id/providers
// Updates the data providers for the dataset
router.patch('/:dataset_id/providers', jsonParser, loadDataset(), updateDatasetProviders);

// PATCH /dataset/:dataset_id/topics
// Updates the topics for the dataset
router.patch('/:dataset_id/topics', jsonParser, loadDataset(), updateDatasetTopics);

// PATCH /dataset/:dataset_id/team
// Updates the team for the dataset
router.patch('/:dataset_id/team', jsonParser, loadDataset(), updateDatasetTeam);
