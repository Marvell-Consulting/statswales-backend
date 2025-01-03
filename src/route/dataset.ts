import 'reflect-metadata';

import express, { NextFunction, Request, Response, Router } from 'express';
import multer from 'multer';
import { FieldValidationError } from 'express-validator';
import { FindOptionsRelations } from 'typeorm';

import { logger } from '../utils/logger';
import {
    attachLookupTableToDimension,
    getDimensionInfo,
    resetDimension,
    sendDimensionPreview,
    updateDimension,
    updateDimensionInfo
} from '../controllers/dimension-processor';
import { DatasetRepository } from '../repositories/dataset';
import { datasetIdValidator, factTableIdValidator, hasError, revisionIdValidator, titleValidator } from '../validators';
import { NotFoundException } from '../exceptions/not-found.exception';
import { FactTableRepository } from '../repositories/fact-table';
import { Dataset } from '../entities/dataset/dataset';
import { FactTable } from '../entities/dataset/fact-table';
import {
    downloadCubeAsCSV,
    downloadCubeAsExcel,
    downloadCubeAsJSON,
    downloadCubeAsParquet,
    downloadCubeFile
} from '../controllers/cube-handler';
import { attachLookupTableToMeasure, getPreviewOfMeasure, resetMeasure } from '../controllers/measure-handler';
import {
    addProvidersToDataset,
    createDataset,
    createFirstRevision,
    cubePreview,
    deleteDatasetById,
    getDatasetById,
    getDatasetTasklist,
    listActiveDatasets,
    listAllDatasets,
    updateDatasetInfo,
    updateDatasetProviders,
    updateDatasetTeam,
    updateDatasetTopics
} from '../controllers/dataset';
import {
    attachFactTableToRevision,
    confirmFactTable,
    downloadRawFactTable,
    downloadRevisionCubeAsCSV,
    downloadRevisionCubeAsExcel,
    downloadRevisionCubeAsJSON,
    downloadRevisionCubeAsParquet,
    downloadRevisionCubeFile,
    getFactTableInfo,
    getFactTablePreview,
    getRevisionInfo,
    getRevisionPreview,
    removeFactTableFromRevision,
    updateRevisionPublicationDate,
    updateSources
} from '../controllers/revision';

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

// middleware that loads a specific file import of a dataset and stores it in res.locals
// requires :dataset_id, :revision_id and :fact_table_id in the path
export const loadFactTable = async (req: Request, res: Response, next: NextFunction) => {
    for (const validator of [datasetIdValidator(), revisionIdValidator(), factTableIdValidator()]) {
        const result = await validator.run(req);
        if (!result.isEmpty()) {
            const error = result.array()[0] as FieldValidationError;
            next(new NotFoundException(`errors.${error.path}_invalid`));
            return;
        }
    }

    // TODO: include user in query to prevent unauthorized access

    try {
        const { dataset_id, revision_id, fact_table_id } = req.params;
        const factTable: FactTable = await FactTableRepository.getFactTableById(dataset_id, revision_id, fact_table_id);
        res.locals.factTable = factTable;
        res.locals.revision = factTable.revision;
        res.locals.dataset = factTable.revision.dataset;
        res.locals.datasetId = dataset_id;
    } catch (err) {
        logger.error(`Failed to load requested fact table, error: ${err}`);
        next(new NotFoundException('errors.no_file_import'));
        return;
    }

    next();
};

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

// POST /dataset
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

// GET /dataset/:dataset_id/revision/by-id/:revision_id/cube
// Returns the specific revision of the dataset as a DuckDB File
router.get('/:dataset_id/revision/by-id/:revision_id/cube', loadDataset(), downloadRevisionCubeFile);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/cube/json
// Returns the specific revision of the dataset as a JSON file
router.get('/:dataset_id/revision/by-id/:revision_id/cube/json', loadDataset(), downloadRevisionCubeAsJSON);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/cube/csv
// Returns the specific revision of the dataset as a CSV file
router.get('/:dataset_id/revision/by-id/:revision_id/cube/csv', loadDataset(), downloadRevisionCubeAsCSV);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/cube/parquet
// Returns the specific revision of the dataset as a Parquet file
router.get('/:dataset_id/revision/by-id/:revision_id/cube/parquet', loadDataset(), downloadRevisionCubeAsParquet);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/cube/excel
// Returns the specific revision of the dataset as an Excel file
router.get('/:dataset_id/revision/by-id/:revision_id/cube/excel', loadDataset(), downloadRevisionCubeAsExcel);

// PATCH /dataset/:dataset_id/info
// Updates the dataset info with the provided data
router.patch('/:dataset_id/info', jsonParser, loadDataset(), updateDatasetInfo);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id
// Returns details of a fact-table with its sources
router.get('/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id', loadFactTable, getFactTableInfo);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id/preview
// Returns a view of the data file attached to the fact-table
router.get(
    '/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id/preview',
    loadFactTable,
    getFactTablePreview
);

router.get('/:dataset_id/revision/by-id/:revision_id/preview', loadDataset(), getRevisionPreview);

// PATCH /dataset/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id/confirm
// Moves the file from temporary blob storage to datalake and creates sources
// returns a JSON object with the current state of the revision including the fact-table
// and sources created from the fact-table.
router.patch(
    '/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id/confirm',
    loadFactTable,
    confirmFactTable
);

// GET /dataset/:dataset_id/revision/id/:revision_id/fact-table/id/:fact_table_id/raw
// Returns the original uploaded file back to the client
router.get(
    '/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id/raw',
    loadFactTable,
    downloadRawFactTable
);

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
router.patch(
    '/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id/sources',
    jsonParser,
    loadFactTable,
    updateSources
);

// GET /dataset/:dataset_id/tasklist
// Returns a JSON object with info on what parts of the dataset have been created
router.get('/:dataset_id/tasklist', loadDataset(), getDatasetTasklist);

// GET /dataset/:dataset_id/dimension/id/:dimension_id
// Returns details of a dimension with its sources and imports
router.get('/:dataset_id/dimension/by-id/:dimension_id', loadDataset(), getDimensionInfo);

router.delete('/:dataset_id/measure/reset', loadDataset(), resetMeasure);

// DELETE /dataset/:dataset_id/dimension/id/:dimension_id/reset
// Resets the dimensions type back to "Raw" and removes the extractor
router.delete('/:dataset_id/dimension/by-id/:dimension_id/reset', loadDataset(), resetDimension);

// GET /dataset/:dataset_id/dimension/id/:dimension_id/preview
// Returns details of a dimension and a preview of the data
// It should be noted that this returns the raw values in the
// preview as opposed to view which returns interpreted values.
router.get('/:dataset_id/dimension/by-id/:dimension_id/preview', loadDataset(), sendDimensionPreview);

// POST /:dataset_id/dimension/by-id/:dimension_id/lookup
// Attaches a lookup table to do a dimension and validates
// the lookup table.
router.post(
    '/:dataset_id/dimension/by-id/:dimension_id/lookup',
    upload.single('csv'),
    loadDataset(),
    attachLookupTableToDimension
);

// POST /:dataset_id/measure
// Attaches a measure lookup table to a dataset and validates it.
router.post('/:dataset_id/measure', upload.single('csv'), loadDataset(), attachLookupTableToMeasure);

// GET /dataset/:dataset_id/dimension/id/:dimension_id/preview
// Returns details of a dimension and a preview of the data
// It should be noted that this returns the raw values in the
// preview as opposed to view which returns interpreted values.
router.get('/:dataset_id/measure/preview', loadDataset(), getPreviewOfMeasure);

// PATCH /dataset/:dataset_id/dimension/id/:dimension_id/
// Takes a patch request and validates the request against the fact table
// If it fails it sends back an error
router.patch('/:dataset_id/dimension/by-id/:dimension_id', jsonParser, loadDataset(), updateDimension);

// PATCH /:dataset_id/dimension/by-id/:dimension_id/info
// Updates the dimension info
router.patch('/:dataset_id/dimension/by-id/:dimension_id/info', jsonParser, loadDataset(), updateDimensionInfo);

// GET /dataset/:dataset_id/revision/id/:revision_id
// Returns details of a revision with its imports
router.get('/:dataset_id/revision/by-id/:revision_id', loadDataset(), getRevisionInfo);

// POST /dataset/:dataset_id/revision/id/:revision_id/fact-table
// Creates a new import on a revision.  This typically only occurs when a user
// decides the file they uploaded wasn't correct.
router.post(
    '/:dataset_id/revision/by-id/:revision_id/fact-table',
    upload.single('csv'),
    loadDataset(),
    attachFactTableToRevision
);

// DELETE /:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id
// Removes the import record and associated file from BlobStorage clearing the way
// for the user to upload a new file for the dataset.
router.delete(
    '/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id',
    loadFactTable,
    removeFactTableFromRevision
);

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

// PATCH /dataset/:dataset_id/revision/by-id/:revision_id/publish-at
// Updates the publishAt date for the specified revision
router.patch(
    '/:dataset_id/revision/by-id/:revision_id/publish-at',
    jsonParser,
    loadDataset(),
    updateRevisionPublicationDate
);
