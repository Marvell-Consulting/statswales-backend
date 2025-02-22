import 'reflect-metadata';

import express, { NextFunction, Request, Response, Router } from 'express';
import multer from 'multer';
import { FindOptionsRelations } from 'typeorm';

import {
    attachDataTableToRevision,
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
    approveForPublication,
    withdrawFromPublication,
    createNewRevision
} from '../controllers/revision';
import { Revision } from '../entities/dataset/revision';
import { hasError, revisionIdValidator } from '../validators';
import { logger } from '../utils/logger';
import { NotFoundException } from '../exceptions/not-found.exception';
import { RevisionRepository } from '../repositories/revision';

// middleware that loads the dataset (with nested relations) and stores it in res.locals
// leave relations undefined to load the default relations
// pass an empty object to load no relations
export const loadRevision = (relations?: FindOptionsRelations<Revision>) => {
    return async (req: Request, res: Response, next: NextFunction) => {
        const revisionIdError = await hasError(revisionIdValidator(), req);
        if (revisionIdError) {
            logger.error(revisionIdError);
            next(new NotFoundException('errors.revision_id_invalid'));
            return;
        }

        // TODO: include user in query to prevent unauthorized access

        try {
            logger.debug(`Loading dataset ${req.params.revision_id}...`);
            const revision = await RevisionRepository.getById(req.params.revision_id, relations);
            res.locals.revision_id = revision.id;
            res.locals.revision = revision;
        } catch (err) {
            logger.error(`Failed to load revision, error: ${err}`);
            next(new NotFoundException('errors.no_revision'));
            return;
        }

        if (!res.locals.dataset.revisions.find((rev: Revision) => rev.id === req.params.revision_id)) {
            logger.error('Revision does not belong to dataset');
            next(new NotFoundException('errors.revision_id_invalid'));
            return;
        }

        next();
    };
};

const jsonParser = express.json();
const upload = multer({ storage: multer.memoryStorage() });

const router = Router();
export const revisionRouter = router;

// POST
// Create a new revision for an update
router.post('/', createNewRevision);

// POST /dataset/:dataset_id/revision/id/:revision_id/data-table
// Creates a new import on a revision.  This typically only occurs when a user
// decides the file they uploaded wasn't correct.
router.post('/by-id/:revision_id/data-table', loadRevision(), upload.single('csv'), attachDataTableToRevision);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/data-table/by-id/:fact_table_id
// Returns details of a data-table with its sources
router.get('/by-id/:revision_id/data-table', loadRevision(), getFactTableInfo);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/data-table/preview
// Returns a view of the data file attached to the data-table
router.get('/by-id/:revision_id/data-table/preview', loadRevision(), getFactTablePreview);

router.get('/by-id/:revision_id/preview', loadRevision(), getRevisionPreview);

// PATCH /dataset/:dataset_id/revision/by-id/:revision_id/data-table/confirm
// Moves the file from temporary blob storage to datalake and creates sources
// returns a JSON object with the current state of the revision including the data-table
// and sources created from the data-table.
router.patch('/by-id/:revision_id/data-table/confirm', loadRevision(), confirmFactTable);

// GET /dataset/:dataset_id/revision/id/:revision_id/data-table/id/:fact_table_id/raw
// Returns the original uploaded file back to the client
router.get('/by-id/:revision_id/data-table/raw', loadRevision(), downloadRawFactTable);

// DELETE /:dataset_id/revision/by-id/:revision_id/data-table
// Removes the import record and associated file from BlobStorage clearing the way
// for the user to upload a new file for the dataset.
router.delete('/by-id/:revision_id/data-table', loadRevision(), removeFactTableFromRevision);

// PATCH /dataset/:dataset_id/revision/by-id/:revision_id/publish-at
// Updates the publishAt date for the specified revision
router.patch('/by-id/:revision_id/publish-at', loadRevision(), jsonParser, updateRevisionPublicationDate);

// POST /dataset/:dataset_id/revision/by-id/<revision id>/approve
// Approve the dataset's latest revision for publication
router.post('/by-id/:revision_id/approve', loadRevision(), approveForPublication);

// POST /dataset/:dataset_id/revision/by-id/<revision id>/withdraw
// Withdraw the dataset's latest revision from scheduled publication
router.post('/by-id/:revision_id/withdraw', loadRevision(), withdrawFromPublication);

// GET /dataset/:dataset_id/revision/id/:revision_id
// Returns details of a revision with its imports
router.get('/by-id/:revision_id', loadRevision(), getRevisionInfo);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/cube
// Returns the specific revision of the dataset as a DuckDB File
router.get('/by-id/:revision_id/cube', loadRevision(), downloadRevisionCubeFile);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/cube/json
// Returns the specific revision of the dataset as a JSON file
router.get('/by-id/:revision_id/cube/json', loadRevision(), downloadRevisionCubeAsJSON);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/cube/csv
// Returns the specific revision of the dataset as a CSV file
router.get('/by-id/:revision_id/cube/csv', loadRevision(), downloadRevisionCubeAsCSV);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/cube/parquet
// Returns the specific revision of the dataset as a Parquet file
router.get('/by-id/:revision_id/cube/parquet', loadRevision(), downloadRevisionCubeAsParquet);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/cube/excel
// Returns the specific revision of the dataset as an Excel file
router.get('/by-id/:revision_id/cube/excel', loadRevision(), downloadRevisionCubeAsExcel);
