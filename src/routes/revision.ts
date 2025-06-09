import 'reflect-metadata';

import express, { NextFunction, Request, Response, Router } from 'express';
import multer from 'multer';
import { FindOptionsRelations } from 'typeorm';

import {
  updateDataTable,
  confirmFactTable,
  downloadRawFactTable,
  downloadRevisionCubeAsCSV,
  downloadRevisionCubeAsExcel,
  downloadRevisionCubeAsJSON,
  downloadRevisionCubeAsParquet,
  getDataTablePreview,
  getRevisionInfo,
  getRevisionPreview,
  removeFactTableFromRevision,
  updateRevisionPublicationDate,
  submitForPublication,
  withdrawFromPublication,
  createNewRevision,
  getDataTable,
  deleteDraftRevision,
  regenerateRevisionCube,
  getRevisionPreviewFilters
} from '../controllers/revision';
import { Revision } from '../entities/dataset/revision';
import { hasError, revisionIdValidator } from '../validators';
import { logger } from '../utils/logger';
import { NotFoundException } from '../exceptions/not-found.exception';
import { RevisionRepository, withMetadataAndProviders } from '../repositories/revision';

// middleware that loads the revision and stores it in res.locals
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

    try {
      const revision = await RevisionRepository.getById(req.params.revision_id, relations);

      if (res.locals.datasetId !== revision.datasetId) {
        logger.error('Revision does not belong to dataset');
        throw new NotFoundException('errors.revision_id_invalid');
      }

      res.locals.revision_id = revision.id;
      res.locals.revision = revision;
    } catch (err) {
      logger.error(err, `Failed to load revision`);
      next(new NotFoundException('errors.no_revision'));
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

// DELETE /dataset/:dataset_id/revision/by-id/:revision_id
// Deletes a revision provided it is not published
router.delete('/by-id/:revision_id', loadRevision({}), deleteDraftRevision);

// POST /dataset/:dataset_id/revision/by-id/:revision_id
// Regenerates the revision cube
router.post('/by-id/:revision_id', loadRevision(), regenerateRevisionCube);

// GET /dataset/:dataset_id/revision/id/:revision_id
// Returns details of a revision with metadata
router.get('/by-id/:revision_id', loadRevision(withMetadataAndProviders), getRevisionInfo);

// GET /dataset/:dataset_id/revision/id/:revision_id/preview
// Returns details of a revision with its imports
router.get('/by-id/:revision_id/preview', loadRevision(), getRevisionPreview);

router.get('/by-id/:revision_id/preview/filters', loadRevision(), getRevisionPreviewFilters);

// POST /dataset/:dataset_id/revision/id/:revision_id/data-table
// Upload an updated data file for the revision
router.post('/by-id/:revision_id/data-table', loadRevision(), upload.single('csv'), updateDataTable);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/data-table
// Returns details of a data-table
router.get('/by-id/:revision_id/data-table', loadRevision(), getDataTable);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/data-table/preview
// Returns a view of the data file attached to the data-table
router.get('/by-id/:revision_id/data-table/preview', loadRevision(), getDataTablePreview);

// PATCH /dataset/:dataset_id/revision/by-id/:revision_id/data-table/confirm
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
router.post('/by-id/:revision_id/submit', loadRevision(), submitForPublication);

// POST /dataset/:dataset_id/revision/by-id/<revision id>/withdraw
// Withdraw the dataset's latest revision from scheduled publication
router.post('/by-id/:revision_id/withdraw', loadRevision(), withdrawFromPublication);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/cube
// Returns the specific revision of the dataset as a DuckDB File
// router.get('/by-id/:revision_id/cube', loadRevision(), downloadRevisionCubeFile);

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
router.get('/by-id/:revision_id/cube/xlsx', loadRevision(), downloadRevisionCubeAsExcel);
