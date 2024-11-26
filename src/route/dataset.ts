import 'reflect-metadata';

import { Readable } from 'node:stream';
// import util from 'node:util';

import express, { NextFunction, Request, Response, Router } from 'express';
import multer from 'multer';
import { FieldValidationError } from 'express-validator';
import { FindOptionsRelations } from 'typeorm';

import { logger } from '../utils/logger';
import { ViewDTO, ViewErrDTO, ViewStream } from '../dtos/view-dto';
import {
    createSources,
    DEFAULT_PAGE_SIZE,
    getFileFromBlobStorage,
    getFileFromDataLake,
    moveFileToDataLake,
    processCSVFromBlobStorage,
    processCSVFromDatalake,
    removeFileFromDatalake,
    removeTempfileFromBlobStorage,
    uploadCSVBufferToBlobStorage
} from '../controllers/csv-processor';
import { createDimensionsFromSourceAssignment, validateSourceAssignment } from '../controllers/dimension-processor';
import { User } from '../entities/user/user';
import { FileImport } from '../entities/dataset/file-import';
import { DatasetDTO } from '../dtos/dataset-dto';
import { DatasetInfoDTO } from '../dtos/dataset-info-dto';
import { FileImportDTO } from '../dtos/file-import-dto';
import { DataLocation } from '../enums/data-location';
import { Locale } from '../enums/locale';
import { DatasetRepository } from '../repositories/dataset';
import { hasError, datasetIdValidator, titleValidator, revisionIdValidator, importIdValidator } from '../validators';
import { NotFoundException } from '../exceptions/not-found.exception';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { UnknownException } from '../exceptions/unknown.exception';
import { getLatestImport, getLatestRevision } from '../utils/latest';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionDTO } from '../dtos/dimension-dto';
import { TasklistStateDTO } from '../dtos/tasklist-state-dto';
import { Revision } from '../entities/dataset/revision';
import { RevisionDTO } from '../dtos/revision-dto';
import { Source } from '../entities/dataset/source';
import { SourceAssignmentException } from '../exceptions/source-assignment.exception';
import { arrayValidator, dtoValidator } from '../validators/dto-validator';
import { RevisionRepository } from '../repositories/revision';
import { FileImportRepository } from '../repositories/file-import';
import { Dataset } from '../entities/dataset/dataset';
import { DatasetProviderDTO } from '../dtos/dataset-provider-dto';
import { TopicSelectionDTO } from '../dtos/topic-selection-dto';

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
            // console.log(util.inspect(dataset, false, null, true));
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
// requires :dataset_id, :revision_id and :import_id in the path
export const loadFileImport = async (req: Request, res: Response, next: NextFunction) => {
    for (const validator of [datasetIdValidator(), revisionIdValidator(), importIdValidator()]) {
        const result = await validator.run(req);
        if (!result.isEmpty()) {
            const error = result.array()[0] as FieldValidationError;
            next(new NotFoundException(`errors.${error.path}_invalid`));
            return;
        }
    }

    // TODO: include user in query to prevent unauthorized access

    try {
        const { dataset_id, revision_id, import_id } = req.params;
        const fileImport: FileImport = await FileImportRepository.getFileImportById(dataset_id, revision_id, import_id);
        res.locals.fileImport = fileImport;
        res.locals.revision = fileImport.revision;
        res.locals.dataset = fileImport.revision.dataset;
        res.locals.datasetId = dataset_id;
    } catch (err) {
        logger.error(`Failed to load file import, error: ${err}`);
        next(new NotFoundException('errors.no_file_import'));
        return;
    }

    next();
};

// GET /dataset
// Returns a JSON object with a list of all datasets and their titles
router.get('/', async (req: Request, res: Response, next: NextFunction) => {
    try {
        const datasets = await DatasetRepository.listAllByLanguage(req.language as Locale);
        res.json({ datasets });
    } catch (err) {
        logger.error('Failed to fetch dataset list:', err);
        next(new UnknownException());
    }
});

// GET /dataset/active
// Returns a list of all active datasets e.g. ones with imports
router.get('/active', async (req: Request, res: Response, next: NextFunction) => {
    try {
        const datasets = await DatasetRepository.listActiveByLanguage(req.language as Locale);
        res.json({ datasets });
    } catch (err) {
        logger.error('Failed to fetch active dataset list:', err);
        next(new UnknownException());
    }
});

// GET /dataset/:dataset_id
// Returns the dataset with the given ID with all available relations hydrated
router.get('/:dataset_id', loadDataset(), async (req: Request, res: Response) => {
    res.json(DatasetDTO.fromDataset(res.locals.dataset));
});

// DELETE /dataset/:dataset_id
// Deletes the dataset with the given ID
router.delete('/:dataset_id', loadDataset({}), async (req: Request, res: Response) => {
    await DatasetRepository.deleteById(res.locals.datasetId);
    res.status(204);
    res.end();
});

// POST /dataset
// Creates a new dataset with a title
// Returns a DatasetDTO object
router.post('/', jsonParser, async (req: Request, res: Response, next: NextFunction) => {
    const titleError = await hasError(titleValidator(), req);
    if (titleError) {
        next(new BadRequestException('errors.no_title'));
        return;
    }

    try {
        const dataset = await DatasetRepository.createWithTitle(req.user as User, req.language, req.body.title);
        logger.info(`Dataset created with id: ${dataset.id}`);
        res.status(201);
        res.json(DatasetDTO.fromDataset(dataset));
    } catch (err) {
        logger.error(`An error occurred trying to create a dataset: ${err}`);
        next(new UnknownException());
    }
});

// POST /dataset
// Upload a CSV file to a dataset
// Returns a DTO object that includes the revisions and import records
router.post(
    '/:dataset_id/data',
    upload.single('csv'),
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        if (!req.file) {
            next(new BadRequestException('errors.upload.no_csv'));
            return;
        }

        let fileImport: FileImport;

        try {
            fileImport = await uploadCSVBufferToBlobStorage(req.file.buffer, req.file?.mimetype);
        } catch (err) {
            logger.error(`An error occurred trying to upload the file: ${err}`);
            next(new UnknownException('errors.upload_error'));
            return;
        }

        try {
            const user = req.user as User;
            await RevisionRepository.createFromImport(res.locals.dataset, fileImport, user);
            const dataset = await DatasetRepository.getById(res.locals.datasetId);
            res.status(201);
            res.json(DatasetDTO.fromDataset(dataset));
        } catch (err) {
            logger.error(`An error occurred trying to create a revision: ${err}`);
            next(new UnknownException('errors.upload_error'));
        }
    }
);

// GET /dataset/:dataset_id/view
// Returns a view of the data file attached to the import
router.get('/:dataset_id/view', loadDataset(), async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const latestRevision = getLatestRevision(dataset);

    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }

    const latestImport = getLatestImport(latestRevision);

    if (!latestImport) {
        next(new UnknownException('errors.no_file_import'));
        return;
    }

    const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
    const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;
    let processedCSV: ViewErrDTO | ViewDTO;

    if (latestImport?.location === DataLocation.BlobStorage) {
        processedCSV = await processCSVFromBlobStorage(dataset, latestImport, page_number, page_size);
    } else if (latestImport?.location === DataLocation.DataLake) {
        processedCSV = await processCSVFromDatalake(dataset, latestImport, page_number, page_size);
    } else {
        logger.error('Import location not supported');
        next(new UnknownException('errors.import_location_not_supported'));
        return;
    }

    if (!processedCSV.success) {
        res.status(500);
    }
    res.json(processedCSV);
});

// PATCH /dataset/:dataset_id/info
// Updates the dataset info with the provided data
router.patch(
    '/:dataset_id/info',
    jsonParser,
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const infoDto = await dtoValidator(DatasetInfoDTO, req.body);
            const updatedDataset = await DatasetRepository.patchInfoById(res.locals.datasetId, infoDto);
            res.status(201);
            res.json(DatasetDTO.fromDataset(updatedDataset));
        } catch (err: any) {
            if (err instanceof BadRequestException) {
                err.validationErrors?.forEach((error) => {
                    if (!error.constraints) return;
                    Object.values(error.constraints).forEach((message) => logger.error(message));
                });
                next(err);
                return;
            }
            next(new UnknownException('errors.info_update_error'));
        }
    }
);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id
// Returns details of an import with its sources
router.get(
    '/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id',
    loadFileImport,
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const fileImport = res.locals.fileImport;
            const dto = await FileImportDTO.fromImport(fileImport);
            res.json(dto);
        } catch (err) {
            next(new UnknownException());
        }
    }
);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id/preview
// Returns a view of the data file attached to the import
router.get(
    '/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id/preview',
    loadFileImport,
    async (req: Request, res: Response, next: NextFunction) => {
        const { dataset, fileImport } = res.locals;

        const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
        const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;

        let processedCSV: ViewErrDTO | ViewDTO;

        if (fileImport.location === DataLocation.BlobStorage) {
            processedCSV = await processCSVFromBlobStorage(dataset, fileImport, page_number, page_size);
        } else if (fileImport.location === DataLocation.DataLake) {
            processedCSV = await processCSVFromDatalake(dataset, fileImport, page_number, page_size);
        } else {
            logger.error('Import location not supported');
            next(new UnknownException('errors.import_location_not_supported'));
            return;
        }

        if (!processedCSV.success) {
            const processErr = processedCSV as ViewErrDTO;
            res.status(processErr.status);
        }

        res.json(processedCSV);
    }
);

// PATCH /dataset/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id/confirm
// Moves the file from temporary blob storage to datalake and creates sources
// returns a JSON object with the current state of the revision including the import
// and sources created from the import.
router.patch(
    '/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id/confirm',
    loadFileImport,
    async (req: Request, res: Response, next: NextFunction) => {
        const fileImport: FileImport = res.locals.fileImport;

        if (fileImport.location === DataLocation.DataLake) {
            const fileImportDto = FileImportDTO.fromImport(fileImport);
            res.status(200);
            res.json(fileImportDto);
            return;
        }

        try {
            await moveFileToDataLake(fileImport);
        } catch (err) {
            logger.error(`An error occurred trying to move the file with the following error: ${err}`);
            next(new UnknownException('errors.move_file_error'));
            return;
        }
        try {
            const fileImportDto = await createSources(fileImport);
            res.status(200);
            res.json(fileImportDto);
        } catch (err) {
            logger.error(`An error occurred trying to create the sources with the following error: ${err}`);
            res.status(500);
            res.json({ message: 'Error creating sources from the uploaded file.  Please try again.' });
        }
    }
);

// GET /dataset/:dataset_id/revision/id/:revision_id/import/id/:import_id/raw
// Returns the original uploaded file back to the client
router.get(
    '/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id/raw',
    loadFileImport,
    async (req: Request, res: Response, next: NextFunction) => {
        const { dataset, fileImport } = res.locals;

        let viewStream: ViewErrDTO | ViewStream;
        if (fileImport.location === DataLocation.BlobStorage) {
            viewStream = await getFileFromBlobStorage(dataset, fileImport);
        } else if (fileImport.location === DataLocation.DataLake) {
            viewStream = await getFileFromDataLake(dataset, fileImport);
        } else {
            logger.error('Import location not supported');
            next(new UnknownException('errors.import_location_not_supported'));
            return;
        }
        if (!viewStream.success) {
            res.status(500);
            res.json(viewStream);
            return;
        }
        const readable: Readable = (viewStream as ViewStream).stream;
        readable.pipe(res);

        // Handle errors in the file stream
        readable.on('error', (err) => {
            logger.error('File stream error:', err);
            // eslint-disable-next-line @typescript-eslint/naming-convention
            res.writeHead(500, { 'Content-Type': 'text/plain' });
            res.end('Server Error');
        });

        // Optionally listen for the end of the stream
        readable.on('end', () => {
            logger.debug('File stream ended');
        });
    }
);

// PATCH /dataset/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id/sources
// Creates the dimensions from relating to the import based on information provided
// from the sources and the user.
// Body should contain the following structure:
// [
//     {
//         "sourceId": "<source_id>",
//         "sourceType": "data_values || "dimension" || "foot_notes" || "ignore"
//     }
// ]
// Notes: There can only be one object with a type of "dataValue" and one object with a type of "footnotes"
// Returns a JSON object with the current state of the dataset including the dimensions created.
router.patch(
    '/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id/sources',
    jsonParser,
    loadFileImport,
    async (req: Request, res: Response, next: NextFunction) => {
        const { dataset, revision, fileImport } = res.locals;
        const sourceAssignment = req.body;

        try {
            const validatedSourceAssignment = await validateSourceAssignment(fileImport, sourceAssignment);
            await createDimensionsFromSourceAssignment(dataset, revision, validatedSourceAssignment);
            const updatedDataset = await DatasetRepository.getById(revision.dataset.id);
            res.json(DatasetDTO.fromDataset(updatedDataset));
        } catch (err) {
            logger.error(`An error occurred trying to process the source assignments: ${err}`);

            if (err instanceof SourceAssignmentException) {
                next(new BadRequestException(err.message));
            } else {
                next(new BadRequestException('errors.invalid_source_assignment'));
            }
        }
    }
);

// GET /dataset/:dataset_id/tasklist
// Returns a JSON object with info on what parts of the dataset have been created
router.get('/:dataset_id/tasklist', loadDataset(), async (req: Request, res: Response, next: NextFunction) => {
    try {
        const tasklistState = TasklistStateDTO.fromDataset(res.locals.dataset, req.language as Locale);
        res.json(tasklistState);
    } catch (err) {
        next(new UnknownException('errors.tasklist_error'));
    }
});

// GET /dataset/:dataset_id/dimension/id/:dimension_id
// Returns details of a dimension with its sources and imports
router.get(
    '/:dataset_id/dimension/by-id/:dimension_id',
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        const dataset = res.locals.dataset;
        const dimension = dataset.dimensions.find((dim: Dimension) => dim.id === req.params.dimension_id);

        if (!dimension) {
            next(new NotFoundException('errors.dimension_id_invalid'));
            return;
        }

        res.json(DimensionDTO.fromDimension(dimension));
    }
);

// GET /dataset/:dataset_id/revision/id/:revision_id
// Returns details of a revision with its imports
router.get(
    '/:dataset_id/revision/by-id/:revision_id',
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        const dataset = res.locals.dataset;
        const revision = dataset.revisions.find((revision: Revision) => revision.id === req.params.revision_id);

        if (!revision) {
            next(new NotFoundException('errors.revision_id_invalid'));
            return;
        }

        res.json(RevisionDTO.fromRevision(revision));
    }
);

// POST /dataset/:dataset_id/revision/id/:revision_id/import
// Creates a new import on a revision.  This typically only occurs when a user
// decides the file they uploaded wasn't correct.
router.post(
    '/:dataset_id/revision/by-id/:revision_id/import',
    upload.single('csv'),
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        const dataset = res.locals.dataset;
        const revision = dataset.revisions?.find((revision: Revision) => revision.id === req.params.revision_id);

        if (!revision) {
            next(new NotFoundException('errors.revision_id_invalid'));
            return;
        }

        if (!req.file) {
            next(new BadRequestException('errors.upload.no_csv'));
            return;
        }

        let fileImport: FileImport;

        try {
            fileImport = await uploadCSVBufferToBlobStorage(req.file.buffer, req.file?.mimetype);
            fileImport.revision = revision;
            await fileImport.save();
            const updatedDataset = await DatasetRepository.getById(dataset.id);
            res.status(201);
            res.json(DatasetDTO.fromDataset(updatedDataset));
        } catch (err) {
            logger.error(`An error occurred trying to upload the file with the following error: ${err}`);
            next(new UnknownException('errors.upload_error'));
        }
    }
);

// DELETE /:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id
// Removes the import record and associated file from BlobStorage clearing the way
// for the user to upload a new file for the dataset.
router.delete(
    '/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id',
    loadFileImport,
    async (req: Request, res: Response, next: NextFunction) => {
        const { dataset, fileImport } = res.locals;
        try {
            if (fileImport.location === DataLocation.DataLake) {
                logger.warn('User has requested to remove a fact table from the datalake');
                await removeFileFromDatalake(fileImport);
                fileImport.sources?.forEach((source: Source) => source.remove());
            } else {
                await removeTempfileFromBlobStorage(fileImport);
            }
            await fileImport.remove();
            const updatedDataset = await DatasetRepository.getById(dataset.id);
            const dto = DatasetDTO.fromDataset(updatedDataset);
            res.json(dto);
        } catch (err) {
            logger.error(`An error occurred trying to remove the file with the following error: ${err}`);
            next(new UnknownException('errors.remove_file'));
        }
    }
);

// POST /dataset/:dataset_id/providers
// Adds a new data provider for the dataset
router.post(
    '/:dataset_id/providers',
    jsonParser,
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const datasetId = res.locals.datasetId;
            const provider = await dtoValidator(DatasetProviderDTO, req.body);
            const updatedDataset = await DatasetRepository.addDatasetProvider(datasetId, provider);
            res.status(201);
            res.json(DatasetDTO.fromDataset(updatedDataset));
        } catch (err: any) {
            if (err instanceof BadRequestException) {
                err.validationErrors?.forEach((error) => {
                    if (!error.constraints) return;
                    Object.values(error.constraints).forEach((message) => logger.error(message));
                });
                next(err);
                return;
            }
            next(new UnknownException('errors.provider_update_error'));
        }
    }
);

// PATCH /dataset/:dataset_id/providers
// Updates the data providers for the dataset
router.patch(
    '/:dataset_id/providers',
    jsonParser,
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const datasetId = res.locals.datasetId;
            const providers = await arrayValidator(DatasetProviderDTO, req.body);
            const updatedDataset = await DatasetRepository.updateDatasetProviders(datasetId, providers);
            res.status(201);
            res.json(DatasetDTO.fromDataset(updatedDataset));
        } catch (err: any) {
            if (err instanceof BadRequestException) {
                err.validationErrors?.forEach((error) => {
                    if (!error.constraints) return;
                    Object.values(error.constraints).forEach((message) => logger.error(message));
                });
                next(err);
                return;
            }
            next(new UnknownException('errors.provider_update_error'));
        }
    }
);

// PATCH /dataset/:dataset_id/topics
// Updates the topics for the dataset
router.patch(
    '/:dataset_id/topics',
    jsonParser,
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const datasetId = res.locals.datasetId;
            const datasetTopics = await dtoValidator(TopicSelectionDTO, req.body);
            const updatedDataset = await DatasetRepository.updateDatasetTopics(datasetId, datasetTopics.topics);
            res.status(201);
            res.json(DatasetDTO.fromDataset(updatedDataset));
        } catch (err: any) {
            if (err instanceof BadRequestException) {
                err.validationErrors?.forEach((error) => {
                    if (!error.constraints) return;
                    Object.values(error.constraints).forEach((message) => logger.error(message));
                });
                next(err);
                return;
            }
            next(new UnknownException('errors.topic_update_error'));
        }
    }
);
