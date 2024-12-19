import 'reflect-metadata';
import { Readable } from 'node:stream';
import fs from 'fs';

import express, { NextFunction, Request, Response, Router } from 'express';
import multer from 'multer';
import { FieldValidationError } from 'express-validator';
import { FindOptionsRelations } from 'typeorm';
import { t } from 'i18next';
import { isBefore, isValid } from 'date-fns';
import tmp from 'tmp';

import { logger } from '../utils/logger';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import {
    DEFAULT_PAGE_SIZE,
    getCSVPreview,
    getFactTableColumnPreview,
    removeFileFromDataLake,
    uploadCSV
} from '../controllers/csv-processor';
import {
    createDimensionsFromSourceAssignment,
    getDimensionPreview,
    validateDateTypeDimension,
    validateSourceAssignment
} from '../controllers/dimension-processor';
import { User } from '../entities/user/user';
import { DatasetDTO } from '../dtos/dataset-dto';
import { DatasetInfoDTO } from '../dtos/dataset-info-dto';
import { FactTableDTO } from '../dtos/fact-table-dto';
import { Locale } from '../enums/locale';
import { DatasetRepository } from '../repositories/dataset';
import { datasetIdValidator, factTableIdValidator, hasError, revisionIdValidator, titleValidator } from '../validators';
import { NotFoundException } from '../exceptions/not-found.exception';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { UnknownException } from '../exceptions/unknown.exception';
import { getLatestImport, getLatestRevision } from '../utils/latest';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionDTO } from '../dtos/dimension-dto';
import { TasklistStateDTO } from '../dtos/tasklist-state-dto';
import { Revision } from '../entities/dataset/revision';
import { RevisionDTO } from '../dtos/revision-dto';
import { SourceAssignmentException } from '../exceptions/source-assignment.exception';
import { arrayValidator, dtoValidator } from '../validators/dto-validator';
import { RevisionRepository } from '../repositories/revision';
import { FactTableRepository } from '../repositories/fact-table';
import { Dataset } from '../entities/dataset/dataset';
import { DatasetProviderDTO } from '../dtos/dataset-provider-dto';
import { TopicSelectionDTO } from '../dtos/topic-selection-dto';
import { DataLakeService } from '../services/datalake';
import { FactTable } from '../entities/dataset/fact-table';
import { DimensionPatchDto } from '../dtos/dimension-partch-dto';
import { DimensionType } from '../enums/dimension-type';
import { LookupTable } from '../entities/dataset/lookup-table';
import { DimensionInfoDTO } from '../dtos/dimension-info-dto';
import { DimensionInfo } from '../entities/dataset/dimension-info';
import { TeamSelectionDTO } from '../dtos/team-selection-dto';
import { validateLookupTable } from '../controllers/lookup-table-handler';
import { FactTableAction } from '../enums/fact-table-action';
import { createBaseCube, getCubePreview, outputCube } from '../controllers/cube-handler';
import { DuckdbOutputType } from '../enums/duckdb-outputs';

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
    const dataLakeService = new DataLakeService();
    await dataLakeService.deleteDirectoryAndFiles(req.params.dataset_id);
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

        let fileImport: FactTable;
        logger.debug('Uploading dataset to datalake');
        try {
            fileImport = await uploadCSV(
                req.file.buffer,
                req.file?.mimetype,
                req.file?.originalname,
                res.locals.datasetId
            );
            fileImport.action = FactTableAction.Add;
        } catch (err) {
            logger.error(`An error occurred trying to upload the file: ${err}`);
            next(new UnknownException('errors.upload_error'));
            return;
        }

        logger.debug('Updating dataset records');
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
    const processedCSV = await getCSVPreview(dataset, latestImport, page_number, page_size);

    if ((processedCSV as ViewErrDTO).errors) {
        res.status(500);
    }
    res.json(processedCSV);
});

// GET /dataset/:dataset_id/cube
// Returns the latest revision of the dataset as a DuckDB File
router.get('/:dataset_id/cube', loadDataset(), async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const latestRevision = getLatestRevision(dataset);
    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, latestRevision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const fileBuffer = Buffer.from(fs.readFileSync(cubeFile));
    logger.info(`Sending original cube file (size: ${fileBuffer.length}) from: ${cubeFile}`);
    res.writeHead(200, {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'Content-Type': 'application/octet-stream',
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'Content-disposition': `attachment;filename=${dataset.id}.duckdb`,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'Content-Length': fileBuffer.length
    });
    res.end(fileBuffer);
});

// GET /dataset/:dataset_id/cube/json
// Returns a JSON file representation of the default view of the cube
router.get('/:dataset_id/cube/json', loadDataset(), async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const lang = req.language.split('-')[0];
    const latestRevision = getLatestRevision(dataset);
    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            logger.info('Creating fresh cube file.');
            cubeFile = await createBaseCube(dataset, latestRevision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Json);
    fs.unlinkSync(cubeFile);
    const downloadStream = fs.createReadStream(downloadFile);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(200, { 'Content-Type': '\tapplication/json' });
    downloadStream.pipe(res);

    // Handle errors in the file stream
    downloadStream.on('error', (err) => {
        logger.error('File stream error:', err);
        // eslint-disable-next-line @typescript-eslint/naming-convention
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        fs.unlinkSync(downloadFile);
        res.end('Server Error');
    });

    // Optionally listen for the end of the stream
    downloadStream.on('end', () => {
        fs.unlinkSync(downloadFile);
        logger.debug('File stream ended');
    });
});

// GET /dataset/:dataset_id/cube/csv
// Returns a CSV file representation of the default view of the cube
router.get('/:dataset_id/cube/csv', loadDataset(), async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const lang = req.language.split('-')[0];
    const latestRevision = getLatestRevision(dataset);
    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, latestRevision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Csv);
    fs.unlinkSync(cubeFile);
    const downloadStream = fs.createReadStream(downloadFile);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(200, { 'Content-Type': '\ttext/csv' });
    downloadStream.pipe(res);

    // Handle errors in the file stream
    downloadStream.on('error', (err) => {
        logger.error('File stream error:', err);
        // eslint-disable-next-line @typescript-eslint/naming-convention
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        fs.unlinkSync(downloadFile);
        res.end('Server Error');
    });

    // Optionally listen for the end of the stream
    downloadStream.on('end', () => {
        fs.unlinkSync(downloadFile);
        logger.debug('File stream ended');
    });
});

// GET /dataset/:dataset_id/cube/parquet
// Returns a CSV file representation of the default view of the cube
router.get('/:dataset_id/cube/parquet', loadDataset(), async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const lang = req.language.split('-')[0];
    const latestRevision = getLatestRevision(dataset);
    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, latestRevision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Parquet);
    fs.unlinkSync(cubeFile);
    const downloadStream = fs.createReadStream(downloadFile);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(200, { 'Content-Type': '\tapplication/vnd.apache.parquet' });
    downloadStream.pipe(res);

    // Handle errors in the file stream
    downloadStream.on('error', (err) => {
        logger.error('File stream error:', err);
        // eslint-disable-next-line @typescript-eslint/naming-convention
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        fs.unlinkSync(downloadFile);
        res.end('Server Error');
    });

    // Optionally listen for the end of the stream
    downloadStream.on('end', () => {
        fs.unlinkSync(downloadFile);
        logger.debug('File stream ended');
    });
});

// GET /dataset/:dataset_id/cube/excel
// Returns a CSV file representation of the default view of the cube
router.get('/:dataset_id/cube/excel', loadDataset(), async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const lang = req.language.split('-')[0];
    const latestRevision = getLatestRevision(dataset);
    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, latestRevision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Excel);
    logger.info(`Cube file located at: ${cubeFile}`);
    // fs.unlinkSync(cubeFile);
    const downloadStream = fs.createReadStream(downloadFile);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(200, { 'Content-Type': '\tapplication/vnd.ms-excel' });
    downloadStream.pipe(res);

    // Handle errors in the file stream
    downloadStream.on('error', (err) => {
        logger.error('File stream error:', err);
        // eslint-disable-next-line @typescript-eslint/naming-convention
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        fs.unlinkSync(downloadFile);
        res.end('Server Error');
    });

    // Optionally listen for the end of the stream
    downloadStream.on('end', () => {
        fs.unlinkSync(downloadFile);
        logger.debug('File stream ended');
    });
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

// GET /dataset/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id
// Returns details of an fact-table with its sources
router.get(
    '/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id',
    loadFactTable,
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const fileImport = res.locals.factTable;
            const dto = FactTableDTO.fromFactTable(fileImport);
            res.json(dto);
        } catch (err) {
            next(new UnknownException());
        }
    }
);

// GET /dataset/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id/preview
// Returns a view of the data file attached to the fact-table
router.get(
    '/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id/preview',
    loadFactTable,
    async (req: Request, res: Response, next: NextFunction) => {
        const { dataset, factTable } = res.locals;

        const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
        const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;

        const processedCSV = await getCSVPreview(dataset, factTable, page_number, page_size);

        if ((processedCSV as ViewErrDTO).errors) {
            const processErr = processedCSV as ViewErrDTO;
            res.status(processErr.status);
        }

        res.json(processedCSV);
    }
);

router.get(
    '/:dataset_id/revision/by-id/:revision_id/preview',
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        const dataset = res.locals.dataset;
        const revision = dataset.revisions.find((revision: Revision) => revision.id === req.params.revision_id);
        const lang = req.language;

        const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
        const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;

        let cubeFile: string;
        if (revision.onlineCubeFilename) {
            logger.debug('Loading cube from datalake for preview');
            const datalakeService = new DataLakeService();
            cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
            const cubeBuffer = await datalakeService.getFileBuffer(revision.onlineCubeFilename, dataset.id);
            fs.writeFileSync(cubeFile, cubeBuffer);
        } else {
            logger.debug('Creating fresh cube for preview');
            try {
                cubeFile = await createBaseCube(dataset, revision);
            } catch (error) {
                logger.error(`Something went wrong trying to create the cube with the error: ${error}`);
                next(new UnknownException('errors.cube_create_error'));
                return;
            }
        }
        const cubePreview = await getCubePreview(cubeFile, lang, dataset, page_number, page_size);
        fs.unlinkSync(cubeFile);
        if ((cubePreview as ViewErrDTO).errors) {
            const processErr = cubePreview as ViewErrDTO;
            res.status(processErr.status);
        }

        res.json(cubePreview);
    }
);

// PATCH /dataset/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id/confirm
// Moves the file from temporary blob storage to datalake and creates sources
// returns a JSON object with the current state of the revision including the fact-table
// and sources created from the fact-table.
router.patch(
    '/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id/confirm',
    loadFactTable,
    async (req: Request, res: Response, next: NextFunction) => {
        const factTable: FactTable = res.locals.factTable;
        const dto = FactTableDTO.fromFactTable(factTable);
        return res.json(dto);
    }
);

// GET /dataset/:dataset_id/revision/id/:revision_id/fact-table/id/:fact_table_id/raw
// Returns the original uploaded file back to the client
router.get(
    '/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id/raw',
    loadFactTable,
    async (req: Request, res: Response, next: NextFunction) => {
        const { dataset, factTable } = res.locals;
        logger.info('User requested to down files...');
        const dataLakeService = new DataLakeService();
        let readable: Readable;
        try {
            readable = await dataLakeService.getFileStream(factTable.filename, dataset.id);
        } catch (error) {
            res.status(500);
            res.json({
                status: 500,
                errors: [
                    {
                        field: 'csv',
                        message: [
                            {
                                lang: Locale.English,
                                message: t('errors.download_from_datalake', { lng: Locale.English })
                            },
                            {
                                lang: Locale.Welsh,
                                message: t('errors.download_from_datalake', { lng: Locale.Welsh })
                            }
                        ],
                        tag: { name: 'errors.download_from_datalake', params: {} }
                    }
                ],
                dataset_id: dataset.id
            });
            return;
        }
        // eslint-disable-next-line @typescript-eslint/naming-convention
        res.writeHead(200, { 'Content-Type': 'text/csv' });
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
    async (req: Request, res: Response, next: NextFunction) => {
        const { dataset, revision, factTable } = res.locals;
        const sourceAssignment = req.body;
        try {
            const validatedSourceAssignment = validateSourceAssignment(factTable, sourceAssignment);
            await createDimensionsFromSourceAssignment(dataset, factTable, validatedSourceAssignment);
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

// DELETE /dataset/:dataset_id/dimension/id/:dimension_id/reset
// Resets the dimensions type back to "Raw" and removes the extractor
router.delete(
    '/:dataset_id/dimension/by-id/:dimension_id/reset',
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        const dataset = res.locals.dataset;
        const dimension = dataset.dimensions.find((dim: Dimension) => dim.id === req.params.dimension_id);

        if (!dimension) {
            next(new NotFoundException('errors.dimension_id_invalid'));
            return;
        }
        dimension.type = DimensionType.Raw;
        dimension.extractor = null;
        if (dimension.lookuptable) {
            const lookupTable: LookupTable = dimension.lookupTable;
            await lookupTable.remove();
            dimension.lookuptable = null;
        }
        await dimension.save();
        const updatedDimension = await Dimension.findOneByOrFail({ id: dimension.id });
        res.status(202);
        res.json(DimensionDTO.fromDimension(updatedDimension));
    }
);

// GET /dataset/:dataset_id/dimension/id/:dimension_id/preview
// Returns details of a dimension and a preview of the data
// It should be noted that this returns the raw values in the
// preview as opposed to view which returns interpreted values.
router.get(
    '/:dataset_id/dimension/by-id/:dimension_id/preview',
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        const dataset = res.locals.dataset;
        const dimension: Dimension = dataset.dimensions.find((dim: Dimension) => dim.id === req.params.dimension_id);
        const factTable = getLatestRevision(dataset)?.factTables[0];
        if (!dimension) {
            next(new NotFoundException('errors.dimension_id_invalid'));
            return;
        }
        if (!factTable) {
            next(new NotFoundException('errors.fact_table_invalid'));
            return;
        }
        try {
            let preview: ViewDTO | ViewErrDTO;
            if (dimension.type === DimensionType.Raw) {
                preview = await getFactTableColumnPreview(dataset, factTable, dimension.factTableColumn);
            } else {
                preview = await getDimensionPreview(dataset, dimension, factTable);
            }
            if ((preview as ViewErrDTO).errors) {
                res.status(500);
                res.json(preview);
            }
            res.status(200);
            res.json(preview);
        } catch (err) {
            logger.error(
                `Something went wrong trying to get a preview of the dimension with the following error: ${err}`
            );
            res.status(500);
            res.json({ message: 'Something went wrong trying to generate a preview of the dimension' });
        }
    }
);

// POST /:dataset_id/dimension/by-id/:dimension_id/lookup
// Attaches a lookup table to do a dimension and validates
// the lookup table.
router.post(
    '/:dataset_id/dimension/by-id/:dimension_id/lookup',
    upload.single('csv'),
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        if (!req.file) {
            next(new BadRequestException('errors.upload.no_csv'));
            return;
        }
        const dataset: Dataset = res.locals.dataset;
        const dimension: Dimension | undefined = dataset.dimensions.find(
            (dim: Dimension) => dim.id === req.params.dimension_id
        );
        if (!dimension) {
            next(new NotFoundException('errors.dimension_id_invalid'));
            return;
        }
        const factTable = getLatestRevision(dataset)?.factTables[0];
        if (!factTable) {
            next(new NotFoundException('errors.fact_table_invalid'));
            return;
        }
        let fileImport: FactTable;
        try {
            fileImport = await uploadCSV(
                req.file.buffer,
                req.file?.mimetype,
                req.file?.originalname,
                res.locals.datasetId
            );
        } catch (err) {
            logger.error(`An error occurred trying to upload the file: ${err}`);
            next(new UnknownException('errors.upload_error'));
            return;
        }

        if (req.body.joinColumn) {
            dimension.joinColumn = req.body.joinColumn;
        }

        try {
            const result = await validateLookupTable(
                fileImport,
                factTable,
                dataset,
                dimension,
                req.file.buffer,
                req.body.join_column
            );
            if ((result as ViewErrDTO).status) {
                const error = result as ViewErrDTO;
                res.status(error.status);
                res.json(result);
                return;
            }
            res.status(200);
            res.json(result);
        } catch (err) {
            logger.error(`An error occurred trying to handle the lookup table: ${err}`);
            next(new UnknownException('errors.upload_error'));
        }
    }
);

// PATCH /dataset/:dataset_id/dimension/id/:dimension_id/
// Takes a patch request and validates the request against the fact table
// If it fails it sends back an error
router.patch(
    '/:dataset_id/dimension/by-id/:dimension_id',
    jsonParser,
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        const dataset = res.locals.dataset;
        const dimension = dataset.dimensions.find((dim: Dimension) => dim.id === req.params.dimension_id);
        const factTable = getLatestRevision(dataset)?.factTables[0];
        if (!dimension) {
            next(new NotFoundException('errors.dimension_id_invalid'));
            return;
        }
        if (!factTable) {
            next(new NotFoundException('errors.fact_table_invalid'));
            return;
        }
        const dimensionPatchRequest = req.body as DimensionPatchDto;
        let preview: ViewDTO | ViewErrDTO;
        try {
            switch (dimensionPatchRequest.dimension_type) {
                case DimensionType.TimePeriod:
                case DimensionType.TimePoint:
                    preview = await validateDateTypeDimension(dimensionPatchRequest, dataset, dimension, factTable);
                    break;
                default:
                    throw new Error('Not Implemented Yet!');
            }
        } catch (error) {
            logger.error(`Something went wrong trying to validate the dimension with the following error: ${error}`);
            res.status(500);
            res.json({ message: 'Unable to validate or match dimension against patch' });
            return;
        }

        if ((preview as ViewErrDTO).errors) {
            res.status((preview as ViewErrDTO).status);
            res.json(preview);
            return;
        }
        res.status(200);
        res.json(preview);
    }
);

// PATCH /:dataset_id/dimension/by-id/:dimension_id/info
// Updates the dimension info
router.patch(
    '/:dataset_id/dimension/by-id/:dimension_id/info',
    jsonParser,
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        const dataset = res.locals.dataset;
        const dimension: Dimension = dataset.dimensions.find((dim: Dimension) => dim.id === req.params.dimension_id);
        const updatedInfo = req.body as DimensionInfoDTO;
        let info = dimension.dimensionInfo.find((info) => info.language === updatedInfo.language);
        if (!info) {
            info = new DimensionInfo();
            info.dimension = dimension;
            info.language = updatedInfo.language;
        }
        if (updatedInfo.name) {
            info.name = updatedInfo.name;
        }
        if (updatedInfo.notes) {
            info.notes = updatedInfo.notes;
        }
        await info.save();
        const updatedDimension = await Dimension.findOneByOrFail({ id: dimension.id });
        res.status(202);
        res.json(DimensionDTO.fromDimension(updatedDimension));
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

// POST /dataset/:dataset_id/revision/id/:revision_id/fact-table
// Creates a new import on a revision.  This typically only occurs when a user
// decides the file they uploaded wasn't correct.
router.post(
    '/:dataset_id/revision/by-id/:revision_id/fact-table',
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

        let fileImport: FactTable;

        try {
            fileImport = await uploadCSV(req.file.buffer, req.file?.mimetype, req.file?.originalname, dataset.id);
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

// DELETE /:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id
// Removes the import record and associated file from BlobStorage clearing the way
// for the user to upload a new file for the dataset.
router.delete(
    '/:dataset_id/revision/by-id/:revision_id/fact-table/by-id/:fact_table_id',
    loadFactTable,
    async (req: Request, res: Response, next: NextFunction) => {
        const { dataset, factTable } = res.locals;
        try {
            logger.warn('User has requested to remove a fact table from the datalake');
            await removeFileFromDataLake(factTable, dataset);
            await factTable.remove();
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

// PATCH /dataset/:dataset_id/team
// Updates the team for the dataset
router.patch(
    '/:dataset_id/team',
    jsonParser,
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        try {
            const datasetId = res.locals.datasetId;
            const datasetTeam = await dtoValidator(TeamSelectionDTO, req.body);
            const updatedDataset = await DatasetRepository.updateDatasetTeam(datasetId, datasetTeam.team_id);
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

// PATCH /dataset/:dataset_id/revision/by-id/:revision_id/publish-at
// Updates the publishAt date for the specified revision
router.patch(
    '/:dataset_id/revision/by-id/:revision_id/publish-at',
    jsonParser,
    loadDataset(),
    async (req: Request, res: Response, next: NextFunction) => {
        const dataset = res.locals.dataset;
        const revision = dataset.revisions.find((revision: Revision) => revision.id === req.params.revision_id);

        if (!revision) {
            next(new NotFoundException('errors.revision_id_invalid'));
            return;
        }

        if (revision.approvedAt) {
            next(new BadRequestException('errors.revision_already_approved'));
            return;
        }

        try {
            const publishAt = req.body.publish_at;

            if (!publishAt || !isValid(new Date(publishAt))) {
                next(new BadRequestException('errors.publish_at.invalid'));
                return;
            }

            if (isBefore(publishAt, new Date())) {
                next(new BadRequestException('errors.publish_at.in_past'));
                return;
            }

            await RevisionRepository.updatePublishDate(revision, publishAt);
            const updatedDataset = await DatasetRepository.getById(req.params.dataset_id);

            res.status(201);
            res.json(DatasetDTO.fromDataset(updatedDataset));
        } catch (err) {
            next(new UnknownException());
        }
    }
);
