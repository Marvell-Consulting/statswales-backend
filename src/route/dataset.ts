import { randomUUID } from 'crypto';
import { Readable } from 'stream';

import { Request, Response, Router } from 'express';
import multer from 'multer';
import bodyParser from 'body-parser';

import { logger } from '../utils/logger';
import { DimensionCreationDTO } from '../dtos/dimension-creation-dto';
import { ViewDTO, ViewErrDTO, ViewStream } from '../dtos/view-dto';
import { ENGLISH, i18next, WELSH } from '../middleware/translation';
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
import {
    createDimensionsFromValidatedDimensionRequest,
    ValidatedDimensionCreationRequest,
    validateDimensionCreationRequest
} from '../controllers/dimension-processor';
import { User } from '../entities/user';
import { Dataset } from '../entities/dataset';
import { DatasetInfo } from '../entities/dataset-info';
import { Dimension } from '../entities/dimension';
import { Revision } from '../entities/revision';
import { FileImport } from '../entities/file-import';
import { DatasetTitle, FileDescription } from '../dtos/file-list';
import { DatasetDTO } from '../dtos/dataset-dto';
import { FileImportDTO } from '../dtos/file-import-dto';
import { DimensionDTO } from '../dtos/dimension-dto';
import { RevisionDTO } from '../dtos/revision-dto';
import { DataLocation } from '../enums/data-location.enum';

const t = i18next.t;

const jsonParser = bodyParser.json();
const storage = multer.memoryStorage();
const upload = multer({ storage });

const router = Router();
export const datasetRouter = router;

const DATASET = 'Dataset';
const REVISION = 'Revision';
const DIMENSION = 'Dimension';
const IMPORT = 'Import';

function isValidUUID(uuid: string): boolean {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return uuid.length === 36 && uuidRegex.test(uuid);
}

function validateIds(id: string, idType: string, res: Response): boolean {
    if (id === undefined) {
        res.status(400);
        res.json({ message: `${idType} ID is null or undefined` });
        return false;
    }
    if (!isValidUUID(id)) {
        res.status(400);
        res.json({ message: `${idType} ID is not valid` });
        return false;
    }
    return true;
}

async function validateDataset(datasetID: string, res: Response): Promise<Dataset | null> {
    if (!validateIds(datasetID, DATASET, res)) return null;
    const dataset = await Dataset.findOneBy({ id: datasetID });
    if (!dataset) {
        res.status(404);
        res.json({ message: 'Dataset not found.' });
        return null;
    }
    return dataset;
}

async function validateDimension(dimensionID: string, res: Response): Promise<Dimension | null> {
    if (!validateIds(dimensionID, DIMENSION, res)) return null;
    const dimension = await Dimension.findOneBy({ id: dimensionID });
    if (!dimension) {
        res.status(404);
        res.json({ message: 'Dimension not found.' });
        return null;
    }
    return dimension;
}

async function validateRevision(revisionID: string, res: Response): Promise<Revision | null> {
    if (!validateIds(revisionID, REVISION, res)) return null;
    const revision = await Revision.findOneBy({ id: revisionID });
    if (!revision) {
        res.status(404);
        res.json({ message: 'Revision not found.' });
        return null;
    }
    return revision;
}

async function validateImport(importID: string, res: Response): Promise<FileImport | null> {
    if (!validateIds(importID, IMPORT, res)) return null;
    const importObj = await FileImport.findOneBy({ id: importID });
    if (!importObj) {
        res.status(404);
        res.json({ message: 'Import not found.' });
        return null;
    }
    return importObj;
}

function errorDtoGenerator(
    field: string,
    statusCode: number,
    translationString: string,
    datasetID: string | undefined = undefined
): ViewErrDTO {
    return {
        success: false,
        status: statusCode,
        dataset_id: datasetID,
        errors: [
            {
                field,
                message: [
                    {
                        lang: ENGLISH,
                        message: t(translationString, { lng: ENGLISH })
                    },
                    {
                        lang: WELSH,
                        message: t(translationString, { lng: WELSH })
                    }
                ],
                tag: {
                    name: translationString,
                    params: {}
                }
            }
        ]
    };
}

// POST /dataset
// Upload a CSV file to the server
// Returns a JSON object with the a DTO object that represents the dataset
// first revision and the import record.
router.post('/', upload.single('csv'), async (req: Request, res: Response) => {
    if (!req.file) {
        res.status(400);
        res.json(errorDtoGenerator('csv', 400, 'errors.no_csv_data'));
        return;
    }

    const lang: string = req.body?.language || req.i18n.language;
    const title: string = req.body?.title;
    if (!title) {
        res.status(400);
        res.json(errorDtoGenerator('title', 400, 'errors.no_title'));
        return;
    }
    let importRecord: FileImport;
    try {
        importRecord = await uploadCSVBufferToBlobStorage(req.file.buffer, req.file?.mimetype);
    } catch (err) {
        logger.error(`An error occurred trying to upload the file with the following error: ${err}`);
        res.status(500);
        res.json({ message: 'Error uploading file' });
        return;
    }

    // Everything looks good so far, let's create the dataset and revision records
    const dataset = new Dataset();
    dataset.id = randomUUID().toLowerCase();
    dataset.createdAt = new Date();

    // req.user is set from the JWT token in the passport-auth middleware
    const user = req.user as User;

    if (!user) {
        throw new Error('Test user not found');
    }

    dataset.createdBy = Promise.resolve(user);
    const datasetInfo = new DatasetInfo();
    datasetInfo.language = lang;
    datasetInfo.title = title;
    datasetInfo.dataset = Promise.resolve(dataset);
    dataset.datasetInfo = Promise.resolve([datasetInfo]);
    const revision = new Revision();
    revision.dataset = Promise.resolve(dataset);
    revision.revisionIndex = 1;
    revision.createdAt = new Date();
    revision.createdBy = Promise.resolve(user);
    dataset.revisions = Promise.resolve([revision]);
    importRecord.revision = Promise.resolve(revision);
    revision.imports = Promise.resolve([importRecord]);
    await dataset.save();

    const uploadDTO = await DatasetDTO.fromDatasetWithRevisionsAndImports(dataset);
    res.status(201);
    res.json(uploadDTO);
});

// GET /dataset/
// Returns a list of all datasets
// Returns a JSON object with a list of all datasets
// and their titles
router.get('/', async (req: Request, res: Response) => {
    const datasets = await Dataset.find();
    const fileList: FileDescription[] = [];
    for (const dataset of datasets) {
        const titles: DatasetTitle[] = [];
        const datasetInfo = await dataset.datasetInfo;
        for (const info of datasetInfo) {
            titles.push({
                title: info.title,
                language: info.language
            });
        }
        fileList.push({
            titles,
            dataset_id: dataset.id
        });
    }
    res.json({ datasets: fileList });
});

// GET /dataset/active
// Returns a list of all active datasets e.g. ones with imports
// Returns a JSON object with a list of all datasets
// and their titles
router.get('/active', async (req: Request, res: Response) => {
    const datasets = await Dataset.find();
    const fileList: FileDescription[] = [];
    for (const dataset of datasets) {
        const titles: DatasetTitle[] = [];
        const revisions = await dataset.revisions;
        if (!revisions) {
            continue;
        }
        const latestRevision = revisions.pop();
        if (!latestRevision) {
            continue;
        }
        const fileImports: FileImport[] = await latestRevision.imports;
        if (!fileImports) {
            continue;
        }
        if (fileImports?.length === 0) {
            continue;
        }
        const datasetInfo = await dataset.datasetInfo;
        for (const info of datasetInfo) {
            titles.push({
                title: info.title,
                language: info.language
            });
        }
        fileList.push({
            titles,
            dataset_id: dataset.id
        });
    }
    res.json({ filelist: fileList });
});

// GET /api/dataset/:dataset_id
// Returns a shallow dto of the dataset with the given ID
// Shallow gives the revisions and dimensions of the dataset only
router.get('/:dataset_id', async (req: Request, res: Response) => {
    const datasetID: string = req.params.dataset_id.toLowerCase();
    const dataset = await validateDataset(datasetID, res);
    if (!dataset) return;
    const dto = await DatasetDTO.fromDatasetComplete(dataset);
    res.json(dto);
});

// DELETE /api/dataset/:dataset_id
// Returns a shallow dto of the dataset with the given ID
// Shallow gives the revisions and dimensions of the dataset only
router.delete('/:dataset_id', async (req: Request, res: Response) => {
    const datasetID: string = req.params.dataset_id.toLowerCase();
    const dataset = await validateDataset(datasetID, res);
    if (!dataset) return;
    logger.warn('Deleting dataset with ID:', datasetID);
    await dataset.remove();
    res.status(204);
    res.end();
});

// GET /api/dataset/:dataset_id/view
// Returns a view of the data file attached to the import
router.get('/:dataset_id/view', async (req: Request, res: Response) => {
    const datasetID: string = req.params.dataset_id.toLowerCase();
    const dataset = await validateDataset(datasetID, res);
    if (!dataset) return;
    const latestRevision = (await dataset.revisions).pop();
    if (!latestRevision) {
        logger.error('Unable to find the last revision');
        res.status(500);
        res.json({ message: 'No revision found for dataset' });
        return;
    }
    const latestImport = (await latestRevision.imports).pop();
    if (!latestImport) {
        logger.error('Unable to find the last import record');
        res.status(500);
        res.json({ message: 'No import record found for dataset' });
        return;
    }
    const page_number_str: string = req.query.page_number || req.body?.page_number;
    const page_size_str: string = req.query.page_size || req.body?.page_size;
    const page_number: number = Number.parseInt(page_number_str, 10) || 1;
    const page_size: number = Number.parseInt(page_size_str, 10) || DEFAULT_PAGE_SIZE;
    let processedCSV: ViewErrDTO | ViewDTO;
    if (latestImport.location === DataLocation.BlobStorage) {
        processedCSV = await processCSVFromBlobStorage(dataset, latestImport, page_number, page_size);
    } else if (latestImport.location === DataLocation.DataLake) {
        processedCSV = await processCSVFromDatalake(dataset, latestImport, page_number, page_size);
    } else {
        res.status(500);
        res.json({ message: 'Import location not supported.' });
        return;
    }
    if (!processedCSV.success) {
        res.status(500);
    }
    res.json(processedCSV);
});

// GET /api/dataset/:dataset_id/dimension/id/:dimension_id
// Returns details of a dimension with its sources and imports
router.get('/:dataset_id/dimension/by-id/:dimension_id', async (req: Request, res: Response) => {
    const datasetID: string = req.params.dataset_id.toLowerCase();
    const dataset = await validateDataset(datasetID, res);
    if (!dataset) return;
    const dimensionID: string = req.params.dimension_id;
    const dimension = await validateDimension(dimensionID, res);
    if (!dimension) return;
    const dto = await DimensionDTO.fromDimension(dimension);
    res.json(dto);
});

// GET /api/dataset/:dataset_id/revision/id/:revision_id
// Returns details of a revision with its imports
router.get('/:dataset_id/revision/by-id/:revision_id', async (req: Request, res: Response) => {
    const datasetID: string = req.params.dataset_id.toLowerCase();
    const dataset = await validateDataset(datasetID, res);
    if (!dataset) return;
    const revisionID: string = req.params.revision_id;
    const revision = await validateRevision(revisionID, res);
    if (!revision) return;
    const dto = await RevisionDTO.fromRevision(revision);
    res.json(dto);
});

// POST /api/dataset/:dataset_id/revision/id/:revision_id/import
// Creates a new import on a revision.  This typically only occurs when a user
// decides the file they uploaded wasn't correct.
router.post(
    '/:dataset_id/revision/by-id/:revision_id/import',
    upload.single('csv'),
    async (req: Request, res: Response) => {
        const datasetID: string = req.params.dataset_id.toLowerCase();
        const dataset = await validateDataset(datasetID, res);
        if (!dataset) return;
        const revisionID: string = req.params.revision_id;
        const revision = await validateRevision(revisionID, res);
        if (!revision) return;
        if (!req.file) {
            res.status(400);
            res.json(errorDtoGenerator('csv', 400, 'errors.no_csv_data'));
            return;
        }
        let importRecord: FileImport;
        try {
            importRecord = await uploadCSVBufferToBlobStorage(req.file.buffer, req.file?.mimetype);
        } catch (err) {
            logger.error(`An error occurred trying to upload the file with the following error: ${err}`);
            res.status(500);
            res.json({ message: 'Error uploading file' });
            return;
        }
        importRecord.revision = Promise.resolve(revision);
        await importRecord.save();
        const updatedDataset = await Dataset.findOneBy({ id: datasetID });
        if (!updatedDataset) return;

        const uploadDTO = await DatasetDTO.fromDatasetWithRevisionsAndImports(updatedDataset);
        res.status(201);
        res.json(uploadDTO);
    }
);

// GET /api/dataset/:dataset_id/revision/id/:revision_id/import/by-id/:import_id
// Returns details of an import with its sources
router.get('/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id', async (req: Request, res: Response) => {
    const datasetID: string = req.params.dataset_id.toLowerCase();
    const dataset = await validateDataset(datasetID, res);
    if (!dataset) return;
    const revisionID: string = req.params.revision_id;
    const revision = await validateRevision(revisionID, res);
    if (!revision) return;
    const importID: string = req.params.import_id;
    const importRecord = await validateImport(importID, res);
    if (!importRecord) return;
    const dto = await FileImportDTO.fromImport(importRecord);
    res.json(dto);
});

// GET /api/dataset/:dataset_id/revision/id/:revision_id/import/id/:import_id/preview
// Returns a view of the data file attached to the import
router.get(
    '/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id/preview',
    async (req: Request, res: Response) => {
        const datasetID: string = req.params.dataset_id.toLowerCase();
        const dataset = await validateDataset(datasetID, res);
        if (!dataset) return;
        const revisionID: string = req.params.revision_id;
        const revision = await validateRevision(revisionID, res);
        if (!revision) return;
        const importID: string = req.params.import_id;
        const importRecord = await validateImport(importID, res);
        if (!importRecord) return;
        const page_number_str: string = req.query.page_number || req.body?.page_number;
        const page_size_str: string = req.query.page_size || req.body?.page_size;
        const page_number: number = Number.parseInt(page_number_str, 10) || 1;
        const page_size: number = Number.parseInt(page_size_str, 10) || DEFAULT_PAGE_SIZE;
        let processedCSV: ViewErrDTO | ViewDTO;
        if (importRecord.location === DataLocation.BlobStorage) {
            processedCSV = await processCSVFromBlobStorage(dataset, importRecord, page_number, page_size);
        } else if (importRecord.location === DataLocation.DataLake) {
            processedCSV = await processCSVFromDatalake(dataset, importRecord, page_number, page_size);
        } else {
            res.status(500);
            res.json({ message: 'Import location not supported.' });
            return;
        }
        if (!processedCSV.success) {
            const processErr = processedCSV as ViewErrDTO;
            res.status(processErr.status);
        }
        res.json(processedCSV);
    }
);

// GET /api/dataset/:dataset_id/revision/id/:revision_id/import/id/:import_id/raw
// Returns the original uploaded file back to the client
router.get(
    '/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id/raw',
    async (req: Request, res: Response) => {
        const datasetID: string = req.params.dataset_id.toLowerCase();
        const dataset = await validateDataset(datasetID, res);
        if (!dataset) return;
        const revisionID: string = req.params.revision_id;
        const revision = await validateRevision(revisionID, res);
        if (!revision) return;
        const importID: string = req.params.import_id;
        const importRecord = await validateImport(importID, res);
        if (!importRecord) return;
        let viewStream: ViewErrDTO | ViewStream;
        if (importRecord.location === DataLocation.BlobStorage) {
            viewStream = await getFileFromBlobStorage(dataset, importRecord);
        } else if (importRecord.location === DataLocation.DataLake) {
            viewStream = await getFileFromDataLake(dataset, importRecord);
        } else {
            res.status(500);
            res.json({ message: 'Import location not supported.' });
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

// PATCH /api/dataset/:dataset_id/revision/id/:revision_id/import/id/:import_id/confirm
// Moves the file from temporary blob storage to datalake and creates sources
// returns a JSON object with the current state of the revision including the import
// and sources created from the import.
router.patch(
    '/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id/confirm',
    async (req: Request, res: Response) => {
        const datasetID: string = req.params.dataset_id.toLowerCase();
        const dataset = await validateDataset(datasetID, res);
        if (!dataset) return;
        const revisionID: string = req.params.revision_id;
        const revision = await validateRevision(revisionID, res);
        if (!revision) return;
        const importID: string = req.params.import_id;
        const importRecord = await validateImport(importID, res);
        if (!importRecord) return;
        if (importRecord.location === DataLocation.DataLake) {
            const fileImportDto = await FileImportDTO.fromImportWithSources(importRecord);
            res.status(200);
            res.json(fileImportDto);
            return;
        }
        try {
            importRecord.location = DataLocation.DataLake;
            await moveFileToDataLake(importRecord);
            await importRecord.save();
        } catch (err) {
            logger.error(`An error occurred trying to move the file with the following error: ${err}`);
            res.status(500);
            res.json({ message: 'Error moving file from temporary blob storage to Data Lake.  Please try again.' });
            return;
        }
        try {
            const fileImportDto = await createSources(importRecord);
            res.status(200);
            res.json(fileImportDto);
        } catch (err) {
            logger.error(`An error occurred trying to create the sources with the following error: ${err}`);
            res.status(500);
            res.json({ message: 'Error creating sources from the uploaded file.  Please try again.' });
        }
    }
);

// DELETE /:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id
// Removes the import record and associated file from BlobStorage clearing the way
// for the user to upload a new file for the dataset.
router.delete(
    '/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id',
    async (req: Request, res: Response) => {
        const datasetID: string = req.params.dataset_id.toLowerCase();
        const dataset = await validateDataset(datasetID, res);
        if (!dataset) return;
        const revisionID: string = req.params.revision_id;
        const revision = await validateRevision(revisionID, res);
        if (!revision) return;
        const importID: string = req.params.import_id;
        const importRecord = await validateImport(importID, res);
        if (!importRecord) return;
        try {
            if (importRecord.location === DataLocation.DATA_LAKE) {
                logger.warn('User has requested to remove a fact table from the datalake.  This is unusual.');
                await removeFileFromDatalake(importRecord);
                const sources = await importRecord.sources;
                sources.forEach((source) => source.remove());
            } else {
                await removeTempfileFromBlobStorage(importRecord);
            }
        } catch (err) {
            logger.error(`An error occurred trying to remove the file with the following error: ${err}`);
            res.status(500);
            res.json({ message: 'Error removing file from temporary blob storage.  Please try again.' });
            return;
        }
        await importRecord.remove();
        const updatedDataset = await Dataset.findOneBy({ id: datasetID });
        if (!updatedDataset) return;
        const dto = await DatasetDTO.fromDatasetWithRevisionsAndImports(updatedDataset);
        res.status(200);
        res.json(dto);
    }
);

// POST /api/dataset/:dataset_id/revision/id/:revision_id/import/id/:import_id/create
// Creates the dimensions from relating to the import based on information provided
// from the sources and the user.
// Body should contain a JSON object with the following structure:
// {
//     "sources": [
//         {
//             "id": "source_id",
//             "action": "create" || "append" || "truncate-then-load" || "ignore",
//             "type": "DataValue || "Dimension" || "Footnotes" || "Ignore"
//         }
//     ]
// }
// Notes: There can only be one object with a type of "dataValue" and one object with a type of "footnotes"
// Returns a JSON object with the current state of the dataset including the dimensions created.
router.patch(
    '/:dataset_id/revision/by-id/:revision_id/import/by-id/:import_id/sources',
    jsonParser,
    async (req: Request, res: Response) => {
        const datasetID: string = req.params.dataset_id.toLowerCase();
        const dataset = await validateDataset(datasetID, res);
        if (!dataset) return;
        const revisionID: string = req.params.revision_id;
        const revision = await validateRevision(revisionID, res);
        if (!revision) return;
        const importID: string = req.params.import_id;
        const importRecord = await validateImport(importID, res);
        if (!importRecord) return;
        const dimensionCreationDTO = req.body as DimensionCreationDTO[];
        logger.info(`Received patch request with the following payload: ${JSON.stringify(dimensionCreationDTO)}`);
        let validatedDTO: ValidatedDimensionCreationRequest;
        try {
            validatedDTO = await validateDimensionCreationRequest(dimensionCreationDTO);
        } catch (err) {
            logger.error(`An error occurred trying to process the user supplied JSON: ${err}`);
            res.status(400);
            res.json({ message: `Error processing the supplied JSON with the following error ${err}` });
            return;
        }
        await createDimensionsFromValidatedDimensionRequest(revision, validatedDTO);
        const dto = await DatasetDTO.fromDatasetComplete(await Dataset.findOneByOrFail({ id: datasetID }));
        res.status(200);
        res.json(dto);
    }
);
