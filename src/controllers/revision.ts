import fs from 'fs';
import { Readable } from 'node:stream';

import { NextFunction, Request, Response } from 'express';
import tmp from 'tmp';
import { t } from 'i18next';
import { isBefore, isValid } from 'date-fns';

import { FactTableDTO } from '../dtos/fact-table-dto';
import { UnknownException } from '../exceptions/unknown.exception';
import { ViewErrDTO } from '../dtos/view-dto';
import { Revision } from '../entities/dataset/revision';
import { logger } from '../utils/logger';
import { DataLakeService } from '../services/datalake';
import { FactTable } from '../entities/dataset/fact-table';
import { Locale } from '../enums/locale';
import { DatasetRepository } from '../repositories/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { SourceAssignmentException } from '../exceptions/source-assignment.exception';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { NotFoundException } from '../exceptions/not-found.exception';
import { RevisionDTO } from '../dtos/revision-dto';
import { RevisionRepository } from '../repositories/revision';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { createDimensionsFromSourceAssignment, validateSourceAssignment } from '../services/dimension-processor';
import { cleanUpCube, createBaseCube } from '../services/cube-handler';
import { DEFAULT_PAGE_SIZE, getCSVPreview, removeFileFromDataLake, uploadCSV } from '../services/csv-processor';
import { convertBufferToUTF8 } from '../utils/file-utils';

import { getCubePreview, outputCube } from './cube-controller';

export const getFactTableInfo = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const fileImport = res.locals.factTable;
        const dto = FactTableDTO.fromFactTable(fileImport);
        res.json(dto);
    } catch (err) {
        next(new UnknownException());
    }
};

export const getFactTablePreview = async (req: Request, res: Response, next: NextFunction) => {
    const { dataset, factTable } = res.locals;

    const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
    const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;

    const processedCSV = await getCSVPreview(dataset, factTable, page_number, page_size);

    if ((processedCSV as ViewErrDTO).errors) {
        const processErr = processedCSV as ViewErrDTO;
        res.status(processErr.status);
    }

    res.json(processedCSV);
};

export const getRevisionPreview = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = dataset.revisions.find((revision: Revision) => revision.id === req.params.revision_id);
    const lang = req.language.split('-')[0];

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
    await cleanUpCube(cubeFile);
    if ((cubePreview as ViewErrDTO).errors) {
        const processErr = cubePreview as ViewErrDTO;
        res.status(processErr.status);
    }

    res.json(cubePreview);
};

export const confirmFactTable = async (req: Request, res: Response, next: NextFunction) => {
    const factTable: FactTable = res.locals.factTable;
    const dto = FactTableDTO.fromFactTable(factTable);
    res.json(dto);
};

export const downloadRawFactTable = async (req: Request, res: Response, next: NextFunction) => {
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
};

export const updateSources = async (req: Request, res: Response, next: NextFunction) => {
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
};

export const getRevisionInfo = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = dataset.revisions.find((revision: Revision) => revision.id === req.params.revision_id);

    if (!revision) {
        next(new NotFoundException('errors.revision_id_invalid'));
        return;
    }

    res.json(RevisionDTO.fromRevision(revision));
};

export const attachFactTableToRevision = async (req: Request, res: Response, next: NextFunction) => {
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
    const utf8Buffer = convertBufferToUTF8(req.file.buffer);
    try {
        fileImport = await uploadCSV(utf8Buffer, req.file?.mimetype, req.file?.originalname, dataset.id);
        fileImport.revision = revision;
        await fileImport.save();
        const updatedDataset = await DatasetRepository.getById(dataset.id);
        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err) {
        logger.error(`An error occurred trying to upload the file with the following error: ${err}`);
        next(new UnknownException('errors.upload_error'));
    }
};

export const removeFactTableFromRevision = async (req: Request, res: Response, next: NextFunction) => {
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
};

export const updateRevisionPublicationDate = async (req: Request, res: Response, next: NextFunction) => {
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
};

export const downloadRevisionCubeFile = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = dataset.revisions.find((revision: Revision) => revision.id === req.params.revision_id);
    if (!revision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (revision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(revision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, revision);
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
    await cleanUpCube(cubeFile);
};

export const downloadRevisionCubeAsJSON = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = dataset.revisions.find((revision: Revision) => revision.id === req.params.revision_id);
    const lang = req.language.split('-')[0];
    if (!revision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (revision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(revision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            logger.info('Creating fresh cube file.');
            cubeFile = await createBaseCube(dataset, revision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Json);
    await cleanUpCube(cubeFile);
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
};

export const downloadRevisionCubeAsCSV = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = dataset.revisions.find((revision: Revision) => revision.id === req.params.revision_id);
    const lang = req.language.split('-')[0];
    if (!revision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (revision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(revision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, revision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Csv);
    await cleanUpCube(cubeFile);
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
};

export const downloadRevisionCubeAsParquet = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = dataset.revisions.find((revision: Revision) => revision.id === req.params.revision_id);
    const lang = req.language.split('-')[0];
    if (!revision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (revision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(revision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, revision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Parquet);
    await cleanUpCube(cubeFile);
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
};

export const downloadRevisionCubeAsExcel = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = dataset.revisions.find((revision: Revision) => revision.id === req.params.revision_id);
    const lang = req.language.split('-')[0];
    if (!revision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (revision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(revision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, revision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Excel);
    logger.info(`Cube file located at: ${cubeFile}`);
    await cleanUpCube(cubeFile);
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
};
