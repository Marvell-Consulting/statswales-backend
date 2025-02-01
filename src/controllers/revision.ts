import fs from 'fs';
import { Readable } from 'node:stream';

import { NextFunction, Request, Response } from 'express';
import tmp from 'tmp';
import { t } from 'i18next';
import { isBefore, isValid } from 'date-fns';

import { User } from '../entities/user/user';
import { DataTableDto } from '../dtos/data-table-dto';
import { UnknownException } from '../exceptions/unknown.exception';
import { ViewErrDTO } from '../dtos/view-dto';
import { Revision } from '../entities/dataset/revision';
import { logger } from '../utils/logger';
import { DataLakeService } from '../services/datalake';
import { DataTable } from '../entities/dataset/data-table';
import { Locale } from '../enums/locale';
import { DatasetRepository } from '../repositories/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { TasklistStateDTO } from '../dtos/tasklist-state-dto';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { NotFoundException } from '../exceptions/not-found.exception';
import { RevisionDTO } from '../dtos/revision-dto';
import { RevisionRepository } from '../repositories/revision';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { cleanUpCube, createBaseCube } from '../services/cube-handler';
import { DEFAULT_PAGE_SIZE, getCSVPreview, removeFileFromDataLake, uploadCSV } from '../services/csv-processor';
import { convertBufferToUTF8 } from '../utils/file-utils';

import { getCubePreview, outputCube } from './cube-controller';

export const getFactTableInfo = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const fileImport = res.locals.revision.dataTable;
        const dto = DataTableDto.fromDataTable(fileImport);
        res.json(dto);
    } catch (err) {
        next(new UnknownException());
    }
};

export const getFactTablePreview = async (req: Request, res: Response, next: NextFunction) => {
    const { dataset, revision } = res.locals;

    const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
    const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;

    if (!revision.dataTable) {
        next(new NotFoundException('errors.no_data_table'));
        return;
    }

    const processedCSV = await getCSVPreview(dataset, revision.dataTable, page_number, page_size);

    if ((processedCSV as ViewErrDTO).errors) {
        const processErr = processedCSV as ViewErrDTO;
        res.status(processErr.status);
    }

    res.json(processedCSV);
};

export const getRevisionPreview = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = res.locals.revision;
    const lang = req.language.split('-')[0];

    const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
    const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;

    let cubeFile: string;
    if (revision.onlineCubeFilename) {
        logger.debug('Loading cube from datalake for preview');
        const datalakeService = new DataLakeService();
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        try {
            const cubeBuffer = await datalakeService.getFileBuffer(revision.onlineCubeFilename, dataset.id);
            fs.writeFileSync(cubeFile, cubeBuffer);
        } catch (err) {
            logger.error('Something went wrong trying to download file from data lake');
            throw err;
        }
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
    const revision = res.locals.revision;
    const dto = DataTableDto.fromDataTable(revision.dataTable);
    res.json(dto);
};

export const downloadRawFactTable = async (req: Request, res: Response, next: NextFunction) => {
    const { dataset, revision } = res.locals;
    logger.info('User requested to down files...');
    const dataLakeService = new DataLakeService();
    let readable: Readable;
    if (!revision.dataTable) {
        logger.error("Revision doesn't have a data table, can't download file");
        next(new NotFoundException('errors.revision_id_invalid'));
        return;
    }

    try {
        readable = await dataLakeService.getFileStream(revision.dataTable.filename, dataset.id);
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

export const getRevisionInfo = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = res.locals.revision;
    res.json(RevisionDTO.fromRevision(revision));
};

export const attachFactTableToRevision = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = res.locals.revision;

    if (!req.file) {
        next(new BadRequestException('errors.upload.no_csv'));
        return;
    }

    let fileImport: DataTable;
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
    const { dataset, revision } = res.locals;

    if (!revision.dataTable) {
        logger.error("Revision doesn't have a data table, can't remove file");
        next(new NotFoundException('errors.revision_id_invalid'));
        return;
    }

    try {
        logger.warn('User has requested to remove a fact table from the datalake');
        await removeFileFromDataLake(revision.dataTable, dataset);
        await revision.dataTable.remove();
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
    const revision = res.locals.revision;

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
        const updatedDataset = await DatasetRepository.getById(dataset.id);

        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err) {
        next(new UnknownException());
    }
};

export const approveForPublication = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { dataset, revision } = res.locals;
        const tasklist = TasklistStateDTO.fromDataset(dataset, req.language);

        if (!tasklist.canPublish) {
            throw new BadRequestException('dataset not ready for publication, please check tasklist');
        }

        logger.debug(`Creating base cube for publication for revision: ${revision.id}`);
        const cubeFilePath = await createBaseCube(dataset, revision);
        const dataLakeService = new DataLakeService();
        const cubeBuffer = fs.readFileSync(cubeFilePath);
        const onlineCubeFilename = `${revision.id}.duckdb`;
        await dataLakeService.uploadFileBuffer(onlineCubeFilename, dataset.id, cubeBuffer);
        await RevisionRepository.approvePublication(revision.id, onlineCubeFilename, req.user as User);
        const updatedDataset = await DatasetRepository.getById(dataset.id);
        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err: any) {
        logger.error(err, 'could not approve publication');
        next(err);
    }
};

export const withdrawFromPublication = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const { dataset, revision } = res.locals;

        if (!revision.publishAt || !revision.approvedAt) {
            throw new BadRequestException('revision is not scheduled for publication');
        }

        if (isBefore(revision.publishAt, new Date())) {
            throw new BadRequestException('publish date has passed, cannot withdraw published revisions');
        }

        const onlineCubeFilename = revision.onlineCubeFilename;
        await RevisionRepository.withdrawPublication(revision.id);
        if (onlineCubeFilename) {
            const dataLakeService = new DataLakeService();
            await dataLakeService.deleteFile(onlineCubeFilename, dataset.id);
        }

        const updatedDataset = await DatasetRepository.getById(dataset.id);
        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err: any) {
        logger.error(err, 'could not withdraw publication');
        next(err);
    }
};

export const downloadRevisionCubeFile = async (req: Request, res: Response, next: NextFunction) => {
    const { dataset, revision } = res.locals;
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
    const { dataset, revision } = res.locals;
    const lang = req.language.split('-')[0];
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
    const { dataset, revision } = res.locals;
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
            logger.error(err, `Something went wrong trying to create the cube with the error`);
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
    const { dataset, revision } = res.locals;
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
    const { dataset, revision } = res.locals;
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

export const createNewRevision = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = new Revision();
    revision.dataset = dataset;
    const updatedRevision = await revision.save();

    if (!req.file) {
        next(new BadRequestException('errors.upload.no_csv'));
        return;
    }

    let fileImport: DataTable;
    const utf8Buffer = convertBufferToUTF8(req.file.buffer);

    try {
        fileImport = await uploadCSV(utf8Buffer, req.file?.mimetype, req.file?.originalname, dataset.id);
        fileImport.revision = updatedRevision;
        await fileImport.save();

        // validate columns
        //
        const updatedDataset = await DatasetRepository.getById(dataset.id);
        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err) {
        logger.error(`An error occurred trying to upload the file with the following error: ${err}`);
        next(new UnknownException('errors.upload_error'));
    }

    res.json(RevisionDTO.fromRevision(revision));
};
