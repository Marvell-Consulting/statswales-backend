import fs from 'fs';
import { Readable } from 'node:stream';
import { performance } from 'node:perf_hooks';

import { NextFunction, Request, Response } from 'express';
import tmp from 'tmp';
import { t } from 'i18next';
import { formatISO, isBefore, isValid, parse, parseISO } from 'date-fns';
import { toZonedTime } from 'date-fns-tz';

import { User } from '../entities/user/user';
import { DataTableDto } from '../dtos/data-table-dto';
import { UnknownException } from '../exceptions/unknown.exception';
import { ViewErrDTO } from '../dtos/view-dto';
import { Revision } from '../entities/dataset/revision';
import { logger } from '../utils/logger';
import { DataLakeService } from '../services/datalake';
import { DataTable } from '../entities/dataset/data-table';
import { Locale } from '../enums/locale';
import { DatasetRepository, withDraftForTasklistState } from '../repositories/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { TasklistStateDTO } from '../dtos/tasklist-state-dto';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { NotFoundException } from '../exceptions/not-found.exception';
import { RevisionDTO } from '../dtos/revision-dto';
import { RevisionRepository } from '../repositories/revision';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import {
    cleanUpCube,
    createAndValidateDateDimension,
    createAndValidateLookupTableDimension,
    createBaseCube,
    getCubeTimePeriods,
    loadCorrectReferenceDataIntoReferenceDataTable,
    loadReferenceDataIntoCube,
    makeCubeSafeString,
    updateFactTableValidator
} from '../services/cube-handler';
import { DEFAULT_PAGE_SIZE, getCSVPreview, removeFileFromDataLake, uploadCSV } from '../services/csv-processor';
import { convertBufferToUTF8 } from '../utils/file-utils';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { DataTableAction } from '../enums/data-table-action';
import { ColumnMatch } from '../interfaces/column-match';
import { DimensionType } from '../enums/dimension-type';
import { CubeValidationException, CubeValidationType } from '../exceptions/cube-error-exception';
import { DimensionUpdateTask } from '../interfaces/revision-task';
import { duckdb } from '../services/duckdb';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { getLatestRevision } from '../utils/latest';

import { getCubePreview, outputCube } from './cube-controller';

export const getFactTableInfo = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const fileImport = await DataTable.findOneOrFail({
            where: {
                id: req.params.id
            },
            relations: ['dataTableDescriptions', 'revision']
        });
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
    const start = performance.now();

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
            cubeFile = await createBaseCube(dataset.id, revision.id);
        } catch (error) {
            logger.error(`Something went wrong trying to create the cube with the error: ${error}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const cubePreview = await getCubePreview(cubeFile, lang, dataset, page_number, page_size);
    const end = performance.now();
    const time = Math.round(end - start);
    logger.info(`Cube revision preview took ${time}ms`);
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

async function attachFirstDatatableToRevision(
    revision: Revision,
    fileImport: DataTable,
    req: Request,
    res: Response,
    next: NextFunction
) {
    logger.debug('Attaching update data table to first revision');
    const dataset = res.locals.dataset;
    fileImport.revision = revision;
    await fileImport.save();
    const updatedDataset = await DatasetRepository.getById(dataset.id);
    res.status(201);
    res.json(DatasetDTO.fromDataset(updatedDataset));
}

async function attachUpdateDataTableToRevision(
    revision: Revision,
    fileImport: DataTable,
    req: Request,
    res: Response,
    next: NextFunction
) {
    logger.debug('Attaching update data table to revision and validating cube');
    const start = performance.now();
    const dataset = res.locals.dataset;
    // Validate all the columns against the fact table
    if (req.body.column_matching) {
        const columnMatcher = JSON.parse(req.body.column_matching) as ColumnMatch[];
        const matchedColumns: string[] = [];
        for (const col of columnMatcher) {
            const factTableCol: FactTableColumn | undefined = dataset.factTable.find(
                (factTableCol: FactTableColumn) =>
                    makeCubeSafeString(factTableCol.columnName) === makeCubeSafeString(col.fact_table_column_name)
            );
            const dataTableCol = fileImport.dataTableDescriptions.find(
                (dataTableCol: DataTableDescription) =>
                    makeCubeSafeString(dataTableCol.columnName) === makeCubeSafeString(col.data_table_column_name)
            );
            if (factTableCol && dataTableCol) {
                matchedColumns.push(factTableCol.columnName);
                dataTableCol.factTableColumn = factTableCol.columnName;
            }
        }
        if (matchedColumns.length !== dataset.factTable.length) {
            logger.error(`Could not match all columns to the fact table.`);
            next(new UnknownException('errors.failed_to_match_columns'));
            return;
        }
    } else {
        // validate columns
        const matchedColumns: string[] = [];
        const unmatchedColumns: string[] = [];
        for (const col of fileImport.dataTableDescriptions) {
            const factTableCol: FactTableColumn = dataset.factTable.find(
                (factTableCol: FactTableColumn) =>
                    makeCubeSafeString(factTableCol.columnName) === makeCubeSafeString(col.columnName)
            );
            if (factTableCol) {
                matchedColumns.push(factTableCol.columnName);
                col.factTableColumn = factTableCol.columnName;
            } else {
                unmatchedColumns.push(col.columnName);
            }
        }

        if (matchedColumns.length !== dataset.factTable.length) {
            logger.error(
                `Could not match all columns to the fact table.  The following columns were not matched: ${unmatchedColumns.join(', ')}`
            );
            const end = performance.now();
            const time = Math.round(end - start);
            logger.info(`Cube update validation took ${time}ms`);
            next(new UnknownException('errors.failed_to_match_columns'));
            return;
        }
    }

    logger.debug(`Setting the update action to: ${req.body.update_action || 'Add'}`);
    let updateAction = DataTableAction.Add;
    if (req.body.update_action) updateAction = req.body.update_action as DataTableAction;
    fileImport.action = updateAction;

    revision.dataTable = fileImport;
    const quack = await duckdb();
    try {
        await updateFactTableValidator(quack, dataset, revision);
    } catch (err) {
        logger.debug('Closing DuckDB instance');
        const end = performance.now();
        const time = Math.round(end - start);
        logger.info(`Cube update validation took ${time}ms`);
        await quack.close();
        logger.error(`An error occurred trying to validate the file with the following error: ${err}`);
        next(new BadRequestException('errors.data_table_validation_error'));
        return;
    }
    const dimensionUpdateTasks: DimensionUpdateTask[] = [];
    await loadReferenceDataIntoCube(quack);
    for (const dimension of dataset.dimensions) {
        try {
            switch (dimension.type) {
                case DimensionType.LookupTable:
                    logger.debug(`Validating lookup table dimension: ${dimension.id}`);
                    await createAndValidateLookupTableDimension(quack, dataset, dimension);
                    break;
                case DimensionType.ReferenceData:
                    logger.debug(`Validating reference data dimension: ${dimension.id}`);
                    await loadCorrectReferenceDataIntoReferenceDataTable(quack, dimension);
                    break;
                case DimensionType.TimePeriod:
                case DimensionType.TimePoint:
                    logger.debug(`Validating time dimension: ${dimension.id}`);
                    await createAndValidateDateDimension(quack, dimension.extractor, dimension.factTableColumn);
            }
        } catch (error) {
            logger.warn(`An error occurred validating dimension ${dimension.id}: ${error}`);
            const err = error as CubeValidationException;
            if (err.type === CubeValidationType.DimensionNonMatchedRows) {
                dimensionUpdateTasks.push({
                    id: dimension.id,
                    lookupTableUpdated: false
                });
            } else {
                logger.debug('Closing DuckDB instance');
                const end = performance.now();
                const time = Math.round(end - start);
                logger.info(`Cube update validation took ${time}ms`);
                await quack.close();
                logger.error(`An error occurred trying to validate the file with the following error: ${err}`);
                next(new BadRequestException('errors.data_table_validation_error'));
                return;
            }
        }
    }

    /*
        TODO Validate measure.  This requires a rewrite of how measures are created and stored
     */

    // eslint-disable-next-line require-atomic-updates
    revision.tasks = {
        dimensions: dimensionUpdateTasks
    };
    logger.debug('Closing DuckDB instance');
    await quack.close();
    await revision.save();
    const end = performance.now();
    const time = Math.round(end - start);
    logger.info(`Cube update validation took ${time}ms`);
    // eslint-disable-next-line require-atomic-updates
    fileImport.revision = revision;
    await fileImport.save();
    const updatedDataset = await DatasetRepository.getById(dataset.id);
    res.status(201);
    res.json(DatasetDTO.fromDataset(updatedDataset));
}

export const attachDataTableToRevision = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = res.locals.revision;

    if (!req.file) {
        next(new BadRequestException('errors.upload.no_csv'));
        return;
    }

    if (revision.dataTable) {
        const dataTableId = revision.dataTable.id;
        logger.debug(`Data table (${dataTableId}) already exists, removing it from the revision.`);
        const dataTableFile = revision.dataTable.filename;
        const dataLakeService = new DataLakeService();
        try {
            dataLakeService.deleteFile(dataTableFile, dataset.id);
        } catch (err) {
            logger.warn(err, 'Failed to delete file from data lake.');
        }
        revision.dataTable = null;
        await DataTable.getRepository().delete(dataTableId);
        await revision.save();
    }

    let fileImport: DataTable;
    const utf8Buffer = convertBufferToUTF8(req.file.buffer);

    try {
        fileImport = await uploadCSV(utf8Buffer, req.file?.mimetype, req.file?.originalname, dataset.id);
    } catch (err) {
        logger.error(`An error occurred trying to upload the file with the following error: ${err}`);
        if ((err as Error).message.includes('Data Lake')) {
            next(new UnknownException('errors.data_lake_error'));
        } else {
            next(new BadRequestException('errors.upload_error'));
        }
        return;
    }

    if (revision.revisionIndex > 0) {
        await attachFirstDatatableToRevision(revision, fileImport, req, res, next);
    } else {
        await attachUpdateDataTableToRevision(revision, fileImport, req, res, next);
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
        if (dataset.revisions.length === 1) {
            for (const factTableCol of dataset.factTable) {
                await factTableCol.remove();
            }
        }
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
        const updatedDataset = await DatasetRepository.getById(dataset.id, {});

        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err) {
        next(new UnknownException());
    }
};

export const approveForPublication = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const revision = getLatestRevision(dataset)!;

    try {
        const datasetForTasklist = await DatasetRepository.getById(dataset.id, withDraftForTasklistState);
        const draftRevision = datasetForTasklist.draftRevision;
        const tasklist = TasklistStateDTO.fromDataset(datasetForTasklist, draftRevision, req.language);

        if (!tasklist.canPublish) {
            throw new BadRequestException('dataset not ready for publication, please check tasklist');
        }
        const start = performance.now();
        logger.debug(`Creating base cube for publication for revision: ${revision.id}`);
        const cubeFilePath = await createBaseCube(dataset.id, revision.id);
        const periodCoverage = await getCubeTimePeriods(cubeFilePath);
        dataset.startDate = new Date(Date.parse(periodCoverage.start_date));
        dataset.endDate = new Date(Date.parse(periodCoverage.end_date));
        await dataset.save();
        const dataLakeService = new DataLakeService();
        const cubeBuffer = fs.readFileSync(cubeFilePath);
        const onlineCubeFilename = `${revision.id}.duckdb`;
        for (const locale of SUPPORTED_LOCALES) {
            const lang = locale.split('-')[0].toLowerCase();
            logger.debug(`Creating parquet file for language "${lang}" and uploading to data lake`);
            const parquetFilePath = await outputCube(cubeFilePath, lang, DuckdbOutputType.Parquet);
            await dataLakeService.uploadFileBuffer(
                `${revision.id}_${lang}.parquet`,
                dataset.id,
                fs.readFileSync(parquetFilePath)
            );
        }
        await dataLakeService.uploadFileBuffer(onlineCubeFilename, dataset.id, cubeBuffer);
        const end = performance.now();
        const time = Math.round(end - start);
        logger.info(`Cube and parquet file creation took ${time}ms (including uploading to data lake)`);
        await RevisionRepository.approvePublication(revision.id, onlineCubeFilename, req.user as User);
        const updatedDataset = await DatasetRepository.getById(dataset.id, {});
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

        const updatedDataset = await DatasetRepository.getById(dataset.id, {});
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
            cubeFile = await createBaseCube(dataset.id, revision.id);
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
            cubeFile = await createBaseCube(dataset.id, revision.id);
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
            cubeFile = await createBaseCube(dataset.id, revision.id);
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
            cubeFile = await createBaseCube(dataset.id, revision.id);
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
            cubeFile = await createBaseCube(dataset.id, revision.id);
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
    revision.createdBy = req.user as User;
    if (dataset.revisions.length > 0) {
        revision.revisionIndex = 0;
    } else {
        revision.revisionIndex = 1;
    }
    logger.info(`Creating new revision for dataset ${dataset.id}`);
    revision.dataset = dataset;
    const savedRevision = await revision.save();
    res.status(201);
    res.json(RevisionDTO.fromRevision(savedRevision));
};
