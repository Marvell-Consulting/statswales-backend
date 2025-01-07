import fs from 'node:fs';

import tmp, { FileResult } from 'tmp';
import { Database } from 'duckdb-async';
import { NextFunction, Request, Response } from 'express';

import { Dataset } from '../entities/dataset/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { logger } from '../utils/logger';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { DatasetRepository } from '../repositories/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DatasetDTO } from '../dtos/dataset-dto';
import { validateParams } from '../utils/paging-validation';
import { getLatestRevision } from '../utils/latest';
import { UnknownException } from '../exceptions/unknown.exception';
import { DataLakeService } from '../services/datalake';
import { createBaseCube } from '../services/cube-handler';

export const getCubePreview = async (
    cubeFile: string,
    lang: string,
    dataset: Dataset,
    page: number,
    size: number
): Promise<ViewDTO | ViewErrDTO> => {
    const quack = await Database.create(cubeFile);
    const totalsQuery = `SELECT count(*) as totalLines, ceil(count(*)/${size}) as totalPages from default_view_${lang};`;
    const totals = await quack.all(totalsQuery);
    const totalPages = Number(totals[0].totalPages);
    const totalLines = Number(totals[0].totalLines);
    const errors = validateParams(page, totalPages, size);
    if (errors.length > 0) {
        return {
            status: 400,
            errors,
            dataset_id: dataset.id
        };
    }
    const previewQuery = `SELECT int_line_number, * FROM (SELECT row_number() OVER () as int_line_number, * FROM default_view_${lang}) LIMIT ${size} OFFSET ${(page - 1) * size}`;
    const preview = await quack.all(previewQuery);
    const startLine = Number(preview[0].int_line_number);
    const lastLine = Number(preview[preview.length - 1].int_line_number);
    const tableHeaders = Object.keys(preview[0]);
    const dataArray = preview.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const headers: CSVHeader[] = [];
    for (let i = 0; i < tableHeaders.length; i++) {
        headers.push({
            index: i - 1,
            name: tableHeaders[i],
            source_type:
                tableHeaders[i] === 'int_line_number' ? FactTableColumnType.LineNumber : FactTableColumnType.Unknown
        });
    }
    return {
        dataset: DatasetDTO.fromDataset(currentDataset),
        current_page: page,
        page_info: {
            total_records: totalLines,
            start_record: startLine,
            end_record: lastLine
        },
        page_size: size,
        total_pages: totalPages,
        headers,
        data: dataArray
    };
};

export const outputCube = async (cubeFile: string, lang: string, mode: DuckdbOutputType) => {
    const quack = await Database.create(cubeFile);
    const outputFile: FileResult = tmp.fileSync({ postfix: `.${mode}` });
    switch (mode) {
        case DuckdbOutputType.Csv:
            await quack.exec(`COPY default_view_${lang} TO '${outputFile.name}' (HEADER, DELIMITER ',');`);
            break;
        case DuckdbOutputType.Parquet:
            await quack.exec(`COPY default_view_${lang} TO '${outputFile.name}' (FORMAT PARQUET);`);
            break;
        case DuckdbOutputType.Excel:
            await quack.exec(`INSTALL spatial;`);
            await quack.exec('LOAD spatial;');
            await quack.exec(`COPY default_view_${lang} TO '${outputFile.name}' WITH (FORMAT GDAL, DRIVER 'xlsx');`);
            break;
        case DuckdbOutputType.Json:
            await quack.exec(`COPY default_view_${lang} TO '${outputFile.name}' (FORMAT JSON);`);
            break;
        case DuckdbOutputType.DuckDb:
            return cubeFile;
        default:
            throw new Error(`Format ${mode} not supported`);
    }
    return outputFile.name;
};

export const downloadCubeFile = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const latestRevision = getLatestRevision(dataset);
    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeBuffer: Buffer;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        cubeBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
    } else {
        try {
            const cubeFile = await createBaseCube(dataset, latestRevision);
            cubeBuffer = Buffer.from(fs.readFileSync(cubeFile));
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    logger.info(`Sending original cube file (size: ${cubeBuffer.length})`);
    res.writeHead(200, {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'Content-Type': 'application/octet-stream',
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'Content-disposition': `attachment;filename=${dataset.id}.duckdb`,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'Content-Length': cubeBuffer.length
    });
    res.end(cubeBuffer);
};

export const downloadCubeAsJSON = async (req: Request, res: Response, next: NextFunction) => {
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
};

export const downloadCubeAsCSV = async (req: Request, res: Response, next: NextFunction) => {
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
};

export const downloadCubeAsParquet = async (req: Request, res: Response, next: NextFunction) => {
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
};

export const downloadCubeAsExcel = async (req: Request, res: Response, next: NextFunction) => {
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
};
