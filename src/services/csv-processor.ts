import { createHash, randomUUID } from 'node:crypto';
import fs from 'fs';

import { Database, TableData } from 'duckdb-async';
import tmp from 'tmp';

import { i18next } from '../middleware/translation';
import { logger as parentLogger } from '../utils/logger';
import { DataTable } from '../entities/dataset/data-table';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetRepository } from '../repositories/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { DataTableDto } from '../dtos/data-table-dto';
import { Error } from '../dtos/error';
import { Locale } from '../enums/locale';
import { FileType } from '../enums/file-type';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { FactTableAction } from '../enums/fact-table-action';
import { convertBufferToUTF8 } from '../utils/file-utils';

import { DataLakeService } from './datalake';

export const MAX_PAGE_SIZE = 500;
export const MIN_PAGE_SIZE = 5;
export const DEFAULT_PAGE_SIZE = 100;

const t = i18next.t;
const logger = parentLogger.child({ module: 'CSVProcessor' });

function validatePageSize(page_size: number): boolean {
    return !(page_size > MAX_PAGE_SIZE || page_size < MIN_PAGE_SIZE);
}

function validatePageNumber(page_number: number): boolean {
    return page_number >= 1;
}

function validatMaxPageNumber(page_number: number, max_page_number: number): boolean {
    return page_number <= max_page_number;
}

function validateParams(page_number: number, max_page_number: number, page_size: number): Error[] {
    const errors: Error[] = [];
    if (!validatePageSize(page_size)) {
        errors.push({
            field: 'page_size',
            message: [
                {
                    lang: Locale.English,
                    message: t('errors.page_size', {
                        lng: Locale.English,
                        max_page_size: MAX_PAGE_SIZE,
                        min_page_size: MIN_PAGE_SIZE
                    })
                },
                {
                    lang: Locale.Welsh,
                    message: t('errors.page_size', {
                        lng: Locale.Welsh,
                        max_page_size: MAX_PAGE_SIZE,
                        min_page_size: MIN_PAGE_SIZE
                    })
                }
            ],
            tag: {
                name: 'errors.page_size',
                params: { max_page_size: MAX_PAGE_SIZE, min_page_size: MIN_PAGE_SIZE }
            }
        });
    }
    if (!validatMaxPageNumber(page_number, max_page_number)) {
        errors.push({
            field: 'page_number',
            message: [
                {
                    lang: Locale.English,
                    message: t('errors.page_number_to_high', { lng: Locale.English, page_number: max_page_number })
                },
                {
                    lang: Locale.Welsh,
                    message: t('errors.page_number_to_high', { lng: Locale.Welsh, page_number: max_page_number })
                }
            ],
            tag: {
                name: 'errors.page_number_to_high',
                params: { page_number: max_page_number }
            }
        });
    }
    if (!validatePageNumber(page_number)) {
        errors.push({
            field: 'page_number',
            message: [
                { lang: Locale.English, message: t('errors.page_number_to_low', { lng: Locale.English }) },
                { lang: Locale.Welsh, message: t('errors.page_number_to_low', { lng: Locale.Welsh }) }
            ],
            tag: { name: 'errors.page_number_to_low', params: {} }
        });
    }
    return errors;
}

export async function extractTableInformation(fileBuffer: Buffer, fileType: FileType): Promise<DataTableDescription[]> {
    const tableName = 'preview_table';
    const quack = await Database.create(':memory:');
    const tempFile = tmp.tmpNameSync({ postfix: `.${fileType}` });
    let tableHeaders: TableData;
    let createTableQuery: string;
    fs.writeFileSync(tempFile, fileBuffer);
    switch (fileType) {
        case FileType.Csv:
        case FileType.GzipCsv:
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${tempFile}', auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR']);`;
            break;
        case FileType.Parquet:
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM '${tempFile}';`;
            break;
        case FileType.Json:
        case FileType.GzipJson:
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_json_auto('${tempFile}');`;
            break;
        case FileType.Excel:
            await quack.exec('INSTALL spatial;');
            await quack.exec('LOAD spatial;');
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM st_read('${tempFile}');`;
            break;
        default:
            throw new Error('Unknown file type');
    }
    try {
        logger.debug(`Executing query to create base fact table: ${createTableQuery}`);
        await quack.exec(createTableQuery);
        tableHeaders = await quack.all(
            `SELECT (row_number() OVER ())-1 as index, column_name, column_type FROM (DESCRIBE ${tableName});`
        );
    } catch (error) {
        logger.error(`Something went wrong trying to extract table information with the following error: ${error}`);
        throw error;
    } finally {
        logger.debug('Closing DuckDB Memory Database');
        await quack.close();
        logger.debug(`Removing temp file ${tempFile} from disk`);
        fs.unlinkSync(tempFile);
    }
    if (tableHeaders.length === 0) {
        throw new Error('This file does not appear to contain any tabular data');
    }
    if (tableHeaders.length === 1 && fileType === FileType.Csv) {
        throw new Error('Unable to process CSV... The resulting read resulted in only one column');
    }
    return tableHeaders.map((header) => {
        const info = new DataTableDescription();
        info.columnName = header.column_name;
        info.columnIndex = header.index;
        info.columnDatatype = header.column_type;
        return info;
    });
}

// Required Methods for refactor
export const uploadCSV = async (
    fileBuffer: Buffer,
    filetype: string,
    originalName: string,
    datasetId: string
): Promise<DataTable> => {
    const dataLakeService = new DataLakeService();
    if (!fileBuffer) {
        logger.error('No buffer to upload to blob storage');
        throw new Error('No buffer to upload to blob storage');
    }
    let uploadBuffer = fileBuffer;
    const dataTable = new DataTable();
    dataTable.id = randomUUID().toLowerCase();
    dataTable.mimeType = filetype;
    dataTable.originalFilename = originalName;
    let extension: string;
    switch (filetype) {
        case 'application/csv':
        case 'text/csv':
            extension = 'csv';
            dataTable.fileType = FileType.Csv;
            uploadBuffer = convertBufferToUTF8(fileBuffer);
            break;
        case 'application/vnd.apache.parquet':
        case 'application/parquet':
            extension = 'parquet';
            dataTable.fileType = FileType.Parquet;
            break;
        case 'application/json':
            extension = 'json';
            dataTable.fileType = FileType.Json;
            break;
        case 'application/vnd.ms-excel':
        case 'application/msexcel':
        case 'application/x-msexcel':
        case 'application/x-ms-excel':
        case 'application/x-excel':
        case 'application/x-dos_ms_excel':
        case 'application/xls':
        case 'application/x-xls':
        case 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            extension = 'xlsx';
            dataTable.fileType = FileType.Excel;
            break;
        case 'application/x-gzip':
            switch (originalName.split('.').reverse()[1]) {
                case 'json':
                    extension = 'json.gz';
                    dataTable.fileType = FileType.GzipJson;
                    break;
                case 'csv':
                    extension = 'csv.gz';
                    dataTable.fileType = FileType.GzipCsv;
                    break;
                default:
                    throw new Error(`unsupported format ${originalName.split('.').reverse()[1]}`);
            }
            break;
        default:
            logger.error(`A user uploaded a file with a mimetype of ${filetype} which is known.`);
            throw new Error('File type has not been recognised.');
    }
    let dataTableDescriptions: DataTableDescription[];
    try {
        dataTableDescriptions = await extractTableInformation(uploadBuffer, dataTable.fileType);
    } catch (error) {
        logger.error(`Something went wrong trying to read the users file with the following error: ${error}`);
        throw error;
    }
    dataTable.dataTableDescriptions = dataTableDescriptions;
    dataTable.filename = `${dataTable.id}.${extension}`;
    dataTable.action = FactTableAction.ReplaceAll;
    const hash = createHash('sha256');
    hash.update(uploadBuffer);
    try {
        await dataLakeService.createDirectory(datasetId);
        await dataLakeService.uploadFileBuffer(dataTable.filename, datasetId, uploadBuffer);
    } catch (err) {
        logger.error(
            `Something went wrong trying to upload the file to the Data Lake with the following error: ${err}`
        );
        throw new Error('Error processing file upload to Data Lake');
    }
    dataTable.hash = hash.digest('hex');
    dataTable.uploadedAt = new Date();
    return dataTable;
};

export const getCSVPreview = async (
    dataset: Dataset,
    importObj: DataTable,
    page: number,
    size: number
): Promise<ViewDTO | ViewErrDTO> => {
    const tableName = 'preview_table';
    const quack = await Database.create(':memory:');
    const tempFile = tmp.tmpNameSync({ postfix: `.${importObj.fileType}` });
    try {
        const dataLakeService = new DataLakeService();
        let fileBuffer: Buffer;
        try {
            fileBuffer = await dataLakeService.getFileBuffer(importObj.filename, dataset.id);
        } catch (err) {
            logger.error(`Something went wrong trying to get file from datalake with error: ${err}`);
            throw err;
        }
        fs.writeFileSync(tempFile, fileBuffer);
        let createTableQuery: string;
        switch (importObj.fileType) {
            case FileType.Csv:
            case FileType.GzipCsv:
                createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${tempFile}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`;
                break;
            case FileType.Parquet:
                createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM '${tempFile}';`;
                break;
            case FileType.Json:
            case FileType.GzipJson:
                createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_json_auto('${tempFile}');`;
                break;
            case FileType.Excel:
                await quack.exec('INSTALL spatial;');
                await quack.exec('LOAD spatial;');
                createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM st_read('${tempFile}');`;
                break;
            default:
                throw new Error('Unknown file type');
        }
        await quack.exec(createTableQuery);
        const totalsQuery = `SELECT count(*) as totalLines, ceil(count(*)/${size}) as totalPages from ${tableName};`;
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
        const previewQuery = `SELECT int_line_number, * from (SELECT row_number() OVER () as int_line_number, * FROM ${tableName}) LIMIT ${size} OFFSET ${(page - 1) * size}`;
        const preview = await quack.all(previewQuery);
        const startLine = Number(preview[0].int_line_number);
        const lastLine = Number(preview[preview.length - 1].int_line_number);
        const tableHeaders = Object.keys(preview[0]);
        const dataArray = preview.map((row) => Object.values(row));
        const currentDataset = await DatasetRepository.getById(dataset.id);
        const currentImport = await DataTable.findOneByOrFail({ id: importObj.id });
        const headers: CSVHeader[] = [];
        for (let i = 0; i < tableHeaders.length; i++) {
            let sourceType: FactTableColumnType;
            if (tableHeaders[i] === 'int_line_number') sourceType = FactTableColumnType.LineNumber;
            else
                sourceType =
                    dataset.factTable?.find((info) => info.columnName === tableHeaders[i])?.columnType ||
                    FactTableColumnType.Unknown;
            headers.push({
                index: i - 1,
                name: tableHeaders[i],
                source_type: sourceType
            });
        }
        return {
            dataset: DatasetDTO.fromDataset(currentDataset),
            fact_table: DataTableDto.fromDataTable(currentImport),
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
    } catch (error) {
        logger.error(error);
        return {
            status: 500,
            errors: [
                {
                    field: 'csv',
                    message: [
                        {
                            lang: Locale.English,
                            message: t('errors.download_from_datalake', { lng: Locale.English })
                        },
                        { lang: Locale.Welsh, message: t('errors.download_from_datalake', { lng: Locale.Welsh }) }
                    ],
                    tag: { name: 'errors.download_from_datalake', params: {} }
                }
            ],
            dataset_id: dataset.id
        };
    } finally {
        await quack.close();
        if (fs.existsSync(tempFile)) fs.unlinkSync(tempFile);
    }
};

export const getFactTableColumnPreview = async (
    dataset: Dataset,
    dataTable: DataTable,
    columnName: string
): Promise<ViewDTO | ViewErrDTO> => {
    logger.debug(`Getting fact table column preview for ${columnName}`);
    const tableName = 'preview_table';
    const quack = await Database.create(':memory:');
    const tempFile = tmp.tmpNameSync({ postfix: `.${dataTable.fileType}` });
    try {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(dataTable.filename, dataset.id);
        fs.writeFileSync(tempFile, fileBuffer);
        let createTableQuery: string;
        switch (dataTable.fileType) {
            case FileType.Csv:
            case FileType.GzipCsv:
                createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${tempFile}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`;
                break;
            case FileType.Parquet:
                createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM '${tempFile}';`;
                break;
            case FileType.Json:
            case FileType.GzipJson:
                createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_json_auto('${tempFile}');`;
                break;
            case FileType.Excel:
                await quack.exec('INSTALL spatial;');
                await quack.exec('LOAD spatial;');
                createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM st_read('${tempFile}');`;
                break;
            default:
                throw new Error('Unknown file type');
        }
        await quack.exec(createTableQuery);
        const previewQuery = `SELECT DISTINCT ${columnName} FROM ${tableName} LIMIT 5`;
        const preview = await quack.all(previewQuery);
        const tableHeaders = Object.keys(preview[0]);
        const dataArray = preview.map((row) => Object.values(row));
        const currentDataset = await DatasetRepository.getById(dataset.id);
        const currentImport = await DataTable.findOneByOrFail({ id: dataTable.id });
        const headers: CSVHeader[] = [];
        for (let i = 0; i < tableHeaders.length; i++) {
            let sourceType: FactTableColumnType;
            if (tableHeaders[i] === 'int_line_number') sourceType = FactTableColumnType.LineNumber;
            else
                sourceType =
                    dataset.factTable?.find((info) => info.columnName === tableHeaders[i])?.columnType ??
                    FactTableColumnType.Unknown;
            headers.push({
                index: i - 1,
                name: tableHeaders[i],
                source_type: sourceType
            });
        }
        return {
            dataset: DatasetDTO.fromDataset(currentDataset),
            fact_table: DataTableDto.fromDataTable(currentImport),
            current_page: 1,
            page_info: {
                total_records: 1,
                start_record: 1,
                end_record: 10
            },
            page_size: 10,
            total_pages: 1,
            headers,
            data: dataArray
        };
    } catch (error) {
        logger.error(error);
        return {
            status: 500,
            errors: [
                {
                    field: 'csv',
                    message: [
                        {
                            lang: Locale.English,
                            message: t('errors.download_from_datalake', { lng: Locale.English })
                        },
                        { lang: Locale.Welsh, message: t('errors.download_from_datalake', { lng: Locale.Welsh }) }
                    ],
                    tag: { name: 'errors.download_from_datalake', params: {} }
                }
            ],
            dataset_id: dataset.id
        };
    } finally {
        await quack.close();
        fs.unlinkSync(tempFile);
    }
};

export const removeFileFromDataLake = async (importObj: DataTable, dataset: Dataset) => {
    const datalakeService = new DataLakeService();
    try {
        await datalakeService.deleteFile(importObj.filename, dataset.id);
    } catch (err) {
        logger.error(err);
        throw new Error('Unable to successfully remove from from Datalake');
    }
};
