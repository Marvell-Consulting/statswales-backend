import { createHash, randomUUID } from 'node:crypto';
import fs from 'fs';

import { Database, TableData } from 'duckdb-async';
import tmp from 'tmp';

import { i18next } from '../middleware/translation';
import { logger as parentLogger } from '../utils/logger';
import { DataLakeService } from '../services/datalake';
import { FactTable } from '../entities/dataset/fact-table';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { SourceType } from '../enums/source-type';
import { DatasetRepository } from '../repositories/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { FactTableDTO } from '../dtos/fact-table-dto';
import { Error } from '../dtos/error';
import { Locale } from '../enums/locale';
import { Filetype } from '../enums/filetype';
import { FactTableInfo } from '../entities/dataset/fact-table-info';

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

export async function extractTableInformation(fileBuffer: Buffer, fileType: Filetype): Promise<FactTableInfo[]> {
    const tableName = 'preview_table';
    const quack = await Database.create(':memory:');
    const tempFile = tmp.fileSync({ postfix: `.${Filetype}` });
    let tableHeaders: TableData;
    let createTableQuery: string;
    fs.writeFileSync(tempFile.name, fileBuffer);
    switch (fileType) {
        case Filetype.Csv:
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${tempFile.name}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`;
            break;
        case Filetype.Parquet:
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM '${tempFile.name}';`;
            break;
        case Filetype.Json:
            createTableQuery = `CREATE TABLE new_tbl AS SELECT * FROM read_json_auto('${tempFile.name}');`;
            break;
        case Filetype.Excel:
            await quack.exec('INSTALL spatial;');
            await quack.exec('LOAD spatial;');
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM st_read('${tempFile.name}');'`;
            break;
        default:
            throw new Error('Unknown file type');
    }
    try {
        await quack.exec(createTableQuery);
        tableHeaders = await quack.all(
            `SELECT (row_number() OVER ())-1 as index, column_name FROM (DESCRIBE ${tableName});`
        );
    } catch (error) {
        logger.error(`Something went wrong trying to read the users file with the following error: ${error}`);
        throw error;
    } finally {
        await quack.close();
        tempFile.removeCallback();
    }
    if (tableHeaders.length === 0) {
        throw new Error('This file does not appear to contain any tabular data');
    }
    if (tableHeaders.length === 1 && fileType === Filetype.Csv) {
        throw new Error('Unable to process CSV... The resulting read resulted in only one column');
    }
    return tableHeaders.map((header) => {
        const info = new FactTableInfo();
        info.columnName = header.column_name;
        info.columnIndex = header.index;
        info.columnType = SourceType.Unknown;
        return info;
    });
}

// Required Methods for refactor
export const uploadCSV = async (fileBuffer: Buffer, filetype: string, datasetId: string): Promise<FactTable> => {
    const dataLakeService = new DataLakeService();
    if (!fileBuffer) {
        logger.error('No buffer to upload to blob storage');
        throw new Error('No buffer to upload to blob storage');
    }
    const factTable = new FactTable();
    factTable.id = randomUUID().toLowerCase();
    factTable.mimeType = filetype;
    let extension: string;
    switch (filetype) {
        case 'text/csv':
            extension = 'csv';
            factTable.fileType = Filetype.Csv;
            factTable.delimiter = ',';
            factTable.quote = '"';
            factTable.linebreak = '\n';
            break;
        case 'application/vnd.apache.parquet':
        case 'application/parquet':
            extension = 'parquet';
            factTable.fileType = Filetype.Parquet;
            break;
        case 'application/json':
            extension = 'json';
            factTable.fileType = Filetype.Json;
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
            factTable.fileType = Filetype.Excel;
            break;
        default:
            logger.error(`A user uploaded a file with a mimetype of ${filetype} which is known.`);
            throw new Error('File type has not been recognised.');
    }
    let factTableDescriptions: FactTableInfo[];
    try {
        factTableDescriptions = await extractTableInformation(fileBuffer, factTable.fileType);
    } catch (error) {
        logger.error(`Something went wrong trying to read the users file with the following error: ${error}`);
        throw error;
    }
    factTable.factTableInfo = factTableDescriptions;
    factTable.filename = `${factTable.id}.${extension}`;
    const hash = createHash('sha256');
    hash.update(fileBuffer);
    try {
        await dataLakeService.createDirectory(datasetId);
        await dataLakeService.uploadFileBuffer(factTable.filename, datasetId, fileBuffer);
    } catch (err) {
        logger.error(
            `Something went wrong trying to upload the file to the Data Lake with the following error: ${err}`
        );
        throw new Error('Error processing file upload to Data Lake');
    }
    factTable.hash = hash.digest('hex');
    factTable.uploadedAt = new Date();
    return factTable;
};

export const getCSVPreview = async (
    dataset: Dataset,
    importObj: FactTable,
    page: number,
    size: number
): Promise<ViewDTO | ViewErrDTO> => {
    const tableName = 'preview_table';
    const quack = await Database.create(':memory:');
    const tempFile = tmp.fileSync({ postfix: `.${importObj.fileType}` });
    try {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(importObj.filename, dataset.id);
        fs.writeFileSync(tempFile.name, fileBuffer);
        let createTableQuery: string;
        switch (importObj.fileType) {
            case Filetype.Csv:
                createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${tempFile.name}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`;
                break;
            case Filetype.Parquet:
                createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM '${tempFile.name}';`;
                break;
            case Filetype.Json:
                createTableQuery = `CREATE TABLE new_tbl AS SELECT * FROM read_json_auto('${tempFile.name}');`;
                break;
            case Filetype.Excel:
                await quack.exec('INSTALL spatial;');
                await quack.exec('LOAD spatial;');
                createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM st_read('${tempFile.name}');'`;
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
        const startLine = Number(preview[0].line);
        const lastLine = Number(preview[preview.length - 1].line);
        const tableHeaders = Object.keys(preview[0]);
        const dataArray = preview.map((row) => Object.values(row));
        const currentDataset = await DatasetRepository.getById(dataset.id);
        const currentImport = await FactTable.findOneByOrFail({ id: importObj.id });
        const headers: CSVHeader[] = [];
        for (let i = 0; i < tableHeaders.length; i++) {
            let sourceType: SourceType;
            if (tableHeaders[i] === 'int_line_number') sourceType = SourceType.LineNumber;
            else
                sourceType =
                    importObj.factTableInfo.find((info) => info.columnName === tableHeaders[i])?.columnType ??
                    SourceType.Unknown;
            headers.push({
                index: i - 1,
                name: tableHeaders[i],
                source_type: sourceType
            });
        }
        return {
            dataset: DatasetDTO.fromDataset(currentDataset),
            fact_table: FactTableDTO.fromFactTable(currentImport),
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
        tempFile.removeCallback();
    }
};

export const removeFileFromDataLake = async (importObj: FactTable, dataset: Dataset) => {
    const datalakeService = new DataLakeService();
    try {
        await datalakeService.deleteFile(importObj.filename, dataset.id);
    } catch (err) {
        logger.error(err);
        throw new Error('Unable to successfully remove from from Datalake');
    }
};
