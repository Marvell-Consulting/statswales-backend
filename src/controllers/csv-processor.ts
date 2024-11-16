import { createHash, randomUUID } from 'node:crypto';
import fs from 'fs';

import { Database } from 'duckdb-async';
import tmp from 'tmp';

import { i18next } from '../middleware/translation';
import { logger as parentLogger } from '../utils/logger';
import { DataLakeService } from '../services/datalake';
import { FileImport } from '../entities/dataset/file-import';
import { ImportType } from '../enums/import-type';
import { DataLocation } from '../enums/data-location';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { SourceType } from '../enums/source-type';
import { DatasetRepository } from '../repositories/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { FileImportDTO } from '../dtos/file-import-dto';
import { Revision } from '../entities/dataset/revision';
import { Source } from '../entities/dataset/source';
import { SourceAction } from '../enums/source-action';
import { Error } from '../dtos/error';
import { Locale } from '../enums/locale';

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

// Required Methods for refactor
export const uploadCSV = async (fileBuffer: Buffer, filetype: string, datasetId: string): Promise<FileImport> => {
    const dataLakeService = new DataLakeService();
    if (!fileBuffer) {
        logger.error('No buffer to upload to blob storage');
        throw new Error('No buffer to upload to blob storage');
    }
    const importRecord = new FileImport();
    importRecord.id = randomUUID().toLowerCase();
    importRecord.mimeType = filetype;
    const extension = filetype === 'text/csv' ? 'csv' : 'zip';
    importRecord.filename = `${importRecord.id}.${extension}`;
    const hash = createHash('sha256');
    hash.update(fileBuffer);
    try {
        await dataLakeService.createDirectory(datasetId);
        await dataLakeService.uploadFileBuffer(importRecord.filename, datasetId, fileBuffer);
        importRecord.hash = hash.digest('hex');
        importRecord.uploadedAt = new Date();
        importRecord.type = ImportType.Draft;
        return importRecord;
    } catch (err) {
        logger.error(err);
        throw new Error('Error processing file upload to datalake');
    }
};

export const getCSVPreview = async (
    dataset: Dataset,
    importObj: FileImport,
    page: number,
    size: number
): Promise<ViewDTO | ViewErrDTO> => {
    const tableName = 'preview_table';
    const quack = await Database.create(':memory:');
    const tempFile = tmp.fileSync({ postfix: '.csv' });
    try {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(importObj.filename, dataset.id);
        fs.writeFileSync(tempFile.name, fileBuffer);
        const createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${tempFile.name}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`;
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
        const currentImport = await FileImport.findOneByOrFail({ id: importObj.id });
        const headers: CSVHeader[] = [];
        for (let i = 0; i < tableHeaders.length; i++) {
            let sourceType: SourceType;
            if (tableHeaders[i] === 'int_line_number') sourceType = SourceType.LineNumber;
            else
                sourceType =
                    importObj.sources.find((source) => source.csvField === tableHeaders[i])?.type ?? SourceType.Unknown;
            headers.push({
                index: i - 1,
                name: tableHeaders[i],
                source_type: sourceType
            });
        }
        return {
            dataset: DatasetDTO.fromDataset(currentDataset),
            import: FileImportDTO.fromImport(currentImport),
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

export const getColumnPreview = async (source: Source, importObj: FileImport, dataset: Dataset) => {
    const tableName = 'preview_table';
    const quack = await Database.create(':memory:');
    const tempFile = tmp.fileSync({ postfix: '.csv' });
    try {
        const size = 10;
        const page = 0;
        const dataLakeService = new DataLakeService();
        const fileStream = await dataLakeService.getFileBuffer(importObj.filename, dataset.id);
        fs.writeFileSync(tempFile.name, fileStream);
        await quack.exec(
            `CREATE TABLE ${tableName} AS FROM read_csv('${tempFile.name}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`
        );
        const preview = await quack.all(
            `SELECT ${source.csvField} FROM ${tableName} LIMIT ${size} OFFSET ${page * size}`
        );
        const totals = await quack.all(`SELECT count(*) as totalLines from ${tableName};`);
        const totalPages = Math.ceil(totals[0].totalLines / MAX_PAGE_SIZE);
        const tableHeaders = Object.keys(preview[0]);
        const dataArray = preview.map((row) => Object.values(row));
        const currentDataset = await DatasetRepository.getById(dataset.id);
        const currentImport = await FileImport.findOneByOrFail({ id: importObj.id });
        const headers: CSVHeader[] = [];
        for (let i = 0; i < tableHeaders.length; i++) {
            headers.push({
                index: i,
                name: tableHeaders[i],
                source_type:
                    importObj.sources.find((source) => source.csvField === tableHeaders[i])?.type ?? SourceType.Unknown
            });
        }
        await quack.close();
        return {
            dataset: DatasetDTO.fromDataset(currentDataset),
            import: FileImportDTO.fromImport(currentImport),
            current_page: page,
            page_info: {
                total_records: dataArray.length,
                start_record: (page - 1) * size + 1,
                end_record: totals[0].total
            },
            page_size: size,
            total_pages: totalPages,
            headers,
            data: dataArray
        };
    } catch (error) {
        logger.error(error);
        throw new Error('Error retrieving CSV preview from Azure');
    } finally {
        await quack.close();
        tempFile.removeCallback();
    }
};

export const removeFileFromDatalake = async (importObj: FileImport, dataset: Dataset) => {
    const datalakeService = new DataLakeService();
    try {
        await datalakeService.deleteFile(importObj.filename, dataset.id);
    } catch (err) {
        logger.error(err);
        throw new Error('Unable to successfully remove from from Datalake');
    }
};

export const createSources = async (fileImport: FileImport): Promise<FileImportDTO> => {
    const revision: Revision = fileImport.revision;
    const dataset: Dataset = revision.dataset;
    let fileView: ViewDTO | ViewErrDTO;

    try {
        fileView = await getCSVPreview(dataset, fileImport, 1, 5);
    } catch (err) {
        logger.error(err);
        throw new Error('Error getting file from datalake');
    }

    const fileData: ViewDTO = fileView as ViewDTO;
    // Filter out line numbers... These aren't part of the CSV
    const headers = fileData.headers.filter((header) => header.source_type !== SourceType.LineNumber);
    const sources: Source[] = headers.map((header) => {
        const source = new Source();
        source.columnIndex = header.index;
        source.csvField = header.name;
        source.action = SourceAction.Unknown;
        source.revision = revision;
        source.import = fileImport;
        return source;
    });

    const updatedImport = await FileImport.findOne({
        where: { id: fileImport.id },
        relations: ['sources', 'revision']
    });

    if (!updatedImport) {
        throw new Error('Import not found');
    }
    try {
        updatedImport.sources = sources;
        await updatedImport.save();
    } catch (err) {
        logger.error(err);
        throw new Error('Error saving sources to import');
    }
    return FileImportDTO.fromImport(updatedImport);
};
