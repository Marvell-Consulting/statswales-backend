import fs from 'node:fs';

import tmp, { FileResult } from 'tmp';
import { Database } from 'duckdb-async';
import iconv from 'iconv-lite';
import jschardet from 'jschardet';

import { Dataset } from '../entities/dataset/dataset';
import { FileImport } from '../entities/dataset/file-import';
import { DataLakeService } from '../services/datalake';
import { FileType } from '../enums/file-type';

import { logger } from './logger';

export const convertBufferToUTF8 = (buffer: Buffer): Buffer => {
    const fileEncoding = jschardet.detect(buffer.toString())?.encoding;
    if (!fileEncoding) {
        logger.warn('Could not detect file encoding for the file');
        throw new Error('errors.csv.invalid');
    }
    logger.debug(`File encoding detected as ${fileEncoding}`);
    if (fileEncoding !== 'UTF-8') {
        logger.warn(`File is not UTF-8 encoded... File appears to be ${fileEncoding}... Going to try to recode it`);
        const decodedString = iconv.decode(buffer, fileEncoding);
        return Buffer.from(decodedString);
    }
    return buffer;
};

export const getFileImportAndSaveToDisk = async (dataset: Dataset, importFile: FileImport): Promise<string> => {
    const dataLakeService = new DataLakeService();
    const importTmpFile = tmp.tmpNameSync({ postfix: `.${importFile.fileType}` });
    const buffer = await dataLakeService.getFileBuffer(importFile.filename, dataset.id);
    fs.writeFileSync(importTmpFile, buffer);
    return importTmpFile;
};

// This function creates a table in a duckdb database based on a file and loads the files contents directly into the table
export const loadFileIntoDatabase = async (
    quack: Database,
    fileImport: FileImport,
    tempFile: string,
    tableName: string
) => {
    let createTableQuery: string;
    switch (fileImport.fileType) {
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
};
