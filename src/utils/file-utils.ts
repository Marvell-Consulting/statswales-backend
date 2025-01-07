import fs from 'node:fs';

import tmp, { FileResult } from 'tmp';
import { Database } from 'duckdb-async';
import detectCharacterEncoding from 'detect-character-encoding';
import iconv from 'iconv-lite';

import { Dataset } from '../entities/dataset/dataset';
import { FileImport } from '../entities/dataset/file-import';
import { DataLakeService } from '../services/datalake';
import { FileType } from '../enums/file-type';

import { logger } from './logger';

export const convertBufferToUTF8 = (buffer: Buffer): Buffer => {
    const fileEncoding = detectCharacterEncoding(buffer)?.encoding;
    if (!fileEncoding) {
        logger.warn('Could not detect file encoding for the file');
        throw new Error('errors.csv.invalid');
    }
    if (fileEncoding !== 'UTF-8') {
        logger.warn(`File is not UTF-8 encoded... File appears to be ${fileEncoding}... Going to try to recode it`);
        const decodedString = iconv.decode(buffer, fileEncoding);
        return Buffer.from(decodedString);
    }
    return buffer;
};

export const getFileImportAndSaveToDisk = async (dataset: Dataset, importFile: FileImport): Promise<FileResult> => {
    const dataLakeService = new DataLakeService();
    const importTmpFile = tmp.fileSync({ postfix: `.${importFile.fileType}` });
    const buffer = await dataLakeService.getFileBuffer(importFile.filename, dataset.id);
    fs.writeFileSync(importTmpFile.name, buffer);
    return importTmpFile;
};

// This function creates a table in a duckdb database based on a file and loads the files contents directly into the table
export const loadFileIntoDatabase = async (
    quack: Database,
    fileImport: FileImport,
    tempFile: FileResult,
    tableName: string
) => {
    let createTableQuery: string;
    switch (fileImport.fileType) {
        case FileType.Csv:
        case FileType.GzipCsv:
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${tempFile.name}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`;
            break;
        case FileType.Parquet:
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM '${tempFile.name}';`;
            break;
        case FileType.Json:
        case FileType.GzipJson:
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_json_auto('${tempFile.name}');`;
            break;
        case FileType.Excel:
            await quack.exec('INSTALL spatial;');
            await quack.exec('LOAD spatial;');
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM st_read('${tempFile.name}');`;
            break;
        default:
            throw new Error('Unknown file type');
    }
    await quack.exec(createTableQuery);
};
