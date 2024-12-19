import fs from 'node:fs';

import tmp, { FileResult } from 'tmp';
import { Database } from 'duckdb-async';

import { Dataset } from '../entities/dataset/dataset';
import { FileImport } from '../entities/dataset/file-import';
import { DataLakeService } from '../services/datalake';
import { FileType } from '../enums/file-type';

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
