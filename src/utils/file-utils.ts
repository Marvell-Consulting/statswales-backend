import { writeFile } from 'node:fs/promises';

import { Database } from 'duckdb-async';
import { format as pgformat } from '@scaleleap/pg-format';

import { Dataset } from '../entities/dataset/dataset';
import { FileImportInterface } from '../entities/dataset/file-import.interface';
import { FileType } from '../enums/file-type';

import { logger } from './logger';
import { getFileService } from './get-file-service';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';
import { asyncTmpName } from './async-tmp';

export const getFileImportAndSaveToDisk = async (
  dataset: Dataset,
  importFile: FileImportInterface
): Promise<string> => {
  const fileService = getFileService();
  const importTmpFile = await asyncTmpName({ postfix: `.${importFile.fileType}` });
  const buffer = await fileService.loadBuffer(importFile.filename, dataset.id);
  await writeFile(importTmpFile, buffer);
  return importTmpFile;
};

// This function creates a table in a duckdb database based on a file and loads the files contents directly into the table
export const loadFileIntoDatabase = async (
  quack: Database,
  fileImport: FileImportInterface,
  tempFile: string,
  tableName: string
) => {
  let createTableQuery: string;
  switch (fileImport.fileType) {
    case FileType.Csv:
    case FileType.GzipCsv:
      createTableQuery = pgformat(
        `CREATE TABLE %I AS SELECT * FROM read_csv(%L, auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR'], encoding = %L, sample_size = -1);`,
        tableName,
        tempFile,
        fileImport.encoding
      );
      break;
    case FileType.Parquet:
      createTableQuery = pgformat(`CREATE TABLE %I AS SELECT * FROM %L;`, tableName, tempFile);
      break;
    case FileType.Json:
    case FileType.GzipJson:
      createTableQuery = pgformat(`CREATE TABLE %I AS SELECT * FROM read_json_auto(%L);`, tableName, tempFile);
      break;
    case FileType.Excel:
      await quack.exec('INSTALL spatial;');
      await quack.exec('LOAD spatial;');
      createTableQuery = pgformat(`CREATE TABLE %I AS SELECT * FROM st_read(%L);`, tableName, tempFile);
      break;
    default:
      throw new FileValidationException(
        `File type is unknown or not supported`,
        FileValidationErrorType.UnknownMimeType
      );
  }
  logger.debug(`Creating table ${tableName} from ${fileImport.fileType} file`);
  await quack.exec(createTableQuery);
};
