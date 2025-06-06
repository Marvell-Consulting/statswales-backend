import { writeFile } from 'node:fs/promises';

import tmp from 'tmp';
import { Database } from 'duckdb-async';
import iconv from 'iconv-lite';
import detectCharacterEncoding from 'detect-character-encoding';

import { Dataset } from '../entities/dataset/dataset';
import { FileImportInterface } from '../entities/dataset/file-import.interface';
import { FileType } from '../enums/file-type';

import { logger } from './logger';
import { getFileService } from './get-file-service';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';

export const convertBufferToUTF8 = (buffer: Buffer): Buffer => {
  const fileEncoding = detectCharacterEncoding(buffer)?.encoding;
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

export const getFileImportAndSaveToDisk = async (
  dataset: Dataset,
  importFile: FileImportInterface
): Promise<string> => {
  const fileService = getFileService();
  const importTmpFile = tmp.tmpNameSync({ postfix: `.${importFile.fileType}` });
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
      throw new FileValidationException(
        `File type is unknown or not supported`,
        FileValidationErrorType.UnknownMimeType
      );
  }
  logger.debug(`Creating table ${tableName} from ${fileImport.fileType} file`);
  await quack.exec(createTableQuery);
};
