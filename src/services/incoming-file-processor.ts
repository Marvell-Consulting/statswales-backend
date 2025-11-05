import fs from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';

import { DuckDBResultReader } from '@duckdb/node-api';
import { format as pgformat } from '@scaleleap/pg-format';

import { logger as parentLogger } from '../utils/logger';
import { DataTable } from '../entities/dataset/data-table';
import { ColumnHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetRepository } from '../repositories/dataset';
import { FileType } from '../enums/file-type';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { DataTableAction } from '../enums/data-table-action';
import { duckdb, DuckDBDatabases } from './duckdb';
import { getFileService } from '../utils/get-file-service';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';
import { DuckDBException } from '../exceptions/duckdb-exception';
import { viewErrorGenerators, viewGenerator } from '../utils/view-error-generators';
import { validateParams } from '../validators/preview-validator';
import { SourceLocation } from '../enums/source-location';
import { UploadTableType } from '../interfaces/upload-table-type';
import { TempFile } from '../interfaces/temp-file';
import { dbManager } from '../db/database-manager';

const logger = parentLogger.child({ module: 'CSVProcessor' });

function getCreateTableQuery(fileType: FileType): string {
  let fileHandlerFunction = '%L';
  switch (fileType) {
    case FileType.Csv:
    case FileType.GzipCsv:
      fileHandlerFunction =
        "read_csv(%L, auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR'], encoding = %L, sample_size = -1)";
      break;
    case FileType.Json:
    case FileType.GzipJson:
      fileHandlerFunction = `read_json_auto(%L)`;
      break;
    case FileType.Excel:
      fileHandlerFunction = `read_xlsx(%L)`;
      break;
  }
  return `CREATE TEMPORARY TABLE %I AS SELECT * FROM ${fileHandlerFunction};`;
}

export async function validateFileAndExtractTableInfo(
  file: TempFile,
  dataTable: DataTable,
  type: 'data_table' | 'lookup_table'
): Promise<DataTableDescription[]> {
  const temporaryTableName: string = randomUUID();
  const quack = await duckdb();
  let tableHeaders: DuckDBResultReader;
  const createTableQuery = getCreateTableQuery(dataTable.fileType);

  try {
    logger.debug(`Attempting to read the file using duckdb`);
    if (dataTable.fileType === FileType.Csv) {
      try {
        dataTable.encoding = 'utf-8';
        await quack.run(pgformat(createTableQuery, temporaryTableName, file.path, dataTable.encoding));
      } catch (err) {
        dataTable.encoding = 'latin-1';
        logger.warn(err, 'Failed to import file into duckDB with UTF-8 encoding trying latin-1');
        await quack.run(pgformat(createTableQuery, temporaryTableName, file.path, dataTable.encoding));
      }
    } else {
      await quack.run(pgformat(createTableQuery, temporaryTableName, file.path));
    }
  } catch (error) {
    logger.error(error, `Something went wrong trying to extract table information using DuckDB.`);
    logger.debug('Releasing duckdb connection');
    quack.disconnectSync();

    if ((error as DuckDBException).stack.includes('Invalid unicode')) {
      throw new FileValidationException(`File encoding is not supported`, FileValidationErrorType.InvalidUnicode);
    } else if ((error as DuckDBException).stack.includes('CSV Error on Line')) {
      throw new FileValidationException(`Errors in CSV file`, FileValidationErrorType.InvalidCsv);
    }
    throw new FileValidationException(
      `Unknown error occurred, please refer to the log for more information`,
      FileValidationErrorType.unknown
    );
  }

  const statements: string[] = ['BEGIN TRANSACTION;'];
  if (type === 'data_table') {
    statements.push(
      ...[
        pgformat(`DROP TABLE IF EXISTS %I.%I;`, DuckDBDatabases.DataTables, dataTable.id),
        pgformat(
          'CREATE TABLE %I.%I AS SELECT * FROM %I;',
          DuckDBDatabases.DataTables,
          dataTable.id,
          temporaryTableName
        )
      ]
    );
  } else {
    statements.push(
      ...[
        pgformat(`DROP TABLE IF EXISTS %I.%I;`, DuckDBDatabases.LookupTables, `${dataTable.id}_tmp`),
        pgformat(
          'CREATE TABLE %I.%I AS SELECT * FROM %I;',
          DuckDBDatabases.LookupTables,
          `${dataTable.id}_tmp`,
          temporaryTableName
        )
      ]
    );
  }
  statements.push('END TRANSACTION;');

  try {
    logger.debug(`Copying data table to postgres ${type} schema using id: ${dataTable.id}`);
    logger.trace(`Running query to create ${type}:\n\n${statements.join('\n')}\n\n`);
    await quack.run(statements.join('\n'));
  } catch (error) {
    logger.error(error, 'Something went wrong saving %{type} to postgres');
    quack.disconnectSync();
    throw new FileValidationException(
      `Unknown error occurred, please refer to the log for more information: ${JSON.stringify(error)}`,
      FileValidationErrorType.unknown
    );
  }

  try {
    tableHeaders = await quack.runAndReadAll(
      pgformat(
        `SELECT (row_number() OVER ())-1 as index, column_name, column_type FROM (DESCRIBE %I);`,
        temporaryTableName
      )
    );
    await quack.run(pgformat('DROP TABLE IF EXISTS %I;', temporaryTableName));
  } catch (error) {
    logger.error(error, 'Something went wrong trying to extract table information using DuckDB.');
    throw new FileValidationException(
      `Unknown error occurred, please refer to the log for more information`,
      FileValidationErrorType.unknown
    );
  } finally {
    quack.disconnectSync();
  }

  if (tableHeaders.getRows().length === 0) {
    throw new FileValidationException(`Failed to parse CSV into columns`, FileValidationErrorType.InvalidCsv);
  }

  if (tableHeaders.getRows().length === 1 && dataTable.fileType === FileType.Csv) {
    throw new FileValidationException(`Failed to parse CSV into columns`, FileValidationErrorType.InvalidCsv);
  }

  return tableHeaders.getRowObjectsJson().map((header) => {
    const info = new DataTableDescription();
    info.columnName = header.column_name as string;
    info.columnIndex = header.index as number;
    info.columnDatatype = header.column_type as string;
    return info;
  });
}

export const validateAndUpload = async (
  file: TempFile,
  datasetId: string,
  revisionId: string,
  type: UploadTableType
): Promise<DataTable> => {
  const { mimetype, originalname } = file;

  const dataTable = new DataTable();
  dataTable.id = randomUUID().toLowerCase();
  dataTable.mimeType = mimetype;
  dataTable.originalFilename = originalname;

  let extension: string;

  switch (mimetype) {
    case 'application/csv':
    case 'text/csv':
      extension = 'csv';
      dataTable.fileType = FileType.Csv;
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
      switch (originalname.split('.').reverse()[1]) {
        case 'json':
          extension = 'json.gz';
          dataTable.fileType = FileType.GzipJson;
          break;
        case 'csv':
          extension = 'csv.gz';
          dataTable.fileType = FileType.GzipCsv;
          break;
        default:
          throw new FileValidationException(
            `unsupported format ${originalname.split('.').reverse()[1]}`,
            FileValidationErrorType.UnknownFileFormat
          );
      }
      break;

    default:
      logger.error(`Unknown mimetype of ${mimetype}`);
      throw new FileValidationException(
        `Mimetype ${mimetype} is unknown or not supported`,
        FileValidationErrorType.UnknownMimeType
      );
  }

  logger.debug('Extracting table information from file');
  const dataTableDescriptions = await validateFileAndExtractTableInfo(file, dataTable, type);

  logger.debug('User file read successfully and loaded in Postgres, saving copy to blob storage.');
  dataTable.dataTableDescriptions = dataTableDescriptions;
  dataTable.filename = `${dataTable.id}.${extension}`;
  dataTable.action = DataTableAction.AddRevise;
  if (type) dataTable.sourceLocation = SourceLocation.Postgres;
  const hash = createHash('sha256');

  dataTable.hash = await new Promise((resolve, reject) => {
    const stream = fs.createReadStream(file.path);
    stream.on('error', (err) => reject(err));
    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
  });

  try {
    const uploadStream = fs.createReadStream(file.path);
    const fileService = getFileService();
    await fileService.saveStream(dataTable.filename, datasetId, uploadStream);
  } catch (err) {
    logger.error(err, `Something went wrong trying to upload the file to the Data Lake`);
    throw new FileValidationException('Error uploading file to blob storage', FileValidationErrorType.datalake, 500);
  }

  dataTable.uploadedAt = new Date();

  return dataTable;
};

export const getFilePreview = async (
  datasetId: string,
  dataTable: DataTable,
  page: number,
  size: number
): Promise<ViewDTO | ViewErrDTO> => {
  const totalsQuery = pgformat(`SELECT count(*) AS total_lines FROM %I.%I;`, 'data_tables', dataTable.id);
  const totalsQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  let totals: { total_lines: number }[];
  try {
    totals = await totalsQueryRunner.query(totalsQuery);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to get totals for data table preview');
    return viewErrorGenerators(500, datasetId, 'csv', 'errors.preview.preview_failed', {});
  } finally {
    void totalsQueryRunner.release();
  }

  const totalLines = Number(totals[0].total_lines);
  const totalPages = Math.max(1, Math.ceil(totalLines / size));
  const errors = validateParams(page, totalPages, size);

  if (errors.length > 0) {
    return { status: 400, errors, dataset_id: datasetId };
  }

  const previewQuery = pgformat(
    `SELECT * FROM (SELECT row_number() OVER () as int_line_number, * FROM %I.%I) LIMIT %L OFFSET %L`,
    'data_tables',
    dataTable.id,
    size,
    (page - 1) * size
  );
  let preview: Record<string, never>[];
  const previewQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    preview = await previewQueryRunner.query(previewQuery);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to get data table preview');
    return viewErrorGenerators(500, datasetId, 'csv', 'errors.preview.preview_failed', {});
  } finally {
    void previewQueryRunner.release();
  }

  const startLine = Number(preview[0].int_line_number);
  const lastLine = Number(preview[preview.length - 1].int_line_number);
  const tableHeaders = Object.keys(preview[0]);
  const dataArray = preview.map((row: Record<string, never>) => Object.values(row));

  const dataset = await DatasetRepository.getById(datasetId, { factTable: true });
  const currentImport = await DataTable.findOneByOrFail({ id: dataTable.id });

  const headers: ColumnHeader[] = tableHeaders.map((header, idx) => {
    let sourceType: FactTableColumnType;

    if (header === 'int_line_number') {
      sourceType = FactTableColumnType.LineNumber;
    } else {
      sourceType =
        dataset.factTable?.find((info) => info.columnName === header)?.columnType || FactTableColumnType.Unknown;
    }
    return { name: header, index: idx - 1, source_type: sourceType };
  });

  const pageInfo = { total_records: totalLines, start_record: startLine, end_record: lastLine };
  return viewGenerator(dataset, page, pageInfo, size, totalPages, headers, dataArray, currentImport);
};
