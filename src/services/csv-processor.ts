import fs from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';

import { Database, TableData } from 'duckdb-async';
import { format as pgformat } from '@scaleleap/pg-format';

import { logger as parentLogger } from '../utils/logger';
import { DataTable } from '../entities/dataset/data-table';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetRepository } from '../repositories/dataset';
import { FileType } from '../enums/file-type';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { DataTableAction } from '../enums/data-table-action';
import { duckdb, linkToPostgresSchema } from './duckdb';
import { getFileService } from '../utils/get-file-service';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';
import { DuckDBException } from '../exceptions/duckdb-exception';
import { viewErrorGenerators, viewGenerator } from '../utils/view-error-generators';
import { validateParams } from '../validators/preview-validator';
import { SourceLocation } from '../enums/source-location';
import { UploadTableType } from '../interfaces/upload-table-type';
import { TempFile } from '../interfaces/temp-file';
import { dbManager } from '../db/database-manager';

const sampleSize = 5;

const logger = parentLogger.child({ module: 'CSVProcessor' });

const getCreateTableQuery = async (fileType: FileType, quack: Database): Promise<string> => {
  switch (fileType) {
    case FileType.Csv:
    case FileType.GzipCsv:
      return `
        CREATE TABLE %I AS
          SELECT *
          FROM read_csv(%L, auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR'], encoding = %L, sample_size = -1);
      `;

    case FileType.Parquet:
      return `CREATE TABLE %I AS SELECT * FROM %L;`;

    case FileType.Json:
    case FileType.GzipJson:
      return `CREATE TABLE %I AS SELECT * FROM read_json_auto(%L);`;

    case FileType.Excel:
      await quack.exec('INSTALL spatial;');
      await quack.exec('LOAD spatial;');
      return `CREATE TABLE %I AS SELECT * FROM st_read(%L);`;

    default:
      throw new Error('Unknown file type');
  }
};

export async function extractTableInformation(
  file: TempFile,
  dataTable: DataTable,
  type: 'data_table' | 'lookup_table'
): Promise<DataTableDescription[]> {
  let tableName = 'preview_table';
  const quack = await duckdb();
  let tableHeaders: TableData;
  let createTableQuery: string;

  try {
    createTableQuery = await getCreateTableQuery(dataTable.fileType, quack);
  } catch (error) {
    logger.error(error, 'Something went wrong creating a temporary file for DuckDB');
    throw new FileValidationException(
      `Failed to create temporary file for DuckDB processing`,
      FileValidationErrorType.unknown
    );
  }

  try {
    logger.debug(`Creating base fact table`);
    if (dataTable.fileType === FileType.Csv) {
      try {
        dataTable.encoding = 'utf-8';
        await quack.exec(pgformat(createTableQuery, tableName, file.path, dataTable.encoding));
      } catch (err) {
        dataTable.encoding = 'latin-1';
        logger.warn(err, 'Failed to import file into duckDB with UTF-8 encoding trying latin-1');
        await quack.exec(pgformat(createTableQuery, tableName, file.path, dataTable.encoding));
      }
    } else {
      await quack.exec(pgformat(createTableQuery, tableName, file.path));
    }
  } catch (error) {
    logger.error(error, `Something went wrong trying to extract table information using DuckDB.`);
    logger.debug('Closing DuckDB Memory Database');
    await quack.close();

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

  if (type === 'data_table') {
    try {
      logger.debug(`Copying data table to postgres using data table id: ${dataTable.id}`);
      await linkToPostgresSchema(quack, 'data_tables');
      await quack.exec(pgformat(`DROP TABLE IF EXISTS %I;`, dataTable.id));
      if (dataTable.fileType === FileType.Csv) {
        await quack.exec(pgformat(createTableQuery, dataTable.id, file.path, dataTable.encoding));
      } else {
        await quack.exec(pgformat(createTableQuery, dataTable.id, file.path));
      }
      tableName = dataTable.id;
    } catch (error) {
      logger.error(error, 'Something went wrong saving data table to postgres');
      await quack.close();
    }
  }

  try {
    tableHeaders = await quack.all(
      pgformat(`SELECT (row_number() OVER ())-1 as index, column_name, column_type FROM (DESCRIBE %I);`, tableName)
    );
  } catch (error) {
    logger.error(error, 'Something went wrong trying to extract table information using DuckDB.');
    throw new FileValidationException(
      `Unknown error occurred, please refer to the log for more information`,
      FileValidationErrorType.unknown
    );
  } finally {
    await quack.close();
  }

  if (tableHeaders.length === 0) {
    throw new FileValidationException(`Failed to parse CSV into columns`, FileValidationErrorType.InvalidCsv);
  }

  if (tableHeaders.length === 1 && dataTable.fileType === FileType.Csv) {
    throw new FileValidationException(`Failed to parse CSV into columns`, FileValidationErrorType.InvalidCsv);
  }

  return tableHeaders.map((header) => {
    const info = new DataTableDescription();
    info.columnName = header.column_name;
    info.columnIndex = header.index;
    info.columnDatatype = header.column_type;
    return info;
  });
}

export const validateAndUpload = async (
  file: TempFile,
  datasetId: string,
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

  let dataTableDescriptions: DataTableDescription[];

  try {
    logger.debug('Extracting table information from file');
    dataTableDescriptions = await extractTableInformation(file, dataTable, type);
  } catch (error) {
    logger.error(error, `Something went wrong trying to read the users upload.`);
    // Error is of type FileValidationException
    throw error;
  }

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

export const getCSVPreview = async (
  datasetId: string,
  dataTable: DataTable,
  page: number,
  size: number
): Promise<ViewDTO | ViewErrDTO> => {
  let tableName = 'fact_table';

  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();

  try {
    await cubeDB.query(pgformat(`SET search_path TO %I;`, 'data_tables'));
    logger.debug('Getting table query from postgres');
    tableName = dataTable.id;
    const totalsQuery = pgformat(
      `SELECT count(*) as total_lines, ceil(count(*)/%L) as total_pages from %I;`,
      size,
      tableName
    );
    logger.debug(`Getting total lines and pages using query ${totalsQuery}`);
    const totals: { total_lines: number; total_pages: number }[] = await cubeDB.query(totalsQuery);
    const totalPages = Number(totals[0].total_pages) === 0 ? 1 : Number(totals[0].total_pages);
    const totalLines = Number(totals[0].total_lines);
    const errors = validateParams(page, totalPages, size);

    if (errors.length > 0) {
      return { status: 400, errors, dataset_id: datasetId };
    }

    const previewQuery = pgformat(
      `
      SELECT *
      FROM (SELECT row_number() OVER () as int_line_number, * FROM %I)
      LIMIT %L
      OFFSET %L
    `,
      tableName,
      size,
      (page - 1) * size
    );

    const preview = await cubeDB.query(previewQuery);
    await cubeDB.query(pgformat(`SET search_path TO %I;`, 'public'));
    cubeDB.release();

    const startLine = Number(preview[0].int_line_number);
    const lastLine = Number(preview[preview.length - 1].int_line_number);
    const tableHeaders = Object.keys(preview[0]);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const dataArray = preview.map((row: any) => Object.values(row));

    const dataset = await DatasetRepository.getById(datasetId, { factTable: true });
    const currentImport = await DataTable.findOneByOrFail({ id: dataTable.id });

    const headers: CSVHeader[] = tableHeaders.map((header, idx) => {
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
  } catch (error) {
    logger.error(error);
    return viewErrorGenerators(500, datasetId, 'csv', 'errors.preview.preview_failed', {});
  } finally {
    cubeDB.release();
  }
};

export const getFactTableColumnPreview = async (
  dataset: Dataset,
  columnName: string
): Promise<ViewDTO | ViewErrDTO> => {
  logger.debug(`Getting fact table column preview for ${columnName}`);
  const tableName = 'fact_table';
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();

  try {
    await cubeDB.query(pgformat(`SET search_path TO %I;`, dataset.draftRevision!.id));
  } catch (error) {
    logger.error(error, 'Could not find revision schema');
    cubeDB.release();
    return viewErrorGenerators(500, dataset.id, 'csv', 'errors.preview.cube_missing', {});
  }

  try {
    const totals: { total_lines: number }[] = await cubeDB.query(
      pgformat('SELECT COUNT(DISTINCT %I) AS total_lines FROM %I', columnName, tableName)
    );
    const totalLines = totals[0].total_lines;
    const preview = await cubeDB.query(
      pgformat('SELECT DISTINCT %I FROM %I LIMIT %L', columnName, tableName, sampleSize)
    );
    const tableHeaders = Object.keys(preview[0]);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const dataArray = preview.map((row: any) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
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
    const pageInfo = {
      total_records: totalLines,
      start_record: 1,
      end_record: preview.length
    };
    const pageSize = preview.length < sampleSize ? preview.length : sampleSize;
    return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
  } catch (error) {
    logger.error(error);
    return viewErrorGenerators(500, dataset.id, 'csv', 'dimension.preview.failed_to_preview_column', {});
  } finally {
    cubeDB.release();
  }
};
