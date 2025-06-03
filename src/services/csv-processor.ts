import { createHash, randomUUID } from 'node:crypto';
import fs from 'fs';

import { TableData } from 'duckdb-async';
import { format as pgformat } from '@scaleleap/pg-format';
import tmp from 'tmp';

import { logger as parentLogger } from '../utils/logger';
import { DataTable } from '../entities/dataset/data-table';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetRepository } from '../repositories/dataset';
import { FileType } from '../enums/file-type';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { DataTableAction } from '../enums/data-table-action';
import { convertBufferToUTF8 } from '../utils/file-utils';

import { duckdb, linkToPostgres, linkToPostgresDataTables } from './duckdb';
import { getFileService } from '../utils/get-file-service';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';
import { DuckDBException } from '../exceptions/duckdb-exception';
import { viewErrorGenerators, viewGenerator } from '../utils/view-error-generators';
import { validateParams } from '../validators/preview-validator';
import { SourceLocation } from '../enums/source-location';

export const DEFAULT_PAGE_SIZE = 100;
const sampleSize = 5;

const logger = parentLogger.child({ module: 'CSVProcessor' });

export async function extractTableInformation(
  fileBuffer: Buffer,
  dataTable: DataTable,
  type: 'data_table' | 'lookup_table'
): Promise<DataTableDescription[]> {
  let tableName = 'preview_table';
  const quack = await duckdb();
  const tempFile = tmp.tmpNameSync({ postfix: `.${dataTable.fileType}` });
  let tableHeaders: TableData;
  let createTableQuery: string;
  fs.writeFileSync(tempFile, fileBuffer);
  switch (dataTable.fileType) {
    case FileType.Csv:
    case FileType.GzipCsv:
      createTableQuery = `CREATE TABLE %I AS SELECT * FROM read_csv(%L, auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR']);`;
      break;
    case FileType.Parquet:
      createTableQuery = `CREATE TABLE %I AS SELECT * FROM %L;`;
      break;
    case FileType.Json:
    case FileType.GzipJson:
      createTableQuery = `CREATE TABLE %I AS SELECT * FROM read_json_auto(%L);`;
      break;
    case FileType.Excel:
      await quack.exec('INSTALL spatial;');
      await quack.exec('LOAD spatial;');
      createTableQuery = `CREATE TABLE %I AS SELECT * FROM st_read(%L);`;
      break;
    default:
      throw new Error('Unknown file type');
  }
  try {
    logger.debug(`Executing query to create base fact table: ${pgformat(createTableQuery, tableName, tempFile)}`);
    await quack.exec(pgformat(createTableQuery, tableName, tempFile));
  } catch (error) {
    logger.error(error, `Something went wrong trying to extract table information using DuckDB.`);
    logger.debug('Closing DuckDB Memory Database');
    await quack.close();
    logger.debug(`Removing temp file ${tempFile} from disk`);
    fs.unlinkSync(tempFile);
    if ((error as DuckDBException).stack.includes('Invalid unicode')) {
      throw new FileValidationException(`File is encoding is not supported`, FileValidationErrorType.InvalidUnicode);
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
      await linkToPostgresDataTables(quack);
      await quack.exec(pgformat(createTableQuery, dataTable.id, tempFile));
      tableName = dataTable.id;
    } catch (error) {
      logger.error(error, 'Something went wrong saving data table to postgres');
      await quack.close();
      fs.unlinkSync(tempFile);
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
    fs.unlinkSync(tempFile);
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

export const validateAndUploadCSV = async (
  fileBuffer: Buffer,
  filetype: string,
  originalName: string,
  datasetId: string,
  type: 'data_table' | 'lookup_table'
): Promise<{ dataTable: DataTable; buffer: Buffer }> => {
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
      uploadBuffer = convertBufferToUTF8(fileBuffer);
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
          throw new FileValidationException(
            `unsupported format ${originalName.split('.').reverse()[1]}`,
            FileValidationErrorType.UnknownFileFormat
          );
      }
      break;
    default:
      logger.error(`Unknown mimetype of ${filetype}`);
      throw new FileValidationException(
        `Mimetype ${filetype} is unknown or not supported`,
        FileValidationErrorType.UnknownMimeType
      );
  }

  let dataTableDescriptions: DataTableDescription[];

  try {
    logger.debug('Extracting table information from file');
    dataTableDescriptions = await extractTableInformation(uploadBuffer, dataTable, type);
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
  hash.update(uploadBuffer);

  try {
    const fileService = getFileService();
    await fileService.saveBuffer(dataTable.filename, datasetId, uploadBuffer);
  } catch (err) {
    logger.error(err, `Something went wrong trying to upload the file to the Data Lake`);
    throw new FileValidationException('Error uploading file to blob storage', FileValidationErrorType.datalake, 500);
  }

  dataTable.hash = hash.digest('hex');
  dataTable.uploadedAt = new Date();
  return { dataTable: dataTable, buffer: uploadBuffer };
};

export const getCSVPreview = async (
  datasetId: string,
  importObj: DataTable,
  page: number,
  size: number
): Promise<ViewDTO | ViewErrDTO> => {
  let tableName = 'fact_table';
  const tempFile = tmp.tmpNameSync({ postfix: `.${importObj.fileType}` });
  let cubeFile: string | undefined;

  logger.debug('Getting table query from postgres');
  const quack = await duckdb();
  await linkToPostgresDataTables(quack);
  tableName = importObj.id;

  try {
    const totalsQuery = pgformat(
      `SELECT count(*) as totalLines, ceil(count(*)/%L) as totalPages from %I;`,
      size,
      tableName
    );
    const totals = await quack.all(totalsQuery);
    const totalPages = Number(totals[0].totalPages);
    const totalLines = Number(totals[0].totalLines);
    const errors = validateParams(page, totalPages, size);

    if (errors.length > 0) {
      return { status: 400, errors, dataset_id: datasetId };
    }

    const previewQuery = pgformat(
      `
      SELECT int_line_number, *
      FROM (SELECT row_number() OVER () as int_line_number, * FROM %I)
      LIMIT %L
      OFFSET %L
    `,
      tableName,
      size,
      (page - 1) * size
    );

    const preview = await quack.all(previewQuery);
    const startLine = Number(preview[0].int_line_number);
    const lastLine = Number(preview[preview.length - 1].int_line_number);
    const tableHeaders = Object.keys(preview[0]);
    const dataArray = preview.map((row) => Object.values(row));
    const dataset = await DatasetRepository.getById(datasetId, { factTable: true });
    const currentImport = await DataTable.findOneByOrFail({ id: importObj.id });

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
    await quack.close();
    if (cubeFile) fs.unlinkSync(cubeFile);
    if (fs.existsSync(tempFile)) fs.unlinkSync(tempFile);
  }
};

export const getFactTableColumnPreview = async (
  dataset: Dataset,
  columnName: string
): Promise<ViewDTO | ViewErrDTO> => {
  logger.debug(`Getting fact table column preview for ${columnName}`);
  const tableName = 'fact_table';
  const quack = await duckdb();
  try {
    await linkToPostgres(quack, dataset.draftRevision!.id, false);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to link to postgres database');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.cube_builder.fact_table_creation_failed', {});
  }
  try {
    const totals = await quack.all(`SELECT COUNT(DISTINCT "${columnName}") AS "totalLines" FROM ${tableName};`);
    const totalLines = Number(totals[0].totalLines);
    const previewQuery = `SELECT DISTINCT "${columnName}" FROM ${tableName} LIMIT ${sampleSize}`;
    const preview = await quack.all(previewQuery);
    const tableHeaders = Object.keys(preview[0]);
    const dataArray = preview.map((row) => Object.values(row));
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
    await quack.close();
  }
};
