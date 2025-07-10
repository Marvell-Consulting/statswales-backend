import { readFile, unlink } from 'node:fs/promises';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

import { from } from 'pg-copy-streams';
import { Database, DuckDbError, RowData } from 'duckdb-async';
import { t } from 'i18next';
import { FindOptionsRelations } from 'typeorm';
import { toZonedTime } from 'date-fns-tz';
import { format as pgformat } from '@scaleleap/pg-format';

import { FileType } from '../enums/file-type';
import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import { getFileImportAndSaveToDisk } from '../utils/file-utils';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { DataTable } from '../entities/dataset/data-table';
import { DataTableAction } from '../enums/data-table-action';
import { Revision } from '../entities/dataset/revision';
import { Locale } from '../enums/locale';
import { DimensionType } from '../enums/dimension-type';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { ReferenceDataExtractor } from '../extractors/reference-data-extractor';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { CubeValidationException } from '../exceptions/cube-error-exception';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { MeasureRow } from '../entities/dataset/measure-row';
import { DatasetRepository } from '../repositories/dataset';
import { PeriodCovered } from '../interfaces/period-covered';

import { dateDimensionReferenceTableCreator } from './time-matching';
import { duckdb, linkToPostgresSchema, safelyCloseDuckDb } from './duckdb';
import { NumberExtractor, NumberType } from '../extractors/number-extractor';
import { CubeValidationType } from '../enums/cube-validation-type';
import { languageMatcherCaseStatement } from '../utils/lookup-table-utils';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import { FactTableValidationExceptionType } from '../enums/fact-table-validation-exception-type';
import { CubeType } from '../enums/cube-type';
import { DateExtractor } from '../extractors/date-extractor';
import { PoolClient, QueryResult } from 'pg';
import { getCubeDB } from '../db/cube-db';
import { getFileService } from '../utils/get-file-service';
import { asyncTmpName } from '../utils/async-tmp';
import { performanceReporting } from '../utils/performance-reporting';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { StorageService } from '../interfaces/storage-service';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { FilterInterface } from '../interfaces/filterInterface';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { createFrontendView } from './consumer-view';
import fs from 'node:fs';
import { LookupTable } from '../entities/dataset/lookup-table';
import { pipeline } from 'node:stream/promises';

export const FACT_TABLE_NAME = 'fact_table';

export const makeCubeSafeString = (str: string): string => {
  return str
    .toLowerCase()
    .replace(/[ ]/g, '_')
    .replace(/[^a-zA-Z_]/g, '');
};

export const createDataTableQuery = async (
  tableName: string,
  tempFileName: string,
  fileType: FileType,
  quack: Database
): Promise<string> => {
  logger.debug(`Creating data table ${tableName} with file ${tempFileName} and file type ${fileType}`);
  switch (fileType) {
    case FileType.Csv:
    case FileType.GzipCsv:
      return pgformat(
        "CREATE TABLE %I AS SELECT * FROM read_csv(%L, auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR'], sample_size = -1);",
        makeCubeSafeString(tableName),
        tempFileName
      );
    case FileType.Parquet:
      return pgformat('CREATE TABLE %I AS SELECT * FROM %L;', makeCubeSafeString(tableName), tempFileName);
    case FileType.Json:
    case FileType.GzipJson:
      return pgformat(
        'CREATE TABLE %I AS SELECT * FROM read_json_auto(%L);',
        makeCubeSafeString(tableName),
        tempFileName
      );
    case FileType.Excel:
      await quack.exec('INSTALL spatial;');
      await quack.exec('LOAD spatial;');
      return pgformat('CREATE TABLE %I AS SELECT * FROM st_read(%L);', makeCubeSafeString(tableName), tempFileName);
    default:
      throw new Error('Unknown file type');
  }
};

export const loadFileIntoCube = async (
  quack: Database,
  fileType: FileType,
  tempFile: string,
  tableName: string
): Promise<void> => {
  logger.debug(`Loading file in to DuckDB`);
  const insertQuery = await createDataTableQuery(tableName, tempFile, fileType, quack);
  try {
    await quack.exec(insertQuery);
  } catch (error) {
    logger.error(`Failed to load file in to DuckDB using query ${insertQuery} with the following error: ${error}`);
    throw error;
  }
};

export const loadTableDataIntoFactTableFromPostgres = async (
  connection: PoolClient,
  factTableDef: string[],
  factTableName: string,
  dataTableId: string
): Promise<void> => {
  logger.debug(`Loading data table ${dataTableId} from data_tables schema into cube fact table`);
  const insertQuery = pgformat(
    'INSERT INTO %I SELECT %I FROM %I.%I;',
    factTableName,
    factTableDef,
    'data_tables',
    dataTableId
  );
  try {
    await connection.query(insertQuery);
  } catch (error) {
    logger.error(error, `Failed to load file into table using query ${insertQuery}`);
    throw new FactTableValidationException(
      'An unknown error occurred trying to load data in to the fact table.  Please contact support.',
      FactTableValidationExceptionType.UnknownError,
      500
    );
  }
  logger.debug(`Successfully loaded data table into fact table`);
};

export const loadTableDataIntoFactTable = async (
  quack: Database,
  factTableDef: string[],
  factTableName: string,
  originTableName: string
): Promise<void> => {
  const tableSize = await quack.all(
    pgformat('SELECT CAST (COUNT(*) AS INTEGER) as table_size FROM %I;', originTableName)
  );
  const rowCount = tableSize[0].table_size;
  if (rowCount === 0) {
    logger.debug(`No data to load into ${factTableName}`);
    return;
  }
  logger.debug(`Loading data table into fact table`);
  const batchSize = 200000;
  let processedRows = 0;
  let insertQuery = pgformat(
    'INSERT INTO %I SELECT %I FROM %I LIMIT %L OFFSET ?;',
    factTableName,
    factTableDef,
    originTableName,
    batchSize,
    processedRows
  );
  try {
    while (processedRows < rowCount) {
      insertQuery = pgformat(
        'INSERT INTO %I SELECT %I FROM %I LIMIT %L OFFSET %L;',
        factTableName,
        factTableDef,
        originTableName,
        batchSize,
        processedRows
      );
      await quack.exec(insertQuery);

      processedRows += batchSize;
      const currentRows = Math.min(processedRows, rowCount);
      const percentComplete = Math.round((currentRows / rowCount) * 100);
      logger.debug(`↳ Copied ${currentRows}/${rowCount} rows (${percentComplete}%)`);
      if (processedRows >= rowCount) break;
    }
  } catch (error) {
    logger.error(error, `Failed to load file into table using query ${insertQuery}`);
    const duckDBError = error as DuckDbError;
    if (duckDBError.errorType === 'Constraint') {
      if (duckDBError.message.includes('NOT NULL constraint')) {
        throw new FactTableValidationException(
          'Fact with empty value in column(s) found in fact table.  Please check the data and try again.',
          FactTableValidationExceptionType.IncompleteFact,
          400
        );
      }
      if (duckDBError.message.includes('PRIMARY KEY or UNIQUE')) {
        throw new FactTableValidationException(
          'Dupllicate facts found in the fact table.  Please check the data and try again.',
          FactTableValidationExceptionType.DuplicateFact,
          400
        );
      }
      if (duckDBError.message.includes('Duplicate key')) {
        throw new FactTableValidationException(
          'Duplicate facts found in the fact table.  Please check the data and try again.',
          FactTableValidationExceptionType.DuplicateFact,
          400
        );
      }
    }
    throw new FactTableValidationException(
      'An unknown error occurred trying to load data in to the fact table.  Please contact support.',
      FactTableValidationExceptionType.UnknownError,
      500
    );
  }
  logger.debug(`Successfully loaded data table into fact table`);
};

// This function differs from loadFileIntoDatabase in that it only loads a file into an existing table
export const loadFileDataTableIntoTable = async (
  quack: Database,
  dataTable: DataTable,
  factTableDef: string[],
  tempFile: string,
  tableName: string
): Promise<void> => {
  const tempTableName = `temp_${tableName}`;
  let insertQuery: string;
  const dataTableColumnSelect: string[] = [];
  for (const factTableCol of factTableDef) {
    const dataTableCol = dataTable.dataTableDescriptions.find(
      (col) => col.factTableColumn === factTableCol
    )?.columnName;
    if (dataTableCol) dataTableColumnSelect.push(dataTableCol);
    else dataTableColumnSelect.push(factTableCol);
  }

  switch (dataTable.fileType) {
    case FileType.Csv:
    case FileType.GzipCsv:
      insertQuery = pgformat(
        "CREATE TABLE %I AS SELECT %I FROM read_csv(%L, auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR'], sample_size = -1);",
        tempTableName,
        dataTableColumnSelect,
        tempFile
      );
      break;
    case FileType.Parquet:
      insertQuery = pgformat('CREATE TABLE %I AS SELECT %I FROM %L;', tempTableName, dataTableColumnSelect, tempFile);
      break;
    case FileType.Json:
    case FileType.GzipJson:
      insertQuery = pgformat(
        'CREATE TABLE %I AS SELECT %I FROM read_json_auto(%L);',
        tempTableName,
        dataTableColumnSelect,
        tempFile
      );
      break;
    case FileType.Excel:
      await quack.exec('INSTALL spatial;');
      await quack.exec('LOAD spatial;');
      insertQuery = pgformat(
        'CREATE TABLE %I AS SELECT %I FROM st_read(%L);',
        tempTableName,
        dataTableColumnSelect,
        tempFile
      );
      break;
    default:
      throw new FactTableValidationException(
        'Fact with empty value in column(s) found in fact table.  Please check the data and try again.',
        FactTableValidationExceptionType.UnknownFileType,
        500
      );
  }
  try {
    logger.debug(`Loading file data table into table ${tableName} with query: ${insertQuery}`);
    await quack.exec(insertQuery);
    await loadTableDataIntoFactTable(quack, factTableDef, tableName, tempTableName);
    await quack.exec(pgformat('DROP TABLE %I', tempTableName));
    await quack.exec('CHECKPOINT;');
  } catch (error) {
    logger.error(error, `Failed to load file into table using query ${insertQuery}`);
    const duckDBError = error as DuckDbError;
    if (duckDBError.errorType === 'Constraint') {
      if (duckDBError.message.includes('NOT NULL constraint')) {
        throw new FactTableValidationException(
          'Fact with empty value in column(s) found in fact table.  Please check the data and try again.',
          FactTableValidationExceptionType.IncompleteFact,
          400
        );
      }
      if (duckDBError.message.includes('PRIMARY KEY or UNIQUE')) {
        throw new FactTableValidationException(
          'Dupllicate facts found in the fact table.  Please check the data and try again.',
          FactTableValidationExceptionType.DuplicateFact,
          400
        );
      }
      if (duckDBError.message.includes('Duplicate key')) {
        throw new FactTableValidationException(
          'Duplicate facts found in the fact table.  Please check the data and try again.',
          FactTableValidationExceptionType.DuplicateFact,
          400
        );
      }
    }
    throw new FactTableValidationException(
      'An unknown error occurred trying to load data in to the fact table.  Please contact support.',
      FactTableValidationExceptionType.UnknownError,
      500
    );
  }
};

export async function createReferenceDataTablesInCube(connection: PoolClient): Promise<void> {
  logger.debug('Creating empty reference data tables');
  try {
    logger.debug('Creating categories tables');
    await connection.query(`CREATE TABLE "categories" ("category" VARCHAR, PRIMARY KEY ("category"));`);
    logger.debug('Creating category_keys table');
    await connection.query(`CREATE TABLE "category_keys" (
                            "category_key" VARCHAR PRIMARY KEY,
                            "category" VARCHAR NOT NULL
                            );`);
    logger.debug('Creating reference_data table');
    await connection.query(`CREATE TABLE "reference_data" (
                            "item_id" VARCHAR NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "sort_order" INTEGER,
                            "category_key" VARCHAR NOT NULL,
                            "validity_start" VARCHAR NOT NULL,
                            "validity_end" VARCHAR
                            );`);
    logger.debug('Creating reference_data_all table');
    await connection.query(`CREATE TABLE "reference_data_all" (
                            "item_id" VARCHAR NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "sort_order" INTEGER,
                            "category_key" VARCHAR NOT NULL,
                            "validity_start" VARCHAR NOT NULL,
                            "validity_end" VARCHAR
                            );`);
    logger.debug('Creating reference_data_info table');
    await connection.query(`CREATE TABLE "reference_data_info" (
                            "item_id" VARCHAR NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "category_key" VARCHAR NOT NULL,
                            "lang" VARCHAR NOT NULL,
                            "description" VARCHAR NOT NULL,
                            "notes" VARCHAR
                            );`);
    logger.debug('Creating category_key_info table');
    await connection.query(`CREATE TABLE "category_key_info" (
                            "category_key" VARCHAR NOT NULL,
                            "lang" VARCHAR NOT NULL,
                            "description" VARCHAR NOT NULL,
                            "notes" VARCHAR
                            );`);
    logger.debug('Creating category_info table');
    await connection.query(`CREATE TABLE "category_info" (
                            "category" VARCHAR NOT NULL,
                            "lang" VARCHAR NOT NULL,
                            "description" VARCHAR NOT NULL,
                            "notes" VARCHAR
                            );`);
    logger.debug('Creating hierarchy table');
    await connection.query(`CREATE TABLE "hierarchy" (
                            "item_id" VARCHAR NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "category_key" VARCHAR NOT NULL,
                            "parent_id" VARCHAR NOT NULL,
                            "parent_version" INTEGER NOT NULL,
                            "parent_category" VARCHAR NOT NULL                            );`);
  } catch (error) {
    logger.error(`Something went wrong trying to create the initial reference data tables with error: ${error}`);
    throw new Error(`Something went wrong trying to create the initial reference data tables with error: ${error}`);
  }
}

export async function loadReferenceDataFromCSV(connection: PoolClient): Promise<void> {
  logger.debug(`Loading reference data from CSV`);
  logger.debug(`Loading categories from CSV`);
  const csvFiles = [
    'categories',
    'category_info',
    'category_key_info',
    'category_keys',
    'hierarchy',
    'reference_data_all',
    'reference_data_info'
  ];
  for (const file of csvFiles) {
    logger.debug(`Loading reference data file ${file} from CSV file`);
    const csvPath = path.resolve(__dirname, `../resources/reference-data/v1/${file}.csv`);
    const fileStream = fs.createReadStream(csvPath, { encoding: 'utf8' });
    const pgStream = connection.query(from(`COPY ${file} FROM STDIN WITH (FORMAT csv, HEADER true)`));
    try {
      await pipeline(fileStream, pgStream);
    } catch (err) {
      logger.error(err, `Something went wrong trying to load reference data from JSON file ${file}`);
    }
  }
}

export const loadReferenceDataIntoCube = async (connection: PoolClient): Promise<void> => {
  await createReferenceDataTablesInCube(connection);
  await loadReferenceDataFromCSV(connection);
  logger.debug(`Reference data tables created and populated successfully.`);
};

export const cleanUpReferenceDataTables = async (connection: PoolClient): Promise<void> => {
  await connection.query('DROP TABLE reference_data_all;');
  await connection.query('DELETE FROM reference_data_info WHERE item_id NOT IN (SELECT item_id FROM reference_data);');
  await connection.query(
    'DELETE FROM category_keys WHERE category_key NOT IN (SELECT category_key FROM reference_data);'
  );
  await connection.query(
    'DELETE FROM category_Key_info WHERE category_key NOT IN (select category_key FROM category_keys);'
  );
  await connection.query('DELETE FROM categories where category NOT IN (SELECT category FROM category_keys);');
  await connection.query('DELETE FROM category_info WHERE category NOT IN (SELECT category FROM categories);');
  await connection.query('DELETE FROM hierarchy WHERE item_id NOT IN (SELECT item_id FROM reference_data);');
};

export const loadCorrectReferenceDataIntoReferenceDataTable = async (
  connection: PoolClient,
  dimension: Dimension
): Promise<void> => {
  const extractor = dimension.extractor as ReferenceDataExtractor;
  for (const category of extractor.categories) {
    const categoryPresent = await connection.query(
      pgformat('SELECT DISTINCT category_key FROM reference_data WHERE category_key=%L', category)
    );
    if (categoryPresent.rows.length > 0) {
      continue;
    }
    logger.debug(`Copying ${category} reference data in to reference_data table`);
    await connection.query(
      pgformat('INSERT INTO reference_data (SELECT * FROM reference_data_all WHERE category_key=%L);', category)
    );
  }
};

async function setupReferenceDataDimension(
  connection: PoolClient,
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[]
): Promise<void> {
  await loadCorrectReferenceDataIntoReferenceDataTable(connection, dimension);
  const refDataInfo = `${makeCubeSafeString(dimension.factTableColumn)}_reference_data_info`;
  const refDataTbl = `${makeCubeSafeString(dimension.factTableColumn)}_reference_data`;
  SUPPORTED_LOCALES.map((locale) => {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    viewSelectStatementsMap.get(locale)?.push(pgformat('%I.description AS %I', refDataInfo, columnName));
    rawSelectStatementsMap.get(locale)?.push(pgformat('%I.description AS %I', refDataInfo, columnName));
  });
  joinStatements.push(
    pgformat(
      'LEFT JOIN reference_data AS %I on CAST(%I.%I AS VARCHAR)=%I.item_id',
      refDataTbl,
      FACT_TABLE_NAME,
      dimension.factTableColumn,
      refDataTbl
    )
  );
  joinStatements.push(
    pgformat(`JOIN reference_data_info AS %I ON %I.item_id=%I.item_id`, refDataInfo, refDataTbl, refDataInfo)
  );
  joinStatements.push(pgformat(`    AND %I.category_key=%I.category_key`, refDataTbl, refDataInfo));
  joinStatements.push(pgformat(`    AND %I.version_no=%I.version_no`, refDataTbl, refDataInfo));
  joinStatements.push(pgformat(`    AND %I.lang=#LANG#`, refDataInfo));
  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const query = pgformat(
      `
      INSERT INTO filter_table
      SELECT DISTINCT
        %I as reference,
        %L as language,
        %L as fact_table_column,
        %L as dimension_name,
        reference_data_info.description as description,
        NULL as hierarchy
      FROM fact_table
      LEFT JOIN reference_data on CAST(fact_table.%I AS VARCHAR)=reference_data.item_id
      JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id
      AND reference_data_info.lang=%L;
      `,
      dimension.factTableColumn,
      locale.toLowerCase(),
      dimension.factTableColumn,
      columnName,
      dimension.factTableColumn,
      locale.toLowerCase()
    );
    // logger.debug(`Query = ${query}`);
    await connection.query(query);
  }
}

export const createDatePeriodTableQuery = (factTableColumn: FactTableColumn, tableName?: string): string => {
  if (!tableName) {
    tableName = `${makeCubeSafeString(factTableColumn.columnName)}_lookup`;
  }
  return pgformat(
    `
  CREATE TABLE %I (
    %I %s,
    language VARCHAR(5),
    description VARCHAR,
    hierarchy VARCHAR,
    date_type varchar,
    start_date timestamp,
    end_date timestamp
  );`,
    tableName,
    factTableColumn.columnName,
    factTableColumn.columnDatatype
  );
};

// This is a short version of validate date dimension code found in the dimension processor.
// This concise version doesn't return any information on why the creation failed.  Just that it failed
export async function createDateDimension(
  connection: PoolClient,
  extractor: object | null,
  factTableColumn: FactTableColumn
): Promise<string> {
  if (!extractor) {
    throw new Error('Extractor not supplied');
  }
  const safeColumnName = makeCubeSafeString(factTableColumn.columnName);
  const columnData: QueryResult<RowData> = await connection.query(
    pgformat(`SELECT DISTINCT %I FROM %I;`, factTableColumn.columnName, FACT_TABLE_NAME)
  );
  const dateDimensionTable = dateDimensionReferenceTableCreator(extractor as DateExtractor, columnData.rows);
  await connection.query(createDatePeriodTableQuery(factTableColumn));

  // Create the date_dimension table
  for (const locale of SUPPORTED_LOCALES) {
    logger.debug(`populating ${safeColumnName}_lookup table for locale ${locale}`);
    const lang = locale.toLowerCase();
    for (const row of dateDimensionTable) {
      await connection.query(
        pgformat('INSERT INTO %I VALUES(%L)', `${safeColumnName}_lookup`, [
          row.dateCode,
          lang,
          row.description,
          null,
          t(row.type, { lng: locale }),
          row.start,
          row.end
        ])
      );
    }
  }

  const periodCoverage: QueryResult<{ start_date: Date; end_date: Date }> = await connection.query(
    `SELECT MIN(start_date) AS start_date, MAX(end_date) AS end_date FROM ${safeColumnName}_lookup;`
  );
  const metaDataCoverage: QueryResult<{ key: string; value: string }> = await connection.query(
    "SELECT * FROM metadata WHERE key in ('start_date', 'end_date');"
  );
  logger.debug(`coverage: ${metaDataCoverage.rows.length}`);
  if (metaDataCoverage.rows.length > 0) {
    for (const metaData of metaDataCoverage.rows) {
      if (metaData.key === 'start_date') {
        if (periodCoverage.rows[0].start_date < toZonedTime(metaData.value, 'UTC')) {
          await connection.query(
            `UPDATE metadata SET value='${periodCoverage.rows[0].start_date.toISOString()}' WHERE key='start_date';`
          );
        }
      } else if (metaData.key === 'end_date') {
        if (periodCoverage.rows[0].end_date > toZonedTime(metaData.value, 'UTC')) {
          await connection.query(
            `UPDATE metadata SET value='${periodCoverage.rows[0].start_date.toISOString()}' WHERE key='end_date';`
          );
        }
      }
    }
  } else {
    await connection.query(
      `INSERT INTO metadata (key, value) VALUES ('start_date', '${periodCoverage.rows[0].start_date.toISOString()}');`
    );
    await connection.query(
      `INSERT INTO metadata (key, value) VALUES ('end_date', '${periodCoverage.rows[0].start_date.toISOString()}');`
    );
  }
  return `${makeCubeSafeString(factTableColumn.columnName)}_lookup`;
}

export const createLookupTableQuery = (
  lookupTableName: string,
  referenceColumnName: string,
  referenceColumnType: string
): string => {
  return pgformat(
    'CREATE TABLE %I (%I %s NOT NULL, language VARCHAR(5) NOT NULL, description TEXT NOT NULL, notes TEXT, sort_order INTEGER, hierarchy %s);',
    lookupTableName,
    referenceColumnName,
    referenceColumnType,
    referenceColumnType
  );
};

async function setupLookupTableDimension(
  connection: PoolClient,
  dataset: Dataset,
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[],
  orderByStatements: string[]
): Promise<string> {
  const factTableColumn = dataset.factTable?.find((col) => col.columnName === dimension.factTableColumn);
  if (!factTableColumn) {
    const error = new CubeValidationException(`Fact table column ${dimension.factTableColumn} not found`);
    error.type = CubeValidationType.FactTableColumnMissing;
    error.datasetId = dataset.id;
    throw error;
  }
  const dimTable = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
  await createLookupTableDimension(connection, dataset, dimension, factTableColumn);
  SUPPORTED_LOCALES.map((locale) => {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    viewSelectStatementsMap.get(locale)?.push(`${dimTable}.description as "${columnName}"`);
    rawSelectStatementsMap.get(locale)?.push(`${dimTable}.description as "${columnName}"`);
  });
  joinStatements.push(
    `LEFT JOIN "${dimTable}" on "${dimTable}"."${factTableColumn.columnName}"=${FACT_TABLE_NAME}."${factTableColumn.columnName}" AND "${dimTable}".language=#LANG#`
  );
  orderByStatements.push(`"${dimTable}".sort_order`);
  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    await connection.query(
      pgformat(
        `INSERT INTO filter_table
         SELECT DISTINCT CAST(%I AS VARCHAR), language, %L, %L, description, hierarchy
         FROM %I WHERE language = %L`,
        dimension.factTableColumn,
        dimension.factTableColumn,
        columnName,
        dimTable,
        locale.toLowerCase()
      )
    );
  }
  return dimTable;
}

export async function loadFileIntoLookupTablesSchema(
  dataset: Dataset,
  lookupTable: LookupTable,
  extractor: LookupTableExtractor,
  factTableColumn: FactTableColumn,
  joinColumn: string,
  filePath?: string
): Promise<void> {
  const start = performance.now();
  const quack = await duckdb();
  const dimTable = `${makeCubeSafeString(factTableColumn.columnName)}_lookup`;
  await quack.exec(createLookupTableQuery(dimTable, factTableColumn.columnName, factTableColumn.columnDatatype));
  let lookupTableFile = '';
  if (filePath) {
    lookupTableFile = filePath;
  } else {
    lookupTableFile = await getFileImportAndSaveToDisk(dataset, lookupTable!);
  }
  const lookupTableName = `${makeCubeSafeString(factTableColumn.columnName)}_lookup_draft`;
  await loadFileIntoCube(quack, lookupTable.fileType, lookupTableFile, lookupTableName);
  if (extractor.isSW2Format) {
    logger.debug('Lookup table is SW2 format');
    const dataExtractorParts = [];
    for (const locale of SUPPORTED_LOCALES) {
      const descriptionCol = extractor.descriptionColumns.find(
        (col) => col.lang.toLowerCase() === locale.toLowerCase()
      );
      const notesCol = extractor.notesColumns?.find((col) => col.lang.toLowerCase() === locale.toLowerCase());
      const notesColStr = notesCol ? pgformat('%I', notesCol.name) : 'NULL';
      const sortStr = extractor.sortColumn ? pgformat('%I', extractor.sortColumn) : 'NULL';
      const hierarchyCol = extractor.hierarchyColumn ? pgformat('%I', extractor.hierarchyColumn) : 'NULL';
      dataExtractorParts.push(
        pgformat(
          'SELECT %I AS %I, %L as language, %I as description, %s as notes, %s as sort_order, %s as hierarchy FROM %I',
          joinColumn,
          factTableColumn.columnName,
          locale.toLowerCase(),
          descriptionCol?.name,
          notesColStr,
          sortStr,
          hierarchyCol,
          lookupTableName
        )
      );
    }
    const builtInsertQuery = pgformat(`INSERT INTO %I %s;`, dimTable, dataExtractorParts.join(' UNION '));
    // const builtInsertQuery = `INSERT INTO ${makeCubeSafeString(dimension.factTableColumn)}_lookup (${dataExtractorParts.join(' UNION ')});`;
    // logger.debug(`Built insert query: ${builtInsertQuery}`);
    await quack.exec(builtInsertQuery);
  } else {
    const languageMatcher = languageMatcherCaseStatement(extractor.languageColumn);
    const notesStr = extractor.notesColumns ? pgformat('%I', extractor.notesColumns[0].name) : 'NULL';
    const sortStr = extractor.sortColumn ? pgformat('%I', extractor.sortColumn) : 'NULL';
    const hierarchyStr = extractor.hierarchyColumn ? pgformat('%I', extractor.hierarchyColumn) : 'NULL';
    const dataExtractorParts = pgformat(
      `SELECT %I AS %I, %s as language, %I as description, %s as notes, %s as sort_order, %s as hierarchy FROM %I;`,
      joinColumn,
      factTableColumn.columnName,
      languageMatcher,
      extractor.descriptionColumns[0].name,
      notesStr,
      sortStr,
      hierarchyStr,
      lookupTableName
    );
    const builtInsertQuery = pgformat(`INSERT INTO %I %s`, dimTable, dataExtractorParts);
    await quack.exec(builtInsertQuery);
  }
  logger.debug(`Dropping original lookup table ${lookupTableName}`);
  await quack.exec(pgformat('DROP TABLE %I', lookupTableName));
  await linkToPostgresSchema(quack, 'lookup_tables');
  await quack.exec(pgformat('CREATE TABLE lookup_tables_db.%I AS SELECT * FROM memory.%I;', lookupTable.id, dimTable));
  await quack.close();
  performanceReporting(start - performance.now(), 500, 'Loading a lookup table in to postgres');
}

export async function loadFileIntoDataTablesSchema(
  dataset: Dataset,
  dataTable: DataTable,
  filePath?: string
): Promise<void> {
  const start = performance.now();
  const quack = await duckdb();
  let dataTableFile = '';
  if (filePath) {
    dataTableFile = filePath;
  } else {
    dataTableFile = await getFileImportAndSaveToDisk(dataset, dataTable);
  }
  await loadFileIntoCube(quack, dataTable.fileType, dataTableFile, FACT_TABLE_NAME);
  await linkToPostgresSchema(quack, 'data_tables');
  await quack.exec(
    pgformat('CREATE TABLE data_tables_db.%I AS SELECT * FROM memory.%I;', dataTable.id, FACT_TABLE_NAME)
  );
  await quack.close();
  performanceReporting(start - performance.now(), 500, 'Loading a lookup table in to postgres');
}

export async function createLookupTableDimension(
  connection: PoolClient,
  dataset: Dataset,
  dimension: Dimension,
  factTableColumn: FactTableColumn
): Promise<string> {
  logger.debug(`Creating and validating lookup table dimension ${dimension.factTableColumn}`);
  const lookupTablePresent: QueryResult = await connection.query(
    pgformat(
      'SELECT * FROM information_schema.tables WHERE table_schema = %L AND table_name = %L',
      'lookup_tables',
      dimension.lookupTable!.id
    )
  );

  if (lookupTablePresent.rows.length === 0) {
    logger.warn('Lookup table not loaded in to lookup table schema.  Loading lookup table from blob storage.');
    await loadFileIntoLookupTablesSchema(
      dataset,
      dimension.lookupTable!,
      dimension.extractor as LookupTableExtractor,
      factTableColumn,
      dimension.joinColumn!
    );
  }

  const dimTable = `${makeCubeSafeString(factTableColumn.columnName)}_lookup`;
  await connection.query(
    pgformat('CREATE TABLE %I AS SELECT * FROM lookup_tables.%I;', dimTable, dimension.lookupTable!.id)
  );
  return dimTable;
}

function setupFactTableUpdateJoins(
  factTableName: string,
  updateTableName: string,
  factIdentifiers: FactTableColumn[],
  dataTableIdentifiers: DataTableDescription[]
): string {
  const joinParts: string[] = [];
  for (const factTableCol of factIdentifiers) {
    const dataTableCol = dataTableIdentifiers.find((col) => col.factTableColumn === factTableCol.columnName);
    joinParts.push(
      pgformat('%I.%I=%I.%I', factTableName, factTableCol.columnName, updateTableName, dataTableCol?.columnName)
    );
  }
  return joinParts.join(' AND ');
}

async function loadFactTablesWithUpdates(
  connection: PoolClient,
  dataset: Dataset,
  allDataTables: DataTable[],
  factTableDef: string[],
  dataValuesColumn: FactTableColumn | undefined,
  notesCodeColumn: FactTableColumn | undefined,
  factIdentifiers: FactTableColumn[]
): Promise<void> {
  for (const dataTable of allDataTables.sort((ftA, ftB) => ftA.uploadedAt.getTime() - ftB.uploadedAt.getTime())) {
    const actionID = crypto.randomUUID();
    logger.debug(`Checking data table data exists in postgres data_tables schema`);
    const dataTablePresent: QueryResult = await connection.query(
      pgformat(
        'SELECT * FROM information_schema.tables WHERE table_schema = %L AND table_name = %L',
        'data_tables',
        dataTable.id
      )
    );

    if (dataTablePresent.rows.length === 0) {
      logger.warn('Data table not loaded in to data_tables schema.  Loading data table from blob storage.');
      await loadFileIntoDataTablesSchema(dataset, dataTable);
    }

    logger.info(`Loading fact table data for fact table ${dataTable.id}`);
    const updateTableDataCol = dataTable.dataTableDescriptions.find(
      (col) => col.factTableColumn === dataValuesColumn?.columnName
    )?.columnName;

    let updateQuery = '';
    let deleteQuery = '';
    const createUpdateTableQuery = pgformat(
      'CREATE TEMPORARY TABLE %I AS SELECT * FROM data_tables.%I;',
      actionID,
      dataTable.id
    );
    let doRevision = false;
    if (dataValuesColumn && notesCodeColumn && factIdentifiers.length > 0) {
      doRevision = true;
      deleteQuery = pgformat(
        `DELETE FROM %I USING %I WHERE %s`,
        actionID,
        FACT_TABLE_NAME,
        setupFactTableUpdateJoins(FACT_TABLE_NAME, actionID, factIdentifiers, dataTable.dataTableDescriptions)
      );
      updateQuery = pgformat(
        `UPDATE %I SET %I=%I.%I,
      %I=(CASE
      WHEN %I.%I IS NULL THEN 'r'
      WHEN %I.%I LIKE '%r%' THEN %I.%I
      ELSE concat(%I.%I,'r') END)
      FROM %I WHERE %s
      AND %I.%I!=%I.%I;`,
        FACT_TABLE_NAME,
        dataValuesColumn.columnName,
        actionID,
        updateTableDataCol,
        notesCodeColumn.columnName,
        FACT_TABLE_NAME,
        notesCodeColumn.columnName,
        FACT_TABLE_NAME,
        notesCodeColumn.columnName,
        FACT_TABLE_NAME,
        notesCodeColumn.columnName,
        FACT_TABLE_NAME,
        notesCodeColumn.columnName,
        actionID,
        setupFactTableUpdateJoins(FACT_TABLE_NAME, actionID, factIdentifiers, dataTable.dataTableDescriptions),
        FACT_TABLE_NAME,
        dataValuesColumn.columnName,
        actionID,
        updateTableDataCol
      );
    } else {
      logger.warn('No notes code or data value columns defined.  Unable to perform revision actions.');
    }
    const dataTableColumnSelect: string[] = [];

    for (const factTableCol of factTableDef) {
      const dataTableCol = dataTable.dataTableDescriptions.find(
        (col) => col.factTableColumn === factTableCol
      )?.columnName;
      if (dataTableCol) dataTableColumnSelect.push(dataTableCol);
    }

    try {
      logger.debug(`Performing action ${dataTable.action} on fact table`);
      switch (dataTable.action) {
        case DataTableAction.ReplaceAll:
          await connection.query(pgformat('DELETE FROM %I;', FACT_TABLE_NAME));
        // eslint-disable-next-line no-fallthrough
        case DataTableAction.Add:
          await loadTableDataIntoFactTableFromPostgres(connection, factTableDef, FACT_TABLE_NAME, dataTable.id);
          break;
        case DataTableAction.Revise:
          if (!doRevision) continue;
          logger.debug(`Executing create temporary update table query: ${createUpdateTableQuery}`);
          await connection.query(createUpdateTableQuery);
          logger.debug(`Executing update query: ${updateQuery}`);
          await connection.query(updateQuery);
          break;
        case DataTableAction.AddRevise:
          if (!doRevision) {
            logger.debug(`Executing create temporary update table query: ${createUpdateTableQuery}`);
            await connection.query(createUpdateTableQuery);
            logger.debug(`Executing update query: ${updateQuery}`);
            await connection.query(updateQuery);
            await connection.query(deleteQuery);
          }
          await connection.query(
            pgformat(
              'INSERT INTO %I (%I) (SELECT %I FROM %I);',
              FACT_TABLE_NAME,
              factTableDef,
              dataTableColumnSelect,
              actionID
            )
          );
          break;
      }
    } catch (error) {
      logger.error(error, `Something went wrong trying to create the core fact table`);
    }
  }
}

export async function loadFactTables(
  connection: PoolClient,
  dataset: Dataset,
  endRevision: Revision,
  factTableDef: string[],
  dataValuesColumn: FactTableColumn | undefined,
  notesCodeColumn: FactTableColumn | undefined,
  factIdentifiers: FactTableColumn[]
): Promise<void> {
  // Find all the fact tables for the given revision
  logger.debug('Finding all fact tables for this revision and those that came before');
  const allFactTables: DataTable[] = [];
  if (endRevision.revisionIndex && endRevision.revisionIndex > 0) {
    // If we have a revision index we start here
    const validRevisions = dataset.revisions.filter(
      (rev) => rev.revisionIndex <= endRevision.revisionIndex && rev.revisionIndex > 0
    );
    validRevisions.forEach((revision) => {
      if (revision.dataTable) allFactTables.push(revision.dataTable);
    });
  } else {
    logger.debug('Must be a draft revision, so we need to find all revisions before this one');
    // If we don't have a revision index we need to find the previous revision to this one that does
    if (endRevision.dataTable) {
      logger.debug('Adding end revision to list of fact tables');
      allFactTables.push(endRevision.dataTable);
    }
    const validRevisions = dataset.revisions.filter((rev) => rev.revisionIndex > 0);
    validRevisions.forEach((revision) => {
      if (revision.dataTable) allFactTables.push(revision.dataTable);
    });
  }

  if (allFactTables.length === 0) {
    logger.error(`No fact tables found in this dataset to revision ${endRevision.id}`);
    throw new Error(`No fact tables found in this dataset to revision ${endRevision.id}`);
  }

  // Process all the fact tables
  try {
    logger.debug(`Loading ${allFactTables.length} fact tables in to database with updates`);
    await loadFactTablesWithUpdates(
      connection,
      dataset,
      allFactTables.reverse(),
      factTableDef,
      dataValuesColumn,
      notesCodeColumn,
      factIdentifiers
    );
  } catch (error) {
    if (error instanceof FactTableValidationException) {
      logger.debug(error, `Throwing Fact Table Validation Exception`);
      throw error;
    }
    logger.error(error, `Something went wrong trying to create the core fact table`);
    const err = new CubeValidationException('Something went wrong trying to create the core fact table');
    err.type = CubeValidationType.FactTable;
    err.stack = (error as Error).stack;
    err.originalError = (error as Error).message;
    throw err;
  }
}

interface NoteCodeItem {
  code: string;
  tag: string;
}

export const NoteCodes: NoteCodeItem[] = [
  { code: 'a', tag: 'average' },
  { code: 'b', tag: 'break_in_series' },
  { code: 'c', tag: 'confidential' },
  { code: 'e', tag: 'estimated' },
  { code: 'f', tag: 'forecast' },
  { code: 'k', tag: 'low_figure' },
  { code: 'ns', tag: 'not_statistically_significant' },
  { code: 'p', tag: 'provisional' },
  { code: 'r', tag: 'revised' },
  { code: 's', tag: 'statistically_significant_at_level_1' },
  { code: 'ss', tag: 'statistically_significant_at_level_2' },
  { code: 'sss', tag: 'statistically_significant_at_level_3' },
  { code: 't', tag: 'total' },
  { code: 'u', tag: 'low_reliability' },
  { code: 'w', tag: 'not_recorded' },
  { code: 'x', tag: 'missing_data' },
  { code: 'z', tag: 'not_applicable' }
];

async function createNotesTable(
  connection: PoolClient,
  notesColumn: FactTableColumn,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[]
): Promise<void> {
  logger.info('Creating notes table...');
  try {
    await connection.query(
      `CREATE TABLE note_codes (code VARCHAR, language VARCHAR, tag VARCHAR, description VARCHAR, notes VARCHAR);`
    );
    for (const locale of SUPPORTED_LOCALES) {
      for (const noteCode of NoteCodes) {
        const query = pgformat('INSERT INTO note_codes (code, language, tag, description, notes) VALUES (%L)', [
          noteCode.code,
          locale.toLowerCase(),
          noteCode.tag,
          t(`note_codes.${noteCode.tag}`, { lng: locale }),
          null
        ]);
        await connection.query(query);
      }
    }
    logger.info('Creating notes table view...');
    // We perform join operations to this view as we want to turn a csv such as `a,r` in to `Average, Revised`.
    await connection.query(
      `CREATE TABLE all_notes AS SELECT fact_table."${notesColumn.columnName}" as code, note_codes.language as language, string_agg(DISTINCT note_codes.description, ', ') as description
            from fact_table JOIN note_codes ON array_position(string_to_array(fact_table."${notesColumn.columnName}", ','), note_codes.code) IS NOT NULL
            GROUP BY fact_table."${notesColumn.columnName}", note_codes.language;`
    );
  } catch (error) {
    logger.error(`Something went wrong trying to create the notes table with error: ${error}`);
    throw new Error(`Something went wrong trying to create the notes code table with the following error: ${error}`);
  }
  for (const locale of SUPPORTED_LOCALES) {
    viewSelectStatementsMap
      .get(locale)
      ?.push(`all_notes.description as "${t('column_headers.notes', { lng: locale })}"`);
    rawSelectStatementsMap
      .get(locale)
      ?.push(`all_notes.description as "${t('column_headers.notes', { lng: locale })}"`);
  }
  joinStatements.push(
    `LEFT JOIN all_notes on all_notes.code=fact_table."${notesColumn.columnName}" AND all_notes.language=#LANG#`
  );
}

interface MeasureFormat {
  name: string;
  method: string;
}

function postgresMeasureFormats(): Map<string, MeasureFormat> {
  const measureFormats: Map<string, MeasureFormat> = new Map();
  measureFormats.set('decimal', {
    name: 'decimal',
    method:
      "WHEN measure.reference = |REF| THEN format('%s', TO_CHAR(ROUND(CAST(|COL| AS DECIMAL), '|DEC|'), '999,999,990|ZEROS|'))"
  });
  measureFormats.set('float', {
    name: 'float',
    method:
      "WHEN measure.reference = |REF| THEN format('%s', TO_CHAR(ROUND(CAST(|COL| AS DECIMAL), '|DEC|'), '999,999,990|ZEROS|'))"
  });
  measureFormats.set('integer', {
    name: 'integer',
    method: "WHEN measure.reference = |REF| THEN format('%s', TO_CHAR(CAST(|COL| AS BIGINT), '999,999,990'))"
  });
  measureFormats.set('long', {
    name: 'long',
    method:
      "WHEN measure.reference = |REF| THEN format('%s', TO_CHAR(ROUND(CAST(|COL| AS DECIMAL), '|DEC|'), '999,999,990|ZEROS|'))"
  });
  measureFormats.set('percentage', {
    name: 'percentage',
    method:
      "WHEN measure.reference = |REF| THEN format('%s', TO_CHAR(ROUND(CAST(|COL| AS DECIMAL), '|DEC|'), '999,999,990|ZEROS|'))"
  });
  measureFormats.set('string', {
    name: 'string',
    method: "WHEN measure.reference = |REF| THEN format('%s', CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('text', {
    name: 'text',
    method: "WHEN measure.reference = |REF| THEN format('%s', CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('date', {
    name: 'date',
    method: "WHEN measure.reference = |REF| THEN format('%s', CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('datetime', {
    name: 'datetime',
    method: "WHEN measure.reference = |REF| THEN format('%s', CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('time', {
    name: 'time',
    method: "WHEN measure.reference = |REF| THEN format('%s', CAST(|COL| AS VARCHAR))"
  });
  return measureFormats;
}

export const measureTableCreateStatement = (joinColumnType: string, tableName = 'measure'): string => {
  return pgformat(
    `
    CREATE TABLE %I (
      reference %s,
      language TEXT,
      description TEXT,
      notes TEXT,
      sort_order INTEGER,
      format TEXT,
      decimals INTEGER,
      measure_type TEXT,
      hierarchy %s
    );
  `,
    tableName,
    joinColumnType,
    joinColumnType
  );
};

export async function createMeasureLookupTable(
  connection: PoolClient,
  measureColumn: FactTableColumn,
  measureTable: MeasureRow[]
): Promise<void> {
  await connection.query(measureTableCreateStatement(measureColumn.columnDatatype));
  for (const row of measureTable) {
    const query = {
      text: 'INSERT INTO measure VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9);',
      values: [
        row.reference,
        row.language.toLowerCase(),
        row.description,
        row.notes ? row.notes : null,
        row.sortOrder ? row.sortOrder : null,
        row.format,
        row.decimal ? row.decimal : null,
        row.measureType ? row.measureType : null,
        row.hierarchy ? row.hierarchy : null
      ]
    };
    await connection.query(query);
  }
}

function setupMeasureNoDataValues(
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  measureColumn?: FactTableColumn,
  dataValuesColumn?: FactTableColumn
): void {
  SUPPORTED_LOCALES.map((locale) => {
    if (dataValuesColumn) {
      viewSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            '%I.%I AS %I',
            FACT_TABLE_NAME,
            dataValuesColumn?.columnName,
            t('column_headers.data_values', { lng: locale })
          )
        );
      rawSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            '%I.%I AS %I',
            FACT_TABLE_NAME,
            dataValuesColumn?.columnName,
            t('column_headers.data_values', { lng: locale })
          )
        );
    }
    if (measureColumn) {
      viewSelectStatementsMap.get(locale)?.push(pgformat('%I.%I', FACT_TABLE_NAME, measureColumn.columnName));
      rawSelectStatementsMap.get(locale)?.push(pgformat('%I.%I', FACT_TABLE_NAME, measureColumn.columnName));
    }
  });
}

interface UniqueMeasureDetails {
  reference: string;
  format: string;
  sort_order: string | null;
  decimals: number | null;
}

async function setupMeasures(
  connection: PoolClient,
  dataset: Dataset,
  dataValuesColumn: FactTableColumn,
  measureColumn: FactTableColumn,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[],
  orderByStatements: string[]
): Promise<void> {
  logger.info('Setting up measure table if present...');

  // Process the column that represents the measure
  if (dataset.measure && dataset.measure.measureTable && dataset.measure.measureTable.length > 0) {
    logger.debug('Measure present in dataset. Creating measure table...');
    await createMeasureLookupTable(connection, measureColumn, dataset.measure.measureTable);

    logger.debug('Creating query part to format the data value correctly');

    const uniqueReferences: QueryResult<UniqueMeasureDetails> = await connection.query(
      pgformat('SELECT DISTINCT reference, format, sort_order, decimals FROM measure;')
    );
    const caseStatements: string[] = ['CASE'];
    for (const row of uniqueReferences.rows) {
      const statement = postgresMeasureFormats()
        .get(row.format.toLowerCase())
        ?.method.replace('|REF|', pgformat('%L', row.reference))
        .replace('|DEC|', row.decimals ? `${row.decimals}` : '0')
        .replace('|ZEROS|', row.decimals ? `.${'0'.repeat(row.decimals)}` : '')
        .replace('|COL|', pgformat('%I.%I', FACT_TABLE_NAME, dataValuesColumn.columnName));
      if (statement) {
        caseStatements.push(statement);
      } else {
        logger.warn(`Failed to create case statement measure row: ${JSON.stringify(row)}`);
      }
    }

    caseStatements.push(pgformat('ELSE CAST(%I.%I AS VARCHAR) END', FACT_TABLE_NAME, dataValuesColumn?.columnName));
    // logger.debug(`Data view case statement ended up as: ${caseStatements.join('\n')}`);

    SUPPORTED_LOCALES.map((locale) => {
      const columnName =
        dataset.measure.metadata.find((info) => info.language === locale)?.name || dataset.measure.factTableColumn;
      if (dataValuesColumn) {
        rawSelectStatementsMap
          .get(locale)
          ?.push(
            pgformat(
              '%I.%I AS %I',
              FACT_TABLE_NAME,
              dataValuesColumn.columnName,
              t('column_headers.data_values', { lng: locale })
            )
          );
        viewSelectStatementsMap
          .get(locale)
          ?.push(pgformat(`%s AS %I`, caseStatements.join('\n'), t('column_headers.data_values', { lng: locale })));
      }
      viewSelectStatementsMap.get(locale)?.push(pgformat('measure.description AS %I', columnName));
      rawSelectStatementsMap.get(locale)?.push(pgformat('measure.description AS %I', columnName));
    });
    joinStatements.push(
      pgformat(
        'LEFT JOIN measure on measure.reference=%I.%I AND measure.language=#LANG#',
        FACT_TABLE_NAME,
        dataset.measure.factTableColumn
      )
    );
    orderByStatements.push(`measure.sort_order, measure.reference`);
    for (const locale of SUPPORTED_LOCALES) {
      const columnName =
        dataset.measure.metadata.find((info) => info.language === locale)?.name || dataset.measure.factTableColumn;
      await connection.query(
        pgformat(
          `INSERT INTO filter_table SELECT CAST(reference AS VARCHAR), language, %L, %L, description, CAST(hierarchy AS VARCHAR) FROM measure WHERE language = %L`,
          measureColumn.columnName,
          columnName,
          locale.toLowerCase()
        )
      );
    }
  } else {
    setupMeasureNoDataValues(viewSelectStatementsMap, rawSelectStatementsMap, measureColumn, dataValuesColumn);
  }
}

async function rawDimensionProcessor(
  connection: PoolClient,
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>
): Promise<void> {
  SUPPORTED_LOCALES.map((locale) => {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    viewSelectStatementsMap.get(locale)?.push(pgformat('%I AS %I', dimension.factTableColumn, columnName));
    rawSelectStatementsMap.get(locale)?.push(pgformat('%I AS %I', dimension.factTableColumn, columnName));
  });
  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    await connection.query(
      pgformat(
        `INSERT INTO filter_table
         SELECT DISTINCT CAST(%I AS VARCHAR), %L, %L, %L, CAST (%I AS VARCHAR), NULL
         FROM %I`,
        dimension.factTableColumn,
        locale.toLowerCase(),
        dimension.factTableColumn,
        columnName,
        dimension.factTableColumn,
        FACT_TABLE_NAME
      )
    );
  }
}

async function dateDimensionProcessor(
  connection: PoolClient,
  factTableColumn: FactTableColumn,
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[],
  orderByStatements: string[]
): Promise<string> {
  const dimTable = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
  await createDateDimension(connection, dimension.extractor, factTableColumn);
  SUPPORTED_LOCALES.map((locale) => {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    viewSelectStatementsMap.get(locale)?.push(pgformat('%I.description AS %I', dimTable, columnName));
    // Leaving commented out for now.  Code will need to be adjusted if we choose to expose the underlying dates for periods
    // viewSelectStatementsMap
    //   .get(locale)
    //   ?.push(
    //     pgformat("strftime(%I.start_date, '%d/%m/%Y') AS %I", dimTable, t('column_headers.start_date', { lng: locale }))
    //   );
    // viewSelectStatementsMap
    //   .get(locale)
    //   ?.push(
    //     pgformat("strftime(%I.end_date, '%d/%m/%Y') AS %I", dimTable, t('column_headers.end_date', { lng: locale }))
    //   );
    rawSelectStatementsMap.get(locale)?.push(pgformat('%I.description AS %I', dimTable, columnName));
    // rawSelectStatementsMap
    //   .get(locale)
    //   ?.push(
    //     pgformat("strftime(%I.start_date, '%d/%m/%Y') AS %I", dimTable, t('column_headers.start_date', { lng: locale }))
    //   );
    // rawSelectStatementsMap
    //   .get(locale)
    //   ?.push(
    //     pgformat("strftime(%I.end_date, '%d/%m/%Y') AS %I", dimTable, t('column_headers.end_date', { lng: locale }))
    //   );
  });
  joinStatements.push(
    pgformat(
      'LEFT JOIN %I ON %I.%I=%I.%I AND %I.language=#LANG#',
      dimTable,
      dimTable,
      factTableColumn.columnName,
      FACT_TABLE_NAME,
      factTableColumn.columnName,
      dimTable
    )
  );
  orderByStatements.push(pgformat('%I.end_date', dimTable));
  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    await connection.query(
      pgformat(
        `INSERT INTO filter_table
         SELECT CAST(%I AS VARCHAR), language, %L, %L, description, CAST (hierarchy AS VARCHAR)
         FROM %I
         WHERE language = %L`,
        factTableColumn.columnName,
        factTableColumn.columnName,
        columnName,
        dimTable,
        locale.toLowerCase()
      )
    );
  }
  return dimTable;
}

async function setupNumericDimension(
  connection: PoolClient,
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>
): Promise<void> {
  SUPPORTED_LOCALES.map((locale) => {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    if ((dimension.extractor as NumberExtractor).type === NumberType.Integer) {
      viewSelectStatementsMap
        .get(locale)
        ?.push(pgformat('CAST(%I.%I AS INTEGER) AS %I', FACT_TABLE_NAME, dimension.factTableColumn, columnName));
      rawSelectStatementsMap
        .get(locale)
        ?.push(pgformat('CAST(%I.%I AS INTEGER) AS %I', FACT_TABLE_NAME, dimension.factTableColumn, columnName));
    } else {
      viewSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            `format('%%s', TO_CHAR(ROUND(CAST(%I.%I AS DECIMAL), %L), '999,999,990.%s'))`,
            FACT_TABLE_NAME,
            dimension.factTableColumn,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            (dimension.extractor as NumberExtractor).decimalPlaces
          )
        );
      rawSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            `format('%%s', TO_CHAR(ROUND(CAST(%I.%I AS DECIMAL), %L), '999,999,990.%s'))`,
            FACT_TABLE_NAME,
            dimension.factTableColumn,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            (dimension.extractor as NumberExtractor).decimalPlaces
          )
        );
    }
  });
  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    await connection.query(
      pgformat(
        `INSERT INTO filter_table
         SELECT DISTINCT CAST(%I AS VARCHAR), %L, %L, %L, CAST (%I AS VARCHAR), NULL
         FROM %I`,
        dimension.factTableColumn,
        locale.toLowerCase(),
        dimension.factTableColumn,
        columnName,
        dimension.factTableColumn,
        FACT_TABLE_NAME
      )
    );
  }
}

async function setupTextDimension(
  connection: PoolClient,
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>
): Promise<void> {
  SUPPORTED_LOCALES.map((locale) => {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    viewSelectStatementsMap
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, columnName));
    rawSelectStatementsMap
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, columnName));
  });
  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    await connection.query(
      pgformat(
        `INSERT INTO filter_table
         SELECT DISTINCT CAST(%I AS VARCHAR), %L, %L, %L, CAST (%I AS VARCHAR), NULL
         FROM %I`,
        dimension.factTableColumn,
        locale.toLowerCase(),
        dimension.factTableColumn,
        columnName,
        dimension.factTableColumn,
        FACT_TABLE_NAME
      )
    );
  }
}

async function setupDimensions(
  connection: PoolClient,
  dataset: Dataset,
  endRevision: Revision,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[],
  orderByStatements: string[]
): Promise<void> {
  logger.info('Setting up dimension tables...');
  const lookupTables: Set<string> = new Set<string>();
  let tableName = '';
  const factTable = dataset.factTable;
  if (!factTable)
    throw new Error(
      `No fact table found in dataset ${dataset.id} for revision ${endRevision.id}.  Cannot create dimension tables`
    );
  const orderedDimension = dataset.dimensions.map((dim) => {
    const col = factTable.find((col) => col.columnName === dim.factTableColumn);
    return {
      dimension: dim,
      index: col ? factTable.indexOf(col) : -1
    };
  });
  for (const dim of orderedDimension.sort((dimA, dimB) => dimA.index - dimB.index)) {
    const dimStart = performance.now();
    const dimension = dim.dimension;
    const factTableColumn = dataset.factTable?.find(
      (col) =>
        col.columnName === dimension.factTableColumn &&
        (col.columnType === FactTableColumnType.Dimension || col.columnType === FactTableColumnType.Unknown)
    );
    if (!factTableColumn) {
      const error = new CubeValidationException(
        `No fact table column found for dimension ${dimension.id} in dataset ${dataset.id}`
      );
      error.type = CubeValidationType.FactTableColumnMissing;
      throw error;
    }
    logger.info(`Setting up dimension ${dimension.id} for fact table column ${dimension.factTableColumn}`);
    try {
      switch (dimension.type) {
        case DimensionType.DatePeriod:
        case DimensionType.Date:
          if (dimension.extractor) {
            tableName = await dateDimensionProcessor(
              connection,
              factTableColumn,
              dimension,
              viewSelectStatementsMap,
              rawSelectStatementsMap,
              joinStatements,
              orderByStatements
            );
            lookupTables.add(tableName);
          } else {
            await rawDimensionProcessor(connection, dimension, viewSelectStatementsMap, rawSelectStatementsMap);
          }
          break;
        case DimensionType.LookupTable:
          // To allow preview to continue working for dimensions which are in progress
          // we check to see if there's a task for the dimension and if it's been update
          // if it's been update we skip it.
          if (endRevision.tasks) {
            const updateInProgressDimension = endRevision.tasks.dimensions.find((dim) => dim.id === dimension.id);
            if (updateInProgressDimension && !updateInProgressDimension.lookupTableUpdated) {
              logger.warn(`Skipping dimension ${dimension.id} as it has not been updated`);
              await rawDimensionProcessor(connection, dimension, viewSelectStatementsMap, rawSelectStatementsMap);
              break;
            }
          }
          tableName = await setupLookupTableDimension(
            connection,
            dataset,
            dimension,
            viewSelectStatementsMap,
            rawSelectStatementsMap,
            joinStatements,
            orderByStatements
          );
          lookupTables.add(tableName);
          break;
        case DimensionType.ReferenceData:
          await setupReferenceDataDimension(
            connection,
            dimension,
            viewSelectStatementsMap,
            rawSelectStatementsMap,
            joinStatements
          );
          lookupTables.add('reference_data');
          lookupTables.add('categories');
          lookupTables.add('category_info');
          lookupTables.add('category_key');
          lookupTables.add('category_key_info');
          lookupTables.add('hierarchy');
          lookupTables.add('reference_data_info');
          break;
        case DimensionType.Numeric:
          await setupNumericDimension(connection, dimension, viewSelectStatementsMap, rawSelectStatementsMap);
          break;
        case DimensionType.Text:
          await setupTextDimension(connection, dimension, viewSelectStatementsMap, rawSelectStatementsMap);
          break;
        case DimensionType.Raw:
        case DimensionType.Symbol:
          await rawDimensionProcessor(connection, dimension, viewSelectStatementsMap, rawSelectStatementsMap);
          break;
      }
      await connection.query(
        pgformat('INSERT INTO metadata VALUES (%L, %L)', 'lookup_tables', JSON.stringify(Array.from(lookupTables)))
      );
    } catch (err) {
      logger.error(err, `Something went wrong trying to load dimension ${dimension.id} in to the cube`);
      throw new Error(`Could not load dimensions ${dimension.id} in to the cube with the following error: ${err}`);
    }
    performanceReporting(Math.round(performance.now() - dimStart), 1000, `Setting up ${dimension.type} dimension type`);
  }
}

interface FactTableInfo {
  measureColumn?: FactTableColumn;
  notesCodeColumn?: FactTableColumn;
  dataValuesColumn?: FactTableColumn;
  factTableDef: string[];
  factIdentifiers: FactTableColumn[];
  compositeKey: string[];
}

export async function createEmptyFactTableInCube(
  connection: PoolClient,
  dataset: Dataset,
  buildId: string
): Promise<FactTableInfo> {
  const start = performance.now();
  if (!dataset.factTable) {
    throw new Error(`Unable to find fact table for dataset ${dataset.id}`);
  }

  const notesCodeColumn = dataset.factTable?.find((field) => field.columnType === FactTableColumnType.NoteCodes);
  const dataValuesColumn = dataset.factTable?.find((field) => field.columnType === FactTableColumnType.DataValues);
  const measureColumn = dataset.factTable?.find((field) => field.columnType === FactTableColumnType.Measure);

  const factTable = dataset.factTable.sort((colA, colB) => colA.columnIndex - colB.columnIndex);
  const compositeKey: string[] = [];
  const factIdentifiers: FactTableColumn[] = [];
  const factTableDef: string[] = [];

  const factTableCreationDef = factTable
    .sort((col1, col2) => col1.columnIndex - col2.columnIndex)
    .map((field) => {
      switch (field.columnType) {
        case FactTableColumnType.Measure:
        // eslint-disable-next-line no-fallthrough
        case FactTableColumnType.Dimension:
        case FactTableColumnType.Time:
          compositeKey.push(field.columnName);
          factIdentifiers.push(field);
          break;
      }
      factTableDef.push(field.columnName);
      return pgformat(
        '%I %s',
        field.columnName,
        field.columnDatatype === 'DOUBLE' ? 'DOUBLE PRECISION' : field.columnDatatype
      );
    });

  logger.info('Creating initial fact table in cube');
  try {
    const factTableCreationQuery = pgformat(
      `CREATE TABLE %I.%I (%s);`,
      buildId,
      FACT_TABLE_NAME,
      factTableCreationDef.join(', ')
    );
    // logger.debug(`Creating fact table with query: '${createQuery}'`);
    await connection.query(factTableCreationQuery);
  } catch (err) {
    logger.error(err, `Failed to create fact table in cube`);
    throw new Error(`Failed to create fact table in cube: ${err}`);
  }
  const end = performance.now();
  const timing = Math.round(end - start);
  logger.debug(`createEmptyFactTableInCube: ${timing}ms`);
  return { measureColumn, notesCodeColumn, dataValuesColumn, factTableDef, factIdentifiers, compositeKey };
}

export const updateFactTableValidator = async (
  connection: PoolClient,
  buildID: string,
  dataset: Dataset,
  revision: Revision
): Promise<void> => {
  const factTableInfo = await createEmptyFactTableInCube(connection, dataset, buildID);
  await loadFactTables(
    connection,
    dataset,
    revision,
    factTableInfo.factTableDef,
    factTableInfo.dataValuesColumn,
    factTableInfo.notesCodeColumn,
    factTableInfo.factIdentifiers
  );
  await createPrimaryKeyOnFactTable(connection, revision, factTableInfo.compositeKey);
};

async function createPrimaryKeyOnFactTable(
  connection: PoolClient,
  revision: Revision,
  compositeKey: string[]
): Promise<void> {
  logger.debug('Creating primary key on fact table');
  try {
    const alterTableQuery = pgformat(
      'ALTER TABLE %I.%I ADD PRIMARY KEY (%I)',
      revision.id,
      FACT_TABLE_NAME,
      compositeKey
    );
    logger.debug(`Alter Table query = ${alterTableQuery}`);
    await connection.query(alterTableQuery);
  } catch (error) {
    logger.error(error, `Failed to add primary key to the fact table`);
    if ((error as Error).message.includes('could not create unique index')) {
      const exception = new CubeValidationException('Duplicate facts present');
      exception.type = CubeValidationType.UnknownDuplicateFact;
      exception.revisionId = revision.id;
      throw exception;
    } else if ((error as Error).message.includes('contains null values')) {
      const exception = new CubeValidationException('Incomplete facts present in fact table');
      exception.type = CubeValidationType.UnknownDuplicateFact;
      exception.revisionId = revision.id;
      throw exception;
    } else {
      const exception = new CubeValidationException(
        'An unknown error occurred trying to add the primary key to the fact table'
      );
      exception.type = CubeValidationType.UnknownError;
      exception.revisionId = revision.id;
    }
  }
}

export async function createCubeMetadataTable(
  connection: PoolClient,
  revisionId: string,
  buildId: string
): Promise<void> {
  logger.debug('Adding metadata table to the cube');
  await connection.query(`CREATE TABLE IF NOT EXISTS metadata (key VARCHAR, value VARCHAR);`);
  await connection.query(pgformat('INSERT INTO metadata VALUES (%L, %L);', 'revision_id', revisionId));
  await connection.query(pgformat('INSERT INTO metadata VALUES (%L, %L);', 'build_id', buildId));
  await connection.query(pgformat('INSERT INTO metadata VALUES (%L, %L);', 'build_start', new Date().toISOString()));
  await connection.query(pgformat('INSERT INTO metadata VALUES (%L, %L);', 'build_status', 'incomplete'));
}

async function createFilterTable(connection: PoolClient): Promise<void> {
  const start = performance.now();
  logger.debug('Creating filter table to the cube');
  const createFilterQuery = pgformat(
    `
      CREATE TABLE %s (
          reference VARCHAR,
          language VARCHAR,
          fact_table_column VARCHAR,
          dimension_name VARCHAR,
          description VARCHAR,
          hierarchy VARCHAR,
          PRIMARY KEY (reference, language, fact_table_column)
      );
    `,
    'filter_table'
  );
  await connection.query(createFilterQuery);
  const end = performance.now();
  const timing = Math.round(end - start);
  logger.debug(`createFilterTable: ${timing}ms`);
}

// Builds a fresh cube from either from a protocube or completely from scratch
// based on if a protocube is supplied and returns the file pointer
// to the duckdb file on disk.  This is based on the recipe in our cube miro
// board and our candidate cube format repo.  It is limited to building a
// simple default view based on the available locales.
//
// If no protocube is supplied a new fact table is created based on all
// revisions containing an index number until we reach the specified end
// revision.
//
// DO NOT put validation against columns which should be present here.
// Function should be able to generate a cube just from a fact table or collection
// of fact tables.
export const createBasePostgresCube = async (
  connection: PoolClient,
  buildId: string,
  dataset: Dataset,
  endRevision: Revision
): Promise<void> => {
  logger.debug(`Starting build ${buildId} and Creating base cube for revision ${endRevision.id}`);
  await connection.query(pgformat(`SET search_path TO %I;`, buildId));
  const functionStart = performance.now();
  const viewSelectStatementsMap = new Map<Locale, string[]>();
  const rawSelectStatementsMap = new Map<Locale, string[]>();

  SUPPORTED_LOCALES.map((locale) => {
    viewSelectStatementsMap.set(locale, []);
    rawSelectStatementsMap.set(locale, []);
  });

  const joinStatements: string[] = [];
  const orderByStatements: string[] = [];

  logger.debug('Finding first revision');
  const firstRevision = dataset.revisions.find((rev) => rev.revisionIndex === 1);

  if (!firstRevision) {
    const err = new CubeValidationException(
      `Could not find first revision for dataset ${dataset.id} in revision ${endRevision.id}`
    );
    err.type = CubeValidationType.NoFirstRevision;
    err.datasetId = dataset.id;
    throw new Error(`Unable to find first revision for dataset ${dataset.id}`);
  }

  const buildStart = performance.now();
  const factTableInfo = await createEmptyFactTableInCube(connection, dataset, buildId);
  await createCubeMetadataTable(connection, endRevision.id, buildId);
  await createFilterTable(connection);
  performanceReporting(Math.round(performance.now() - functionStart), 1000, 'Base table creation');
  try {
    const loadFactTablesStart = performance.now();
    await loadFactTables(
      connection,
      dataset,
      endRevision,
      factTableInfo.factTableDef,
      factTableInfo.dataValuesColumn,
      factTableInfo.notesCodeColumn,
      factTableInfo.factIdentifiers
    );
    performanceReporting(Math.round(performance.now() - loadFactTablesStart), 1000, 'Loading all the data tables');
  } catch (err) {
    await connection.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
    logger.error(err, `Failed to load fact tables into the cube`);
    throw new Error(`Failed to load fact tables into the cube: ${err}`);
  }

  const measureSetupMark = performance.now();
  if (factTableInfo.measureColumn && factTableInfo.dataValuesColumn) {
    try {
      await setupMeasures(
        connection,
        dataset,
        factTableInfo.dataValuesColumn,
        factTableInfo.measureColumn,
        viewSelectStatementsMap,
        rawSelectStatementsMap,
        joinStatements,
        orderByStatements
      );
    } catch (err) {
      await connection.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
      logger.error(err, `Failed to setup measures`);
      throw new Error(`Failed to setup measures: ${err}`);
    }
  } else {
    setupMeasureNoDataValues(
      viewSelectStatementsMap,
      rawSelectStatementsMap,
      factTableInfo.measureColumn,
      factTableInfo.dataValuesColumn
    );
  }
  performanceReporting(Math.round(performance.now() - measureSetupMark), 1000, 'Setting up the measure');

  if (dataset.dimensions.find((dim) => dim.type === DimensionType.ReferenceData)) {
    const loadReferenceDataMark = performance.now();
    await loadReferenceDataIntoCube(connection);
    performanceReporting(
      Math.round(performance.now() - loadReferenceDataMark),
      1000,
      'Loading reference data in to cube'
    );
  }

  const dimensionSetupMark = performance.now();
  try {
    await setupDimensions(
      connection,
      dataset,
      endRevision,
      viewSelectStatementsMap,
      rawSelectStatementsMap,
      joinStatements,
      orderByStatements
    );
  } catch (err) {
    await connection.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
    logger.error(err, `Failed to setup dimensions`);
    throw new Error(`Failed to setup dimensions`);
  }
  performanceReporting(Math.round(performance.now() - dimensionSetupMark), 1000, 'Setting up the dimensions in total');

  const noteCodeCreation = performance.now();
  logger.debug('Adding notes code column to the select statement.');
  if (factTableInfo.notesCodeColumn) {
    await createNotesTable(
      connection,
      factTableInfo.notesCodeColumn,
      viewSelectStatementsMap,
      rawSelectStatementsMap,
      joinStatements
    );
  }
  performanceReporting(Math.round(performance.now() - noteCodeCreation), 1000, 'Setting up the note codes');

  logger.info(`Creating default views...`);
  const viewCreation = performance.now();
  // Build the default views
  try {
    for (const locale of SUPPORTED_LOCALES) {
      if (viewSelectStatementsMap.get(locale)?.length === 0) {
        viewSelectStatementsMap.get(locale)?.push('*');
      }
      if (rawSelectStatementsMap.get(locale)?.length === 0) {
        rawSelectStatementsMap.get(locale)?.push('*');
      }
      const lang = locale.toLowerCase().split('-')[0];

      const defaultViewSQL = pgformat(
        'SELECT %s FROM %I %s %s',
        viewSelectStatementsMap.get(locale)?.join(',\n'),
        FACT_TABLE_NAME,
        joinStatements.join('\n').replace(/#LANG#/g, pgformat('%L', locale.toLowerCase())),
        orderByStatements.length > 0 ? `ORDER BY ${orderByStatements.join(', ')}` : ''
      );
      await connection.query(pgformat('CREATE VIEW %I AS %s', `default_view_${lang}`, defaultViewSQL));
      await connection.query(pgformat(`INSERT INTO metadata VALUES (%L, %L)`, `default_view_${lang}`, defaultViewSQL));

      const rawViewSQL = pgformat(
        'SELECT %s FROM %I %s %s',
        rawSelectStatementsMap.get(locale)?.join(',\n'),
        FACT_TABLE_NAME,
        joinStatements.join('\n').replace(/#LANG#/g, pgformat('%L', locale.toLowerCase())),
        orderByStatements.length > 0 ? `ORDER BY ${orderByStatements.join(', ')}` : ''
      );
      await connection.query(pgformat('CREATE VIEW %I AS %s', `raw_view_${lang}`, rawViewSQL));
      await connection.query(pgformat(`INSERT INTO metadata VALUES (%L, %L)`, `raw_view_${lang}`, rawViewSQL));
    }
    await connection.query(`UPDATE metadata SET value = 'awaiting_materialization' WHERE key = 'build_status'`);
  } catch (error) {
    await connection.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
    performanceReporting(Math.round(performance.now() - viewCreation), 3000, 'Setting up the default views');
    logger.error(error, 'Something went wrong trying to create the default views in the cube.');
    const exception = new CubeValidationException('Cube Build Failed');
    exception.type = CubeValidationType.CubeCreationFailed;
    throw exception;
  }

  performanceReporting(Math.round(performance.now() - viewCreation), 3000, 'Setting up the default views');
  const end = performance.now();
  const functionTime = Math.round(end - functionStart);
  const buildTime = Math.round(end - buildStart);
  performanceReporting(buildTime, 5000, 'Cube build process');
  performanceReporting(functionTime, 5000, 'Cube build function in total');
  endRevision.cubeType = CubeType.PostgresCube;
  await endRevision.save();
};

export const createFilesForDownload = async (
  quack: Database,
  datasetId: string,
  endRevisionId: string
): Promise<void> => {
  logger.debug('Creating download files for whole dataset');
  try {
    const fileService = getFileService();
    // TODO Write code to to use native libraries to produce parquet, csv, excel and json outputs
    for (const locale of SUPPORTED_LOCALES) {
      const lang = locale.toLowerCase().split('-')[0];
      logger.debug(`Creating and uploading parquet file for local ${locale}`);
      const parquetFileName = await asyncTmpName({ postfix: '.parquet' });
      await quack.exec(`COPY default_view_${lang} TO '${parquetFileName}' (FORMAT PARQUET);`);
      await fileService.saveBuffer(`${endRevisionId}_${lang}.parquet`, datasetId, await readFile(parquetFileName));
      await unlink(parquetFileName);
    }
    logger.debug('File creation done... Closing duckdb');
  } catch (err) {
    logger.error(err, 'Failed to create cube files');
  } finally {
    await safelyCloseDuckDb(quack);
  }
  logger.debug('Async processes completed.');
};

export const createMaterialisedView = async (revisionId: string): Promise<void> => {
  const connection = await getCubeDB().connect();
  await connection.query(pgformat(`SET search_path TO %I;`, revisionId));
  logger.info(`Creating default views...`);
  const viewCreation = performance.now();
  // Build the default views
  try {
    for (const locale of SUPPORTED_LOCALES) {
      const lang = locale.toLowerCase().split('-')[0];
      const originalDefaultViewMetadata: QueryResult<{ value: string }> = await connection.query(
        pgformat('SELECT value FROM metadata WHERE key = %L', `default_view_${lang}`)
      );
      await connection.query(
        pgformat(
          'CREATE MATERIALIZED VIEW %I AS %s',
          `default_mat_view_${lang}`,
          originalDefaultViewMetadata.rows[0].value
        )
      );

      const originalRawViewMetadata: QueryResult<{ value: string }> = await connection.query(
        pgformat('SELECT value FROM metadata WHERE key = %L', `raw_view_${lang}`)
      );
      await connection.query(
        pgformat('CREATE MATERIALIZED VIEW %I AS %s', `raw_mat_view_${lang}`, originalRawViewMetadata.rows[0].value)
      );
    }
    await connection.query(`UPDATE metadata SET value = 'complete' WHERE key = 'build_status'`);
    await connection.query(`INSERT INTO metadata VALUES('build_finished', '${new Date().toISOString()}')`);
  } catch (error) {
    try {
      await connection.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
    } catch (err) {
      logger.error(err, 'Apparently cube no longer exists');
    }
    performanceReporting(Math.round(performance.now() - viewCreation), 3000, 'Setting up the materialized views');
    logger.error(error, 'Something went wrong trying to create the materialized views in the cube.');
  } finally {
    connection.release();
  }
  performanceReporting(Math.round(performance.now() - viewCreation), 3000, 'Setting up the materialized views');
};

export const createAllCubeFiles = async (datasetId: string, endRevisionId: string): Promise<void> => {
  const datasetRelations: FindOptionsRelations<Dataset> = {
    factTable: true,
    dimensions: { metadata: true, lookupTable: true },
    measure: { metadata: true, measureTable: true },
    revisions: { dataTable: { dataTableDescriptions: true } }
  };

  logger.debug('Loading dataset and relations');
  const dataset = await DatasetRepository.getById(datasetId, datasetRelations);
  logger.debug('Loading revision and relations');
  const endRevision = dataset.revisions.find((rev) => rev.id === dataset.endRevisionId);

  if (!endRevision) {
    logger.error('Unable to find endRevision in dataset.');
    throw new CubeValidationException('Failed to find endRevision in dataset.');
  }

  const connection = await getCubeDB().connect();
  const buildId = `build_${crypto.randomUUID()}`;

  try {
    logger.info(`Creating schema for cube build ${buildId}`);
    await connection.query(pgformat(`CREATE SCHEMA IF NOT EXISTS %I;`, buildId));
  } catch (error) {
    logger.error(error, 'Something went wrong trying to create the cube schema');
    connection.release();
    throw error;
  }

  try {
    logger.debug('Creating cube in postgres.');
    await createBasePostgresCube(connection, buildId, dataset, endRevision);
    await connection.query(pgformat('DROP SCHEMA IF EXISTS %I CASCADE;', endRevision.id));
    await connection.query(pgformat('ALTER SCHEMA %I RENAME TO %I;', buildId, endRevision.id));
  } catch (err) {
    logger.error(err, 'Failed to create cube in Postgres');
    throw err;
  } finally {
    connection.release();
  }

  // don't wait for this, can happen in the background so we can send the response earlier
  logger.debug('Running async process...');
  void createMaterialisedView(endRevisionId);
  //void createFilesForDownload(quack, datasetId, endRevisionId);
};

export const getCubeTimePeriods = async (revisionId: string): Promise<PeriodCovered> => {
  const connection = await getCubeDB().connect();
  const periodCoverage: QueryResult<{ key: string; value: string }> = await connection.query(
    pgformat(
      `SELECT key, value
              FROM %I.metadata
              WHERE key in ('start_date', 'end_date')`,
      revisionId
    )
  );
  connection.release();
  if (periodCoverage.rows.length > 0) {
    return {
      start_date: new Date(periodCoverage.rows[0].value),
      end_date: new Date(periodCoverage.rows[1].value)
    };
  } else {
    return {
      start_date: null,
      end_date: null
    };
  }
};
export const outputCube = async (
  mode: DuckdbOutputType,
  datasetId: string,
  revisionId: string,
  lang: string,
  storageService: StorageService
): Promise<Buffer> => {
  try {
    return storageService.loadBuffer(`${revisionId}_${lang}.${mode}`, datasetId);
  } catch (err) {
    logger.error(err, `Something went wrong trying to create the cube output file`);
    throw err;
  }
};

export const getPostgresCubePreview = async (
  revision: Revision,
  lang: string,
  dataset: Dataset,
  page: number,
  size: number,
  sortBy?: SortByInterface[],
  filter?: FilterInterface[]
): Promise<ViewDTO | ViewErrDTO> => {
  try {
    return createFrontendView(dataset, revision, lang, page, size, sortBy, filter);
  } catch (err) {
    logger.error(err, `Something went wrong trying to create the cube preview`);
    return { status: 500, errors: [], dataset_id: dataset.id };
  }
};
