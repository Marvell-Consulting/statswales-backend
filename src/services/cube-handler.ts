import fs from 'node:fs';
import { readFile, unlink } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

import { from } from 'pg-copy-streams';
import { Database, DuckDbError, RowData } from 'duckdb-async';
import { FindOptionsRelations, QueryRunner } from 'typeorm';
import { toZonedTime } from 'date-fns-tz';
import { format as pgformat } from '@scaleleap/pg-format';

import { FileType } from '../enums/file-type';
import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import { getFileImportAndSaveToDisk } from '../utils/file-utils';
import { SUPPORTED_LOCALES, t } from '../middleware/translation';
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
import { dateDimensionReferenceTableCreator } from './date-matching';
import { duckdb, linkToPostgresSchema, safelyCloseDuckDb } from './duckdb';
import { NumberExtractor, NumberType } from '../extractors/number-extractor';
import { CubeValidationType } from '../enums/cube-validation-type';
import { languageMatcherCaseStatement } from '../utils/lookup-table-utils';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import { FactTableValidationExceptionType } from '../enums/fact-table-validation-exception-type';
import { CubeType } from '../enums/cube-type';
import { DateExtractor } from '../extractors/date-extractor';
import { getFileService } from '../utils/get-file-service';
import { asyncTmpName } from '../utils/async-tmp';
import { performanceReporting } from '../utils/performance-reporting';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { StorageService } from '../interfaces/storage-service';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { FilterInterface } from '../interfaces/filterInterface';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { createFrontendView } from './consumer-view';
import { LookupTable } from '../entities/dataset/lookup-table';
import { dbManager } from '../db/database-manager';
import { PoolClient } from 'pg';

export const FACT_TABLE_NAME = 'fact_table';
export const CORE_VIEW_NAME = 'core_view';

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
  cubeDB: QueryRunner,
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
    await cubeDB.query(insertQuery);
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

export async function createReferenceDataTablesInCube(searchPath: string): Promise<void> {
  logger.debug(`Creating empty reference data tables in schema: ${searchPath}`);
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();

  try {
    await cubeDB.query(pgformat(`SET search_path TO %I;`, searchPath));
    logger.debug('Creating categories tables');
    await cubeDB.query(`CREATE TABLE IF NOT EXISTS "categories" ("category" VARCHAR, PRIMARY KEY ("category"));`);

    logger.debug('Creating category_keys table');
    await cubeDB.query(`
      CREATE TABLE IF NOT EXISTS "category_keys" (
        "category_key" VARCHAR PRIMARY KEY,
        "category" VARCHAR NOT NULL
      );
    `);
    logger.debug('Creating reference_data table');
    await cubeDB.query(`
      CREATE TABLE IF NOT EXISTS "reference_data" (
        "item_id" VARCHAR NOT NULL,
        "version_no" INTEGER NOT NULL,
        "sort_order" INTEGER,
        "category_key" VARCHAR NOT NULL,
        "validity_start" VARCHAR NOT NULL,
        "validity_end" VARCHAR
      );
    `);
    logger.debug('Creating reference_data_all table');
    await cubeDB.query(`
      CREATE TABLE IF NOT EXISTS "reference_data_all" (
        "item_id" VARCHAR NOT NULL,
        "version_no" INTEGER NOT NULL,
        "sort_order" INTEGER,
        "category_key" VARCHAR NOT NULL,
        "validity_start" VARCHAR NOT NULL,
        "validity_end" VARCHAR
      );
    `);
    logger.debug('Creating reference_data_info table');
    await cubeDB.query(`
      CREATE TABLE IF NOT EXISTS "reference_data_info" (
        "item_id" VARCHAR NOT NULL,
        "version_no" INTEGER NOT NULL,
        "category_key" VARCHAR NOT NULL,
        "lang" VARCHAR NOT NULL,
        "description" VARCHAR NOT NULL,
        "notes" VARCHAR
      );
    `);
    logger.debug('Creating category_key_info table');
    await cubeDB.query(`
      CREATE TABLE IF NOT EXISTS "category_key_info" (
        "category_key" VARCHAR NOT NULL,
        "lang" VARCHAR NOT NULL,
        "description" VARCHAR NOT NULL,
        "notes" VARCHAR
      );
    `);
    logger.debug('Creating category_info table');
    await cubeDB.query(`
      CREATE TABLE IF NOT EXISTS "category_info" (
        "category" VARCHAR NOT NULL,
        "lang" VARCHAR NOT NULL,
        "description" VARCHAR NOT NULL,
        "notes" VARCHAR
      );
    `);
    logger.debug('Creating hierarchy table');
    await cubeDB.query(`
      CREATE TABLE IF NOT EXISTS "hierarchy" (
        "item_id" VARCHAR NOT NULL,
        "version_no" INTEGER NOT NULL,
        "category_key" VARCHAR NOT NULL,
        "parent_id" VARCHAR NOT NULL,
        "parent_version" INTEGER NOT NULL,
        "parent_category" VARCHAR NOT NULL
      );
    `);
  } catch (error) {
    logger.error(error, `Something went wrong trying to create the initial reference data tables`);
    throw error;
  } finally {
    cubeDB.release();
  }
}

export async function loadReferenceDataFromCSV(searchPath: string): Promise<void> {
  logger.debug(`Loading categories and reference data from CSV into schema: ${searchPath}`);
  const csvFiles = [
    'categories',
    'category_info',
    'category_key_info',
    'category_keys',
    'hierarchy',
    'reference_data_all',
    'reference_data_info'
  ];

  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  await cubeDBConn.query(pgformat(`SET search_path TO %I;`, searchPath));

  try {
    for (const file of csvFiles) {
      logger.debug(`Loading data from ${file}.csv...`);
      const csvPath = path.resolve(__dirname, `../resources/reference-data/v1/${file}.csv`);
      const fileStream = fs.createReadStream(csvPath, { encoding: 'utf8' });
      const pgStream = cubeDBConn.query(from(`COPY ${file} FROM STDIN WITH (FORMAT csv, HEADER true)`));
      await pipeline(fileStream, pgStream).catch((error) => {
        logger.error(error, `Failed to load data from ${file}.csv`);
        throw error;
      });
      logger.debug(`Successfully loaded ${file}.csv`);
    }
  } finally {
    cubeDBConn.release();
  }
}

export const loadReferenceDataIntoCube = async (searchPath: string): Promise<void> => {
  logger.debug(`Loading reference data into cube ${searchPath}...`);
  await createReferenceDataTablesInCube(searchPath);
  await loadReferenceDataFromCSV(searchPath);
  logger.debug(`Reference data tables created and populated successfully.`);
};

export const cleanUpReferenceDataTables = async (cubeDB: QueryRunner): Promise<void> => {
  await cubeDB.query('DROP TABLE reference_data_all;');
  await cubeDB.query('DELETE FROM reference_data_info WHERE item_id NOT IN (SELECT item_id FROM reference_data);');
  await cubeDB.query('DELETE FROM category_keys WHERE category_key NOT IN (SELECT category_key FROM reference_data);');
  await cubeDB.query(
    'DELETE FROM category_Key_info WHERE category_key NOT IN (select category_key FROM category_keys);'
  );
  await cubeDB.query('DELETE FROM categories where category NOT IN (SELECT category FROM category_keys);');
  await cubeDB.query('DELETE FROM category_info WHERE category NOT IN (SELECT category FROM categories);');
  await cubeDB.query('DELETE FROM hierarchy WHERE item_id NOT IN (SELECT item_id FROM reference_data);');
};

export const loadCorrectReferenceDataIntoReferenceDataTable = async (
  cubeDB: QueryRunner,
  dimension: Dimension
): Promise<void> => {
  const extractor = dimension.extractor as ReferenceDataExtractor;
  for (const category of extractor.categories) {
    const categoryPresent = await cubeDB.query(
      pgformat('SELECT DISTINCT category_key FROM reference_data WHERE category_key=%L', category)
    );
    if (categoryPresent.length > 0) {
      continue;
    }
    logger.debug(`Copying ${category} reference data in to reference_data table`);
    await cubeDB.query(
      pgformat('INSERT INTO reference_data (SELECT * FROM reference_data_all WHERE category_key=%L);', category)
    );
  }
};

async function setupReferenceDataDimension(
  cubeDB: QueryRunner,
  dimension: Dimension,
  extendedSelectStatementsMap: Map<Locale, string[]>,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  defaultSortSelectStatementsMap: Map<Locale, string[]>,
  rawSortSelectStatementsMap: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[]
): Promise<void> {
  await loadCorrectReferenceDataIntoReferenceDataTable(cubeDB, dimension);
  const refDataInfo = `${makeCubeSafeString(dimension.factTableColumn)}_reference_data_info`;
  const refDataTbl = `${makeCubeSafeString(dimension.factTableColumn)}_reference_data`;
  SUPPORTED_LOCALES.map((locale) => {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    columnNames.get(locale)?.add(columnName);
    extendedSelectStatementsMap.get(locale)?.push(pgformat('%I.description AS %I', refDataInfo, columnName));
    extendedSelectStatementsMap.get(locale)?.push(pgformat('%I.item_id AS %I', refDataTbl, `${columnName}_ref`));
    extendedSelectStatementsMap.get(locale)?.push(pgformat('%I.sort_order AS %I', refDataTbl, `${columnName}_sort`));

    viewSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
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
      SELECT reference, language, fact_table_column, dimension_name, description, hierarchy
      FROM (SELECT DISTINCT
          %I as reference,
          %L as language,
          %L as fact_table_column,
          %L as dimension_name,
          reference_data_info.description as description,
          NULL as hierarchy,
          reference_data.sort_order as sort_order
        FROM fact_table
        LEFT JOIN reference_data on CAST(fact_table.%I AS VARCHAR)=reference_data.item_id
        JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id
        AND reference_data_info.lang=%L
        ORDER BY sort_order, description);
      `,
      dimension.factTableColumn,
      locale.toLowerCase(),
      dimension.factTableColumn,
      columnName,
      dimension.factTableColumn,
      locale.toLowerCase()
    );
    logger.debug(`Query = ${query}`);
    await cubeDB.query(query);
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
    start_date timestamp,
    end_date timestamp,
    date_type varchar,
    hierarchy %s
  );`,
    tableName,
    factTableColumn.columnName,
    factTableColumn.columnDatatype,
    factTableColumn.columnDatatype
  );
};

// This is a short version of validate date dimension code found in the dimension processor.
// This concise version doesn't return any information on why the creation failed.  Just that it failed
export async function createDateDimension(
  cubeDB: QueryRunner,
  extractor: object | null,
  factTableColumn: FactTableColumn
): Promise<string> {
  if (!extractor) {
    throw new Error('Extractor not supplied');
  }
  const safeColumnName = makeCubeSafeString(factTableColumn.columnName);
  const columnData: RowData[] = await cubeDB.query(
    pgformat(`SELECT DISTINCT %I FROM %I;`, factTableColumn.columnName, FACT_TABLE_NAME)
  );
  const dateDimensionTable = dateDimensionReferenceTableCreator(extractor as DateExtractor, columnData);
  await cubeDB.query(createDatePeriodTableQuery(factTableColumn));

  // Create the date_dimension table
  for (const row of dateDimensionTable) {
    await cubeDB.query(
      pgformat('INSERT INTO %I VALUES (%L)', `${safeColumnName}_lookup`, [
        row.dateCode,
        row.lang,
        row.description,
        row.start,
        row.end,
        row.type,
        row.hierarchy
      ])
    );
  }

  const periodCoverage: { start_date: Date; end_date: Date }[] = await cubeDB.query(
    `SELECT MIN(start_date) AS start_date, MAX(end_date) AS end_date FROM ${safeColumnName}_lookup;`
  );
  const metaDataCoverage: { key: string; value: string }[] = await cubeDB.query(
    "SELECT * FROM metadata WHERE key in ('start_date', 'end_date');"
  );
  logger.debug(`coverage: ${metaDataCoverage.length}`);
  if (metaDataCoverage.length > 0) {
    for (const metaData of metaDataCoverage) {
      if (metaData.key === 'start_date') {
        if (periodCoverage[0].start_date < toZonedTime(metaData.value, 'UTC')) {
          await cubeDB.query(
            `UPDATE metadata SET value='${periodCoverage[0].start_date.toISOString()}' WHERE key='start_date';`
          );
        }
      } else if (metaData.key === 'end_date') {
        if (periodCoverage[0].end_date > toZonedTime(metaData.value, 'UTC')) {
          await cubeDB.query(
            `UPDATE metadata SET value='${periodCoverage[0].start_date.toISOString()}' WHERE key='end_date';`
          );
        }
      }
    }
  } else {
    await cubeDB.query(
      `INSERT INTO metadata (key, value) VALUES ('start_date', '${periodCoverage[0].start_date.toISOString()}');`
    );
    await cubeDB.query(
      `INSERT INTO metadata (key, value) VALUES ('end_date', '${periodCoverage[0].start_date.toISOString()}');`
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
  cubeDB: QueryRunner,
  dataset: Dataset,
  dimension: Dimension,
  extendedSelectStatementsMap: Map<Locale, string[]>,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  defaultSortSelectStatementsMap: Map<Locale, string[]>,
  rawSortSelectStatementsMap: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
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
  await createLookupTableDimension(cubeDB, dataset, dimension, factTableColumn);

  SUPPORTED_LOCALES.map((locale) => {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    columnNames.get(locale)?.add(columnName);
    extendedSelectStatementsMap.get(locale)?.push(pgformat(`%I.description AS %I`, dimTable, columnName));
    extendedSelectStatementsMap
      .get(locale)
      ?.push(pgformat(`%I.%I AS %I`, dimTable, factTableColumn.columnName, `${columnName}_ref`));
    extendedSelectStatementsMap.get(locale)?.push(pgformat(`%I.sort_order AS %I`, dimTable, `${columnName}_sort`));
    extendedSelectStatementsMap.get(locale)?.push(pgformat(`%I.hierarchy AS %I`, dimTable, `${columnName}_hierarchy`));

    viewSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
  });
  joinStatements.push(
    `LEFT JOIN "${dimTable}" on "${dimTable}"."${factTableColumn.columnName}"=${FACT_TABLE_NAME}."${factTableColumn.columnName}" AND "${dimTable}".language=#LANG#`
  );

  orderByStatements.push(`"${dimTable}".sort_order`);

  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    await cubeDB.query(
      pgformat(
        `INSERT INTO filter_table
              SELECT reference, language, fact_table_column, dimension_name, description, hierarchy
              FROM (SELECT DISTINCT
              CAST(%I AS VARCHAR) AS reference, language, %L AS fact_table_column, %L AS dimension_name, description, hierarchy, sort_order
            FROM %I
            WHERE language = %L
            ORDER BY sort_order, description)`,
        dimension.factTableColumn,
        dimension.factTableColumn,
        columnName,
        dimTable,
        locale.toLowerCase(),
        dimension.factTableColumn
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
  performanceReporting(Math.round(start - performance.now()), 500, 'Loading a lookup table in to postgres');
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
  performanceReporting(Math.round(start - performance.now()), 500, 'Loading a data table in to postgres');
}

export async function createLookupTableDimension(
  cubeDB: QueryRunner,
  dataset: Dataset,
  dimension: Dimension,
  factTableColumn: FactTableColumn
): Promise<string> {
  logger.debug(`Creating and validating lookup table dimension ${dimension.factTableColumn}`);
  const lookupTablePresent = await cubeDB.query(
    pgformat(
      'SELECT * FROM information_schema.tables WHERE table_schema = %L AND table_name = %L',
      'lookup_tables',
      dimension.lookupTable!.id
    )
  );

  if (lookupTablePresent.length === 0) {
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
  await cubeDB.query(
    pgformat('CREATE TABLE %I AS SELECT * FROM lookup_tables.%I;', dimTable, dimension.lookupTable!.id)
  );
  return dimTable;
}

async function stripExistingRevisionCodes(
  cubeDB: QueryRunner,
  tableName: string,
  notesCodeColumn?: FactTableColumn
): Promise<void> {
  if (!notesCodeColumn) return;
  const removeProvisionalCodesQuery = pgformat(
    `UPDATE %I SET %I = array_to_string(array_remove(string_to_array(replace(lower(%I.%I), ' ', ''), ','),'r'),',');`,
    tableName,
    notesCodeColumn.columnName,
    tableName,
    notesCodeColumn.columnName
  );
  await cubeDB.query(removeProvisionalCodesQuery);
}

async function stripExistingProvisionalCodes(cubeDB: QueryRunner, notesCodeColumn?: FactTableColumn): Promise<void> {
  if (!notesCodeColumn) return;
  const removeProvisionalCodesQuery = pgformat(
    `UPDATE %I SET %I = array_to_string(array_remove(string_to_array(replace(lower(%I.%I), ' ', ''), ','),'p'),',');`,
    FACT_TABLE_NAME,
    notesCodeColumn.columnName,
    FACT_TABLE_NAME,
    notesCodeColumn.columnName
  );
  await cubeDB.query(removeProvisionalCodesQuery);
}

async function stripExistingForecastCodes(cubeDB: QueryRunner, notesCodeColumn?: FactTableColumn): Promise<void> {
  if (!notesCodeColumn) return;
  const removeProvisionalCodesQuery = pgformat(
    `UPDATE %I SET %I = array_to_string(array_remove(string_to_array(replace(lower(%I.%I), ' ', ''), ','),'f'),',');`,
    FACT_TABLE_NAME,
    notesCodeColumn.columnName,
    FACT_TABLE_NAME,
    notesCodeColumn.columnName
  );
  await cubeDB.query(removeProvisionalCodesQuery);
}

function setupFactTableUpdateJoins(
  factTableName: string,
  updateTableName: string,
  dataValuesColumn: FactTableColumn | undefined,
  factIdentifiers: FactTableColumn[],
  dataTableIdentifiers: DataTableDescription[]
): string {
  const joinParts: string[] = [];
  for (const factTableCol of factIdentifiers) {
    const dataTableCol = dataTableIdentifiers.find((col) => col.factTableColumn === factTableCol.columnName);
    joinParts.push(
      pgformat(
        'CAST(%I.%I AS VARCHAR) = CAST(%I.%I AS VARCHAR)',
        factTableName,
        factTableCol.columnName,
        updateTableName,
        dataTableCol?.columnName
      )
    );
  }
  if (dataValuesColumn) {
    joinParts.push(
      pgformat(
        '%I.%I != %I.%I',
        FACT_TABLE_NAME,
        dataValuesColumn.columnName,
        updateTableName,
        dataValuesColumn.columnName
      )
    );
  }
  return joinParts.join(' AND ');
}

async function fixNoteCodesOnUpdateTable(
  cubeDB: QueryRunner,
  updateTableName: string,
  notesCodeColumn: FactTableColumn,
  dataValuesColumn: FactTableColumn | undefined,
  factIdentifiers: FactTableColumn[],
  dataTableIdentifiers: DataTableDescription[]
): Promise<void> {
  await stripExistingRevisionCodes(cubeDB, updateTableName, notesCodeColumn!);
  const updateQuery = pgformat(
    `UPDATE %I SET %I = array_to_string(array_append(array_remove(string_to_array(lower(%I.%I), ','), 'r'), 'r'), ',') FROM %I WHERE %s`,
    updateTableName,
    notesCodeColumn.columnName,
    updateTableName,
    notesCodeColumn.columnName,
    FACT_TABLE_NAME,
    setupFactTableUpdateJoins(FACT_TABLE_NAME, updateTableName, dataValuesColumn, factIdentifiers, dataTableIdentifiers)
  );
  await cubeDB.query(updateQuery);
}

async function updateFactsTableFromUpdateTable(
  cubeDB: QueryRunner,
  updateTableName: string,
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  factIdentifiers: FactTableColumn[],
  dataTableIdentifiers: DataTableDescription[]
): Promise<void> {
  const joinParts: string[] = [];
  for (const factTableCol of factIdentifiers) {
    const dataTableCol = dataTableIdentifiers.find((col) => col.factTableColumn === factTableCol.columnName);
    joinParts.push(
      pgformat(
        'CAST(%I.%I AS VARCHAR) = CAST(%I.%I AS VARCHAR)',
        FACT_TABLE_NAME,
        factTableCol.columnName,
        updateTableName,
        dataTableCol?.columnName
      )
    );
  }
  const updateQuery = pgformat(
    `UPDATE %I SET %I = %I.%I, %I = %I.%I FROM %I WHERE %s`,
    FACT_TABLE_NAME,
    dataValuesColumn.columnName,
    updateTableName,
    dataValuesColumn.columnName,
    notesCodeColumn.columnName,
    updateTableName,
    notesCodeColumn.columnName,
    updateTableName,
    joinParts.join(' AND ')
  );
  await cubeDB.query(updateQuery);
}

async function createUpdateTable(cubeDB: QueryRunner, tempTableName: string, dataTable: DataTable): Promise<void> {
  const createUpdateTableQuery = pgformat(
    'CREATE TEMPORARY TABLE %I AS SELECT * FROM data_tables.%I;',
    tempTableName,
    dataTable.id
  );
  await cubeDB.query(createUpdateTableQuery);
}

async function copyUpdateTableToFactTable(
  cubeDB: QueryRunner,
  updateTableName: string,
  factTableDef: string[],
  factIdentifiers: FactTableColumn[],
  dataTableIdentifiers: DataTableDescription[]
): Promise<void> {
  const joinParts: string[] = [];
  for (const factTableCol of factIdentifiers) {
    const dataTableCol = dataTableIdentifiers.find((col) => col.factTableColumn === factTableCol.columnName);
    if (!dataTableCol) continue;
    joinParts.push(
      pgformat('%I.%I = %I.%I', FACT_TABLE_NAME, factTableCol.columnName, updateTableName, dataTableCol.columnName)
    );
  }
  const dataTableSelect: string[] = [];
  for (const col of factTableDef) {
    const dataTableCol = dataTableIdentifiers.find((dataTableCol) => dataTableCol.factTableColumn === col);
    if (dataTableCol) dataTableSelect.push(dataTableCol.factTableColumn);
  }
  // First remove values which already exist in the fact table
  const cleanUpUpdateTableQuery = pgformat(
    `DELETE FROM %I USING %I WHERE %s`,
    FACT_TABLE_NAME,
    updateTableName,
    joinParts.join(' AND ')
  );
  await cubeDB.query(cleanUpUpdateTableQuery);
  // Now copy over anything else which remains
  const copyQuery = pgformat(
    'INSERT INTO %I (%I) (SELECT %I FROM %I);',
    FACT_TABLE_NAME,
    factTableDef,
    dataTableSelect,
    updateTableName
  );
  logger.debug(copyQuery);
  await cubeDB.query(copyQuery);
}

async function resetFactTable(cubeDB: QueryRunner): Promise<void> {
  await cubeDB.query(pgformat('DELETE FROM %I;', FACT_TABLE_NAME));
}

async function dropUpdateTable(cubeDB: QueryRunner, updateTableName: string): Promise<void> {
  await cubeDB.query(pgformat('DROP TABLE %I', updateTableName));
}

async function finaliseValues(
  cubeDB: QueryRunner,
  updateTableName: string,
  factIdentifiers: FactTableColumn[],
  dataTableIdentifiers: DataTableDescription[],
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn
): Promise<void> {
  const joinParts: string[] = [];
  for (const factTableCol of factIdentifiers) {
    const dataTableCol = dataTableIdentifiers.find((col) => col.factTableColumn === factTableCol.columnName);
    joinParts.push(
      pgformat(
        'CAST(%I.%I AS VARCHAR) = CAST(%I.%I AS VARCHAR)',
        FACT_TABLE_NAME,
        factTableCol.columnName,
        updateTableName,
        dataTableCol?.columnName
      )
    );
  }
  const updateQuery = pgformat(
    `UPDATE %I SET %I = %I.%I, %I = array_to_string(array_append(string_to_array(lower(%I.%I), ','), '!'), ',') FROM %I WHERE %s AND string_to_array(lower(%I.%I), ',') && string_to_array('p,f', ',');`,
    FACT_TABLE_NAME,
    dataValuesColumn.columnName,
    updateTableName,
    dataValuesColumn.columnName,
    notesCodeColumn.columnName,
    updateTableName,
    notesCodeColumn.columnName,
    updateTableName,
    joinParts.join(' AND '),
    FACT_TABLE_NAME,
    notesCodeColumn.columnName
  );
  await cubeDB.query(updateQuery);
  await cubeDB.query(
    pgformat(
      `DELETE FROM %I USING %I WHERE %s AND string_to_array(%I.%I, ',') && string_to_array('!', ',');`,
      updateTableName,
      FACT_TABLE_NAME,
      joinParts.join(' AND '),
      FACT_TABLE_NAME,
      notesCodeColumn.columnName
    )
  );
  await cubeDB.query(
    pgformat(
      `UPDATE %I SET %I = array_to_string(array_remove(string_to_array(%I, ','), '!'), ',')`,
      FACT_TABLE_NAME,
      notesCodeColumn.columnName,
      notesCodeColumn.columnName
    )
  );
}

async function updateProvisionalsAndForecasts(
  cubeDB: QueryRunner,
  updateTableName: string,
  factIdentifiers: FactTableColumn[],
  dataTableIdentifiers: DataTableDescription[],
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn
): Promise<void> {
  const joinParts: string[] = [];
  for (const factTableCol of factIdentifiers) {
    const dataTableCol = dataTableIdentifiers.find((col) => col.factTableColumn === factTableCol.columnName);
    joinParts.push(
      pgformat(
        'CAST(%I.%I AS VARCHAR) = CAST(%I.%I AS VARCHAR)',
        FACT_TABLE_NAME,
        factTableCol.columnName,
        updateTableName,
        dataTableCol?.columnName
      )
    );
  }
  const updateQuery = pgformat(
    `UPDATE %I SET %I = %I.%I, %I = %I.%I FROM %I WHERE %s AND string_to_array(%I.%I, ',') && string_to_array('p,f', ',');`,
    FACT_TABLE_NAME,
    dataValuesColumn.columnName,
    updateTableName,
    dataValuesColumn.columnName,
    notesCodeColumn.columnName,
    updateTableName,
    notesCodeColumn.columnName,
    updateTableName,
    joinParts.join(' AND '),
    updateTableName,
    notesCodeColumn.columnName
  );
  await cubeDB.query(updateQuery);
  await cubeDB.query(
    pgformat(
      `DELETE FROM %I WHERE string_to_array(%I, ',') && string_to_array('p,f', ',');`,
      updateTableName,
      notesCodeColumn.columnName
    )
  );
}

async function loadFactTablesWithUpdates(
  cubeDB: QueryRunner,
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
    const dataTablePresent = await cubeDB.query(
      pgformat(
        'SELECT * FROM information_schema.tables WHERE table_schema = %L AND table_name = %L',
        'data_tables',
        dataTable.id
      )
    );

    if (dataTablePresent.length === 0) {
      logger.warn('Data table not loaded in to data_tables schema.  Loading data table from blob storage.');
      await loadFileIntoDataTablesSchema(dataset, dataTable);
    }

    let doRevision = false;
    if (dataValuesColumn && notesCodeColumn && factIdentifiers.length > 0) {
      doRevision = true;
    } else {
      logger.warn(
        'No notes code or data value columns defined.  Unable to do revise and add/revise actions.  These tables will be skipped.'
      );
    }

    try {
      logger.debug(`Performing action ${dataTable.action} on fact table for data table ${dataTable.id}`);
      switch (dataTable.action) {
        case DataTableAction.ReplaceAll:
          await resetFactTable(cubeDB);
          await loadTableDataIntoFactTableFromPostgres(cubeDB, factTableDef, FACT_TABLE_NAME, dataTable.id);
          break;
        case DataTableAction.Add:
          await stripExistingProvisionalCodes(cubeDB, notesCodeColumn);
          await stripExistingForecastCodes(cubeDB, notesCodeColumn!);
          await stripExistingRevisionCodes(cubeDB, FACT_TABLE_NAME, notesCodeColumn);
          await loadTableDataIntoFactTableFromPostgres(cubeDB, factTableDef, FACT_TABLE_NAME, dataTable.id);
          break;
        case DataTableAction.Revise:
          if (!doRevision) continue;
          await createUpdateTable(cubeDB, actionID, dataTable);
          await finaliseValues(
            cubeDB,
            actionID,
            factIdentifiers,
            dataTable.dataTableDescriptions,
            dataValuesColumn!,
            notesCodeColumn!
          );
          await stripExistingProvisionalCodes(cubeDB, notesCodeColumn!);
          await stripExistingForecastCodes(cubeDB, notesCodeColumn!);
          await stripExistingRevisionCodes(cubeDB, FACT_TABLE_NAME, notesCodeColumn!);
          await updateProvisionalsAndForecasts(
            cubeDB,
            actionID,
            factIdentifiers,
            dataTable.dataTableDescriptions,
            dataValuesColumn!,
            notesCodeColumn!
          );
          await fixNoteCodesOnUpdateTable(
            cubeDB,
            actionID,
            notesCodeColumn!,
            dataValuesColumn,
            factIdentifiers,
            dataTable.dataTableDescriptions
          );
          await updateFactsTableFromUpdateTable(
            cubeDB,
            actionID,
            dataValuesColumn!,
            notesCodeColumn!,
            factIdentifiers,
            dataTable.dataTableDescriptions
          );
          await dropUpdateTable(cubeDB, actionID);
          break;
        case DataTableAction.AddRevise:
          if (!doRevision) continue;
          await createUpdateTable(cubeDB, actionID, dataTable);
          await finaliseValues(
            cubeDB,
            actionID,
            factIdentifiers,
            dataTable.dataTableDescriptions,
            dataValuesColumn!,
            notesCodeColumn!
          );
          await stripExistingProvisionalCodes(cubeDB, notesCodeColumn!);
          await stripExistingForecastCodes(cubeDB, notesCodeColumn!);
          await stripExistingRevisionCodes(cubeDB, FACT_TABLE_NAME, notesCodeColumn!);
          await updateProvisionalsAndForecasts(
            cubeDB,
            actionID,
            factIdentifiers,
            dataTable.dataTableDescriptions,
            dataValuesColumn!,
            notesCodeColumn!
          );
          await fixNoteCodesOnUpdateTable(
            cubeDB,
            actionID,
            notesCodeColumn!,
            dataValuesColumn,
            factIdentifiers,
            dataTable.dataTableDescriptions
          );
          await updateFactsTableFromUpdateTable(
            cubeDB,
            actionID,
            dataValuesColumn!,
            notesCodeColumn!,
            factIdentifiers,
            dataTable.dataTableDescriptions
          );
          await copyUpdateTableToFactTable(
            cubeDB,
            actionID,
            factTableDef,
            factIdentifiers,
            dataTable.dataTableDescriptions
          );
          await dropUpdateTable(cubeDB, actionID);
          break;
        case DataTableAction.Correction:
          if (!doRevision) continue;
          await createUpdateTable(cubeDB, actionID, dataTable);
          await updateFactsTableFromUpdateTable(
            cubeDB,
            actionID,
            dataValuesColumn!,
            notesCodeColumn!,
            factIdentifiers,
            dataTable.dataTableDescriptions
          );
          await dropUpdateTable(cubeDB, actionID);
          break;
      }
    } catch (error) {
      logger.error(error, `Something went wrong trying to create the core fact table`);
    }
  }
}

async function cleanupNotesCodeColumn(cubeDB: QueryRunner, notesCodeColumn: FactTableColumn): Promise<void> {
  await cubeDB.query(
    pgformat(
      `UPDATE %I SET %I = NULL WHERE %I = '';`,
      FACT_TABLE_NAME,
      notesCodeColumn.columnName,
      notesCodeColumn.columnName
    )
  );
}

export async function loadFactTables(
  cubeDB: QueryRunner,
  dataset: Dataset,
  endRevision: Revision,
  factTableDef: string[],
  dataValuesColumn: FactTableColumn | undefined,
  notesCodeColumn: FactTableColumn | undefined,
  factIdentifiers: FactTableColumn[]
): Promise<void> {
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
      cubeDB,
      dataset,
      allFactTables.reverse(),
      factTableDef,
      dataValuesColumn,
      notesCodeColumn,
      factIdentifiers
    );
    if (notesCodeColumn) {
      await cleanupNotesCodeColumn(cubeDB, notesCodeColumn);
    }
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
  cubeDB: QueryRunner,
  notesColumn: FactTableColumn,
  extendedSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  defaultSortSelectStatementsMap: Map<Locale, string[]>,
  rawSortSelectStatementsMap: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[]
): Promise<void> {
  logger.info('Creating notes table...');
  try {
    await cubeDB.query(
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
        await cubeDB.query(query);
      }
    }
    logger.info('Creating notes table view...');
    // We perform join operations to this view as we want to turn a csv such as `a,r` in to `Average, Revised`.
    await cubeDB.query(
      `CREATE TABLE all_notes AS SELECT fact_table."${notesColumn.columnName}" as code, note_codes.language as language, string_agg(DISTINCT note_codes.description, ', ') as description
            from fact_table JOIN note_codes ON array_position(string_to_array(fact_table."${notesColumn.columnName}", ','), note_codes.code) IS NOT NULL
            GROUP BY fact_table."${notesColumn.columnName}", note_codes.language;`
    );
  } catch (error) {
    logger.error(`Something went wrong trying to create the notes table with error: ${error}`);
    throw new Error(`Something went wrong trying to create the notes code table with the following error: ${error}`);
  }
  for (const locale of SUPPORTED_LOCALES) {
    const columnName = t('column_headers.notes', { lng: locale });
    extendedSelectStatementsMap.get(locale)?.push(pgformat('all_notes.description AS %I', columnName));
    extendedSelectStatementsMap.get(locale)?.push(pgformat('all_notes.description AS %I', `${columnName}_sort`));

    rawSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
  }
  joinStatements.push(
    `LEFT JOIN all_notes on all_notes.code=fact_table."${notesColumn.columnName}" AND all_notes.language=#LANG#`
  );
  await cubeDB.query(
    pgformat(
      `INSERT INTO metadata VALUES ('note_codes', (SELECT ARRAY_TO_STRING(ARRAY(SELECT DISTINCT unnest(string_to_array(%I, ',')) from %I WHERE %I IS NOT NULL), ',') AS note_codes));`,
      notesColumn.columnName,
      FACT_TABLE_NAME,
      notesColumn.columnName
    )
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
  cubeDB: QueryRunner,
  measureColumn: FactTableColumn,
  measureTable: MeasureRow[]
): Promise<void> {
  await cubeDB.query(measureTableCreateStatement(measureColumn.columnDatatype));
  for (const row of measureTable) {
    const values = [
      row.reference,
      row.language.toLowerCase(),
      row.description,
      row.notes ? row.notes : null,
      row.sortOrder ? row.sortOrder : null,
      row.format,
      row.decimal ? row.decimal : null,
      row.measureType ? row.measureType : null,
      row.hierarchy ? row.hierarchy : null
    ];
    await cubeDB.query('INSERT INTO measure VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)', values);
  }
}

function setupMeasureNoDataValues(
  extendedSelectStatementsMap: Map<Locale, string[]>,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  defaultSortSelectStatementsMap: Map<Locale, string[]>,
  rawSortSelectStatementsMap: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  measureColumn?: FactTableColumn,
  dataValuesColumn?: FactTableColumn,
  notesCodeColumn?: FactTableColumn
): void {
  SUPPORTED_LOCALES.map((locale) => {
    if (dataValuesColumn) {
      columnNames.get(locale)?.add(t('column_headers.data_values', { lng: locale }));
      extendedSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            '%I.%I AS %I',
            FACT_TABLE_NAME,
            dataValuesColumn.columnName,
            t('column_headers.data_values', { lng: locale })
          )
        );
      extendedSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            '%I.%I AS %I',
            FACT_TABLE_NAME,
            dataValuesColumn.columnName,
            `${t('column_headers.data_values', { lng: locale })}_sort`
          )
        );
      if (notesCodeColumn) {
        extendedSelectStatementsMap
          .get(locale)
          ?.push(
            pgformat(
              `CASE WHEN %I.%I IS NULL THEN CAST(%I.%I AS VARCHAR) ELSE %I.%I || ' [' || array_to_string(string_to_array(%I.%I, ','), '] [') || ']' END AS %I`,
              FACT_TABLE_NAME,
              notesCodeColumn.columnName,
              FACT_TABLE_NAME,
              dataValuesColumn.columnName,
              FACT_TABLE_NAME,
              dataValuesColumn.columnName,
              FACT_TABLE_NAME,
              notesCodeColumn.columnName,
              `${t('column_headers.data_values', { lng: locale })}_annotated`
            )
          );
      } else {
        extendedSelectStatementsMap
          .get(locale)
          ?.push(
            pgformat(
              '%I.%I AS %I',
              FACT_TABLE_NAME,
              dataValuesColumn.columnName,
              `${t('column_headers.data_values', { lng: locale })}_annotated`
            )
          );
      }
      viewSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            '%I AS %I',
            `${t('column_headers.data_values', { lng: locale })}_annotated`,
            t('column_headers.data_values', { lng: locale })
          )
        );
      rawSelectStatementsMap.get(locale)?.push(pgformat('%I', t('column_headers.data_values', { lng: locale })));
      defaultSortSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            '%I AS %I',
            `${t('column_headers.data_values', { lng: locale })}_annotated`,
            t('column_headers.data_values', { lng: locale })
          )
        );
      defaultSortSelectStatementsMap
        .get(locale)
        ?.push(pgformat('%I', `${t('column_headers.data_values', { lng: locale })}_sort`));
      rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', t('column_headers.data_values', { lng: locale })));
      rawSortSelectStatementsMap
        .get(locale)
        ?.push(pgformat('%I', `${t('column_headers.data_values', { lng: locale })}_sort`));
    }
    if (measureColumn) {
      columnNames.get(locale)?.add(t('column_headers.measure', { lng: locale }));
      extendedSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            '%I.%I AS %I',
            FACT_TABLE_NAME,
            measureColumn.columnName,
            t('column_headers.measure', { lng: locale })
          )
        );
      extendedSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            '%I.%I AS %I',
            FACT_TABLE_NAME,
            measureColumn.columnName,
            `${t('column_headers.measure', { lng: locale })}_sort`
          )
        );
      extendedSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            '%I.%I AS %I',
            FACT_TABLE_NAME,
            measureColumn.columnName,
            `${t('column_headers.measure', { lng: locale })}_ref`
          )
        );
      extendedSelectStatementsMap
        .get(locale)
        ?.push(pgformat('NULL AS %I', `${t('column_headers.measure', { lng: locale })}_hierarchy`));

      viewSelectStatementsMap.get(locale)?.push(pgformat('%I', t('column_headers.measure', { lng: locale })));
      rawSelectStatementsMap.get(locale)?.push(pgformat('%I', t('column_headers.measure', { lng: locale })));
      defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', t('column_headers.measure', { lng: locale })));
      defaultSortSelectStatementsMap
        .get(locale)
        ?.push(pgformat('%I', `${t('column_headers.measure', { lng: locale })}_sort`));
      rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', t('column_headers.measure', { lng: locale })));
      rawSortSelectStatementsMap
        .get(locale)
        ?.push(pgformat('%I', `${t('column_headers.measure', { lng: locale })}_sort`));
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
  cubeDB: QueryRunner,
  dataset: Dataset,
  dataValuesColumn: FactTableColumn,
  measureColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  extendedSelectStatementsMap: Map<Locale, string[]>,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  defaultSortSelectStatementsMap: Map<Locale, string[]>,
  rawSortSelectStatementsMap: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[]
): Promise<void> {
  logger.info('Setting up measure table if present...');

  // Process the column that represents the measure
  if (dataset.measure && dataset.measure.measureTable && dataset.measure.measureTable.length > 0) {
    logger.debug('Measure present in dataset. Creating measure table...');
    await createMeasureLookupTable(cubeDB, measureColumn, dataset.measure.measureTable);

    logger.debug('Creating query part to format the data value correctly');

    const uniqueReferences: UniqueMeasureDetails[] = await cubeDB.query(
      pgformat('SELECT DISTINCT reference, format, sort_order, decimals FROM measure;')
    );
    const caseStatements: string[] = ['CASE'];
    for (const row of uniqueReferences) {
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
      const columnName = t('column_headers.measure', { lng: locale });
      columnNames.get(locale)?.add(t('column_headers.data_values', { lng: locale }));
      // columnNames.get(locale)?.add(t('column_headers.data_description', { lng: locale }));
      columnNames.get(locale)?.add(columnName);
      if (dataValuesColumn) {
        // Add all variations of the column to the core or extended view of the dataset
        extendedSelectStatementsMap
          .get(locale)
          ?.push(
            pgformat(
              '%I.%I AS %I',
              FACT_TABLE_NAME,
              dataValuesColumn.columnName,
              t('column_headers.data_values', { lng: locale })
            )
          );
        extendedSelectStatementsMap
          .get(locale)
          ?.push(
            pgformat(
              `%s AS %I`,
              caseStatements.join('\n'),
              `${t('column_headers.data_values', { lng: locale })}_formatted`
            )
          );
        extendedSelectStatementsMap
          .get(locale)
          ?.push(
            pgformat(
              `CASE WHEN %I.%I IS NULL THEN %s ELSE %s || ' [' || array_to_string(string_to_array(lower(%I.%I), ','), '] [') || ']' END AS %I`,
              FACT_TABLE_NAME,
              notesCodeColumn.columnName,
              caseStatements.join('\n'),
              caseStatements.join('\n'),
              FACT_TABLE_NAME,
              notesCodeColumn.columnName,
              `${t('column_headers.data_values', { lng: locale })}_annotated`
            )
          );
        extendedSelectStatementsMap
          .get(locale)
          ?.push(
            pgformat(
              '%I.%I AS %I',
              FACT_TABLE_NAME,
              dataValuesColumn.columnName,
              `${t('column_headers.data_values', { lng: locale })}_sort`
            )
          );

        // Individual Views are now created
        rawSelectStatementsMap.get(locale)?.push(pgformat('%I', t('column_headers.data_values', { lng: locale })));
        viewSelectStatementsMap
          .get(locale)
          ?.push(
            pgformat(
              `%I AS %I`,
              `${t('column_headers.data_values', { lng: locale })}_annotated`,
              t('column_headers.data_values', { lng: locale })
            )
          );
        defaultSortSelectStatementsMap
          .get(locale)
          ?.push(
            pgformat(
              `%I AS %I`,
              `${t('column_headers.data_values', { lng: locale })}_annotated`,
              t('column_headers.data_values', { lng: locale })
            )
          );
        defaultSortSelectStatementsMap
          .get(locale)
          ?.push(pgformat(`%I`, `${t('column_headers.data_values', { lng: locale })}_sort`));
        rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', t('column_headers.data_values', { lng: locale })));
        rawSortSelectStatementsMap
          .get(locale)
          ?.push(pgformat(`%I`, `${t('column_headers.data_values', { lng: locale })}_sort`));
      }
      extendedSelectStatementsMap.get(locale)?.push(pgformat('measure.description AS %I', columnName));
      extendedSelectStatementsMap.get(locale)?.push(pgformat('measure.reference AS %I', `${columnName}_ref`));
      extendedSelectStatementsMap.get(locale)?.push(pgformat('measure.sort_order AS %I', `${columnName}_sort`));
      extendedSelectStatementsMap.get(locale)?.push(pgformat('measure.hierarchy AS %I', `${columnName}_hierarchy`));

      viewSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
      rawSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
      defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
      defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
      rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
      rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
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
      const columnName = t('column_headers.measure', { lng: locale });
      await cubeDB.query(
        pgformat(
          `INSERT INTO filter_table SELECT CAST(reference AS VARCHAR), language, %L, %L, description, CAST(hierarchy AS VARCHAR) FROM measure WHERE language = %L ORDER BY sort_order, reference`,
          measureColumn.columnName,
          columnName,
          locale.toLowerCase()
        )
      );
    }
  } else {
    setupMeasureNoDataValues(
      extendedSelectStatementsMap,
      viewSelectStatementsMap,
      rawSelectStatementsMap,
      defaultSortSelectStatementsMap,
      rawSortSelectStatementsMap,
      columnNames,
      measureColumn,
      dataValuesColumn,
      notesCodeColumn
    );
  }
}

function updateColumnName(existingColumnNames: Set<string>, proposedColumnName: string): string {
  let columnName = proposedColumnName;
  let count = 1;
  while (existingColumnNames.has(columnName)) {
    columnName = `${proposedColumnName}_${count}`;
    count++;
  }
  return columnName;
}

async function rawDimensionProcessor(
  cubeDB: QueryRunner,
  dimension: Dimension,
  extendedSelectStatementsMap: Map<Locale, string[]>,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  defaultSortSelectStatementsMap: Map<Locale, string[]>,
  rawSortSelectStatementsMap: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>
): Promise<void> {
  for (const locale of SUPPORTED_LOCALES) {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    columnNames.get(locale)?.add(columnName);
    extendedSelectStatementsMap.get(locale)?.push(pgformat('%I AS %I', dimension.factTableColumn, columnName));
    extendedSelectStatementsMap
      .get(locale)
      ?.push(pgformat('%I AS %I', dimension.factTableColumn, `${columnName}_sort`));
    extendedSelectStatementsMap.get(locale)?.push(pgformat('NULL AS %I', `${columnName}_hierarchy`));

    viewSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
  }

  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    await cubeDB.query(
      pgformat(
        `INSERT INTO filter_table
         SELECT DISTINCT CAST(%I AS VARCHAR), %L, %L, %L, CAST (%I AS VARCHAR), NULL
         FROM %I ORDER BY %I`,
        dimension.factTableColumn,
        locale.toLowerCase(),
        dimension.factTableColumn,
        columnName,
        dimension.factTableColumn,
        FACT_TABLE_NAME,
        dimension.factTableColumn
      )
    );
  }
}

async function dateDimensionProcessor(
  cubeDB: QueryRunner,
  factTableColumn: FactTableColumn,
  dimension: Dimension,
  extendedSelectStatementsMap: Map<Locale, string[]>,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  defaultSortSelectStatementsMap: Map<Locale, string[]>,
  rawSortSelectStatementsMap: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[]
): Promise<string> {
  const dimTable = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
  await createDateDimension(cubeDB, dimension.extractor, factTableColumn);
  for (const locale of SUPPORTED_LOCALES) {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    columnNames.get(locale)?.add(columnName);
    extendedSelectStatementsMap.get(locale)?.push(pgformat('%I.description AS %I', dimTable, columnName));
    extendedSelectStatementsMap
      .get(locale)
      ?.push(pgformat('%I.%I AS %I', dimTable, factTableColumn.columnName, `${columnName}_ref`));
    extendedSelectStatementsMap.get(locale)?.push(pgformat('%I.end_date AS %I', dimTable, `${columnName}_sort`));
    extendedSelectStatementsMap
      .get(locale)
      ?.push(
        pgformat(
          "TO_CHAR(%I.start_date, 'DD/MM/YYYY') AS %I",
          dimTable,
          `${columnName} ${t('column_headers.start_date')}`
        )
      );
    extendedSelectStatementsMap
      .get(locale)
      ?.push(
        pgformat("TO_CHAR(%I.end_date, 'DD/MM/YYYY') AS %I", dimTable, `${columnName} ${t('column_headers.end_date')}`)
      );
    extendedSelectStatementsMap.get(locale)?.push(pgformat('%I.hierarchy AS %I', dimTable, `${columnName}_hierarchy`));

    viewSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));

    const insertQuery = pgformat(
      `INSERT INTO filter_table
         SELECT CAST(%I AS VARCHAR), language, %L, %L, description, CAST (hierarchy AS VARCHAR)
         FROM %I
         WHERE language = %L ORDER BY end_date`,
      factTableColumn.columnName,
      factTableColumn.columnName,
      columnName,
      dimTable,
      locale.toLowerCase()
    );
    await cubeDB.query(insertQuery);
  }
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
  return dimTable;
}

async function setupNumericDimension(
  cubeDB: QueryRunner,
  dimension: Dimension,
  extendedSelectStatementsMap: Map<Locale, string[]>,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  defaultSortSelectStatementsMap: Map<Locale, string[]>,
  rawSortSelectStatementsMap: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>
): Promise<void> {
  SUPPORTED_LOCALES.map((locale) => {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    columnNames.get(locale)?.add(columnName);
    if ((dimension.extractor as NumberExtractor).type === NumberType.Integer) {
      extendedSelectStatementsMap
        .get(locale)
        ?.push(pgformat('CAST(%I.%I AS INTEGER) AS %I', FACT_TABLE_NAME, dimension.factTableColumn, columnName));
      extendedSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat('CAST(%I.%I AS INTEGER) AS %I', FACT_TABLE_NAME, dimension.factTableColumn, `${columnName}_sort`)
        );
    } else {
      extendedSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            `format('%%s', TO_CHAR(ROUND(CAST(%I.%I AS DECIMAL), %L), '999,999,990.%s')) AS %I`,
            FACT_TABLE_NAME,
            dimension.factTableColumn,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            columnName
          )
        );
      extendedSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            `format('%%s', TO_CHAR(ROUND(CAST(%I.%I AS DECIMAL), %L), '999,999,990.%s')) AS %I`,
            FACT_TABLE_NAME,
            dimension.factTableColumn,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            `${columnName}_sort`
          )
        );
    }
    viewSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
  });

  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    await cubeDB.query(
      pgformat(
        `INSERT INTO filter_table
         SELECT DISTINCT CAST(%I AS VARCHAR), %L, %L, %L, CAST (%I AS VARCHAR), NULL
         FROM %I ORDER BY %I`,
        dimension.factTableColumn,
        locale.toLowerCase(),
        dimension.factTableColumn,
        columnName,
        dimension.factTableColumn,
        FACT_TABLE_NAME,
        dimension.factTableColumn
      )
    );
  }
}

async function setupTextDimension(
  cubeDB: QueryRunner,
  dimension: Dimension,
  extendedSelectStatementsMap: Map<Locale, string[]>,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  defaultSortSelectStatementsMap: Map<Locale, string[]>,
  rawSortSelectStatementsMap: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>
): Promise<void> {
  SUPPORTED_LOCALES.map((locale) => {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    columnNames.get(locale)?.add(columnName);
    extendedSelectStatementsMap
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, columnName));
    extendedSelectStatementsMap
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, `${columnName}_sort`));
    viewSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    defaultSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', columnName));
    rawSortSelectStatementsMap.get(locale)?.push(pgformat('%I', `${columnName}_sort`));
  });

  for (const locale of SUPPORTED_LOCALES) {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    await cubeDB.query(
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
  cubeDB: QueryRunner,
  dataset: Dataset,
  endRevision: Revision,
  extendedSelectStatementsMap: Map<Locale, string[]>,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  defaultSortSelectStatementsMap: Map<Locale, string[]>,
  rawSortSelectStatementsMap: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
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
    if (endRevision.tasks && endRevision.tasks.dimensions.find((dim) => dim.id === dimension.id)) {
      await rawDimensionProcessor(
        cubeDB,
        dimension,
        extendedSelectStatementsMap,
        viewSelectStatementsMap,
        rawSelectStatementsMap,
        defaultSortSelectStatementsMap,
        rawSortSelectStatementsMap,
        columnNames
      );
      continue;
    }
    try {
      switch (dimension.type) {
        case DimensionType.DatePeriod:
        case DimensionType.Date:
          if (dimension.extractor) {
            tableName = await dateDimensionProcessor(
              cubeDB,
              factTableColumn,
              dimension,
              extendedSelectStatementsMap,
              viewSelectStatementsMap,
              rawSelectStatementsMap,
              defaultSortSelectStatementsMap,
              rawSortSelectStatementsMap,
              columnNames,
              joinStatements,
              orderByStatements
            );
            lookupTables.add(tableName);
          } else {
            await rawDimensionProcessor(
              cubeDB,
              dimension,
              extendedSelectStatementsMap,
              viewSelectStatementsMap,
              rawSelectStatementsMap,
              defaultSortSelectStatementsMap,
              rawSortSelectStatementsMap,
              columnNames
            );
          }
          break;
        case DimensionType.LookupTable:
          tableName = await setupLookupTableDimension(
            cubeDB,
            dataset,
            dimension,
            extendedSelectStatementsMap,
            viewSelectStatementsMap,
            rawSelectStatementsMap,
            defaultSortSelectStatementsMap,
            rawSortSelectStatementsMap,
            columnNames,
            joinStatements,
            orderByStatements
          );
          lookupTables.add(tableName);
          break;
        case DimensionType.ReferenceData:
          await setupReferenceDataDimension(
            cubeDB,
            dimension,
            extendedSelectStatementsMap,
            viewSelectStatementsMap,
            rawSelectStatementsMap,
            defaultSortSelectStatementsMap,
            rawSortSelectStatementsMap,
            columnNames,
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
          await setupNumericDimension(
            cubeDB,
            dimension,
            extendedSelectStatementsMap,
            viewSelectStatementsMap,
            rawSelectStatementsMap,
            defaultSortSelectStatementsMap,
            rawSortSelectStatementsMap,
            columnNames
          );
          break;
        case DimensionType.Text:
          await setupTextDimension(
            cubeDB,
            dimension,
            extendedSelectStatementsMap,
            viewSelectStatementsMap,
            rawSelectStatementsMap,
            defaultSortSelectStatementsMap,
            rawSortSelectStatementsMap,
            columnNames
          );
          break;
        case DimensionType.Raw:
        case DimensionType.Symbol:
          await rawDimensionProcessor(
            cubeDB,
            dimension,
            extendedSelectStatementsMap,
            viewSelectStatementsMap,
            rawSelectStatementsMap,
            defaultSortSelectStatementsMap,
            rawSortSelectStatementsMap,
            columnNames
          );
          break;
      }
    } catch (err) {
      logger.error(err, `Something went wrong trying to load dimension ${dimension.id} in to the cube`);
      throw new Error(`Could not load dimensions ${dimension.id} in to the cube with the following error: ${err}`);
    }
    await cubeDB.query(
      pgformat('INSERT INTO metadata VALUES (%L, %L)', 'lookup_tables', JSON.stringify(Array.from(lookupTables)))
    );
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
  cubeDB: QueryRunner,
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
    await cubeDB.query(factTableCreationQuery);
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
  cubeDB: QueryRunner,
  buildID: string,
  dataset: Dataset,
  revision: Revision
): Promise<void> => {
  const factTableInfo = await createEmptyFactTableInCube(cubeDB, dataset, buildID);
  await loadFactTables(
    cubeDB,
    dataset,
    revision,
    factTableInfo.factTableDef,
    factTableInfo.dataValuesColumn,
    factTableInfo.notesCodeColumn,
    factTableInfo.factIdentifiers
  );
  await createPrimaryKeyOnFactTable(cubeDB, buildID, revision, factTableInfo.compositeKey);
};

async function createPrimaryKeyOnFactTable(
  cubeDB: QueryRunner,
  schema: string,
  revision: Revision,
  compositeKey: string[]
): Promise<void> {
  logger.debug('Creating primary key on fact table');
  try {
    const alterTableQuery = pgformat('ALTER TABLE %I.%I ADD PRIMARY KEY (%I)', schema, FACT_TABLE_NAME, compositeKey);
    logger.debug(`Alter Table query = ${alterTableQuery}`);
    await cubeDB.query(alterTableQuery);
  } catch (error) {
    logger.warn(error, `Failed to add primary key to the fact table`);
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

export async function createCubeMetadataTable(cubeDB: QueryRunner, revisionId: string, buildId: string): Promise<void> {
  logger.debug('Adding metadata table to the cube');
  await cubeDB.query(`CREATE TABLE IF NOT EXISTS metadata (key VARCHAR, value VARCHAR);`);
  await cubeDB.query(pgformat('INSERT INTO metadata VALUES (%L, %L);', 'revision_id', revisionId));
  await cubeDB.query(pgformat('INSERT INTO metadata VALUES (%L, %L);', 'build_id', buildId));
  await cubeDB.query(pgformat('INSERT INTO metadata VALUES (%L, %L);', 'build_start', new Date().toISOString()));
  await cubeDB.query(pgformat('INSERT INTO metadata VALUES (%L, %L);', 'build_status', 'incomplete'));
}

async function createCubeFilterTable(cubeDB: QueryRunner): Promise<void> {
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
  await cubeDB.query(createFilterQuery);
  const end = performance.now();
  const timing = Math.round(end - start);
  logger.debug(`createCubeFilterTable: ${timing}ms`);
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
  cubeDB: QueryRunner,
  buildId: string,
  dataset: Dataset,
  endRevision: Revision
): Promise<void> => {
  logger.debug(`Starting build ${buildId} and Creating base cube for revision ${endRevision.id}`);
  await cubeDB.query(pgformat(`SET search_path TO %I;`, buildId));
  const functionStart = performance.now();
  const extendedSelectStatementsMap = new Map<Locale, string[]>();
  const viewSelectStatementsMap = new Map<Locale, string[]>();
  const rawSelectStatementsMap = new Map<Locale, string[]>();
  const defaultSortSelectStatementsMap = new Map<Locale, string[]>();
  const rawSortSelectStatementsMap = new Map<Locale, string[]>();
  const columnNames = new Map<Locale, Set<string>>();

  SUPPORTED_LOCALES.map((locale) => {
    extendedSelectStatementsMap.set(locale, []);
    viewSelectStatementsMap.set(locale, []);
    rawSelectStatementsMap.set(locale, []);
    defaultSortSelectStatementsMap.set(locale, []);
    rawSortSelectStatementsMap.set(locale, []);
    columnNames.set(locale, new Set<string>());
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
  const factTableInfo = await createEmptyFactTableInCube(cubeDB, dataset, buildId);
  await createCubeMetadataTable(cubeDB, endRevision.id, buildId);
  await createCubeFilterTable(cubeDB);
  performanceReporting(Math.round(performance.now() - functionStart), 1000, 'Base table creation');
  try {
    const loadFactTablesStart = performance.now();
    await loadFactTables(
      cubeDB,
      dataset,
      endRevision,
      factTableInfo.factTableDef,
      factTableInfo.dataValuesColumn,
      factTableInfo.notesCodeColumn,
      factTableInfo.factIdentifiers
    );
    performanceReporting(Math.round(performance.now() - loadFactTablesStart), 1000, 'Loading all the data tables');
  } catch (err) {
    await cubeDB.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
    logger.error(err, `Failed to load fact tables into the cube`);
    throw new Error(`Failed to load fact tables into the cube: ${err}`);
  }

  if (factTableInfo.compositeKey.length > 0) {
    const primaryKeyAddStart = performance.now();
    try {
      await createPrimaryKeyOnFactTable(cubeDB, buildId, endRevision, factTableInfo.compositeKey);
    } catch (err) {
      await cubeDB.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
      logger.error(
        err,
        'Failed to apply primary key to fact table.  This implies there are duplicate or incomplete facts'
      );
      performanceReporting(Math.round(performance.now() - primaryKeyAddStart), 1000, 'Add primary key to fact table');
      throw err;
    }
    performanceReporting(Math.round(performance.now() - primaryKeyAddStart), 1000, 'Add primary key to fact table');
  }

  const measureSetupMark = performance.now();
  if (factTableInfo.measureColumn && factTableInfo.dataValuesColumn) {
    try {
      await setupMeasures(
        cubeDB,
        dataset,
        factTableInfo.dataValuesColumn,
        factTableInfo.measureColumn,
        factTableInfo.notesCodeColumn!,
        extendedSelectStatementsMap,
        viewSelectStatementsMap,
        rawSelectStatementsMap,
        defaultSortSelectStatementsMap,
        rawSortSelectStatementsMap,
        columnNames,
        joinStatements,
        orderByStatements
      );
    } catch (err) {
      await cubeDB.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
      logger.error(err, `Failed to setup measures`);
      throw new Error(`Failed to setup measures: ${err}`);
    }
  } else {
    setupMeasureNoDataValues(
      extendedSelectStatementsMap,
      viewSelectStatementsMap,
      rawSelectStatementsMap,
      defaultSortSelectStatementsMap,
      rawSortSelectStatementsMap,
      columnNames,
      factTableInfo.measureColumn,
      factTableInfo.dataValuesColumn
    );
  }
  performanceReporting(Math.round(performance.now() - measureSetupMark), 1000, 'Setting up the measure');

  if (dataset.dimensions.find((dim) => dim.type === DimensionType.ReferenceData)) {
    const loadReferenceDataMark = performance.now();
    await loadReferenceDataIntoCube(buildId);
    performanceReporting(
      Math.round(performance.now() - loadReferenceDataMark),
      1000,
      'Loading reference data in to cube'
    );
  }

  const dimensionSetupMark = performance.now();
  try {
    await setupDimensions(
      cubeDB,
      dataset,
      endRevision,
      extendedSelectStatementsMap,
      viewSelectStatementsMap,
      rawSelectStatementsMap,
      defaultSortSelectStatementsMap,
      rawSortSelectStatementsMap,
      columnNames,
      joinStatements,
      orderByStatements
    );
  } catch (err) {
    await cubeDB.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
    logger.error(err, `Failed to setup dimensions`);
    throw new Error(`Failed to setup dimensions`);
  }
  performanceReporting(Math.round(performance.now() - dimensionSetupMark), 1000, 'Setting up the dimensions in total');

  const noteCodeCreation = performance.now();
  logger.debug('Adding notes code column to the select statement.');
  if (factTableInfo.notesCodeColumn) {
    await createNotesTable(
      cubeDB,
      factTableInfo.notesCodeColumn,
      extendedSelectStatementsMap,
      rawSelectStatementsMap,
      defaultSortSelectStatementsMap,
      rawSortSelectStatementsMap,
      columnNames,
      joinStatements
    );
  }
  performanceReporting(Math.round(performance.now() - noteCodeCreation), 1000, 'Setting up the note codes');

  logger.info(`Creating default views...`);
  const viewCreation = performance.now();
  // Build the default views
  try {
    for (const locale of SUPPORTED_LOCALES) {
      if (extendedSelectStatementsMap.get(locale)?.length === 0) {
        extendedSelectStatementsMap.get(locale)?.push('*');
      }
      if (viewSelectStatementsMap.get(locale)?.length === 0) {
        viewSelectStatementsMap.get(locale)?.push('*');
      }
      if (rawSelectStatementsMap.get(locale)?.length === 0) {
        rawSelectStatementsMap.get(locale)?.push('*');
      }
      if (defaultSortSelectStatementsMap.get(locale)?.length === 0) {
        defaultSortSelectStatementsMap.get(locale)?.push('*');
      }
      if (rawSortSelectStatementsMap.get(locale)?.length === 0) {
        rawSortSelectStatementsMap.get(locale)?.push('*');
      }
      const lang = locale.toLowerCase().split('-')[0];

      const coreMaterialisedView = `${CORE_VIEW_NAME}_${lang}`;

      const extendedViewSQL = pgformat(
        'SELECT %s FROM %I %s %s',
        extendedSelectStatementsMap.get(locale)?.join(',\n'),
        FACT_TABLE_NAME,
        joinStatements.join('\n').replace(/#LANG#/g, pgformat('%L', locale.toLowerCase())),
        orderByStatements.length > 0 ? `ORDER BY ${orderByStatements.join(', ')}` : ''
      );
      logger.debug(extendedViewSQL);
      await cubeDB.query(pgformat('CREATE VIEW %I AS %s', `${CORE_VIEW_NAME}_${lang}`, extendedViewSQL));
      await cubeDB.query(
        pgformat(`INSERT INTO metadata VALUES (%L, %L)`, `${CORE_VIEW_NAME}_${lang}`, extendedViewSQL)
      );
      await cubeDB.query(pgformat(`INSERT INTO metadata VALUES (%L, %L)`, coreMaterialisedView, extendedViewSQL));

      const defaultViewSQL = pgformat(
        'SELECT %s FROM %I',
        viewSelectStatementsMap.get(locale)?.join(',\n'),
        coreMaterialisedView
      );
      await cubeDB.query(pgformat(`INSERT INTO metadata VALUES (%L, %L)`, `default_view_${lang}`, defaultViewSQL));

      const rawViewSQL = pgformat(
        'SELECT %s FROM %I',
        rawSelectStatementsMap.get(locale)?.join(',\n'),
        coreMaterialisedView
      );
      await cubeDB.query(pgformat(`INSERT INTO metadata VALUES (%L, %L)`, `raw_view_${lang}`, rawViewSQL));

      const defaultSortViewSQL = pgformat(
        'SELECT %s FROM %I',
        defaultSortSelectStatementsMap.get(locale)?.join(',\n'),
        coreMaterialisedView
      );
      await cubeDB.query(
        pgformat(`INSERT INTO metadata VALUES (%L, %L)`, `default_sort_view_${lang}`, defaultSortViewSQL)
      );

      const rawSortViewSQL = pgformat(
        'SELECT %s FROM %I',
        rawSortSelectStatementsMap.get(locale)?.join(',\n'),
        coreMaterialisedView
      );
      await cubeDB.query(pgformat(`INSERT INTO metadata VALUES (%L, %L)`, `raw_sort_view_${lang}`, rawSortViewSQL));

      if (Array.from(columnNames.get(locale)?.values() || []).length > 0) {
        await cubeDB.query(
          pgformat(
            `INSERT INTO metadata VALUES (%L, %L)`,
            `display_columns_${lang}`,
            JSON.stringify(Array.from(columnNames.get(locale)?.values() || []))
          )
        );
      } else {
        const cols = dataset.factTable?.map((col) => col.columnName);
        await cubeDB.query(
          pgformat(`INSERT INTO metadata VALUES (%L, %L)`, `display_columns_${lang}`, JSON.stringify(cols))
        );
      }
    }
    await cubeDB.query(`UPDATE metadata SET value = 'awaiting_materialization' WHERE key = 'build_status'`);
  } catch (error) {
    await cubeDB.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
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
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  logger.info(`Creating default views...`);
  const viewCreation = performance.now();
  // Build the default views
  try {
    await cubeDB.query(pgformat(`SET search_path TO %I;`, revisionId));
    for (const locale of SUPPORTED_LOCALES) {
      const lang = locale.toLowerCase().split('-')[0];
      const originalCoreViewSQL: { value: string }[] = await cubeDB.query(
        pgformat('SELECT value FROM metadata WHERE key = %L', `${CORE_VIEW_NAME}_${lang}`)
      );
      await cubeDB.query(
        pgformat('CREATE MATERIALIZED VIEW %I AS %s', `${CORE_VIEW_NAME}_mat_${lang}`, originalCoreViewSQL[0].value)
      );

      const originalDefaultViewMetadata: { value: string }[] = await cubeDB.query(
        pgformat('SELECT value FROM metadata WHERE key = %L', `default_view_${lang}`)
      );
      await cubeDB.query(
        pgformat('CREATE VIEW %I AS %s', `default_view_${lang}`, originalDefaultViewMetadata[0].value)
      );

      const originalRawViewMetadata: { value: string }[] = await cubeDB.query(
        pgformat('SELECT value FROM metadata WHERE key = %L', `raw_view_${lang}`)
      );
      await cubeDB.query(pgformat('CREATE VIEW %I AS %s', `raw_view_${lang}`, originalRawViewMetadata[0].value));

      const originalDefaultSortViewMetadata: { value: string }[] = await cubeDB.query(
        pgformat('SELECT value FROM metadata WHERE key = %L', `default_sort_view_${lang}`)
      );
      await cubeDB.query(
        pgformat('CREATE VIEW %I AS %s', `default_sort_mat_view_${lang}`, originalDefaultSortViewMetadata[0].value)
      );

      const originalRawSortViewMetadata: { value: string }[] = await cubeDB.query(
        pgformat('SELECT value FROM metadata WHERE key = %L', `raw_sort_view_${lang}`)
      );
      await cubeDB.query(
        pgformat('CREATE VIEW %I AS %s', `raw_sort_mat_view_${lang}`, originalRawSortViewMetadata[0].value)
      );
    }
    await cubeDB.query(`UPDATE metadata SET value = 'complete' WHERE key = 'build_status'`);
    await cubeDB.query(`INSERT INTO metadata VALUES('build_finished', '${new Date().toISOString()}')`);
  } catch (error) {
    try {
      await cubeDB.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
    } catch (err) {
      logger.error(err, 'Apparently cube no longer exists');
    }
    performanceReporting(Math.round(performance.now() - viewCreation), 3000, 'Setting up the materialized views');
    logger.error(error, 'Something went wrong trying to create the materialized views in the cube.');
  } finally {
    cubeDB.release();
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

  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  const buildId = `build_${crypto.randomUUID()}`;

  try {
    logger.info(`Creating schema for cube ${buildId}`);
    await cubeDB.query(pgformat(`CREATE SCHEMA IF NOT EXISTS %I;`, buildId));
  } catch (error) {
    logger.error(error, 'Something went wrong trying to create the cube schema');
    cubeDB.release();
    throw error;
  }

  try {
    logger.debug(`Renaming ${buildId} to cube rev ${endRevision.id}`);
    await createBasePostgresCube(cubeDB, buildId, dataset, endRevision);
    await cubeDB.query(pgformat('DROP SCHEMA IF EXISTS %I CASCADE;', endRevision.id));
    await cubeDB.query(pgformat('ALTER SCHEMA %I RENAME TO %I;', buildId, endRevision.id));
  } catch (err) {
    logger.error(err, 'Failed to create cube in Postgres');
    await cubeDB.query(pgformat('DROP SCHEMA IF EXISTS %I CASCADE;', buildId));
    throw err;
  } finally {
    cubeDB.release();
  }

  // don't wait for this, can happen in the background so we can send the response earlier
  logger.debug('Running async process...');
  void createMaterialisedView(endRevisionId);
  //void createFilesForDownload(quack, datasetId, endRevisionId);
};

export const getCubeTimePeriods = async (revisionId: string): Promise<PeriodCovered> => {
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  const periodCoverage: { key: string; value: string }[] = await cubeDB.query(
    pgformat(`SELECT key, value FROM %I.metadata WHERE key in ('start_date', 'end_date')`, revisionId)
  );
  cubeDB.release();
  if (periodCoverage.length > 0) {
    return { start_date: new Date(periodCoverage[0].value), end_date: new Date(periodCoverage[1].value) };
  }
  return { start_date: null, end_date: null };
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
