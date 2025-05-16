import fs from 'node:fs';
import path from 'path';
import { performance } from 'node:perf_hooks';

import { Database, DuckDbError } from 'duckdb-async';
import tmp from 'tmp';
import { t } from 'i18next';
import { FindOptionsRelations } from 'typeorm';
import { toZonedTime } from 'date-fns-tz';
import { formatISO } from 'date-fns';
import { format as pgformat } from '@scaleleap/pg-format';

import { FileType } from '../enums/file-type';
import { FileImportInterface } from '../entities/dataset/file-import.interface';
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
import { RevisionRepository } from '../repositories/revision';
import { PeriodCovered } from '../interfaces/period-covered';

import { dateDimensionReferenceTableCreator } from './time-matching';
import { duckdb, safelyCloseDuckDb } from './duckdb';
import { NumberExtractor, NumberType } from '../extractors/number-extractor';
import { CubeValidationType } from '../enums/cube-validation-type';
import { languageMatcherCaseStatement } from '../utils/lookup-table-utils';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import { FactTableValidationExceptionType } from '../enums/fact-table-validation-exception-type';

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
  switch (fileType) {
    case FileType.Csv:
    case FileType.GzipCsv:
      return pgformat(
        "CREATE TABLE %I AS SELECT * FROM read_csv(%L, auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR']);",
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
  fileImport: FileImportInterface,
  tempFile: string,
  tableName: string
) => {
  logger.debug(`Loading file in to the cube`);
  const insertQuery = await createDataTableQuery(tableName, tempFile, fileImport.fileType, quack);
  try {
    await quack.exec(insertQuery);
  } catch (error) {
    logger.error(`Failed to load file in to the cube using query ${insertQuery} with the following error: ${error}`);
    throw error;
  }
};

export const loadTableDataIntoFactTable = async (
  quack: Database,
  factTableDef: string[],
  factTableName: string,
  originTableName: string
) => {
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
          FactTableValidationExceptionType.EmptyValue,
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
) => {
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
        "CREATE TABLE %I AS SELECT %I FROM read_csv(%L, auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR']);",
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
          FactTableValidationExceptionType.EmptyValue,
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

async function createReferenceDataTablesInCube(quack: Database) {
  logger.debug('Creating empty reference data tables');
  try {
    logger.debug('Creating categories tables');
    await quack.exec(`CREATE TABLE "categories" ("category" TEXT PRIMARY KEY);`);
    logger.debug('Creating category_keys table');
    await quack.exec(`CREATE TABLE "category_keys" (
                            "category_key" TEXT PRIMARY KEY,
                            "category" TEXT NOT NULL,
                            );`);
    logger.debug('Creating reference_data table');
    await quack.exec(`CREATE TABLE "reference_data" (
                            "item_id" TEXT NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "sort_order" INTEGER,
                            "category_key" TEXT NOT NULL,
                            "validity_start" TEXT NOT NULL,
                            "validity_end" TEXT,
                            PRIMARY KEY("item_id","version_no","category_key"),
                            );`);
    logger.debug('Creating reference_data_all table');
    await quack.exec(`CREATE TABLE "reference_data_all" (
                            "item_id" TEXT NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "sort_order" INTEGER,
                            "category_key" TEXT NOT NULL,
                            "validity_start" TEXT NOT NULL,
                            "validity_end" TEXT,
                            PRIMARY KEY("item_id","version_no","category_key"),
                            );`);
    logger.debug('Creating reference_data_info table');
    await quack.exec(`CREATE TABLE "reference_data_info" (
                            "item_id" TEXT NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "category_key" TEXT NOT NULL,
                            "lang" TEXT NOT NULL,
                            "description" TEXT NOT NULL,
                            "notes" TEXT,
                            PRIMARY KEY("item_id","version_no","category_key","lang"),
                            );`);
    logger.debug('Creating category_key_info table');
    await quack.exec(`CREATE TABLE "category_key_info" (
                            "category_key" TEXT NOT NULL,
                            "lang" TEXT NOT NULL,
                            "description" TEXT NOT NULL,
                            "notes" TEXT,
                            PRIMARY KEY("category_key","lang"),
                            );`);
    logger.debug('Creating category_info table');
    await quack.exec(`CREATE TABLE "category_info" (
                            "category" TEXT NOT NULL,
                            "lang" TEXT NOT NULL,
                            "description" TEXT NOT NULL,
                            "notes" TEXT,
                            PRIMARY KEY("category","lang"),
                            );`);
    logger.debug('Creating hierarchy table');
    await quack.exec(`CREATE TABLE "hierarchy" (
                            "item_id" TEXT NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "category_key" TEXT NOT NULL,
                            "parent_id" TEXT NOT NULL,
                            "parent_version" INTEGER NOT NULL,
                            "parent_category" TEXT NOT NULL,
                            PRIMARY KEY("item_id","version_no","category_key","parent_id","parent_version","parent_category")
                            );`);
  } catch (error) {
    logger.error(`Something went wrong trying to create the initial reference data tables with error: ${error}`);
    throw new Error(`Something went wrong trying to create the initial reference data tables with error: ${error}`);
  }
}

export async function loadReferenceDataFromCSV(quack: Database) {
  logger.debug(`Loading reference data from CSV`);
  logger.debug(`Loading categories from CSV`);
  await quack.exec(
    `COPY categories FROM '${path.resolve(__dirname, `../resources/reference-data/v1/categories.csv`)}';`
  );
  logger.debug(`Loading category_keys from CSV`);
  await quack.exec(
    `COPY category_keys FROM '${path.resolve(__dirname, `../resources/reference-data/v1/category_key.csv`)}';`
  );
  logger.debug(`Loading reference_data_all from CSV`);
  await quack.exec(
    `COPY reference_data_all FROM '${path.resolve(__dirname, `../resources/reference-data/v1/reference_data.csv`)}';`
  );
  logger.debug(`Loading reference_data_info from CSV`);
  await quack.exec(
    `COPY reference_data_info FROM '${path.resolve(__dirname, `../resources/reference-data/v1/reference_data_info.csv`)}';`
  );
  logger.debug(`Loading category_key_info from CSV`);
  await quack.exec(
    `COPY category_key_info FROM '${path.resolve(__dirname, `../resources/reference-data/v1/category_key_info.csv`)}';`
  );
  logger.debug(`Loading category_info from CSV`);
  await quack.exec(
    `COPY category_info FROM '${path.resolve(__dirname, `../resources/reference-data/v1/category_info.csv`)}';`
  );
  logger.debug(`Loading hierarchy from CSV`);
  await quack.exec(`COPY hierarchy FROM '${path.resolve(__dirname, `../resources/reference-data/v1/hierarchy.csv`)}';`);
}

export const loadReferenceDataIntoCube = async (quack: Database) => {
  await createReferenceDataTablesInCube(quack);
  await loadReferenceDataFromCSV(quack);
  logger.debug(`Reference data tables created and populated successfully.`);
};

export const cleanUpReferenceDataTables = async (quack: Database) => {
  await quack.exec('DROP TABLE reference_data_all;');
  await quack.exec('DELETE FROM reference_data_info WHERE item_id NOT IN (SELECT item_id FROM reference_data);');
  await quack.exec('DELETE FROM category_keys WHERE category_key NOT IN (SELECT category_key FROM reference_data);');
  await quack.exec('DELETE FROM category_Key_info WHERE category_key NOT IN (select category_key FROM category_keys);');
  await quack.exec('DELETE FROM categories where category NOT IN (SELECT category FROM category_keys);');
  await quack.exec('DELETE FROM category_info WHERE category NOT IN (SELECT category FROM categories);');
  await quack.exec('DELETE FROM hierarchy WHERE item_id NOT IN (SELECT item_id FROM reference_data);');
};

export const loadCorrectReferenceDataIntoReferenceDataTable = async (quack: Database, dimension: Dimension) => {
  const extractor = dimension.extractor as ReferenceDataExtractor;
  for (const category of extractor.categories) {
    const categoryPresent = await quack.all(
      pgformat('SELECT DISTINCT category_key FROM reference_data WHERE category_key=%L', category)
    );
    if (categoryPresent.length > 0) {
      continue;
    }
    logger.debug(`Copying ${category} reference data in to reference_data table`);
    await quack.exec(
      pgformat('INSERT INTO reference_data (SELECT * FROM reference_data_all WHERE category_key=%L);', category)
    );
  }
};

async function setupReferenceDataDimension(
  quack: Database,
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[]
) {
  await loadCorrectReferenceDataIntoReferenceDataTable(quack, dimension);
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
}

export const createDatePeriodTableQuery = (factTableColumn: FactTableColumn) => {
  return `
  CREATE TABLE ${makeCubeSafeString(factTableColumn.columnName)}_lookup (
    "${factTableColumn.columnName}" ${factTableColumn.columnDatatype},
    language VARCHAR(5),
    description VARCHAR,
    hierarchy VARCHAR,
    date_type varchar,
    start_date datetime,
    end_date datetime
  );`;
};

// This is a short version of validate date dimension code found in the dimension processor.
// This concise version doesn't return any information on why the creation failed.  Just that it failed
export async function createDateDimension(quack: Database, extractor: object | null, factTableColumn: FactTableColumn) {
  if (!extractor) {
    throw new Error('Extractor not supplied');
  }
  const columnData = await quack.all(`SELECT DISTINCT "${factTableColumn.columnName}" FROM ${FACT_TABLE_NAME};`);
  const dateDimensionTable = dateDimensionReferenceTableCreator(extractor, columnData);
  await quack.exec(createDatePeriodTableQuery(factTableColumn));
  // Create the date_dimension table
  const stmt = await quack.prepare(
    `INSERT INTO ${makeCubeSafeString(factTableColumn.columnName)}_lookup
    ("${factTableColumn.columnName}", language, description, hierarchy, date_type, start_date, end_date) VALUES (?,?,?,?,?,?,?);`
  );
  for (const locale of SUPPORTED_LOCALES) {
    logger.debug(`populating ${makeCubeSafeString(factTableColumn.columnName)}_lookup table for locale ${locale}`);
    dateDimensionTable.map(async (row) => {
      await stmt.run(
        row.dateCode,
        locale.toLowerCase(),
        row.description,
        null,
        t(`date_type.${row.type}`, { lng: locale }),
        row.start,
        row.end
      );
    });
  }

  await stmt.finalize();
  const periodCoverage = await quack.all(
    `SELECT MIN(start_date) as startDate, MAX(end_date) as endDate FROM ${makeCubeSafeString(factTableColumn.columnName)}_lookup;`
  );
  logger.debug(
    `Period coverage: ${toZonedTime(periodCoverage[0].startDate, 'UTC')} to ${toZonedTime(periodCoverage[0].endDate, 'UTC')}`
  );
  await quack.exec(`CREATE TABLE IF NOT EXISTS metadata (key VARCHAR, value VARCHAR);`);
  const metaDataCoverage = await quack.all("SELECT * FROM metadata WHERE key = 'start_data' OR key = 'end_date';");
  if (metaDataCoverage.length > 0) {
    for (const metaData of metaDataCoverage) {
      if (metaData.key === 'start_date') {
        if (periodCoverage[0].startDate < metaData.value) {
          await quack.exec(
            `UPDATE metadata SET value='${formatISO(toZonedTime(periodCoverage[0].startDate, 'UTC'))}' WHERE key='start_data';`
          );
        }
      } else if (metaData.key === 'end_date') {
        if (periodCoverage[0].endDate > metaData.value) {
          await quack.exec(
            `UPDATE metadata SET value='${formatISO(toZonedTime(periodCoverage[0].endDate, 'UTC'))}' WHERE key='end_date';`
          );
        }
      }
    }
  } else {
    await quack.exec(
      `INSERT INTO metadata (key, value) VALUES ('start_date', '${formatISO(toZonedTime(periodCoverage[0].startDate, 'UTC'))}');`
    );
    await quack.exec(
      `INSERT INTO metadata (key, value) VALUES ('end_date', '${formatISO(toZonedTime(periodCoverage[0].endDate, 'UTC'))}');`
    );
  }
  return `${makeCubeSafeString(factTableColumn.columnName)}_lookup`;
}

export const createLookupTableQuery = (
  lookupTableName: string,
  referenceColumnName: string,
  referenceColumnType: string
) => {
  return pgformat(
    'CREATE TABLE %I (%I %s NOT NULL, language VARCHAR(5) NOT NULL, description TEXT NOT NULL, notes TEXT, sort_order INTEGER, hierarchy %s);',
    lookupTableName,
    referenceColumnName,
    referenceColumnType,
    referenceColumnType
  );
};

async function setupLookupTableDimension(
  quack: Database,
  dataset: Dataset,
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[],
  orderByStatements: string[]
) {
  const factTableColumn = dataset.factTable?.find((col) => col.columnName === dimension.factTableColumn);
  if (!factTableColumn) {
    const error = new CubeValidationException(`Fact table column ${dimension.factTableColumn} not found`);
    error.type = CubeValidationType.FactTableColumnMissing;
    error.datasetId = dataset.id;
    throw error;
  }
  const dimTable = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
  await createLookupTableDimension(quack, dataset, dimension, factTableColumn);
  SUPPORTED_LOCALES.map((locale) => {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    viewSelectStatementsMap.get(locale)?.push(`${dimTable}.description as "${columnName}"`);
    rawSelectStatementsMap.get(locale)?.push(`${dimTable}.description as "${columnName}"`);
  });
  joinStatements.push(
    `LEFT JOIN "${dimTable}" on "${dimTable}"."${factTableColumn.columnName}"=${FACT_TABLE_NAME}."${factTableColumn.columnName}" AND "${dimTable}".language=#LANG#`
  );
  orderByStatements.push(`"${dimTable}".sort_order`);
}

export async function createLookupTableDimension(
  quack: Database,
  dataset: Dataset,
  dimension: Dimension,
  factTableColumn: FactTableColumn
) {
  logger.debug(`Creating and validating lookup table dimension ${dimension.factTableColumn}`);
  if (!dimension.lookupTable) return;
  if (!dimension.extractor) return;
  const dimTable = `${makeCubeSafeString(factTableColumn.columnName)}_lookup`;
  await quack.exec(createLookupTableQuery(dimTable, factTableColumn.columnName, factTableColumn.columnDatatype));
  const extractor = dimension.extractor as LookupTableExtractor;
  const lookupTableFile = await getFileImportAndSaveToDisk(dataset, dimension.lookupTable);
  const lookupTableName = `${makeCubeSafeString(dimension.factTableColumn)}_lookup_draft`;
  await loadFileIntoCube(quack, dimension.lookupTable, lookupTableFile, lookupTableName);
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
          dimension.joinColumn,
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
    logger.debug(`Built insert query: ${builtInsertQuery}`);
    await quack.exec(builtInsertQuery);
  } else {
    const languageMatcher = languageMatcherCaseStatement(extractor.languageColumn);
    const notesStr = extractor.notesColumns ? pgformat('%I', extractor.notesColumns[0].name) : 'NULL';
    const sortStr = extractor.sortColumn ? pgformat('%I', extractor.sortColumn) : 'NULL';
    const hierarchyStr = extractor.hierarchyColumn ? pgformat('%I', extractor.hierarchyColumn) : 'NULL';
    const dataExtractorParts = pgformat(
      `SELECT %I AS %I, %L as language, %I as description, %s as notes, %s as sort_order, %s as hierarchy FROM %I;`,
      dimension.joinColumn,
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
}

function setupFactTableUpdateJoins(
  factTableName: string,
  factIdentifiers: FactTableColumn[],
  dataTableIdentifiers: DataTableDescription[]
): string {
  const joinParts: string[] = [];
  for (const factTableCol of factIdentifiers) {
    const dataTableCol = dataTableIdentifiers.find((col) => col.factTableColumn === factTableCol.columnName);
    joinParts.push(pgformat('%I.%I=update_table.%I', factTableName, factTableCol.columnName, dataTableCol?.columnName));
  }
  return joinParts.join(' AND ');
}

async function loadFactTablesWithUpdates(
  quack: Database,
  dataset: Dataset,
  allDataTables: DataTable[],
  factTableDef: string[],
  dataValuesColumn: FactTableColumn | undefined,
  notesCodeColumn: FactTableColumn | undefined,
  factIdentifiers: FactTableColumn[]
) {
  for (const dataTable of allDataTables.sort((ftA, ftB) => ftA.uploadedAt.getTime() - ftB.uploadedAt.getTime())) {
    logger.info(`Loading fact table data for fact table ${dataTable.id}`);

    const factTableFile: string = await getFileImportAndSaveToDisk(dataset, dataTable);
    const updateTableDataCol = dataTable.dataTableDescriptions.find(
      (col) => col.factTableColumn === dataValuesColumn?.columnName
    )?.columnName;

    let updateQuery = '';
    if (dataValuesColumn && notesCodeColumn) {
      updateQuery = pgformat(
        `UPDATE %I SET %I=update_table.%I,
      %I=(CASE
      WHEN %I.%I IS NULL THEN 'r'
      WHEN %I.%I LIKE '%r%' THEN %I.%I
      ELSE concat(%I.%I,'r') END)
      FROM update_table WHERE %s
      AND %I.%I!=update_table.%I;`,
        FACT_TABLE_NAME,
        dataValuesColumn.columnName,
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
        setupFactTableUpdateJoins(FACT_TABLE_NAME, factIdentifiers, dataTable.dataTableDescriptions),
        FACT_TABLE_NAME,
        dataValuesColumn.columnName,
        updateTableDataCol
      );
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
          await quack.exec(pgformat('DELETE FROM %I;', FACT_TABLE_NAME));
          await quack.exec('CHECKPOINT;');
          await loadFileDataTableIntoTable(quack, dataTable, factTableDef, factTableFile, FACT_TABLE_NAME);
          break;
        case DataTableAction.Add:
          await loadFileDataTableIntoTable(quack, dataTable, factTableDef, factTableFile, FACT_TABLE_NAME);
          break;
        case DataTableAction.Revise:
          await loadFileIntoCube(quack, dataTable, factTableFile, 'update_table');
          await quack.exec(updateQuery);
          await quack.exec('DROP TABLE update_table;');
          break;
        case DataTableAction.AddRevise:
          await loadFileIntoCube(quack, dataTable, factTableFile, 'update_table');
          logger.debug(`Executing update query: ${updateQuery}`);
          await quack.exec(updateQuery);
          await quack.exec(
            pgformat(
              `DELETE FROM update_table USING %I WHERE %s`,
              FACT_TABLE_NAME,
              setupFactTableUpdateJoins(FACT_TABLE_NAME, factIdentifiers, dataTable.dataTableDescriptions)
            )
          );
          await quack.exec('CHECKPOINT;');
          await quack.exec(
            pgformat(
              'INSERT INTO %I (%I) (SELECT %I FROM update_table);',
              FACT_TABLE_NAME,
              factTableDef,
              dataTableColumnSelect
            )
          );
          await quack.exec('DROP TABLE update_table;');
          await quack.exec('CHECKPOINT;');
          break;
      }
    } catch (error) {
      logger.error(error, `Something went wrong trying to create the core fact table`);
    } finally {
      fs.unlinkSync(factTableFile);
    }
  }
}

export async function loadFactTables(
  quack: Database,
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
      quack,
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
  quack: Database,
  notesColumn: FactTableColumn,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[]
): Promise<void> {
  logger.info('Creating notes table...');
  try {
    await quack.exec(
      `CREATE TABLE note_codes (code VARCHAR, language VARCHAR, tag VARCHAR, description VARCHAR, notes VARCHAR);`
    );
    const insertStmt = await quack.prepare(
      `INSERT INTO note_codes (code, language, tag, description, notes) VALUES (?,?,?,?,?);`
    );
    for (const locale of SUPPORTED_LOCALES) {
      for (const noteCode of NoteCodes) {
        await insertStmt.run(
          noteCode.code,
          locale.toLowerCase(),
          noteCode.tag,
          t(`note_codes.${noteCode.tag}`, { lng: locale }),
          null
        );
      }
    }
    await insertStmt.finalize();
    logger.info('Creating notes table view...');
    // We perform join operations to this view as we want to turn a csv such as `a,r` in to `Average, Revised`.
    await quack.exec(
      `CREATE TABLE all_notes AS SELECT fact_table."${notesColumn.columnName}" as code, note_codes.language as language, string_agg(DISTINCT note_codes.description, ', ') as description
            from fact_table JOIN note_codes ON LIST_CONTAINS(string_split(fact_table."${notesColumn.columnName}", ','), note_codes.code)
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

function measureFormats(): Map<string, MeasureFormat> {
  const measureFormats: Map<string, MeasureFormat> = new Map();
  measureFormats.set('decimal', {
    name: 'decimal',
    method: "WHEN measure.reference = |REF| THEN printf('%,.|DEC|f', TRY_CAST(|COL| AS DECIMAL))"
  });
  measureFormats.set('float', {
    name: 'float',
    method: "WHEN measure.reference = |REF| THEN printf('%,.|DEC|f', TRY_CAST(|COL| AS DECIMAL))"
  });
  measureFormats.set('integer', {
    name: 'integer',
    method: "WHEN measure.reference = |REF| THEN printf('%,d', TRY_CAST(|COL| AS BIGINT))"
  });
  measureFormats.set('long', {
    name: 'long',
    method: "WHEN measure.reference = |REF| THEN printf('%f', TRY_CAST(|COL| AS DECIMAL))"
  });
  measureFormats.set('percentage', {
    name: 'percentage',
    method: "WHEN measure.reference = |REF| THEN printf('%,.|DEC|f %%', TRY_CAST(|COL| AS DECIMAL))"
  });
  measureFormats.set('string', {
    name: 'string',
    method: "WHEN measure.reference = |REF| THEN printf('%s', TRY_CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('text', {
    name: 'text',
    method: "WHEN measure.reference = |REF| THEN printf('%s', TRY_CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('date', {
    name: 'date',
    method: "WHEN measure.reference = |REF| THEN printf('%s', TRY_CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('datetime', {
    name: 'datetime',
    method: "WHEN measure.reference = |REF| THEN printf('%s', TRY_CAST(|COL| AS VARCHAR))"
  });
  measureFormats.set('time', {
    name: 'time',
    method: "WHEN measure.reference = |REF| THEN printf('%s', TRY_CAST(|COL| AS VARCHAR))"
  });
  return measureFormats;
}

export const measureTableCreateStatement = (joinColumnType: string) => {
  return `
    CREATE TABLE measure (
      reference ${joinColumnType},
      language TEXT,
      description TEXT,
      notes TEXT,
      sort_order INTEGER,
      format TEXT,
      decimals INTEGER,
      measure_type TEXT,
      hierarchy ${joinColumnType}
    );
  `;
};

export async function createMeasureLookupTable(
  quack: Database,
  measureColumn: FactTableColumn,
  measureTable: MeasureRow[]
) {
  await quack.exec(measureTableCreateStatement(measureColumn.columnDatatype));
  const stmt = await quack.prepare('INSERT INTO measure VALUES (?,?,?,?,?,?,?,?,?);');
  for (const row of measureTable) {
    await stmt.run(
      row.reference,
      row.language.toLowerCase(),
      row.description,
      row.notes ? row.notes : null,
      row.sortOrder ? row.sortOrder : null,
      row.format,
      row.decimal ? row.decimal : null,
      row.measureType ? row.measureType : null,
      row.hierarchy ? row.hierarchy : null
    );
  }
}

function setupMeasureNoDataValues(
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  measureColumn?: FactTableColumn,
  dataValuesColumn?: FactTableColumn
) {
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

async function setupMeasures(
  quack: Database,
  dataset: Dataset,
  dataValuesColumn: FactTableColumn,
  measureColumn: FactTableColumn,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[],
  orderByStatements: string[]
) {
  logger.info('Setting up measure table if present...');
  logger.debug(`Dataset Measure = ${JSON.stringify(dataset.measure)}`);
  logger.debug(`Measure column = ${JSON.stringify(measureColumn)}`);
  // Process the column that represents the measure
  if (dataset.measure && dataset.measure.measureTable && dataset.measure.measureTable.length > 0) {
    logger.debug('Measure present in dataset.  Creating measure table...');
    await createMeasureLookupTable(quack, measureColumn, dataset.measure.measureTable);
    logger.debug('Creating query part to format the data value correctly');

    const uniqueReferences = await quack.all(
      pgformat('SELECT DISTINCT reference, format, sort_order, decimals FROM measure;')
    );
    const caseStatements: string[] = ['CASE'];
    for (const row of uniqueReferences) {
      const statement = measureFormats()
        .get(row.format.toLowerCase())
        ?.method.replace('|REF|', pgformat('%L', row.reference))
        .replace('|DEC|', row.decimals ? row.decimals : 0)
        .replace('|COL|', pgformat('%I.%I', FACT_TABLE_NAME, dataValuesColumn.columnName));
      if (statement) caseStatements.push(statement);
      else logger.warn(`Failed to create case statement measure row: ${JSON.stringify(row)}`);
    }
    caseStatements.push(pgformat('ELSE CAST(%I.%I AS VARCHAR) END', FACT_TABLE_NAME, dataValuesColumn?.columnName));
    logger.debug(`Data view case statement ended up as: ${caseStatements.join('\n')}`);
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
  } else {
    setupMeasureNoDataValues(viewSelectStatementsMap, rawSelectStatementsMap, measureColumn, dataValuesColumn);
  }
}

function rawDimensionProcessor(
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>
) {
  SUPPORTED_LOCALES.map((locale) => {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    viewSelectStatementsMap.get(locale)?.push(pgformat('%I AS %I', dimension.factTableColumn, columnName));
    rawSelectStatementsMap.get(locale)?.push(pgformat('%I AS %I', dimension.factTableColumn, columnName));
  });
}

async function dateDimensionProcessor(
  quack: Database,
  factTableColumn: FactTableColumn,
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[],
  orderByStatements: string[]
) {
  const dimTable = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
  await createDateDimension(quack, dimension.extractor, factTableColumn);
  SUPPORTED_LOCALES.map((locale) => {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    viewSelectStatementsMap.get(locale)?.push(pgformat('%I.description AS %I', dimTable, columnName));
    viewSelectStatementsMap
      .get(locale)
      ?.push(
        pgformat("strftime(%I.start_date, '%d/%m/%Y') AS %I", dimTable, t('column_headers.start_date', { lng: locale }))
      );
    viewSelectStatementsMap
      .get(locale)
      ?.push(
        pgformat("strftime(%I.end_date, '%d/%m/%Y') AS %I", dimTable, t('column_headers.end_date', { lng: locale }))
      );
    rawSelectStatementsMap.get(locale)?.push(pgformat('%I.description AS %I', dimTable, columnName));
    rawSelectStatementsMap
      .get(locale)
      ?.push(
        pgformat("strftime(%I.start_date, '%d/%m/%Y') AS %I", dimTable, t('column_headers.start_date', { lng: locale }))
      );
    rawSelectStatementsMap
      .get(locale)
      ?.push(
        pgformat("strftime(%I.end_date, '%d/%m/%Y') AS %I", dimTable, t('column_headers.end_date', { lng: locale }))
      );
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
}

function setupNumericDimension(
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>
) {
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
            'CAST(CAST(%I.%I AS DECIMAL(18,%L)) AS VARCHAR) AS %I',
            FACT_TABLE_NAME,
            dimension.factTableColumn,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            columnName
          )
        );
      rawSelectStatementsMap
        .get(locale)
        ?.push(
          pgformat(
            'CAST(%I.%I AS DECIMAL(18,%L)) AS %I',
            FACT_TABLE_NAME,
            dimension.factTableColumn,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            columnName
          )
        );
    }
  });
}

function setupTextDimension(
  dimension: Dimension,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>
) {
  SUPPORTED_LOCALES.map((locale) => {
    const columnName = dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    viewSelectStatementsMap
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, columnName));
    rawSelectStatementsMap
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, columnName));
  });
}

async function setupDimensions(
  quack: Database,
  dataset: Dataset,
  endRevision: Revision,
  viewSelectStatementsMap: Map<Locale, string[]>,
  rawSelectStatementsMap: Map<Locale, string[]>,
  joinStatements: string[],
  orderByStatements: string[]
) {
  logger.info('Setting up dimension tables...');
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
            await dateDimensionProcessor(
              quack,
              factTableColumn,
              dimension,
              viewSelectStatementsMap,
              rawSelectStatementsMap,
              joinStatements,
              orderByStatements
            );
          } else {
            rawDimensionProcessor(dimension, viewSelectStatementsMap, rawSelectStatementsMap);
          }
          break;
        case DimensionType.LookupTable:
          // To allow preview to continue working for dimensions which are in progress
          // we check to see if there's a task for the dimension and if its been update
          // if its been update we skip it.
          if (endRevision.tasks) {
            const updateInProgressDimension = endRevision.tasks.dimensions.find((dim) => dim.id === dimension.id);
            if (updateInProgressDimension && !updateInProgressDimension.lookupTableUpdated) {
              logger.warn(`Skipping dimension ${dimension.id} as it has not been updated`);
              rawDimensionProcessor(dimension, viewSelectStatementsMap, rawSelectStatementsMap);
              break;
            }
          }
          await setupLookupTableDimension(
            quack,
            dataset,
            dimension,
            viewSelectStatementsMap,
            rawSelectStatementsMap,
            joinStatements,
            orderByStatements
          );
          break;
        case DimensionType.ReferenceData:
          await setupReferenceDataDimension(
            quack,
            dimension,
            viewSelectStatementsMap,
            rawSelectStatementsMap,
            joinStatements
          );
          break;
        case DimensionType.Numeric:
          setupNumericDimension(dimension, viewSelectStatementsMap, rawSelectStatementsMap);
          break;
        case DimensionType.Text:
          setupTextDimension(dimension, viewSelectStatementsMap, rawSelectStatementsMap);
          break;
        case DimensionType.Raw:
        case DimensionType.Symbol:
          rawDimensionProcessor(dimension, viewSelectStatementsMap, rawSelectStatementsMap);
          break;
      }
    } catch (err) {
      logger.error(err, `Something went wrong trying to load dimension ${dimension.id} in to the cube`);
      await quack.close();
      throw new Error(`Could not load dimensions ${dimension.id} in to the cube with the following error: ${err}`);
    }
  }
}

function referenceDataPresent(dataset: Dataset) {
  if (dataset.dimensions.find((dim) => dim.type === DimensionType.ReferenceData)) {
    return true;
  }
  return false;
}

export async function createEmptyFactTableInCube(quack: Database, dataset: Dataset) {
  let notesCodeColumn: FactTableColumn | undefined;
  let dataValuesColumn: FactTableColumn | undefined;
  let measureColumn: FactTableColumn | undefined;

  if (!dataset.factTable) {
    throw new Error(`Unable to find fact table for dataset ${dataset.id}`);
  }
  const factTable = dataset.factTable.sort((colA, colB) => colA.columnIndex - colB.columnIndex);
  const compositeKey: string[] = [];
  const factIdentifiers: FactTableColumn[] = [];
  const factTableDef: string[] = [];

  const factTableCreationDef = factTable
    .sort((col1, col2) => col1.columnIndex - col2.columnIndex)
    .map((field) => {
      switch (field.columnType) {
        case FactTableColumnType.Measure:
          measureColumn = field;
        // eslint-disable-next-line no-fallthrough
        case FactTableColumnType.Dimension:
        case FactTableColumnType.Time:
          compositeKey.push(field.columnName);
          factIdentifiers.push(field);
          break;
        case FactTableColumnType.NoteCodes:
          notesCodeColumn = field;
          break;
        case FactTableColumnType.DataValues:
          dataValuesColumn = field;
          break;
      }
      factTableDef.push(field.columnName);
      return pgformat('%I %s', field.columnName, field.columnDatatype);
    });

  logger.info('Creating initial fact table in cube');
  try {
    let createTableQuery: string = pgformat(`CREATE TABLE %I (%s);`, FACT_TABLE_NAME, factTableCreationDef.join(','));
    if (compositeKey.length > 0) {
      createTableQuery = pgformat(
        'CREATE TABLE %I (%s, PRIMARY KEY(%I));',
        FACT_TABLE_NAME,
        factTableCreationDef,
        compositeKey
      );
    }
    logger.debug(`Creating fact table with query: '${createTableQuery}'`);
    await quack.exec(createTableQuery);
  } catch (err) {
    logger.error(`Failed to create fact table in cube: ${err}`);
    await quack.close();
    throw new Error(`Failed to create fact table in cube: ${err}`);
  }
  return { measureColumn, notesCodeColumn, dataValuesColumn, factTableDef, factIdentifiers };
}

export const updateFactTableValidator = async (
  quack: Database,
  dataset: Dataset,
  revision: Revision
): Promise<Database> => {
  const { notesCodeColumn, dataValuesColumn, factTableDef, factIdentifiers } = await createEmptyFactTableInCube(
    quack,
    dataset
  );
  await loadFactTables(quack, dataset, revision, factTableDef, dataValuesColumn, notesCodeColumn, factIdentifiers);
  return quack;
};

async function createCubeMetadataTable(quack: Database) {
  logger.debug('Adding metadata table to the cube');
  await quack.exec(`CREATE TABLE metadata (key VARCHAR, value VARCHAR);`);
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
export const createBaseCubeFromProtoCube = async (
  datasetId: string,
  endRevisionId: string,
  protoCubeFile?: string
): Promise<string> => {
  logger.debug(`Creating base cube for revision: ${endRevisionId}`);
  const functionStart = performance.now();
  const viewSelectStatementsMap = new Map<Locale, string[]>();
  const rawSelectStatementsMap = new Map<Locale, string[]>();
  SUPPORTED_LOCALES.map((locale) => {
    viewSelectStatementsMap.set(locale, []);
    rawSelectStatementsMap.set(locale, []);
  });
  const joinStatements: string[] = [];
  const orderByStatements: string[] = [];

  const datasetRelations: FindOptionsRelations<Dataset> = {
    dimensions: {
      metadata: true,
      lookupTable: true
    },
    factTable: true,
    measure: {
      metadata: true,
      measureTable: true
    },
    revisions: {
      dataTable: {
        dataTableDescriptions: true
      }
    }
  };

  const endRevisionRelations: FindOptionsRelations<Revision> = {
    dataTable: {
      dataTableDescriptions: true
    }
  };

  logger.debug(`Loading dataset with id: ${datasetId} using relations: ${JSON.stringify(datasetRelations)}`);
  const dataset = await DatasetRepository.getById(datasetId, datasetRelations);
  const endRevision = await RevisionRepository.getById(endRevisionId, endRevisionRelations);

  const firstRevision = dataset.revisions.find((rev) => rev.revisionIndex === 1);
  if (!firstRevision) {
    const err = new CubeValidationException(
      `Could not find first revision for dataset ${datasetId} in revision ${endRevisionId}`
    );
    err.type = CubeValidationType.NoFirstRevision;
    err.datasetId = datasetId;
    throw new Error(`Unable to find first revision for dataset ${dataset.id}`);
  }

  const buildStart = performance.now();
  let quack: Database;
  if (protoCubeFile) {
    logger.debug(`Loading protocube from file: ${protoCubeFile} to DuckDB 🐤`);
    quack = await duckdb(protoCubeFile);
  } else {
    logger.debug('Creating an in-memory database to hold the cube using DuckDB 🐤');
    protoCubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    quack = await duckdb(protoCubeFile);

    const { notesCodeColumn, dataValuesColumn, factTableDef, factIdentifiers } = await createEmptyFactTableInCube(
      quack,
      dataset
    );

    try {
      await loadFactTables(
        quack,
        dataset,
        endRevision,
        factTableDef,
        dataValuesColumn,
        notesCodeColumn,
        factIdentifiers
      );
    } catch (err) {
      logger.error(err, `Failed to load fact tables into the cube`);
      await quack.close();
      throw new Error(`Failed to load fact tables into the cube: ${err}`);
    }
  }

  await createCubeMetadataTable(quack);

  const notesCodeColumn = dataset.factTable?.find((field) => field.columnType === FactTableColumnType.NoteCodes);
  const dataValuesColumn = dataset.factTable?.find((field) => field.columnType === FactTableColumnType.DataValues);
  const measureColumn = dataset.factTable?.find((field) => field.columnType === FactTableColumnType.Measure);

  if (measureColumn && dataValuesColumn) {
    try {
      await setupMeasures(
        quack,
        dataset,
        dataValuesColumn,
        measureColumn,
        viewSelectStatementsMap,
        rawSelectStatementsMap,
        joinStatements,
        orderByStatements
      );
    } catch (err) {
      logger.error(err, `Failed to setup measures`);
      await quack.close();
      throw new Error(`Failed to setup measures: ${err}`);
    }
  } else {
    setupMeasureNoDataValues(viewSelectStatementsMap, rawSelectStatementsMap, measureColumn, dataValuesColumn);
  }

  if (referenceDataPresent(dataset)) {
    await loadReferenceDataIntoCube(quack);
  }

  try {
    await setupDimensions(
      quack,
      dataset,
      endRevision,
      viewSelectStatementsMap,
      rawSelectStatementsMap,
      joinStatements,
      orderByStatements
    );
  } catch (err) {
    logger.error(err, `Failed to setup dimensions`);
    await quack.close();
    throw new Error(`Failed to setup dimensions`);
  }

  if (referenceDataPresent(dataset)) {
    await cleanUpReferenceDataTables(quack);
  }

  logger.debug('Adding notes code column to the select statement.');
  if (notesCodeColumn) {
    await createNotesTable(quack, notesCodeColumn, viewSelectStatementsMap, rawSelectStatementsMap, joinStatements);
  }

  logger.info(`Creating default views...`);
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
        'CREATE TABLE %I AS SELECT %s FROM %I %s %s',
        `default_view_${lang}`,
        viewSelectStatementsMap.get(locale)?.join(',\n'),
        FACT_TABLE_NAME,
        joinStatements.join('\n').replace(/#LANG#/g, pgformat('%L', locale.toLowerCase())),
        orderByStatements.length > 0 ? `ORDER BY ${orderByStatements.join(', ')}` : ''
      );
      logger.debug(defaultViewSQL);
      await quack.exec(defaultViewSQL);

      const rawViewSQL = pgformat(
        'CREATE TABLE %I AS SELECT %s FROM %I %s %s',
        `raw_view_${lang}`,
        rawSelectStatementsMap.get(locale)?.join(',\n'),
        FACT_TABLE_NAME,
        joinStatements.join('\n').replace(/#LANG#/g, pgformat('%L', locale.toLowerCase())),
        orderByStatements.length > 0 ? `ORDER BY ${orderByStatements.join(', ')}` : ''
      );
      logger.debug(rawViewSQL);
      await quack.exec(rawViewSQL);
    }
  } catch (error) {
    logger.error(error, 'Something went wrong trying to create the default views in the cube.');
    await quack.close();
    const exception = new CubeValidationException('Cube Build Failed');
    exception.type = CubeValidationType.CubeCreationFailed;
    throw exception;
  }
  await safelyCloseDuckDb(quack);
  const end = performance.now();
  const functionTime = Math.round(end - functionStart);
  const buildTime = Math.round(end - buildStart);
  logger.warn(`Cube function took ${functionTime}ms to complete and it took ${buildTime}ms to build the cube.`);
  return protoCubeFile;
};

export const cleanUpCube = async (tmpFile: string) => {
  logger.debug('Cleaning up cube file');
  if (fs.existsSync(tmpFile)) {
    fs.unlink(tmpFile, async (err) => {
      if (err) logger.error(`Unable to remove file ${tmpFile} with error: ${err}`);
    });
  }
};

export const getCubeTimePeriods = async (cubeFile: string): Promise<PeriodCovered> => {
  const quack = await duckdb(cubeFile);
  try {
    const periodCoverage = await quack.all(`SELECT key, value FROM metadata`);
    return periodCoverage.reduce(
      (acc, curr) => {
        acc[curr.key] = new Date(Date.parse(curr.value));
        return acc;
      },
      {} as Record<string, Date>
    ) as PeriodCovered;
  } finally {
    await quack.close();
  }
};

export const getCubeDataTable = async (cubeFile: string, lang: string) => {
  const quack = await duckdb(cubeFile);
  try {
    const defaultView = await quack.all(`SELECT * FROM default_view_${lang};`);
    return defaultView;
  } finally {
    await quack.close();
  }
};
