import { performance } from 'node:perf_hooks';

import { FindOptionsRelations, QueryRunner } from 'typeorm';
import { toZonedTime } from 'date-fns-tz';
import { format as pgformat } from '@scaleleap/pg-format';

import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { SUPPORTED_LOCALES, t } from '../middleware/translation';
import { DataTable } from '../entities/dataset/data-table';
import { DataTableAction } from '../enums/data-table-action';
import { Revision } from '../entities/dataset/revision';
import { Locale } from '../enums/locale';
import { DimensionType } from '../enums/dimension-type';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { CubeValidationException } from '../exceptions/cube-error-exception';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { MeasureRow } from '../entities/dataset/measure-row';
import { DatasetRepository } from '../repositories/dataset';
import { PeriodCovered } from '../interfaces/period-covered';
import { dateDimensionReferenceTableCreator } from './date-matching';
import { NumberExtractor, NumberType } from '../extractors/number-extractor';
import { CubeValidationType } from '../enums/cube-validation-type';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import { FactTableValidationExceptionType } from '../enums/fact-table-validation-exception-type';
import { CubeType } from '../enums/cube-type';
import { DateExtractor } from '../extractors/date-extractor';
import { performanceReporting } from '../utils/performance-reporting';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { StorageService } from '../interfaces/storage-service';
import { dbManager } from '../db/database-manager';
import cubeConfig from '../config/cube-view.json';
import { CubeViewBuilder } from '../interfaces/cube-view-builder';
import { NoteCodes } from '../enums/note-code';
import { MeasureFormat } from '../interfaces/measure-format';
import { UniqueMeasureDetails } from '../interfaces/unique-measure-details';
import { FactTableInfo } from '../interfaces/fact-table-info';

export const FACT_TABLE_NAME = 'fact_table';
export const CORE_VIEW_NAME = 'core_view';

export const makeCubeSafeString = (str: string): string => {
  return str
    .toLowerCase()
    .replace(/[ ]/g, '_')
    .replace(/[^a-zA-Z_]/g, '');
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
    start_date TIMESTAMP WITHOUT TIME ZONE,
    end_date TIMESTAMP WITHOUT TIME ZONE,
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
  const columnData: Record<string, string>[] = await cubeDB.query(
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
        row.start.toUTCString(),
        row.end.toUTCString(),
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
            `UPDATE metadata SET value='${periodCoverage[0].end_date.toISOString()}' WHERE key='end_date';`
          );
        }
      }
    }
  } else {
    await cubeDB.query(
      `INSERT INTO metadata (key, value) VALUES ('start_date', '${periodCoverage[0].start_date.toISOString()}');`
    );
    await cubeDB.query(
      `INSERT INTO metadata (key, value) VALUES ('end_date', '${periodCoverage[0].end_date.toISOString()}');`
    );
  }
  return `${makeCubeSafeString(factTableColumn.columnName)}_lookup`;
}

async function setupLookupTableDimension(
  cubeDB: QueryRunner,
  dataset: Dataset,
  dimension: Dimension,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[],
  viewConfig: CubeViewBuilder[]
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
    const columnRefName = `${columnName}_${t('column_headers.reference', { lng: locale })}`;
    const columnSortName = `${columnName}_${t('column_headers.sort', { lng: locale })}`;
    const columnHierarchyName = `${columnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    columnNames.get(locale)?.add(columnName);
    columnNames.get(locale)?.add(columnRefName);
    columnNames.get(locale)?.add(columnSortName);
    columnNames.get(locale)?.add(columnHierarchyName);
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat(`%I.description AS %I`, dimTable, columnName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat(`%I.%I AS %I`, dimTable, factTableColumn.columnName, columnRefName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat(`%I.sort_order AS %I`, dimTable, columnSortName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat(`%I.hierarchy AS %I`, dimTable, columnHierarchyName));
    for (const view of viewConfig) {
      view.columns.get(locale)?.add(pgformat('%I', columnName));
      if (view.config.refcodes) {
        view.columns.get(locale)?.add(pgformat('%I', columnRefName));
      }
      if (view.config.sort_orders) {
        view.columns.get(locale)?.add(pgformat('%I', columnSortName));
      }
      if (view.config.hierarchies) {
        view.columns.get(locale)?.add(pgformat('%I', columnHierarchyName));
      }
    }
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
    logger.error('Lookup table not loaded in to lookup table schema.  Loading lookup table from blob storage.');
    throw new CubeValidationException(
      `Lookup table (${dimension.lookupTable!.id}) not loaded in to lookup table schema.`
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
  logger.trace(`copy query: ${copyQuery}`);
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
  logger.trace('Finalizing values');
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
  logger.trace(`Update Query:\n${updateQuery}`);
  await cubeDB.query(updateQuery);
  // Seems to fix the issue around provisional codes not being removed from the fact table for SW-1016
  // Leaving code in place for now, but will remove in future as long as no other bugs are reported.
  // const deleteQuery = pgformat(
  //   `DELETE FROM %I USING %I WHERE %s AND string_to_array(%I.%I, ',') && string_to_array('!', ',');`,
  //   updateTableName,
  //   FACT_TABLE_NAME,
  //   joinParts.join(' AND '),
  //   FACT_TABLE_NAME,
  //   notesCodeColumn.columnName
  // );
  // logger.trace(`Delete query:\n${deleteQuery}`);
  // await cubeDB.query(deleteQuery);
  const updateNoteCodeQuery = pgformat(
    `UPDATE %I SET %I = array_to_string(array_remove(string_to_array(%I, ','), '!'), ',')`,
    FACT_TABLE_NAME,
    notesCodeColumn.columnName,
    notesCodeColumn.columnName
  );
  logger.trace(`Update note code query:\n${updateNoteCodeQuery}`);
  await cubeDB.query(updateNoteCodeQuery);
}

async function updateProvisionalsAndForecasts(
  cubeDB: QueryRunner,
  updateTableName: string,
  factIdentifiers: FactTableColumn[],
  dataTableIdentifiers: DataTableDescription[],
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn
): Promise<void> {
  logger.trace('Update provisional and forecast values');
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
  logger.trace(`Update query:\n${updateQuery}`);
  await cubeDB.query(updateQuery);
  const deleteQuery = pgformat(
    `DELETE FROM %I USING %I WHERE string_to_array(%I.%I, ',') && string_to_array('p,f', ',') AND %s;`,
    updateTableName,
    FACT_TABLE_NAME,
    updateTableName,
    notesCodeColumn.columnName,
    joinParts.join(' AND ')
  );
  logger.trace(`Delete query:\n${deleteQuery}`);
  await cubeDB.query(deleteQuery);
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
      logger.error(
        `Data table ${dataTable.id} }not loaded in to data_tables schema.  Loading data table from blob storage.`
      );
      throw new CubeValidationException('Data table not loaded in to data_tables schema.');
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

async function createNotesTable(
  cubeDB: QueryRunner,
  notesColumn: FactTableColumn,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  viewConfig: CubeViewBuilder[]
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
    columnNames.get(locale)?.add(columnName);
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('all_notes.description AS %I', columnName));
    for (const view of viewConfig) {
      if (view.config.note_descriptions) {
        view.columns.get(locale)?.add(pgformat('%I', columnName));
      }
    }
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

export const measureTableCreateStatement = (
  joinColumnType: string,
  schemaName?: string,
  tableName = 'measure',
  temporary = false
): string => {
  const finalTableName = schemaName ? pgformat('%I.%I', schemaName, tableName) : pgformat('%I', tableName);
  return pgformat(
    `
    CREATE %s TABLE %s (
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
    temporary ? 'TEMPORARY' : '',
    finalTableName,
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

function setupMeasureAndDataValuesNoLookup(
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  viewConfig: CubeViewBuilder[],
  measureColumn?: FactTableColumn,
  dataValuesColumn?: FactTableColumn,
  notesCodeColumn?: FactTableColumn
): void {
  SUPPORTED_LOCALES.map((locale) => {
    if (dataValuesColumn) {
      const dataValuesColumnName = t('column_headers.data_values', { lng: locale });
      const dataFormattedColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.formatted', { lng: locale })}`;
      const dataAnnotatedColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.annotated', { lng: locale })}`;
      const dataSortColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.sort', { lng: locale })}`;
      columnNames.get(locale)?.add(dataValuesColumnName);
      columnNames.get(locale)?.add(dataFormattedColName);
      columnNames.get(locale)?.add(dataAnnotatedColName);
      columnNames.get(locale)?.add(dataSortColName);
      columnNames.get(locale)?.add(t('column_headers.data_values', { lng: locale }));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataValuesColumnName));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataFormattedColName));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataSortColName));
      if (notesCodeColumn) {
        coreCubeViewSelectBuilder
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
              dataAnnotatedColName
            )
          );
      } else {
        coreCubeViewSelectBuilder
          .get(locale)
          ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataAnnotatedColName));
      }
      setupDataValueViews(
        locale,
        viewConfig,
        dataValuesColumnName,
        dataAnnotatedColName,
        dataFormattedColName,
        dataSortColName
      );
    }
    if (measureColumn) {
      const measureColumnName = t('column_headers.measure', { lng: locale });
      const measureColumnRefName = `${measureColumnName}_${t('column_headers.reference', { lng: locale })}`;
      const measureColumnSortName = `${measureColumnName}_${t('column_headers.sort', { lng: locale })}`;
      const measureColumnHierarchyName = `${measureColumnName}_${t('column_headers.hierarchy', { lng: locale })}`;
      columnNames.get(locale)?.add(measureColumnName);
      columnNames.get(locale)?.add(measureColumnRefName);
      columnNames.get(locale)?.add(measureColumnSortName);
      columnNames.get(locale)?.add(measureColumnHierarchyName);
      columnNames.get(locale)?.add(t('column_headers.measure', { lng: locale }));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, measureColumn.columnName, measureColumnName));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, measureColumn.columnName, measureColumnRefName));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, measureColumn.columnName, measureColumnSortName));
      coreCubeViewSelectBuilder.get(locale)?.push(pgformat('NULL AS %I', measureColumnHierarchyName));
      setupMeasureViews(
        locale,
        viewConfig,
        measureColumnName,
        measureColumnRefName,
        measureColumnSortName,
        measureColumnHierarchyName
      );
    }
  });
}

function setupDataValueViews(
  locale: Locale,
  viewConfig: CubeViewBuilder[],
  dataValuesColumnName: string,
  dataAnnotatedColName: string,
  dataFormattedColName: string,
  dataSortColName: string
): void {
  for (const view of viewConfig) {
    if (view.config.dataValues === 'annotated') {
      view.columns.get(locale)?.add(pgformat(`%I AS %I`, dataAnnotatedColName, dataValuesColumnName));
    } else if (view.config.dataValues === 'formatted') {
      view.columns.get(locale)?.add(pgformat(`%I AS %I`, dataFormattedColName, dataValuesColumnName));
    } else {
      view.columns.get(locale)?.add(pgformat('%I', dataValuesColumnName));
    }
    if (view.config.sort_orders) {
      view.columns.get(locale)?.add(pgformat('%I', dataSortColName));
    }
  }
}

function setupMeasureViews(
  locale: Locale,
  viewConfig: CubeViewBuilder[],
  measureColumnName: string,
  measureColumnRefName: string,
  measureColumnSortName: string,
  measureColumnHierarchyName: string
): void {
  for (const view of viewConfig) {
    view.columns.get(locale)?.add(pgformat('%I', measureColumnName));
    if (view.config.refcodes) {
      view.columns.get(locale)?.add(pgformat('%I', measureColumnRefName));
    }
    if (view.config.sort_orders) {
      view.columns.get(locale)?.add(pgformat('%I', measureColumnSortName));
    }
    if (view.config.hierarchies) {
      view.columns.get(locale)?.add(pgformat('%I', measureColumnHierarchyName));
    }
  }
}

async function setupMeasureAndDataValuesWithLookup(
  cubeDB: QueryRunner,
  measureTable: MeasureRow[],
  dataValuesColumn: FactTableColumn,
  measureColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn | undefined,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  viewConfig: CubeViewBuilder[],
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[]
): Promise<void> {
  logger.debug('Measure present in dataset. Creating measure table...');
  await createMeasureLookupTable(cubeDB, measureColumn, measureTable);

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
  SUPPORTED_LOCALES.map((locale) => {
    if (dataValuesColumn) {
      const dataValuesColumnName = t('column_headers.data_values', { lng: locale });
      const dataFormattedColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.formatted', { lng: locale })}`;
      const dataAnnotatedColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.annotated', { lng: locale })}`;
      const dataSortColName = `${t('column_headers.data_values', { lng: locale })}_${t('column_headers.sort', { lng: locale })}`;
      columnNames.get(locale)?.add(dataValuesColumnName);
      columnNames.get(locale)?.add(dataFormattedColName);
      columnNames.get(locale)?.add(dataAnnotatedColName);
      columnNames.get(locale)?.add(dataSortColName);
      // Add all variations of the column to the core or extended view of the dataset
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataValuesColumnName));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat(`%s AS %I`, caseStatements.join('\n'), dataFormattedColName));
      if (notesCodeColumn) {
        coreCubeViewSelectBuilder
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
              dataAnnotatedColName
            )
          );
        coreCubeViewSelectBuilder
          .get(locale)
          ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataSortColName));
      } else {
        coreCubeViewSelectBuilder
          .get(locale)
          ?.push(pgformat('%I.%I AS %I', FACT_TABLE_NAME, dataValuesColumn.columnName, dataAnnotatedColName));
      }
      setupDataValueViews(
        locale,
        viewConfig,
        dataValuesColumnName,
        dataAnnotatedColName,
        dataFormattedColName,
        dataSortColName
      );
    }
    const measureColumnName = t('column_headers.measure', { lng: locale });
    const measureColumnRefName = `${measureColumnName}_${t('column_headers.reference', { lng: locale })}`;
    const measureColumnSortName = `${measureColumnName}_${t('column_headers.sort', { lng: locale })}`;
    const measureColumnHierarchyName = `${measureColumnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    columnNames.get(locale)?.add(measureColumnName);
    columnNames.get(locale)?.add(measureColumnRefName);
    columnNames.get(locale)?.add(measureColumnSortName);
    columnNames.get(locale)?.add(measureColumnHierarchyName);
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('measure.description AS %I', measureColumnName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('measure.reference AS %I', `${measureColumnRefName}`));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('measure.sort_order AS %I', `${measureColumnSortName}`));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('measure.hierarchy AS %I', `${measureColumnHierarchyName}`));
    setupMeasureViews(
      locale,
      viewConfig,
      measureColumnName,
      measureColumnRefName,
      measureColumnSortName,
      measureColumnHierarchyName
    );
  });
  joinStatements.push(
    pgformat(
      'LEFT JOIN measure on measure.reference=%I.%I AND measure.language=#LANG#',
      FACT_TABLE_NAME,
      measureColumn.columnName
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
}

async function setupMeasuresAndDataValues(
  cubeDB: QueryRunner,
  dataset: Dataset,
  revsion: Revision,
  dataValuesColumn: FactTableColumn | undefined,
  measureColumn: FactTableColumn | undefined,
  notesCodeColumn: FactTableColumn | undefined,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  viewConfig: CubeViewBuilder[],
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[]
): Promise<void> {
  logger.info('Setting up measure table if present...');

  // Process the column that represents the measure
  if (revsion.tasks && revsion.tasks.measure) {
    setupMeasureAndDataValuesNoLookup(
      coreCubeViewSelectBuilder,
      columnNames,
      viewConfig,
      measureColumn,
      dataValuesColumn,
      notesCodeColumn
    );
  } else if (
    measureColumn &&
    dataValuesColumn &&
    dataset.measure.measureTable &&
    dataset.measure.measureTable.length > 0
  ) {
    await setupMeasureAndDataValuesWithLookup(
      cubeDB,
      dataset.measure.measureTable,
      dataValuesColumn,
      measureColumn,
      notesCodeColumn,
      coreCubeViewSelectBuilder,
      viewConfig,
      columnNames,
      joinStatements,
      orderByStatements
    );
  } else {
    setupMeasureAndDataValuesNoLookup(
      coreCubeViewSelectBuilder,
      columnNames,
      viewConfig,
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
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  viewConfig: CubeViewBuilder[]
): Promise<void> {
  for (const locale of SUPPORTED_LOCALES) {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    const columnRefName = `${columnName}_${t('column_headers.reference', { lng: locale })}`;
    const columnSortName = `${columnName}_${t('column_headers.sort', { lng: locale })}`;
    const columnHierarchyName = `${columnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    columnNames.get(locale)?.add(columnName);
    columnNames.get(locale)?.add(columnRefName);
    columnNames.get(locale)?.add(columnSortName);
    columnNames.get(locale)?.add(columnHierarchyName);
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('%I AS %I', dimension.factTableColumn, columnName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('%I AS %I', dimension.factTableColumn, columnRefName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('%I AS %I', dimension.factTableColumn, columnSortName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('NULL AS %I', columnHierarchyName));
    for (const view of viewConfig) {
      view.columns.get(locale)?.add(pgformat('%I', columnName));
      if (view.config.refcodes) {
        view.columns.get(locale)?.add(pgformat('%I', columnRefName));
      }
      if (view.config.sort_orders) {
        view.columns.get(locale)?.add(pgformat('%I', columnSortName));
      }
      if (view.config.hierarchies) {
        view.columns.get(locale)?.add(pgformat('%I', columnHierarchyName));
      }
    }
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
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[],
  viewConfig: CubeViewBuilder[]
): Promise<string> {
  const dimTable = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
  await createDateDimension(cubeDB, dimension.extractor, factTableColumn);
  for (const locale of SUPPORTED_LOCALES) {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    const columnRefName = `${columnName}_${t('column_headers.reference', { lng: locale })}`;
    const columnSortName = `${columnName}_${t('column_headers.sort', { lng: locale })}`;
    const columnHierarchyName = `${columnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    const columnStartDateName = `${columnName}_${t('column_headers.start_date', { lng: locale })}`;
    const columnEndDateName = `${columnName}_${t('column_headers.end_date', { lng: locale })}`;
    const columnISOStartDateName = `${columnName}_ISO_${t('column_headers.start_date', { lng: locale })}`;
    const columnISOEndDateName = `${columnName}_ISO_${t('column_headers.end_date', { lng: locale })}`;
    columnNames.get(locale)?.add(columnName);
    columnNames.get(locale)?.add(columnRefName);
    columnNames.get(locale)?.add(columnStartDateName);
    columnNames.get(locale)?.add(columnEndDateName);
    columnNames.get(locale)?.add(columnISOStartDateName);
    columnNames.get(locale)?.add(columnISOEndDateName);
    columnNames.get(locale)?.add(columnSortName);
    columnNames.get(locale)?.add(columnHierarchyName);
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('%I.description AS %I', dimTable, columnName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('%I.%I AS %I', dimTable, factTableColumn.columnName, columnRefName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat("TO_CHAR(%I.start_date, 'DD/MM/YYYY') AS %I", dimTable, columnStartDateName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat("TO_CHAR(%I.end_date, 'DD/MM/YYYY') AS %I", dimTable, columnEndDateName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('%I.start_date AS %I', dimTable, columnISOStartDateName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('%I.end_date AS %I', dimTable, columnISOEndDateName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('%I.end_date AS %I', dimTable, columnSortName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('%I.hierarchy AS %I', dimTable, columnHierarchyName));
    for (const view of viewConfig) {
      view.columns.get(locale)?.add(pgformat('%I', columnName));
      if (view.config.refcodes) {
        columnNames.get(locale)?.add(columnRefName);
      }
      if (view.config.dates === 'formatted') {
        columnNames.get(locale)?.add(columnStartDateName);
        columnNames.get(locale)?.add(columnEndDateName);
      }
      if (view.config.dates === 'raw') {
        columnNames.get(locale)?.add(columnISOStartDateName);
        columnNames.get(locale)?.add(columnISOEndDateName);
      }
      if (view.config.sort_orders) {
        columnNames.get(locale)?.add(columnSortName);
      }
      if (view.config.hierarchies) {
        columnNames.get(locale)?.add(columnHierarchyName);
      }
    }
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
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  viewConfig: CubeViewBuilder[]
): Promise<void> {
  SUPPORTED_LOCALES.map((locale) => {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    const columnRefName = `${columnName}_${t('column_headers.reference', { lng: locale })}`;
    const columnSortName = `${columnName}_${t('column_headers.sort', { lng: locale })}`;
    const columnHierarchyName = `${columnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    columnNames.get(locale)?.add(columnName);
    columnNames.get(locale)?.add(columnRefName);
    columnNames.get(locale)?.add(columnSortName);
    columnNames.get(locale)?.add(columnHierarchyName);
    if ((dimension.extractor as NumberExtractor).type === NumberType.Integer) {
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('CAST(%I.%I AS INTEGER) AS %I', FACT_TABLE_NAME, dimension.factTableColumn, columnName));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('CAST(%I.%I AS INTEGER) AS %I', FACT_TABLE_NAME, dimension.factTableColumn, columnRefName));
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(pgformat('CAST(%I.%I AS INTEGER) AS %I', FACT_TABLE_NAME, dimension.factTableColumn, columnSortName));
    } else {
      coreCubeViewSelectBuilder
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
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(
          pgformat(
            `CAST(%I.%I AS DECIMAL) AS %I`,
            FACT_TABLE_NAME,
            dimension.factTableColumn,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            columnRefName
          )
        );
      coreCubeViewSelectBuilder
        .get(locale)
        ?.push(
          pgformat(
            `CAST(%I.%I AS DECIMAL) AS %I`,
            FACT_TABLE_NAME,
            dimension.factTableColumn,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            (dimension.extractor as NumberExtractor).decimalPlaces,
            columnSortName
          )
        );
    }
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('NULL AS %I', FACT_TABLE_NAME, dimension.factTableColumn, columnHierarchyName));
    for (const view of viewConfig) {
      view.columns.get(locale)?.add(pgformat('%I', columnName));
      if (view.config.refcodes) {
        view.columns.get(locale)?.add(pgformat('%I', columnRefName));
      }
      if (view.config.sort_orders) {
        view.columns.get(locale)?.add(pgformat('%I', columnSortName));
      }
      if (view.config.hierarchies) {
        view.columns.get(locale)?.add(pgformat('%I', columnHierarchyName));
      }
    }
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
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  viewConfig: CubeViewBuilder[]
): Promise<void> {
  SUPPORTED_LOCALES.map((locale) => {
    const proposedColumnName =
      dimension.metadata.find((info) => info.language === locale)?.name || dimension.factTableColumn;
    const columnName = updateColumnName(columnNames.get(locale)!, proposedColumnName);
    const columnRefName = `${columnName}_${t('column_headers.reference', { lng: locale })}`;
    const columnSortName = `${columnName}_${t('column_headers.sort', { lng: locale })}`;
    const columnHierarchyName = `${columnName}_${t('column_headers.hierarchy', { lng: locale })}`;
    columnNames.get(locale)?.add(columnName);
    columnNames.get(locale)?.add(columnRefName);
    columnNames.get(locale)?.add(columnSortName);
    columnNames.get(locale)?.add(columnHierarchyName);
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, columnName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, columnRefName));
    coreCubeViewSelectBuilder
      .get(locale)
      ?.push(pgformat('CAST(%I AS VARCHAR) AS %I', dimension.factTableColumn, columnSortName));
    coreCubeViewSelectBuilder.get(locale)?.push(pgformat('NULL AS %I', columnHierarchyName));
    for (const view of viewConfig) {
      view.columns.get(locale)?.add(pgformat('%I', columnName));
      if (view.config.refcodes) {
        view.columns.get(locale)?.add(pgformat('%I', columnRefName));
      }
      if (view.config.sort_orders) {
        view.columns.get(locale)?.add(pgformat('%I', columnSortName));
      }
      if (view.config.hierarchies) {
        view.columns.get(locale)?.add(pgformat('%I', columnHierarchyName));
      }
    }
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
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  viewConfig: CubeViewBuilder[],
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
    if (
      endRevision.tasks &&
      endRevision.tasks.dimensions.find((dim) => dim.id === dimension.id && !dim.lookupTableUpdated)
    ) {
      await rawDimensionProcessor(cubeDB, dimension, coreCubeViewSelectBuilder, columnNames, viewConfig);
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
              coreCubeViewSelectBuilder,
              columnNames,
              joinStatements,
              orderByStatements,
              viewConfig
            );
            lookupTables.add(tableName);
          } else {
            await rawDimensionProcessor(cubeDB, dimension, coreCubeViewSelectBuilder, columnNames, viewConfig);
          }
          break;
        case DimensionType.LookupTable:
          tableName = await setupLookupTableDimension(
            cubeDB,
            dataset,
            dimension,
            coreCubeViewSelectBuilder,
            columnNames,
            joinStatements,
            orderByStatements,
            viewConfig
          );
          lookupTables.add(tableName);
          break;
        case DimensionType.Numeric:
          await setupNumericDimension(cubeDB, dimension, coreCubeViewSelectBuilder, columnNames, viewConfig);
          break;
        case DimensionType.Text:
          await setupTextDimension(cubeDB, dimension, coreCubeViewSelectBuilder, columnNames, viewConfig);
          break;
        case DimensionType.Raw:
        case DimensionType.Symbol:
          await rawDimensionProcessor(cubeDB, dimension, coreCubeViewSelectBuilder, columnNames, viewConfig);
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
    logger.trace(`add primary key query: ${alterTableQuery}`);
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
  endRevision: Revision,
  viewConfig: CubeViewBuilder[]
): Promise<void> => {
  logger.debug(`Starting build ${buildId} and Creating base cube for revision ${endRevision.id}`);
  await cubeDB.query(pgformat(`SET search_path TO %I;`, buildId));
  const functionStart = performance.now();
  const coreCubeViewSelectBuilder = new Map<Locale, string[]>();
  const columnNames = new Map<Locale, Set<string>>();

  SUPPORTED_LOCALES.map((locale) => {
    coreCubeViewSelectBuilder.set(locale, []);
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
  try {
    await setupMeasuresAndDataValues(
      cubeDB,
      dataset,
      endRevision,
      factTableInfo.dataValuesColumn,
      factTableInfo.measureColumn,
      factTableInfo.notesCodeColumn,
      coreCubeViewSelectBuilder,
      viewConfig,
      columnNames,
      joinStatements,
      orderByStatements
    );
  } catch (err) {
    await cubeDB.query(`UPDATE metadata SET value = 'failed' WHERE key = 'build_status'`);
    logger.error(err, `Failed to setup measures`);
    throw new Error(`Failed to setup measures: ${err}`);
  }
  performanceReporting(Math.round(performance.now() - measureSetupMark), 1000, 'Setting up the measure');

  const dimensionSetupMark = performance.now();
  try {
    await setupDimensions(
      cubeDB,
      dataset,
      endRevision,
      coreCubeViewSelectBuilder,
      viewConfig,
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
      coreCubeViewSelectBuilder,
      columnNames,
      joinStatements,
      viewConfig
    );
  }
  performanceReporting(Math.round(performance.now() - noteCodeCreation), 1000, 'Setting up the note codes');

  logger.info(`Creating default views...`);
  const viewCreation = performance.now();
  // Build the default views
  try {
    for (const locale of SUPPORTED_LOCALES) {
      if (coreCubeViewSelectBuilder.get(locale)?.length === 0) {
        coreCubeViewSelectBuilder.get(locale)?.push('*');
      }
      const lang = locale.toLowerCase().split('-')[0];

      const coreViewName = `${CORE_VIEW_NAME}_${lang}`;

      const coreCubeViewSQL = pgformat(
        'SELECT %s FROM %I %s %s',
        coreCubeViewSelectBuilder.get(locale)?.join(',\n'),
        FACT_TABLE_NAME,
        joinStatements.join('\n').replace(/#LANG#/g, pgformat('%L', locale.toLowerCase())),
        orderByStatements.length > 0 ? `ORDER BY ${orderByStatements.join(', ')}` : ''
      );

      logger.trace(`core cube view SQL: ${coreCubeViewSQL}`);
      await cubeDB.query(pgformat('CREATE VIEW %I AS %s', `${CORE_VIEW_NAME}_${lang}`, coreCubeViewSQL));
      await cubeDB.query(pgformat(`INSERT INTO metadata VALUES (%L, %L)`, coreViewName, coreCubeViewSQL));

      if (Array.from(columnNames.get(locale)?.values() || []).length > 0) {
        await cubeDB.query(
          pgformat(
            `INSERT INTO metadata VALUES (%L, %L)`,
            `${CORE_VIEW_NAME}_columns_${lang}`,
            JSON.stringify(Array.from(columnNames.get(locale)?.values() || []))
          )
        );
      } else {
        const cols = dataset.factTable?.map((col) => col.columnName);
        await cubeDB.query(
          pgformat(`INSERT INTO metadata VALUES (%L, %L)`, `${CORE_VIEW_NAME}_columns_${lang}`, JSON.stringify(cols))
        );
      }
      await createViewsFromConfig(cubeDB, coreViewName, locale, viewConfig, dataset.factTable!);
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

async function createViewsFromConfig(
  cubeDB: QueryRunner,
  baseViewName: string,
  locale: Locale,
  viewConfig: CubeViewBuilder[],
  factTable: FactTableColumn[]
): Promise<void> {
  const lang = locale.toLowerCase().split('-')[0];
  for (const view of viewConfig) {
    const viewName = `${view.name}_${lang}`;
    let cols: string[] = [];
    if (Array.from(view.columns.get(locale)?.values() || []).length > 0) {
      cols = Array.from(view.columns.get(locale)!.values());
    } else {
      cols = factTable.map((col) => pgformat('%I', col.columnName)) || ['*'];
    }
    const SQL = pgformat('SELECT %s FROM %I', cols.join(', '), baseViewName);
    await cubeDB.query(pgformat('DELETE FROM metadata WHERE key = %L', viewName));
    await cubeDB.query(pgformat('INSERT INTO metadata VALUES (%L, %L)', viewName, SQL));
    await cubeDB.query(pgformat('DELETE FROM metadata WHERE key = %L', `${viewName}_columns`));
    await cubeDB.query(pgformat('INSERT INTO metadata VALUES (%L, %L)', `${viewName}_columns`, JSON.stringify(cols)));
    await cubeDB.query(pgformat('DROP VIEW IF EXISTS %I;', viewName));
    await cubeDB.query(pgformat('CREATE VIEW %I AS %s;', viewName, SQL));
  }
}

export const createMaterialisedView = async (
  revisionId: string,
  dataset: Dataset,
  viewConfig: CubeViewBuilder[]
): Promise<void> => {
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  logger.info(`Creating default views...`);
  const viewCreation = performance.now();
  // Build the default views
  try {
    await cubeDB.query(pgformat(`SET search_path TO %I;`, revisionId));
    for (const locale of SUPPORTED_LOCALES) {
      const lang = locale.toLowerCase().split('-')[0];
      const materializedViewName = `${CORE_VIEW_NAME}_mat_${lang}`;
      const originalCoreViewSQL: { value: string }[] = await cubeDB.query(
        pgformat('SELECT value FROM metadata WHERE key = %L', `${CORE_VIEW_NAME}_${lang}`)
      );
      await cubeDB.query(
        pgformat('CREATE MATERIALIZED VIEW %I AS %s', materializedViewName, originalCoreViewSQL[0].value)
      );
      await createViewsFromConfig(cubeDB, materializedViewName, locale, viewConfig, dataset.factTable!);
      await cubeDB.query(pgformat('DROP VIEW %I;', `${CORE_VIEW_NAME}_${lang}`));
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

  const cubeBuildConfig = cubeConfig.map((config) => {
    const columns = new Map<Locale, Set<string>>();
    const viewParts = new Map<Locale, string[]>();
    SUPPORTED_LOCALES.forEach((locale) => {
      columns.set(locale, new Set<string>());
      viewParts.set(locale, []);
    });
    return {
      name: config.name,
      config: config,
      columns,
      viewParts
    } as CubeViewBuilder;
  });

  try {
    logger.debug(`Renaming ${buildId} to cube rev ${endRevision.id}`);
    await createBasePostgresCube(cubeDB, buildId, dataset, endRevision, cubeBuildConfig);
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
  void createMaterialisedView(endRevisionId, dataset, cubeBuildConfig);
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
