import { Dataset } from '../entities/dataset/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { Revision } from '../entities/dataset/revision';
import { logger } from '../utils/logger';
import { performance } from 'node:perf_hooks';
import { Locale } from '../enums/locale';
import { SUPPORTED_LOCALES, t } from '../middleware/translation';
import { CubeValidationException } from '../exceptions/cube-error-exception';
import { CubeValidationType } from '../enums/cube-validation-type';
import { performanceReporting } from '../utils/performance-reporting';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { dbManager } from '../db/database-manager';
import { CubeViewConfig } from '../interfaces/cube-view-config';
import { DataTable } from '../entities/dataset/data-table';
import { DataTableAction } from '../enums/data-table-action';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { MeasureRow } from '../entities/dataset/measure-row';
import { DimensionType } from '../enums/dimension-type';
import { QueryRunner } from 'typeorm';
import { Dimension } from '../entities/dataset/dimension';
import { dateDimensionReferenceTableCreator } from './date-matching';
import { DateExtractor } from '../extractors/date-extractor';
import { toZonedTime } from 'date-fns-tz/dist/esm';

export const FACT_TABLE_NAME = 'fact_table';
export const CORE_VIEW_NAME = 'core_view';

interface CubeBuilder {
  name: string;
  config: CubeViewConfig;
  columns: Map<Locale, Set<string>>;
}

interface FactTableInfo {
  factTableCreationQuery: string;
  measureColumn?: FactTableColumn;
  notesCodeColumn?: FactTableColumn;
  dataValuesColumn?: FactTableColumn;
  factTableDef: string[];
  factIdentifiers: FactTableColumn[];
  compositeKey: string[];
}

interface UniqueMeasureDetails {
  reference: string;
  format: string;
  sort_order: number | null;
  decimals: number | null;
}

interface MeasureFormat {
  name: string;
  method: string;
}

enum CubeBuildStatus {
  Building = 'building',
  Failed = 'failed',
  Completed = 'completed',
  Materializing = 'materializing'
}

enum CubeMetaDataKeys {
  BuildStatus = 'build_status',
  Revision = 'revision_id',
  Build = 'build_id',
  BuildStart = 'build_start',
  BuildFinished = 'build_finished',
  StartDate = 'start_date',
  EndDate = 'end_date',
  NoteCodes = 'note_codes',
  LookupTables = 'lookup_tables',
  BuildScript = 'build_script',
  BuildResults = 'build_results'
}

export const makeCubeSafeString = (str: string): string => {
  return str
    .toLowerCase()
    .replace(/[ ]/g, '_')
    .replace(/[^a-zA-Z_]/g, '');
};

export async function setupCubeBuilder(dataset: Dataset, buildId: string): Promise<FactTableInfo> {
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
  const factTableCreationQuery = pgformat(
    `CREATE TABLE %I.%I (%s);`,
    buildId,
    FACT_TABLE_NAME,
    factTableCreationDef.join(', ')
  );

  return {
    factTableCreationQuery,
    measureColumn,
    notesCodeColumn,
    dataValuesColumn,
    factTableDef,
    factIdentifiers,
    compositeKey
  };
}

export async function createCubeBaseTables(
  revisionId: string,
  buildId: string,
  factTableQuery: string
): Promise<string[]> {
  const statements: string[] = [factTableQuery];
  statements.push(`CREATE TABLE IF NOT EXISTS %I.metadata (key VARCHAR, value VARCHAR);`);
  statements.push(pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', buildId, CubeMetaDataKeys.Revision, revisionId));
  statements.push(pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', buildId, CubeMetaDataKeys.Build, buildId));
  statements.push(
    pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', buildId, CubeMetaDataKeys.BuildStart, new Date().toISOString())
  );
  statements.push(
    pgformat(
      'INSERT INTO %I.metadata VALUES (%L, %L);',
      buildId,
      CubeMetaDataKeys.BuildStatus,
      CubeBuildStatus.Building
    )
  );
  statements.push(
    pgformat(
      `
      CREATE TABLE %I.%I (
        reference VARCHAR,
        language VARCHAR,
        fact_table_column VARCHAR,
        dimension_name VARCHAR,
        description VARCHAR,
        hierarchy VARCHAR,
        PRIMARY KEY (reference, language, fact_table_column)
      );
    `,
      buildId,
      'filter_table'
    )
  );
  statements.push(
    pgformat('INSERT INTO %I.metadata VALUES (%L, %L);', buildId, CubeMetaDataKeys.BuildScript, statements.join('\n'))
  );
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    await cubeDB.query(statements.join('\n'));
  } catch (err) {
    logger.error(err, 'Something went wrong creating cube base tables');
  } finally {
    cubeDB.release();
  }
  return statements;
}

export const loadTableDataIntoFactTableFromPostgresStatement = (
  buildId: string,
  factTableDef: string[],
  factTableName: string,
  dataTableId: string
): string => {
  return pgformat(
    'INSERT INTO %I.%I SELECT %I FROM %I.%I;',
    buildId,
    factTableName,
    factTableDef,
    'data_tables',
    dataTableId
  );
};

function createCoreCubeViewSQL(
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  locale: Locale,
  joinStatements: string[],
  orderByStatements: string[]
): string {
  return pgformat(
    'SELECT %s FROM %I %s %s',
    coreCubeViewSelectBuilder.get(locale)?.join(',\n'),
    FACT_TABLE_NAME,
    joinStatements.join('\n').replace(/#LANG#/g, pgformat('%L', locale.toLowerCase())),
    orderByStatements.length > 0 ? `ORDER BY ${orderByStatements.join(', ')}` : ''
  );
}

function createCubeNoSources(
  buildId: string,
  endRevision: Revision,
  factTableInfo: FactTableInfo,
  coreCubeViewSelectBuilder: Map<Locale, string[]>
): string[] {
  const buildStatements: string[] = [];
  buildStatements.push(
    loadTableDataIntoFactTableFromPostgresStatement(
      buildId,
      factTableInfo.factTableDef,
      FACT_TABLE_NAME,
      endRevision.dataTableId!
    )
  );
  for (const locale of SUPPORTED_LOCALES) {
    const lang = locale.toLowerCase().split('-')[0];
    const coreViewName = `${CORE_VIEW_NAME}_${lang}`;
    coreCubeViewSelectBuilder.get(locale)?.push('*');
    const coreCubeViewSQL = createCoreCubeViewSQL(coreCubeViewSelectBuilder, locale, [], []);
    buildStatements.push(pgformat('CREATE VIEW %I.%I AS %s', buildId, coreViewName, coreCubeViewSQL));
    buildStatements.push(pgformat(`INSERT INTO %I.metadata VALUES (%L, %L)`, buildId, coreViewName, coreCubeViewSQL));
  }
  buildStatements.push(
    pgformat(
      'UPDATE %I.metadata SET value = %L WHERE key = %L',
      buildId,
      CubeMetaDataKeys.BuildStatus,
      CubeBuildStatus.Materializing
    )
  );
  return buildStatements;
}

function resetFactTable(buildId: string): string {
  return pgformat('DELETE FROM %I.%I;', buildId, FACT_TABLE_NAME);
}

function dropUpdateTable(buildId: string, updateTableName: string): string {
  return pgformat('DROP TABLE %I', buildId, updateTableName);
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

enum NoteCode {
  Average = 'a',
  BreakInSeries = 'b',
  Confidential = 'c',
  Estimated = 'e',
  Forecast = 'f',
  LowFigure = 'k',
  LowReliability = 'u',
  MissingData = 'x',
  NotApplicable = 'z',
  NotRecorded = 'w',
  NotStatisticallySignificant = 'ns',
  Provisional = 'p',
  Revised = 'r',
  StatisticallySignificantL1 = 's',
  StatisticallySignificantL2 = 'ss',
  StatisticallySignificantL3 = 'sss',
  Total = 't'
}

function stripExistingCodes(
  buildId: string,
  tableName: string,
  notesCodeColumn: FactTableColumn,
  noteCode: NoteCode
): string {
  return pgformat(
    `UPDATE %I.%I SET %I = array_to_string(array_remove(string_to_array(replace(lower(%I.%I), ' ', ''), ','),%L),',');`,
    buildId,
    tableName,
    notesCodeColumn.columnName,
    tableName,
    notesCodeColumn.columnName,
    noteCode
  );
}

function createUpdateTable(buildId: string, tempTableName: string, dataTable: DataTable): string {
  return pgformat(
    'CREATE TEMPORARY TABLE %I.%I AS SELECT * FROM data_tables.%I;',
    buildId,
    tempTableName,
    dataTable.id
  );
}

function finaliseValues(
  buildId: string,
  updateTableName: string,
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  joinParts: string[]
): string[] {
  const statements: string[] = [];
  statements.push(
    pgformat(
      `UPDATE %I.%I SET %I = %I.%I, %I = array_to_string(array_append(string_to_array(lower(%I.%I), ','), '!'), ',') FROM %I WHERE %s AND string_to_array(lower(%I.%I), ',') && string_to_array('p,f', ',');`,
      buildId,
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
    )
  );

  // Seems to fix the issue around provisional codes not being removed from the fact table for SW-1016
  // Leaving code in place for now, but will remove in future as long as no other bugs are reported.
  // statements.push(pgformat(
  //   `DELETE FROM %I USING %I WHERE %s AND string_to_array(%I.%I, ',') && string_to_array('!', ',');`,
  //   updateTableName,
  //   FACT_TABLE_NAME,
  //   joinParts.join(' AND '),
  //   FACT_TABLE_NAME,
  //   notesCodeColumn.columnName
  // ));

  statements.push(
    pgformat(
      `UPDATE %I.%I SET %I = array_to_string(array_remove(string_to_array(%I, ','), '!'), ',')`,
      buildId,
      FACT_TABLE_NAME,
      notesCodeColumn.columnName,
      notesCodeColumn.columnName
    )
  );
  return statements;
}

function updateProvisionalAndForecastValues(
  buildId: string,
  updateTableName: string,
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  joinParts: string[]
): string[] {
  const statements: string[] = [];
  statements.push(
    pgformat(
      `UPDATE %I.%I SET %I = %I.%I, %I = %I.%I FROM %I WHERE %s AND string_to_array(%I.%I, ',') && string_to_array('p,f', ',');`,
      buildId,
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
    )
  );
  statements.push(
    pgformat(
      `DELETE FROM %I.%I USING %I WHERE string_to_array(%I.%I, ',') && string_to_array('p,f', ',') AND %s;`,
      buildId,
      updateTableName,
      FACT_TABLE_NAME,
      updateTableName,
      notesCodeColumn.columnName,
      joinParts.join(' AND ')
    )
  );
  return statements;
}

function fixNoteCodesOnUpdateTable(
  buildId: string,
  updateTableName: string,
  notesCodeColumn: FactTableColumn,
  joinParts: string[]
): string[] {
  const statements: string[] = [];
  statements.push(stripExistingCodes(buildId, updateTableName, notesCodeColumn, NoteCode.Revised));
  statements.push(
    pgformat(
      `UPDATE %I.%I SET %I = array_to_string(array_append(array_remove(string_to_array(lower(%I.%I), ','), %L), %L), ',') FROM %I.%I WHERE %s;`,
      buildId,
      updateTableName,
      notesCodeColumn.columnName,
      updateTableName,
      notesCodeColumn.columnName,
      NoteCode.Revised,
      NoteCode.Revised,
      buildId,
      FACT_TABLE_NAME,
      joinParts.join(' AND ')
    )
  );
  return statements;
}

function updateFactsTableFromUpdateTable(
  buildId: string,
  updateTableName: string,
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  joinParts: string[]
): string {
  return pgformat(
    `UPDATE %I.%I SET %I = %I.%I, %I = %I.%I FROM %I.%I WHERE %s;`,
    buildId,
    FACT_TABLE_NAME,
    dataValuesColumn.columnName,
    updateTableName,
    dataValuesColumn.columnName,
    notesCodeColumn.columnName,
    updateTableName,
    notesCodeColumn.columnName,
    buildId,
    updateTableName,
    joinParts.join(' AND ')
  );
}

function copyUpdateTableToFactTable(
  buildId: string,
  updateTableName: string,
  factTableDef: string[],
  joinParts: string[],
  dataTableIdentifiers: DataTableDescription[]
): string[] {
  const statements: string[] = [];
  const dataTableSelect: string[] = [];
  for (const col of factTableDef) {
    const dataTableCol = dataTableIdentifiers.find((dataTableCol) => dataTableCol.factTableColumn === col);
    if (dataTableCol) dataTableSelect.push(dataTableCol.factTableColumn);
  }
  // First remove values which already exist in the fact table
  statements.push(
    pgformat(
      `DELETE FROM %I.%I USING %I.%I WHERE %s`,
      buildId,
      FACT_TABLE_NAME,
      buildId,
      updateTableName,
      joinParts.join(' AND ')
    )
  );
  // Now copy over anything else which remains
  statements.push(
    pgformat(
      'INSERT INTO %I.%I (%I) (SELECT %I FROM %I.%I);',
      buildId,
      FACT_TABLE_NAME,
      factTableDef,
      dataTableSelect,
      buildId,
      updateTableName
    )
  );
  return statements;
}

function cleanupNotesCodeColumn(buildId: string, notesCodeColumn: FactTableColumn): string {
  return pgformat(
    `UPDATE %I.%I SET %I = NULL WHERE %I = '';`,
    buildId,
    FACT_TABLE_NAME,
    notesCodeColumn.columnName,
    notesCodeColumn.columnName
  );
}

export function loadFactTables(
  dataset: Dataset,
  endRevision: Revision,
  buildId: string,
  factTableDef: string[],
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  factIdentifiers: FactTableColumn[]
): string[] {
  const buildStatements: string[] = [];
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
    // If we don't have a revision index, we need to find the previous revision to this one that does
    if (endRevision.dataTable) {
      logger.debug('Adding end revision to list of fact tables');
      allFactTables.push(endRevision.dataTable);
    }
    const validRevisions = dataset.revisions.filter((rev) => rev.revisionIndex > 0);
    validRevisions.forEach((revision) => {
      if (revision.dataTable) allFactTables.push(revision.dataTable);
    });
  }

  const allDataTables = allFactTables.reverse().sort((ftA, ftB) => ftA.uploadedAt.getTime() - ftB.uploadedAt.getTime());
  for (const dataTable of allDataTables) {
    const actionID = crypto.randomUUID();
    const joinParts: string[] = [];
    for (const factTableCol of factIdentifiers) {
      const dataTableCol = dataTable.dataTableDescriptions.find(
        (col) => col.factTableColumn === factTableCol.columnName
      );
      joinParts.push(
        pgformat(
          'CAST(%I.%I AS VARCHAR) = CAST(%I.%I AS VARCHAR)',
          FACT_TABLE_NAME,
          factTableCol.columnName,
          actionID,
          dataTableCol?.columnName
        )
      );
    }
    logger.debug(`Performing action ${dataTable.action} on fact table for data table ${dataTable.id}`);
    switch (dataTable.action) {
      case DataTableAction.ReplaceAll:
        buildStatements.push(resetFactTable(buildId));
        buildStatements.push(
          loadTableDataIntoFactTableFromPostgresStatement(buildId, factTableDef, FACT_TABLE_NAME, dataTable.id)
        );
        break;
      case DataTableAction.Add:
        buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Provisional));
        buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Forecast));
        buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Revised));
        buildStatements.push(
          loadTableDataIntoFactTableFromPostgresStatement(buildId, factTableDef, FACT_TABLE_NAME, dataTable.id)
        );
        break;
      case DataTableAction.Revise:
        buildStatements.push(createUpdateTable(buildId, actionID, dataTable));
        buildStatements.push(...finaliseValues(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts));
        buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Provisional));
        buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Forecast));
        buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Revised));
        buildStatements.push(
          ...updateProvisionalAndForecastValues(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts)
        );
        buildStatements.push(...fixNoteCodesOnUpdateTable(buildId, actionID, notesCodeColumn, joinParts));
        buildStatements.push(
          updateFactsTableFromUpdateTable(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts)
        );
        buildStatements.push(dropUpdateTable(buildId, actionID));
        break;
      case DataTableAction.AddRevise:
        buildStatements.push(createUpdateTable(buildId, actionID, dataTable));
        buildStatements.push(...finaliseValues(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts));
        buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Provisional));
        buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Forecast));
        buildStatements.push(stripExistingCodes(buildId, FACT_TABLE_NAME, notesCodeColumn, NoteCode.Revised));
        buildStatements.push(
          ...updateProvisionalAndForecastValues(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts)
        );
        buildStatements.push(...fixNoteCodesOnUpdateTable(buildId, actionID, notesCodeColumn, joinParts));
        buildStatements.push(
          updateFactsTableFromUpdateTable(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts)
        );
        buildStatements.push(
          ...copyUpdateTableToFactTable(buildId, actionID, factTableDef, joinParts, dataTable.dataTableDescriptions)
        );
        buildStatements.push(dropUpdateTable(buildId, actionID));
        break;
      case DataTableAction.Correction:
        buildStatements.push(createUpdateTable(buildId, actionID, dataTable));
        buildStatements.push(
          updateFactsTableFromUpdateTable(buildId, actionID, dataValuesColumn, notesCodeColumn, joinParts)
        );
        buildStatements.push(dropUpdateTable(buildId, actionID));
        break;
    }
  }
  buildStatements.push(cleanupNotesCodeColumn(buildId, notesCodeColumn));
  return buildStatements;
}

function createPrimaryKeyOnFactTable(buildId: string, compositeKey: string[]): string {
  return pgformat('ALTER TABLE %I.%I ADD PRIMARY KEY (%I)', buildId, FACT_TABLE_NAME, compositeKey);
}

function setupDataValueViews(
  locale: Locale,
  viewConfig: CubeBuilder[],
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
  viewConfig: CubeBuilder[],
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

function setupMeasureAndDataValuesNoLookup(
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  viewConfig: CubeBuilder[],
  measureColumn: FactTableColumn,
  dataValuesColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn
): string[] {
  const statements: string[] = [];
  SUPPORTED_LOCALES.map((locale) => {
    // Set up data values column (with no measure lookup)
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
    // Set up measure column (no lookup)
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
    const columnName = t('column_headers.measure', { lng: locale });
    statements.push(
      pgformat(
        `INSERT INTO filter_table SELECT DISTINT CAST(%I AS VARCHAR), %L, %L, %L, CAST(%I AS VARCHAR), null FROM %I ORDER BY %I`,
        columnName,
        locale.toLowerCase(),
        columnName,
        measureColumnName,
        FACT_TABLE_NAME,
        columnName
      )
    );
  });
  return statements;
}

export const measureTableCreateStatement = (
  joinColumnType: string,
  tableName = 'measure',
  buildId?: string
): string => {
  return pgformat(
    `
    CREATE TABLE %s (
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
    buildId ? pgformat('%I.%I', buildId, tableName) : pgformat('%I', tableName),
    joinColumnType,
    joinColumnType
  );
};

export function createMeasureLookupTable(
  buildId: string,
  measureColumn: FactTableColumn,
  measureTable: MeasureRow[]
): string[] {
  const statements: string[] = [];
  statements.push(measureTableCreateStatement(buildId, measureColumn.columnDatatype));
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
    statements.push(pgformat('INSERT INTO %I.measure VALUES (%L)', buildId, values));
  }
  return statements;
}

function toUniqueMeasureDetailsByRef(measureTable: MeasureRow[]): UniqueMeasureDetails[] {
  const map = new Map<string, UniqueMeasureDetails>();
  for (const r of measureTable) {
    if (!map.has(r.reference)) {
      map.set(r.reference, {
        reference: r.reference,
        format: r.format,
        sort_order: r.sortOrder ?? null,
        decimals: r.decimal ?? null
      });
    }
  }
  return Array.from(map.values());
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

function setupMeasureAndDataValuesWithLookup(
  buildId: string,
  measureTable: MeasureRow[],
  dataValuesColumn: FactTableColumn,
  measureColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn | undefined,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  viewConfig: CubeBuilder[],
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[]
): string[] {
  const statements: string[] = [];
  statements.push(...createMeasureLookupTable(buildId, measureColumn, measureTable));

  logger.debug('Creating query part to format the data value correctly');

  const uniqueReferences = toUniqueMeasureDetailsByRef(measureTable);
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
    statements.push(
      pgformat(
        `INSERT INTO filter_table SELECT CAST(reference AS VARCHAR), language, %L, %L, description, CAST(hierarchy AS VARCHAR) FROM measure WHERE language = %L ORDER BY sort_order, reference`,
        measureColumn.columnName,
        columnName,
        locale.toLowerCase()
      )
    );
  }
  return statements;
}

async function rawDimensionProcessor(
  cubeDB: QueryRunner,
  dimension: Dimension,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  viewConfig: CubeBuilder[]
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

function dateDimensionProcessor(
  buildId: string,
  factTableColumn: FactTableColumn,
  dimension: Dimension,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[],
  viewConfig: CubeBuilder[]
): string[] {
  const dimTable = await createDateDimension(cubeDB, dimension.extractor, factTableColumn);
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

function setupDimensions(
  buildId: string,
  dataset: Dataset,
  endRevision: Revision,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  viewConfig: CubeBuilder[],
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[]
): string[] {
  logger.info('Setting up dimension tables...');
  const statements: string[] = [];
  const lookupTables: Set<string> = new Set<string>();
  const factTable = dataset.factTable!;

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
    const tableName = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
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
      dimension.type = DimensionType.Raw;
    }

    try {
      switch (dimension.type) {
        case DimensionType.DatePeriod:
        case DimensionType.Date:
          if (dimension.extractor) {
            statements.push(
              ...dateDimensionProcessor(
                buildId,
                factTableColumn,
                dimension,
                coreCubeViewSelectBuilder,
                columnNames,
                joinStatements,
                orderByStatements,
                viewConfig
              )
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

function setupMeasuresAndDataValues(
  buildId: string,
  dataset: Dataset,
  revision: Revision,
  dataValuesColumn: FactTableColumn,
  measureColumn: FactTableColumn,
  notesCodeColumn: FactTableColumn,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  viewConfig: CubeBuilder[],
  columnNames: Map<Locale, Set<string>>,
  joinStatements: string[],
  orderByStatements: string[]
): string[] {
  let createMeasureTable = true;
  if (revision.tasks && revision.tasks.measure) {
    createMeasureTable = false;
  }

  if (createMeasureTable && dataset.measure.measureTable && dataset.measure.measureTable.length > 0) {
    return setupMeasureAndDataValuesWithLookup(
      buildId,
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
  }
  return setupMeasureAndDataValuesNoLookup(
    coreCubeViewSelectBuilder,
    columnNames,
    viewConfig,
    measureColumn,
    dataValuesColumn,
    notesCodeColumn
  );
}

function createFullCube(
  buildId: string,
  dataset: Dataset,
  endRevision: Revision,
  factTableInfo: FactTableInfo,
  coreCubeViewSelectBuilder: Map<Locale, string[]>,
  viewConfig: CubeBuilder[]
): string[] {
  const columnNames = new Map<Locale, Set<string>>();
  SUPPORTED_LOCALES.map((locale) => {
    columnNames.set(locale, new Set<string>());
  });

  const buildStatements: string[] = [];
  const loadFactTablesStatements = loadFactTables(
    dataset,
    endRevision,
    buildId,
    factTableInfo.factTableDef,
    factTableInfo.dataValuesColumn!,
    factTableInfo.notesCodeColumn!,
    factTableInfo.factIdentifiers
  );
  buildStatements.push(...loadFactTablesStatements);
  buildStatements.push(createPrimaryKeyOnFactTable(buildId, factTableInfo.compositeKey));

  const joinStatements: string[] = [];
  const orderByStatements: string[] = [];

  buildStatements.push(
    ...setupMeasuresAndDataValues(
      buildId,
      dataset,
      endRevision,
      factTableInfo.dataValuesColumn!,
      factTableInfo.measureColumn!,
      factTableInfo.notesCodeColumn!,
      coreCubeViewSelectBuilder,
      viewConfig,
      columnNames,
      joinStatements,
      orderByStatements
    )
  );

  setupDimensions(
    cubeDB,
    dataset,
    endRevision,
    coreCubeViewSelectBuilder,
    viewConfig,
    columnNames,
    joinStatements,
    orderByStatements
  );

  return buildStatements;
}

export const createBasePostgresCube = async (
  buildId: string,
  dataset: Dataset,
  endRevision: Revision,
  viewConfig: CubeBuilder[]
): Promise<void> => {
  logger.debug(`Starting build ${buildId} and Creating base cube for revision ${endRevision.id}`);
  const functionStart = performance.now();
  const coreCubeViewSelectBuilder = new Map<Locale, string[]>();
  const columnNames = new Map<Locale, Set<string>>();

  SUPPORTED_LOCALES.map((locale) => {
    coreCubeViewSelectBuilder.set(locale, []);
    columnNames.set(locale, new Set<string>());
  });

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
  const factTableInfo = await setupCubeBuilder(dataset, buildId);
  const fullBuildScript = await createCubeBaseTables(endRevision.id, buildId, factTableInfo.factTableCreationQuery);
  performanceReporting(Math.round(performance.now() - functionStart), 1000, 'Base table creation');
  let buildStatements: string[];
  if (factTableInfo.dataValuesColumn && factTableInfo.notesCodeColumn) {
    buildStatements = createFullCube(buildId, endRevision, factTableInfo, coreCubeViewSelectBuilder, viewConfig);
  } else {
    buildStatements = createCubeNoSources(buildId, endRevision, factTableInfo, coreCubeViewSelectBuilder);
  }
  fullBuildScript.push(buildStatements.join('\n'));
  buildStatements.push(
    pgformat('UPDATE %I.metadata SET %I = %L;', buildId, CubeMetaDataKeys.BuildScript, fullBuildScript.join('\n'))
  );

  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    await cubeDB.query(buildStatements.join('\n'));
  } catch (err) {
    logger.error(err, 'Something went wrong trying to build the cube');
    const metaDataUpdateStatements = [
      pgformat('UPDATE %I.metadata SET %I = %L;', buildId, CubeMetaDataKeys.BuildScript, fullBuildScript.join('\n')),
      pgformat('INSERT INTO %I.metadata SET value = %L WHERE key = %L', buildId, CubeMetaDataKeys.BuildResults, err)
    ];
    await cubeDB.query(metaDataUpdateStatements.join('\n'));
  } finally {
    cubeDB.release();
  }
};
