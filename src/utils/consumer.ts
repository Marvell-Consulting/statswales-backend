import { FilterTable, FilterValues } from '../interfaces/filter-table';
import { FilterRow } from '../interfaces/filter-row';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';
import { dbManager } from '../db/database-manager';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { CORE_VIEW_NAME } from '../services/cube-builder';
import { logger } from './logger';
import cubeConfig from '../config/cube-view.json';
import { Locale } from '../enums/locale';
import { t } from 'i18next';
import { DataOptionsDTO } from '../dtos/data-options-dto';

export function transformHierarchy(factTableColumn: string, columnName: string, input: FilterRow[]): FilterTable {
  const nodeMap = new Map<string, FilterValues>(); // reference → node
  const childrenMap = new Map<string, FilterValues[]>(); // parentRef → children
  const roots: FilterValues[] = [];

  // First, create node instances for all inputs
  for (const row of input) {
    const node: FilterValues = {
      reference: row.reference,
      description: row.description
    };
    nodeMap.set(row.reference, node);

    // Queue up children by parent ref
    if (row.hierarchy) {
      if (!childrenMap.has(row.hierarchy)) {
        childrenMap.set(row.hierarchy, []);
      }
      childrenMap.get(row.hierarchy)!.push(node);
    }
  }

  // Link children to their parents
  for (const [parentRef, children] of childrenMap) {
    const parentNode = nodeMap.get(parentRef);
    if (parentNode) {
      parentNode.children = parentNode.children || [];
      parentNode.children.push(...children);
    }
  }

  // Find root nodes: those that are NOT a child of anyone
  const childRefs = new Set<string>();
  for (const children of childrenMap.values()) {
    for (const child of children) {
      childRefs.add(child.reference);
    }
  }

  for (const [ref, node] of nodeMap.entries()) {
    if (!childRefs.has(ref)) {
      roots.push(node);
    }
  }
  return {
    factTableColumn: factTableColumn,
    columnName: columnName,
    values: roots
  };
}

export function resolveDimensionToFactTableColumn(
  columnName: string,
  factTableToDimensionNames: FactTableToDimensionName[]
): string {
  switch (columnName.toLowerCase()) {
    case 'data_values':
    case 'data':
    case t('column_headers.data_values', { lng: 'en-GB' }).toLowerCase():
    case t('column_headers.data_values', { lng: 'cy-GB' }).toLowerCase():
      return 'data_values';
  }
  const col = factTableToDimensionNames.find(
    (col) => columnName.toLowerCase().trim() === col.dimension_name.toLowerCase()
  );
  if (!col) {
    throw new Error('Column not found');
  }
  return col.fact_table_column;
}

export function resolveFactColumnToDimension(
  columnName: string,
  locale: string,
  filterTable: FactTableToDimensionName[]
): string {
  logger.debug(`Resolving column: ${columnName} with locale: ${locale}`);
  switch (columnName.toLowerCase().trim()) {
    case 'data_values':
    case 'data':
    case t('column_headers.data_values').toLowerCase():
      return t('column_headers.data_values', { lng: locale });
  }
  const col = filterTable.find(
    (col) =>
      col.fact_table_column.toLowerCase() === columnName.toLowerCase().trim() &&
      col.language.toLowerCase().includes(locale.toLowerCase())
  );
  if (!col) {
    throw new Error(`Column not found: ${columnName}`);
  }
  return col.dimension_name;
}

export function resolveFactDescriptionToReference(referenceValues: string[], filterTable: FilterRow[]): string[] {
  const resolvedValues: string[] = [];
  for (const val of referenceValues) {
    const resVal = filterTable.find((row) => row.description.toLowerCase() === val.toLowerCase());
    if (resVal) resolvedValues.push(resVal?.reference);
    else throw new Error('Value not found');
  }
  return resolvedValues;
}

export async function coreViewChooser(lang: string, revisionId: string): Promise<string> {
  logger.debug(`Checking available views for revision ${revisionId} and language ${lang}...`);
  let availableMaterializedView: { matviewname: string }[];
  const cubeDataSource = dbManager.getCubeDataSource();

  try {
    availableMaterializedView = await cubeDataSource.query(
      pgformat(
        `SELECT * FROM pg_matviews WHERE matviewname = %L AND schemaname = %L;`,
        `${CORE_VIEW_NAME}_mat_${lang}`,
        revisionId
      )
    );
  } catch (err) {
    logger.error(err, 'Unable to query available views from postgres');
    throw err;
  }

  if (availableMaterializedView.length > 0) {
    return `${CORE_VIEW_NAME}_mat_${lang}`;
  } else {
    return `${CORE_VIEW_NAME}_${lang}`;
  }
}

export function checkAvailableViews(view: string | undefined): string {
  if (!view) return 'raw';
  if (view === 'with_note_codes') view = 'frontend';
  const foundView = cubeConfig.find((config) => config.name === view);
  if (!foundView) return 'raw';
  else return view;
}

export async function getColumns(revisionId: string, lang: string, view: string): Promise<string[]> {
  logger.debug(`Getting columns for revision ${revisionId}, view ${view}, language ${lang}...`);
  let columnsMetadata: { value: string }[];
  const cubeDataSource = dbManager.getCubeDataSource();

  try {
    columnsMetadata = await cubeDataSource.query(
      pgformat(`SELECT value FROM %I.metadata WHERE key = %L`, revisionId, `${view}_${lang}_columns`)
    );
  } catch (err) {
    logger.error(err, 'Unable to get columns from cube metadata table');
    throw err;
  }

  let columns = ['*'];
  if (columnsMetadata.length > 0) {
    columns = JSON.parse(columnsMetadata[0].value) as string[];
  }
  return columns;
}

export async function getFilterTable(revisionId: string): Promise<FilterRow[]> {
  const cubeDataSource = dbManager.getCubeDataSource();
  try {
    const result = await cubeDataSource.query(getFilterTableQuery(revisionId));
    return result as FilterRow[];
  } catch (err) {
    logger.error(err, `Something went wrong trying to get the filter table from cube ${revisionId}`);
    throw err;
  }
}

export function getFilterTableQuery(revisionId: string, locale?: Locale): string {
  if (!locale) {
    return pgformat(
      'SELECT reference, language, fact_table_column, dimension_name, description, hierarchy FROM %I.filter_table;',
      revisionId
    );
  }
  return pgformat(
    'SELECT reference, language, fact_table_column, dimension_name, description, hierarchy FROM %I.filter_table WHERE language LIKE %L;',
    revisionId,
    `${locale.toLowerCase().split('-')[0]}%`
  );
}

export function createBaseQuery(
  revisionId: string,
  view: string,
  locale: string,
  columns: string[],
  filterTable: FilterRow[],
  dataOptions?: DataOptionsDTO
): string {
  logger.debug(`Creating base query for revision ${revisionId}, view ${view}, locale ${locale}...`);

  const refColumnPostfix = `_${t('column_headers.reference', { lng: locale })}`;
  const useRefValues = dataOptions?.options?.use_reference_values ?? true;
  const useRawColumns = dataOptions?.options?.use_raw_column_names ?? true;
  const filters: string[] = [];

  if (dataOptions?.filters && dataOptions?.filters.length > 0) {
    for (const filter of dataOptions.filters) {
      let colName = Object.keys(filter)[0];
      let filterValues = Object.values(filter)[0];
      if (useRawColumns) {
        colName = resolveFactColumnToDimension(colName, locale, filterTable);
      } else {
        colName = resolveDimensionToFactTableColumn(colName, filterTable);
      }
      if (!useRefValues) {
        const filterTableValues = filterTable.filter(
          (row) => row.fact_table_column.toLowerCase() === colName.toLowerCase()
        );
        filterValues = resolveFactDescriptionToReference(filterValues, filterTableValues);
      }
      colName = `${colName}${refColumnPostfix}`;
      filters.push(pgformat('%I in (%L)', colName, filterValues));
    }
  }

  if (columns[0] === '*') {
    return pgformat(
      'SELECT * FROM %I.%I %s',
      revisionId,
      view,
      filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : ''
    );
  } else {
    return pgformat(
      'SELECT %s FROM %I.%I %s',
      columns.join(', '),
      revisionId,
      view,
      filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : ''
    );
  }
}
