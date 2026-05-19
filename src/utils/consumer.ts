import { FilterTable, FilterValues } from '../interfaces/filter-table';
import { FilterRow } from '../interfaces/filter-row';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';
import { dbManager } from '../db/database-manager';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { CORE_VIEW_NAME, FILTER_TABLE_NAME, METADATA_TABLE_NAME } from '../services/cube-builder';
import { logger } from './logger';
import cubeConfig from '../config/cube-view.json';
import { Locale } from '../enums/locale';
import { t } from 'i18next';
import { DataOptionsDTO } from '../dtos/data-options-dto';
import { CubeMetaDataKeys } from '../enums/cube-metadata-keys';
import { DataSource } from 'typeorm';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionType } from '../enums/dimension-type';

export function flattenHierarchy(nodes: FilterValues[]): FilterValues[] {
  return nodes.flatMap((node) => [node, ...(node.children ? flattenHierarchy(node.children) : [])]);
}

export function transformHierarchy(factTableColumn: string, columnName: string, input: FilterRow[]): FilterTable {
  const nodeMap = new Map<string, FilterValues>(); // reference → node
  const childrenMap = new Map<string, FilterValues[]>(); // parentRef → children
  const roots: FilterValues[] = [];

  // First, create node instances for all inputs
  for (const row of input) {
    const node: FilterValues = {
      reference: row.reference,
      description: row.description,
      count: row.reference_count != null ? row.reference_count : undefined
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

/**
 * Parses a filter_table.sort_order value (stored as TEXT) into a number. Returns null when the
 * value is absent or non-numeric, so callers can fall back to alphabetical ordering.
 */
function parseSortOrder(value?: string | null): number | null {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Returns the set of fact-table columns backed by a date dimension. Date filter values are
 * ordered descending (newest first), so the read path needs to know which columns are dates.
 *
 * Only Date and DatePeriod are treated as descending — this must stay in sync with the
 * descending branch of setupLookupTableDimension() in cube-builder.ts (the `sort_order DESC`
 * insert). DimensionType also defines TimePeriod and Time, but the cube builder has no
 * processing path for them yet, so they are deliberately excluded here.
 */
export function dateColumnsFromDimensions(dimensions: Dimension[]): Set<string> {
  return new Set(
    dimensions
      .filter((dim) => dim.type === DimensionType.Date || dim.type === DimensionType.DatePeriod)
      .map((dim) => dim.factTableColumn)
  );
}

/**
 * Orders the filter values for a single column:
 *  - values with a sort order from the lookup table are ordered by it numerically;
 *  - values with no sort order fall back to ascending alphabetical order of the description;
 *  - date dimensions are reversed so the most recent period comes first.
 *
 * filter_table.sort_order is TEXT, so it must be compared numerically here rather than in SQL
 * (a lexical sort would place "10" before "2"). Sorting the flat row list before
 * transformHierarchy() is enough — that function preserves input order at every hierarchy level.
 */
export function sortFilterRows(rows: FilterRow[], isDateDimension: boolean): FilterRow[] {
  const sorted = [...rows].sort((rowA, rowB) => {
    const sortA = parseSortOrder(rowA.sort_order);
    const sortB = parseSortOrder(rowB.sort_order);

    if (sortA !== null && sortB !== null) {
      if (sortA !== sortB) return sortA - sortB;
    } else if (sortA !== null) {
      return -1; // a value with a sort order ranks ahead of one without
    } else if (sortB !== null) {
      return 1;
    }

    const byDescription = (rowA.description ?? '').localeCompare(rowB.description ?? '');
    if (byDescription !== 0) return byDescription;
    return (rowA.reference ?? '').localeCompare(rowB.reference ?? '');
  });

  return isDateDimension ? sorted.reverse() : sorted;
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
    (col) => columnName.toLowerCase().trim() === col.dimension_name.toLowerCase().trim()
  );
  if (!col) {
    logger.debug(`${columnName} Column not found`);
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
  logger.debug(`Getting columns for revision: '${revisionId}', view: '${view}', language: '${lang}'...`);
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
  let filterTableVersion = 1;
  let cubeDataSource: DataSource;
  try {
    cubeDataSource = dbManager.getCubeDataSource();
    const filterTableVersionRes: { value: string }[] = await cubeDataSource.query(
      pgformat(
        'SELECT value FROM %I.%I WHERE key = %L',
        revisionId,
        METADATA_TABLE_NAME,
        CubeMetaDataKeys.FilterTableVersion
      )
    );
    if (filterTableVersionRes.length > 0) {
      const parsedFilterTableVersion = Number.parseInt(filterTableVersionRes[0].value, 10);
      if (Number.isInteger(parsedFilterTableVersion)) {
        filterTableVersion = parsedFilterTableVersion;
      }
    }
  } catch (err) {
    logger.warn(err, 'Unable to query cubes metadata');
  }

  try {
    cubeDataSource = dbManager.getCubeDataSource();
    const result = await cubeDataSource.query(getFilterTableQuery(revisionId, filterTableVersion));
    return result as FilterRow[];
  } catch (err) {
    logger.error(err, `Something went wrong trying to get the filter table from cube ${revisionId}`);
    throw err;
  }
}

export function getFilterTableQuery(revisionId: string, version: number, locale?: Locale): string {
  // reference_count defaults to 1 otherwise we'll disable all filters on the frontend
  let columns =
    'reference, language, fact_table_column, dimension_name, description, NULL as sort_order, hierarchy, CAST(1 as BIGINT) as reference_count';
  if (version > 1) {
    columns =
      'reference, language, fact_table_column, dimension_name, description, sort_order, hierarchy, reference_count';
  }
  // Row ordering is applied per column in sortFilterRows(), not in SQL: sort_order is stored as
  // TEXT and needs numeric parsing plus a fallback for missing/non-numeric values, and date
  // dimensions need a descending direction — all kept in one shared comparator across the
  // preview, v1 and v2 read paths.
  if (!locale) {
    return pgformat('SELECT %s FROM %I.%I;', columns, revisionId, FILTER_TABLE_NAME);
  }
  return pgformat(
    'SELECT %s FROM %I.%I WHERE language LIKE %L;',
    columns,
    revisionId,
    FILTER_TABLE_NAME,
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

  if (columns.length === 0 || columns[0] === '*') {
    return pgformat(
      'SELECT * FROM %I.%I %s',
      revisionId,
      view,
      filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : ''
    );
  }

  return pgformat(
    'SELECT %s FROM %I.%I %s',
    columns.join(', '),
    revisionId,
    view,
    filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : ''
  );
}
