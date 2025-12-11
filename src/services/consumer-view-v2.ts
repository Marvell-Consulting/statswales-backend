import Cursor from 'pg-cursor';
import { NextFunction, Response } from 'express';
import { format as csvFormat } from '@fast-csv/format';
import ExcelJS from 'exceljs';
import { ConsumerOutFormats } from '../enums/consumer-output-formats';
import { FilterRow } from '../interfaces/filter-row';
import { FilterTable } from '../interfaces/filter-table';
import { dbManager } from '../db/database-manager';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { CORE_VIEW_NAME } from './cube-builder';
import { logger } from '../utils/logger';
import { t } from 'i18next';
import { ConsumerOptions } from '../interfaces/consumer-options';
import { QueryStore } from '../entities/query-store';
import cubeConfig from '../config/cube-view.json';
import { Locale } from '../enums/locale';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { createHash } from 'node:crypto';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { UnknownException } from '../exceptions/unknown.exception';
import { customAlphabet } from 'nanoid/non-secure';
import { Dataset } from '../entities/dataset/dataset';
import { NotFoundException } from '../exceptions/not-found.exception';
import { DEFAULT_PAGE_SIZE } from '../utils/page-defaults';
import { PoolClient } from 'pg';

const EXCEL_ROW_LIMIT = 1048500; // Excel Limit is 1048576 but removed 76 rows
const CURSOR_ROW_LIMIT = 500;

const DEFAULT_CONSUMER_OPTIONS: ConsumerOptions = { filters: [], options: { data_value_type: 'raw' } };
function generationOptionsHash(datasetId: string, options?: ConsumerOptions) {
  if (!options) options = DEFAULT_CONSUMER_OPTIONS;
  return createHash('sha256')
    .update(`${datasetId}:${JSON.stringify(options)}`)
    .digest('hex');
}

interface FactTableToDimensionName {
  fact_table_column: string;
  dimension_name: string;
  language: string;
}

async function processCursorToCsv(cursor: Cursor, queryStore: QueryStore, res: Response): Promise<void> {
  let rows = await cursor.read(CURSOR_ROW_LIMIT);
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'text/csv',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${queryStore.datasetId}.csv`
  });
  if (rows.length > 0) {
    const stream = csvFormat({ delimiter: ',', headers: true });
    stream.pipe(res);
    while (rows.length > 0) {
      rows.map((row: unknown) => {
        stream.write(row);
      });
      rows = await cursor.read(CURSOR_ROW_LIMIT);
    }
  } else {
    res.write('\n');
  }
  res.end();
}

async function processCursorToExcel(cursor: Cursor, filename: string, res: Response): Promise<void> {
  let rows = await cursor.read(CURSOR_ROW_LIMIT);
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${filename}.xlsx`
  });
  res.flushHeaders();
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
    filename: `${filename}.xlsx`,
    useStyles: true,
    useSharedStrings: true,
    stream: res
  });
  let sheetCount = 1;
  let totalRows = 0;
  let worksheet = workbook.addWorksheet(`Sheet-${sheetCount}`);
  if (rows.length > 0) {
    worksheet.addRow(Object.keys(rows[0]));
    while (rows.length > 0) {
      for (const row of rows) {
        if (row === null) break;
        const data = Object.values(row).map((val) => (isNaN(Number(val)) ? val : Number(val)));
        worksheet.addRow(Object.values(data)).commit();
      }
      totalRows += CURSOR_ROW_LIMIT;
      if (totalRows > EXCEL_ROW_LIMIT) {
        worksheet.commit();
        sheetCount++;
        totalRows = 0;
        worksheet = workbook.addWorksheet(`Sheet-${sheetCount}`);
      }
      rows = await cursor.read(CURSOR_ROW_LIMIT);
    }
  }
  worksheet.commit();
  await workbook.commit();
}

async function processCursorToJson(cursor: Cursor, queryStore: QueryStore, res: Response): Promise<void> {
  let rows = await cursor.read(CURSOR_ROW_LIMIT);
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/json',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${queryStore.datasetId}.json`
  });
  res.write('[');
  let firstRow = true;
  while (rows.length > 0) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    rows.forEach((row: any) => {
      if (firstRow) {
        firstRow = false;
      } else {
        res.write(',\n');
      }
      res.write(JSON.stringify(row));
    });
    rows = await cursor.read(CURSOR_ROW_LIMIT);
  }
  res.write(']');
  res.end();
}

async function processCursorToFrontendFilterView(cursor: Cursor, res: Response): Promise<void> {
  let rows: FilterRow[] = await cursor.read(CURSOR_ROW_LIMIT);
  const columnData = new Map<string, FilterRow[]>();
  while (rows.length > 0) {
    rows.forEach((row: FilterRow) => {
      let data = columnData.get(row.fact_table_column);
      if (data) {
        data.push(row);
      } else {
        data = [row];
      }
      columnData.set(row.fact_table_column, data);
    });
    rows = await cursor.read(CURSOR_ROW_LIMIT);
  }
  const filterData: FilterTable[] = [];
  for (const col of columnData.keys()) {
    const data = columnData.get(col);
    if (!data) {
      continue;
    }
    const hierarchy = transformHierarchy(data[0].fact_table_column, data[0].dimension_name, data);
    filterData.push(hierarchy);
  }
  res.write(filterData);
}

async function processCursorToFrontend(cursor: Cursor, queryStore: QueryStore, res: Response): Promise<void> {

}

const processCursor = (
  cursor: Cursor,
  format: ConsumerOutFormats,
  queryStore: QueryStore,
  res: Response
): void => {
  switch (format) {
    case ConsumerOutFormats.Csv:
      void processCursorToCsv(cursor, queryStore, res);
      break;
    case ConsumerOutFormats.Json:
      void processCursorToJson(cursor, queryStore, res);
      break;
    case ConsumerOutFormats.View:
      void processCursorToFrontend(cursor, queryStore, res);
      break;
    case ConsumerOutFormats.Filter:
      void processCursorToFrontendFilterView(cursor, queryStore, res);
      break;
    case ConsumerOutFormats.Excel:
      void processCursorToExcel(cursor, queryStore, res);
      break;
    default:
      res.status(400).json({ error: 'Format not supported' });
  }
};

function resolveDimensionToFactTableColumn(
  columnName: string,
  factTableToDimensionNames: FactTableToDimensionName[]
): string {
  const col = factTableToDimensionNames.find((col) => columnName.toLowerCase() === col.dimension_name.toLowerCase());
  if (!col) {
    throw new Error('Column not found');
  }
  return col.fact_table_column;
}

function resolveFactColumnToDimension(
  columnName: string,
  locale: string,
  filterTable: FactTableToDimensionName[]
): string {
  const col = filterTable.find(
    (col) =>
      col.fact_table_column.toLowerCase() === columnName.toLowerCase() &&
      col.language.toLowerCase() === locale.toLowerCase()
  );
  if (!col) {
    throw new Error('Column not found');
  }
  return col.dimension_name;
}

function resolveFactDescriptionToReference(referenceValues: string[], filterTable: FilterRow[]): string[] {
  const resolvedValues: string[] = [];
  for (const val of referenceValues) {
    const resVal = filterTable.find((row) => row.description.toLowerCase() === val.toLowerCase());
    if (resVal) resolvedValues.push(resVal?.reference);
    else throw new Error('Value not found');
  }
  return resolvedValues;
}

function createBaseQuery(
  revisionId: string,
  view: string,
  locale: string,
  columns: string[],
  filterTable: FilterRow[],
  request?: ConsumerOptions
): string {
  const refColumnPostfix = `_${t('column_headers.reference', { lng: locale })}`;
  const useRefValues = request?.options.use_reference_values ? request?.options.use_reference_values : true;
  const useRawColumns = request?.options.use_raw_column_names ? request?.options.use_raw_column_names : true;

  const filters: string[] = [];
  if (request?.filters && request?.filters.length > 0) {
    for (const filter of request.filters) {
      let colName = Object.keys(filter)[0];
      let filterValues = Object.values(filter)[0];
      if (useRawColumns) {
        colName = resolveFactColumnToDimension(colName, locale, filterTable);
      } else {
        resolveDimensionToFactTableColumn(colName, filterTable);
      }
      if (!useRefValues) {
        const filterTableValues = filterTable.filter(
          (row) => row.fact_table_column.toLowerCase() === colName.toLowerCase()
        );
        filterValues = resolveFactDescriptionToReference(filterValues, filterTableValues);
      }
      colName = `${colName}-${refColumnPostfix}`;
      filters.push(pgformat('%I in (%L)', colName, filterValues));
    }
  }

  if (columns[0] === '*') {
    return pgformat(
      'SELECT * FROM %I.%I %s %s',
      revisionId,
      view,
      filters ? `WHERE ${filters.join(' AND ')}` : ''
    );
  } else {
    return pgformat(
      'SELECT %s FROM %I.%I %s %s',
      columns.join(', '),
      revisionId,
      view,
      filters ? `WHERE ${filters.join(' AND ')}` : ''
    );
  }
}

async function coreViewChooser(lang: string, revisionId: string): Promise<string> {
  let availableMaterializedView: { matviewname: string }[];
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    availableMaterializedView = await cubeDB.query(
      pgformat(
        `SELECT * FROM pg_matviews WHERE matviewname = %L AND schemaname = %L;`,
        `${CORE_VIEW_NAME}_mat_${lang}`,
        revisionId
      )
    );
  } catch (err) {
    logger.error(err, 'Unable to query available views from postgres');
    throw err;
  } finally {
    void cubeDB.release();
  }

  if (availableMaterializedView.length > 0) {
    return `${CORE_VIEW_NAME}_mat_${lang}`;
  } else {
    return `${CORE_VIEW_NAME}_${lang}`;
  }
}

function checkAvailableViews(view: string | undefined): string {
  if (!view) return 'raw';
  if (view === 'with_note_codes') view = 'frontend';
  const foundView = cubeConfig.find((config) => config.name === view);
  if (!foundView) return 'raw';
  else return view;
}

async function getColumns(revisionId: string, lang: string, view: string): Promise<string[]> {
  let columnsMetadata: { value: string }[];
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    columnsMetadata = await cubeDB.query(
      pgformat(`SELECT value FROM %I.metadata WHERE key = %L`, revisionId, `${view}_${lang}_columns`)
    );
  } catch (err) {
    logger.error(err, 'Unable to get columns from cube metadata table');
    throw err;
  } finally {
    void cubeDB.release();
  }

  let columns = ['*'];
  if (columnsMetadata.length > 0) {
    columns = JSON.parse(columnsMetadata[0].value) as string[];
  }
  return columns;
}

async function generateQueryStore(
  datasetId: string,
  revisionId: string,
  consumerOptions: ConsumerOptions,
  consumerOptionsHash: string,
  queryStore: QueryStore | null
): Promise<QueryStore> {
  const viewName = checkAvailableViews(consumerOptions.options.data_value_type);
  const queryMap = new Map<Locale, string>();
  const filterTable = await getFactTableToDimensionName(revisionId);

  for (const locale of SUPPORTED_LOCALES) {
    const lang = locale.split('-')[0];
    const coreView = await coreViewChooser(lang, revisionId);
    const selectColumns = await getColumns(revisionId, lang, viewName);
    const baseQuery = createBaseQuery(revisionId, coreView, locale, selectColumns, filterTable, consumerOptions);
    queryMap.set(locale, baseQuery);
  }

  let totals: { total_lines: number }[] = [];
  for (const locale of SUPPORTED_LOCALES) {
    const queryRunner = dbManager.getCubeDataSource().createQueryRunner();
    let query = queryMap.get(locale)!;
    query = pgformat('SELECT COUNT(*) as total_lines FROM (%s);');
    try {
      logger.trace(`Testing query and getting a line count: ${query}`);
      totals.push(...(await queryRunner.query(query)));
    } catch (err) {
      logger.error(err, 'Failed to run generated base query');
      throw err;
    } finally {
      void queryRunner.release();
    }
  }

  const queryStoreId = customAlphabet('1234567890abcdef-', 12)();

  const totalLines = totals[0].total_lines;
  for (const line of totals) {
    if (line.total_lines != totalLines) {
      logger.warn(`Base query for query store object ${queryStoreId} is producing inconsistent result`);
      break;
    }
  }

  const factTableToDimensionNameSet = new Set<FactTableToDimensionName>();
  for (const row of filterTable) {
    factTableToDimensionNameSet.add({
      fact_table_column: row.fact_table_column,
      dimension_name: row.dimension_name,
      language: row.language
    });
  }

  if (!queryStore) {
    queryStore = new QueryStore();
    queryStore.id = queryStoreId;
    queryStore.hash = consumerOptionsHash;
    queryStore.datasetId = datasetId;
    queryStore.revisionId = revisionId;
    queryStore.query = queryMap;
    queryStore.totalLines = totalLines;
    queryStore.columnMapping = Array.from(factTableToDimensionNameSet);
  } else {
    queryStore.query = queryMap;
    queryStore.revisionId = revisionId;
    queryStore.totalLines = totalLines;
    queryStore.columnMapping = Array.from(factTableToDimensionNameSet);
  }
  return queryStore.save();
}

export const createStreamingFilteredView = async (
  res: Response,
  next: NextFunction,
  datasetId: string,
  revisionId: string,
  consumerOptions: ConsumerOptions
): Promise<void> => {
  // create hash from filtering options
  const consumerOptionsHash = generationOptionsHash(datasetId, consumerOptions);
  let queryStore = await QueryStore.findOneBy({ hash: consumerOptionsHash });
  if (queryStore && queryStore.revisionId === revisionId) {
    res.redirect(`/dataset/${datasetId}/data/${queryStore.id}`);
    return;
  }

  try {
    queryStore = await generateQueryStore(datasetId, revisionId, consumerOptions, consumerOptionsHash, queryStore);
  } catch (err) {
    if ((err as Error).message === 'No column found') {
      logger.debug(err, 'An error occurred trying to create the base query');
      next(new BadRequestException('errors.column_no_found'));
    } else if ((err as Error).message === 'Value not found') {
      logger.debug(err, 'An error occurred trying to create the base query');
      next(new BadRequestException('errors.value_not_found'));
    } else {
      logger.error(err, 'An unknown error occurred trying to produce the base queries');
      next(new UnknownException('errors.unknown_error'));
    }
    return;
  }
  res.redirect(`/dataset/${datasetId}/data/${queryStore.id}`);
};

export const sendConsumerDataToUser = async (
  res: Response,
  next: NextFunction,
  locale: Locale,
  dataset: Dataset,
  filter_id?: string,
  page?: number,
  pageSize?: number,
  format?: ConsumerOutFormats,
  sort?: string[]
): Promise<void> => {
  let queryStore: QueryStore | null = null;
  if (!filter_id) {
    queryStore = await QueryStore.findOneBy({ hash: generationOptionsHash(dataset.id) });
  } else {
    queryStore = await QueryStore.findOneBy({ id: filter_id });
  }

  if (!queryStore && !filter_id) {
    queryStore = await generateQueryStore(
      dataset.id,
      dataset.publishedRevisionId!,
      DEFAULT_CONSUMER_OPTIONS,
      generationOptionsHash(dataset.id),
      null
    );
  } else if (queryStore && queryStore.revisionId !== dataset.publishedRevisionId) {
    queryStore = await generateQueryStore(
      dataset.id,
      dataset.publishedRevisionId!,
      queryStore.requestObject,
      generationOptionsHash(dataset.id, queryStore.requestObject),
      null
    );
  } else {
    next(new NotFoundException());
    return;
  }

  let query = queryStore.query.get(locale)!;

  if (sort && sort.length > 0) {
    const sortBy: string[] = []
    for (const sortOption of sort) {
      const colName = sortOption.split('|')[0];
      const directionStr = sortOption.split('|')[1].toUpperCase();
      if (directionStr !== 'ASC' && directionStr !== 'DESC') {
        next(new BadRequestException(`Sort directions must be ASC or DESC`));
        return;
      }
      let confirmedCol: string;
      let colType: 'fact' | 'dimension'
      if (resolveFactColumnToDimension(colName, 'en-GB', queryStore.columnMapping)) {
        colType = 'fact';
      } else if (resolveDimensionToFactTableColumn(colName, queryStore.columnMapping)) {
        colType = 'dimension';
      } else {
        next(new BadRequestException(`Sort column ${colName} not found`));
        return;
      }
      if (colType === 'dimension') {
        confirmedCol = colName;
      } else {
        confirmedCol = resolveFactColumnToDimension(colName, locale, queryStore.columnMapping);
      }
      sortBy.push(pgformat('%I %s', confirmedCol, directionStr));
    }
    query = pgformat('%s ORDER BY %s', query, sortBy.join(', '));
  }

  if (page) {
    pageSize = pageSize || DEFAULT_PAGE_SIZE;
    if (page > queryStore.totalLines / pageSize) {
      next(new BadRequestException('errors.page_size_to_high'));
      return;
    }
    query = pgformat(`%s LIMIT %L OFFSET %L;`, pageSize, page);
  }

  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  const cursor = cubeDBConn.query(new Cursor(query));
  try {
    await processCursor(cursor, format ? format : ConsumerOutFormats.Json, queryStore, res);
  }

};
