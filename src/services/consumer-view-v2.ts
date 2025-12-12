import Cursor from 'pg-cursor';
import { NextFunction, Response } from 'express';
import { format as csvFormat } from '@fast-csv/format';
import ExcelJS from 'exceljs';
import { ConsumerOutFormats } from '../enums/consumer-output-formats';
import { FilterRow } from '../interfaces/filter-row';
import { FilterTable } from '../interfaces/filter-table';
import { dbManager } from '../db/database-manager';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { logger } from '../utils/logger';
import { t } from 'i18next';
import { ConsumerOptions } from '../interfaces/consumer-options';
import { QueryStore } from '../entities/query-store';
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
import {
  checkAvailableViews,
  coreViewChooser,
  getColumns,
  getFilterTable,
  getFilterTableQuery,
  resolveDimensionToFactTableColumn,
  resolveFactColumnToDimension,
  resolveFactDescriptionToReference,
  transformHierarchy
} from '../utils/consumer';
import { Revision } from '../entities/dataset/revision';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';
import { DatasetRepository } from '../repositories/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';

const EXCEL_ROW_LIMIT = 1048500; // Excel Limit is 1,048,576 but removed 76 rows
const CURSOR_ROW_LIMIT = 500;

const DEFAULT_CONSUMER_OPTIONS: ConsumerOptions = { filters: [], options: { data_value_type: 'raw' } };

function generationOptionsHash(datasetId: string, options?: ConsumerOptions): string {
  if (!options) options = DEFAULT_CONSUMER_OPTIONS;
  return createHash('sha256')
    .update(`${datasetId}:${JSON.stringify(options)}`)
    .digest('hex');
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

async function processCursorToExcel(cursor: Cursor, queryStore: QueryStore, res: Response): Promise<void> {
  let rows = await cursor.read(CURSOR_ROW_LIMIT);
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${queryStore.datasetId}.xlsx`
  });
  res.flushHeaders();
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
    filename: `${queryStore.datasetId}.xlsx`,
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

async function processCursorToFrontend(
  cursor: Cursor,
  queryStore: QueryStore,
  pageNumber: number | undefined,
  pageSize: number | undefined,
  res: Response
): Promise<void> {
  const currentDataset = await DatasetRepository.getById(queryStore.datasetId);
  pageSize = pageSize ? pageSize : queryStore.totalLines;
  pageNumber = pageNumber ? pageNumber : 1;
  const page_info = {
    total_records: queryStore.totalLines,
    start_record: pageSize * pageNumber,
    end_record: pageSize * pageNumber + pageSize
  };
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/json',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${queryStore.datasetId}.json`
  });
  res.write('{');
  res.write(`"dataset": ${JSON.stringify(DatasetDTO.fromDataset(currentDataset))},`);
  res.write(`"current_page": ${pageNumber},`);
  res.write(`"page_info": ${JSON.stringify(page_info)},`);
  res.write(`"page_size": ${pageSize},`);
  res.write(`"total_pages": ${Math.max(1, Math.ceil(queryStore.totalLines / pageSize))},`);
  let rows = await cursor.read(CURSOR_ROW_LIMIT);
  res.write(`"headers": ${JSON.stringify(Object.keys(rows[0]))},`);
  res.write('"data": [');
  let firstRow = true;
  while (rows.length > 0) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    rows.forEach((row: any) => {
      if (firstRow) {
        firstRow = false;
      } else {
        res.write(',\n');
      }
      res.write(JSON.stringify(Object.values(row)));
    });
    rows = await cursor.read(CURSOR_ROW_LIMIT);
  }
  res.write(']');
  res.write(`}`);
  res.end();
}

const processCursor = async (
  cursor: Cursor,
  format: ConsumerOutFormats,
  queryStore: QueryStore,
  pageSize: number | undefined,
  pageNumber: number | undefined,
  res: Response
): Promise<void> => {
  switch (format) {
    case ConsumerOutFormats.Csv:
      await processCursorToCsv(cursor, queryStore, res);
      break;
    case ConsumerOutFormats.Json:
      await processCursorToJson(cursor, queryStore, res);
      break;
    case ConsumerOutFormats.View:
      await processCursorToFrontend(cursor, queryStore, pageNumber, pageSize, res);
      break;
    case ConsumerOutFormats.Filter:
      await processCursorToFrontendFilterView(cursor, res);
      break;
    case ConsumerOutFormats.Excel:
      await processCursorToExcel(cursor, queryStore, res);
      break;
    default:
      res.status(400).json({ error: 'Format not supported' });
  }
};

function createBaseQuery(
  revisionId: string,
  view: string,
  locale: string,
  columns: string[],
  filterTable: FilterRow[],
  request?: ConsumerOptions
): string {
  const refColumnPostfix = `_${t('column_headers.reference', { lng: locale })}`;
  const useRefValues = request?.options?.use_reference_values ? request?.options.use_reference_values : true;
  const useRawColumns = request?.options?.use_raw_column_names ? request?.options.use_raw_column_names : true;

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

function convertMapToObject(queryMap: Map<Locale, string>): Record<string, string> {
  const newObject: Record<string, string> = {};
  for (const [key, value] of queryMap) {
    newObject[key] = value;
  }
  return newObject;
}

async function generateQueryStore(
  datasetId: string,
  revisionId: string,
  consumerOptions: ConsumerOptions,
  consumerOptionsHash: string,
  queryStore: QueryStore | null
): Promise<QueryStore> {
  const dataValueType = consumerOptions.options?.data_value_type ? consumerOptions.options.data_value_type : undefined;
  const viewName = checkAvailableViews(dataValueType);
  const queryMap = new Map<Locale, string>();
  const filterTable = await getFilterTable(revisionId);

  for (const locale of SUPPORTED_LOCALES) {
    const lang = locale.split('-')[0];
    const coreView = await coreViewChooser(lang, revisionId);
    const selectColumns = await getColumns(revisionId, lang, viewName);
    const baseQuery = createBaseQuery(revisionId, coreView, locale, selectColumns, filterTable, consumerOptions);
    queryMap.set(locale, baseQuery);
  }

  const totals: { total_lines: number }[] = [];
  for (const locale of SUPPORTED_LOCALES) {
    const queryRunner = dbManager.getCubeDataSource().createQueryRunner();
    let query = queryMap.get(locale)!;
    query = pgformat('SELECT COUNT(*) as total_lines FROM (%s);', query);
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

  const nanoId = customAlphabet('1234567890abcdefghjklmnpqrstuvwxy_', 12);
  let queryStoreId = nanoId();
  let checkStore = QueryStore.findOneBy({ id: queryStoreId });
  while (checkStore) {
    logger.warn('Conflicting Nano ID found.  Regenerating...');
    queryStoreId = nanoId();
    checkStore = QueryStore.findOneBy({ id: queryStoreId });
  }

  const totalLines = totals[0].total_lines;
  for (const line of totals) {
    if (line.total_lines != totalLines) {
      logger.warn(`Base query for query store object ${queryStoreId} is producing inconsistent result`);
      break;
    }
  }

  const factTableToDimensionNameArr: FactTableToDimensionName[] = filterTable.map((row) => {
    return {
      fact_table_column: row.fact_table_column,
      dimension_name: row.dimension_name,
      language: row.language
    };
  });
  const factTableToDimensionNameSet = new Set<FactTableToDimensionName>(factTableToDimensionNameArr);

  if (!queryStore) {
    queryStore = new QueryStore();
    queryStore.id = queryStoreId;
    queryStore.requestObject = consumerOptions;
    queryStore.hash = consumerOptionsHash;
    queryStore.datasetId = datasetId;
    queryStore.revisionId = revisionId;
    queryStore.query = convertMapToObject(queryMap);
    queryStore.totalLines = totalLines;
    queryStore.columnMapping = Array.from(factTableToDimensionNameSet);
  } else {
    queryStore.query = convertMapToObject(queryMap);
    queryStore.revisionId = revisionId;
    queryStore.totalLines = totalLines;
    queryStore.columnMapping = Array.from(factTableToDimensionNameSet);
  }
  return queryStore.save();
}

export const createQueryStoreEntry = async (
  res: Response,
  next: NextFunction,
  dataset: Dataset,
  revision: Revision,
  consumerOptions: ConsumerOptions
): Promise<void> => {
  // create hash from filtering options
  const consumerOptionsHash = generationOptionsHash(dataset.id, consumerOptions);
  let queryStore = await QueryStore.findOneBy({ hash: consumerOptionsHash });
  if (queryStore && queryStore.revisionId === revision.id) {
    res.redirect(`/v2/${dataset.id}/data/${queryStore.id}`);
    return;
  }

  try {
    queryStore = await generateQueryStore(dataset.id, revision.id, consumerOptions, consumerOptionsHash, queryStore);
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
  res.redirect(`/v2/${dataset.id}/data/${queryStore.id}`);
};

async function processQueryStore(
  res: Response,
  next: NextFunction,
  locale: Locale,
  queryStore: QueryStore,
  page?: number,
  pageSize?: number,
  format?: ConsumerOutFormats,
  sort?: string[]
): Promise<void> {
  let query = queryStore.query[`${locale}-GB`];
  if (sort && sort.length > 0) {
    const sortBy: string[] = [];
    for (const sortOption of sort) {
      const colName = sortOption.split('|')[0];
      const directionStr = sortOption.split('|')[1].toUpperCase();
      if (directionStr !== 'ASC' && directionStr !== 'DESC') {
        next(new BadRequestException(`Sort directions must be ASC or DESC`));
        return;
      }
      let confirmedCol: string;
      let colType: 'fact' | 'dimension';
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
    await processCursor(cursor, format ? format : ConsumerOutFormats.Json, queryStore, page, pageSize, res);
  } catch (error) {
    logger.warn(error, 'Something went wrong while trying to process the database cursor');
    next(new UnknownException());
  } finally {
    void cubeDBConn.release();
  }
}

export const sendConsumerDataToUserNoFilter = async (
  res: Response,
  next: NextFunction,
  locale: Locale,
  dataset: Dataset,
  page?: number,
  pageSize?: number,
  format?: ConsumerOutFormats,
  sort?: string[]
): Promise<void> => {
  let queryStore = await QueryStore.findOneBy({ hash: generationOptionsHash(dataset.id) });
  if (!queryStore) {
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
      DEFAULT_CONSUMER_OPTIONS,
      generationOptionsHash(dataset.id),
      null
    );
  }
  await processQueryStore(res, next, locale, queryStore, page, pageSize, format, sort);
};

export const sendConsumerDataToUser = async (
  res: Response,
  next: NextFunction,
  locale: Locale,
  dataset: Dataset,
  filterId: string,
  page?: number,
  pageSize?: number,
  format?: ConsumerOutFormats,
  sort?: string[]
): Promise<void> => {
  let queryStore: QueryStore | null = null;
  queryStore = await QueryStore.findOneBy({ id: filterId });

  if (!queryStore) {
    logger.trace(`Query stroe object with ID ${filterId} Not Found`);
    next(new NotFoundException());
    return;
  }

  if (queryStore && queryStore.revisionId !== dataset.publishedRevisionId) {
    logger.trace('Query store object is out of step with revision.  Regenerating.');
    queryStore = await generateQueryStore(
      dataset.id,
      dataset.publishedRevisionId!,
      queryStore.requestObject,
      generationOptionsHash(dataset.id, queryStore.requestObject),
      null
    );
  }

  await processQueryStore(res, next, locale, queryStore, page, pageSize, format, sort);
};

export const sendFilterTableToUser = async (
  res: Response,
  next: NextFunction,
  locale: Locale,
  revision: Revision,
  format?: ConsumerOutFormats
): Promise<void> => {
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  const cursor = cubeDBConn.query(new Cursor(getFilterTableQuery(revision.id, locale)));
  const queryStore = new QueryStore();
  queryStore.datasetId = revision.datasetId;
  queryStore.revisionId = revision.id;
  try {
    await processCursor(cursor, format ? format : ConsumerOutFormats.Json, queryStore, undefined, undefined, res);
  } catch (error) {
    logger.warn(error, 'Something went wrong while trying to process the database cursor');
    next(new UnknownException());
  } finally {
    void cubeDBConn.release();
  }
};
