import { Response } from 'express';
import ExcelJS from 'exceljs';
import { PoolClient, QueryResult } from 'pg';
import Cursor from 'pg-cursor';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { format as csvFormat } from '@fast-csv/format';

import { validateParams } from '../validators/preview-validator';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DatasetDTO } from '../dtos/dataset-dto';
import { Dataset } from '../entities/dataset/dataset';
import { logger } from '../utils/logger';
import { DatasetRepository } from '../repositories/dataset';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { FilterInterface } from '../interfaces/filterInterface';
import { dbManager } from '../db/database-manager';
import { CORE_VIEW_NAME } from './cube-builder';
import { getColumnHeaders } from '../utils/column-headers';
import { t } from 'i18next';
import cubeConfig from '../config/cube-view.json';
import { ConsumerOutFormats } from '../enums/consumer-output-formats';
import { ConsumerRequestBody } from '../dtos/consumer/consumer-request-body';

const EXCEL_ROW_LIMIT = 1048500; // Excel Limit is 1048576 but removed 76 rows
const CURSOR_ROW_LIMIT = 500;

interface FilterValues {
  reference: string;
  description: string;
  children?: FilterValues[];
}

interface FilterTable {
  columnName: string;
  factTableColumn: string;
  values: FilterValues[];
}

interface FilterRow {
  reference: string;
  language: string;
  fact_table_column: string;
  dimension_name: string;
  description: string;
  hierarchy: string;
}

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

async function processCursorToCsv(cursor: Cursor, filename: string, res: Response): Promise<void> {
  let rows = await cursor.read(CURSOR_ROW_LIMIT);
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'text/csv',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${filename}.csv`
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

async function processCursorToJson(cursor: Cursor, filename: string, res: Response): Promise<void> {
  let rows = await cursor.read(CURSOR_ROW_LIMIT);
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/json',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${filename}.json`
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

async function processCursorToFrontend(cursor: Cursor, res: Response): Promise<void> {}

const processCursor = async (
  cursor: Cursor,
  format: ConsumerOutFormats,
  revisionId: string,
  res: Response
): Promise<void> => {
  switch (format) {
    case ConsumerOutFormats.Csv:
      await processCursorToCsv(cursor, revisionId, res);
      break;
    case ConsumerOutFormats.Json:
      await processCursorToJson(cursor, revisionId, res);
      break;
    case ConsumerOutFormats.View:
      await processCursorToFrontend(cursor, res);
      break;
    case ConsumerOutFormats.Filter:
      await processCursorToFrontendFilterView(cursor, res);
      break;
    case ConsumerOutFormats.Excel:
      await processCursorToExcel(cursor, revisionId, res);
      break;
    default:
      res.status(400).json({ error: 'Format not supported' });
  }
};

export const getFilters = async (
  revisionId: string,
  res: Response,
  language: string,
  format: ConsumerOutFormats
): Promise<void> => {
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  const filterTableQuery = pgformat('SELECT * FROM %I.filter_table WHERE language = %L;', revisionId, language);
  try {
    const cursor = cubeDBConn.query(new Cursor(filterTableQuery));
    await processCursor(cursor, format, revisionId, res);
  } catch (err) {
    logger.error(err, 'Something went wrong trying to get the filter table from the database server');
    throw err;
  } finally {
    cubeDBConn.release();
  }
};

function createBaseQuery(
  revisionId: string,
  view: string,
  locale: string,
  columns: string[],
  factTableToDimensionNames: FactTableToDimensionName[],
  sortBy?: SortByInterface[],
  filterBy?: FilterInterface[]
): string {
  let sortByQuery: string | undefined;
  const sortColumnPostfix = `_${t('column_headers.sort', { lng: locale })}`;
  const refColumnPostfix = `_${t('column_headers.reference', { lng: locale })}`;

  try {
    if (sortBy && sortBy.length > 0) {
      logger.debug('Multiple sort by columns are present. Creating sort by query');
      sortByQuery = sortBy
        .map((sort) => {
          let columnName = sort.columnName;
          const dimensionColumn = factTableToDimensionNames.find((row) => {
            if (row.fact_table_column === columnName && row.language === locale.toLowerCase()) return true;
          });
          if (dimensionColumn) columnName = dimensionColumn.dimension_name;
          return pgformat(
            `%I %s, %I %s`,
            `${columnName}${sortColumnPostfix}`,
            sort.direction ? sort.direction : 'ASC',
            columnName,
            sort.direction ? sort.direction : 'ASC'
          );
        })
        .join(', ');
    }
  } catch (err) {
    logger.error(err, `Something went wrong trying to sort: ${JSON.stringify(sortBy)}`);
    throw err;
  }

  let filterQuery: string | undefined;

  try {
    if (filterBy && filterBy.length > 0) {
      logger.debug('Filters are present. Creating filter query');
      filterQuery = filterBy
        .map((whereClause) => {
          let columnName = whereClause.columnName;
          const dimensionColumn = factTableToDimensionNames.find((row) => {
            if (row.fact_table_column === columnName && row.language === locale.toLowerCase()) return true;
          });
          if (dimensionColumn) columnName = `${dimensionColumn.dimension_name}${refColumnPostfix}`;
          return pgformat('%I in (%L)', columnName, whereClause.values);
        })
        .join(' and ');
    }
  } catch (err) {
    logger.error(err, `Something went wrong trying to filter: ${JSON.stringify(filterBy)}`);
    throw err;
  }

  if (columns[0] === '*') {
    return pgformat(
      'SELECT * FROM %I.%I %s %s',
      revisionId,
      view,
      filterQuery ? `WHERE ${filterQuery}` : '',
      sortByQuery ? `ORDER BY ${sortByQuery}` : ''
    );
  } else {
    return pgformat(
      'SELECT %s FROM %I.%I %s %s',
      columns.join(', '),
      revisionId,
      view,
      filterQuery ? `WHERE ${filterQuery}` : '',
      sortByQuery ? `ORDER BY ${sortByQuery}` : ''
    );
  }
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

interface FactTableToDimensionName {
  fact_table_column: string;
  dimension_name: string;
  language: string;
}

export const createFrontendView = async (
  dataset: Dataset,
  revisionId: string,
  locale: string,
  pageNumber?: number,
  pageSize?: number,
  sortBy?: SortByInterface[],
  filterBy?: FilterInterface[]
): Promise<ViewDTO | ViewErrDTO> => {
  const lang = locale.split('-')[0];

  let filterTableColumnQueryResult: FactTableToDimensionName[];
  const filterTableQuery = dbManager.getCubeDataSource().createQueryRunner();
  try {
    filterTableColumnQueryResult = await filterTableQuery.query(
      pgformat('SELECT DISTINCT fact_table_column, dimension_name, language FROM %I.filter_table;', revisionId)
    );
  } catch (err) {
    logger.error(err, 'Unable to get dimension and fact table column names from cube');
    throw err;
  } finally {
    void filterTableQuery.release();
  }

  const coreView = await coreViewChooser(lang, revisionId);
  const selectColumns = await getColumns(revisionId, lang, 'frontend');

  const baseQuery = createBaseQuery(
    revisionId,
    coreView,
    locale,
    selectColumns,
    filterTableColumnQueryResult,
    sortBy,
    filterBy
  );

  const totalsQuery = pgformat('SELECT count(*) as "totalLines" from (%s);', baseQuery);
  const totalsQueryConnection = dbManager.getCubeDataSource().createQueryRunner();
  let totals: { totalLines: string }[];
  try {
    totals = await totalsQueryConnection.query(totalsQuery);
  } catch (err) {
    logger.error(err, 'Failed to extract totals using the base query');
    throw err;
  } finally {
    void totalsQueryConnection.release();
  }
  const totalLines = Number(totals[0].totalLines);
  let dataQuery = baseQuery;
  if (pageSize && pageNumber) {
    const totalPages = Math.max(1, Math.ceil(totalLines / pageSize));
    const errors = validateParams(pageNumber, totalPages, pageSize);
    if (errors.length > 0) {
      return { status: 400, errors, dataset_id: dataset.id };
    }
    dataQuery = pgformat('%s LIMIT %L OFFSET %L', baseQuery, pageSize, (pageNumber - 1) * pageSize);
  }

  const currentDataset = await DatasetRepository.getById(dataset.id);


  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];

  try {
    const cursor = cubeDBConn.query(new Cursor(dataQuery));
    await processCursor(cursor, ConsumerOutFormats.View, revisionId, res);
  } catch (err) {
    logger.error(err, `Something went wrong trying to get cube data`);
    throw err;
  } finally {
    void cubeDB.release();
  }
  const startLine = pageSize * (pageNumber - 1) + 1;

  // PATCH: Handle empty preview result
  if (!preview || preview.length === 0) {
    const currentDataset = await DatasetRepository.getById(revision.datasetId);
    return {
      dataset: DatasetDTO.fromDataset(currentDataset),
      current_page: pageNumber,
      page_info: {
        total_records: totalLines,
        start_record: 0,
        end_record: 0
      },
      page_size: pageSize,
      total_pages: totalPages,
      headers: [],
      data: []
    };
  }

  const filterTable = filterTableColumnQueryResult.reduce(
    (acc: { fact_table_column: string; dimension_name: string }[], row) => {
      if (row.language === `${lang}-gb`) {
        acc.push({
          fact_table_column: row.fact_table_column,
          dimension_name: row.dimension_name
        });
      }
      return acc;
    },
    []
  );

  const tableHeaders = Object.keys(preview[0] as Record<string, never>);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const data = preview.map((row: any) => Object.values(row));
  const currentDataset = await DatasetRepository.getById(dataset.id, { factTable: true, dimensions: true });
  const lastLine = startLine + data.length - 1;
  const headers = getColumnHeaders(currentDataset, tableHeaders, filterTable);
  let note_codes: string[] = [];

  const noteCodeQueryConnection = dbManager.getCubeDataSource().createQueryRunner();
  try {
    note_codes = (
      await noteCodeQueryConnection.query(
        `SELECT DISTINCT UNNEST(STRING_TO_ARRAY(code, ',')) AS code
          FROM "${revisionId}".all_notes
          ORDER BY code ASC`
      )
    )?.map((row: { code: string }) => row.code);
  } catch (err) {
    logger.error(err, `Something went wrong trying to fetch the used note codes`);
  } finally {
    void noteCodeQueryConnection.release();
  }

  return {
    dataset: DatasetDTO.fromDataset(currentDataset),
    current_page: pageNumber,
    page_info: {
      total_records: totalLines,
      start_record: startLine,
      end_record: lastLine
    },
    page_size: pageSize,
    total_pages: totalPages,
    headers,
    data,
    note_codes
  };
};

function resolveColumns() {}

function resolveFilterValues() {}

export const createQueryId = async (
  res: Response,
  revisionId: string,
  queryRequest: ConsumerRequestBody
): Promise<void> => {

}

export const createSteamingDataView = async (
  res: Response,
  revisionId: string,
  queryId: string
) {

}

export const createStreamingJSONFilteredView = async (

): Promise<void> => {
  // queryRunner.query() does not support Cursor so we need to obtain underlying PostgreSQL connection
  const lang = locale.split('-')[0];
  const viewName = checkAvailableViews(view);
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  const filterTableColumnQueryResult: QueryResult<FactTableToDimensionName> = await cubeDBConn.query(
    pgformat('SELECT DISTINCT fact_table_column, dimension_name, language FROM %I.filter_table;', revisionId)
  );

  try {
    const coreView = await coreViewChooser(lang, revisionId);
    const selectColumns = await getColumns(revisionId, lang, viewName);
    const baseQuery = createBaseQuery(
      revisionId,
      coreView,
      locale,
      selectColumns,
      filterTableColumnQueryResult.rows,
      sortBy,
      filterBy
    );
    const cursor = cubeDBConn.query(new Cursor(baseQuery));
    let rows = await cursor.read(CURSOR_ROW_LIMIT);
    res.setHeader('content-type', 'application/json');
    res.flushHeaders();
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
  } catch (error) {
    logger.error(error, 'Something went wrong trying to read from the view of the cube');
  } finally {
    cubeDBConn.release();
  }
};

export const createStreamingCSVFilteredView = async (
  res: Response,
  revisionId: string,
  locale: string,
  view = 'raw',
  sortBy?: SortByInterface[],
  filterBy?: FilterInterface[]
): Promise<void> => {
  // queryRunner.query() does not support Cursor so we need to obtain underlying PostgreSQL connection
  const lang = locale.split('-')[0];
  const viewName = checkAvailableViews(view);
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  const filterTableColumnQueryResult: QueryResult<FactTableToDimensionName> = await cubeDBConn.query(
    pgformat('SELECT DISTINCT fact_table_column, dimension_name, language FROM %I.filter_table;', revisionId)
  );

  try {
    const coreView = await coreViewChooser(lang, revisionId);
    const selectColumns = await getColumns(revisionId, lang, viewName);
    const baseQuery = createBaseQuery(
      revisionId,
      coreView,
      locale,
      selectColumns,
      filterTableColumnQueryResult.rows,
      sortBy,
      filterBy
    );
    const cursor = cubeDBConn.query(new Cursor(baseQuery));
    let rows = await cursor.read(CURSOR_ROW_LIMIT);
    res.setHeader('content-type', 'text/csv');
    res.flushHeaders();
    if (rows.length > 0) {
      const stream = csvFormat({ delimiter: ',', headers: true });
      stream.pipe(res);
      while (rows.length > 0) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        rows.map((row: any) => {
          stream.write(row);
        });
        rows = await cursor.read(CURSOR_ROW_LIMIT);
      }
    } else {
      res.write('\n');
    }
    res.end();
  } catch (error) {
    logger.error(error, 'Something went wrong trying to read from the view of the cube');
  } finally {
    cubeDBConn.release();
  }
};

export const createStreamingExcelFilteredView = async (
  res: Response,
  revisionId: string,
  locale: string,
  view = 'raw',
  sortBy?: SortByInterface[],
  filterBy?: FilterInterface[]
): Promise<void> => {
  // queryRunner.query() does not support Cursor so we need to obtain underlying PostgreSQL connection
  const lang = locale.split('-')[0];
  const viewName = checkAvailableViews(view);
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  const filterTableColumnQueryResult: QueryResult<FactTableToDimensionName> = await cubeDBConn.query(
    pgformat('SELECT DISTINCT fact_table_column, dimension_name, language FROM %I.filter_table;', revisionId)
  );

  try {
    const coreView = await coreViewChooser(lang, revisionId);
    const selectColumns = await getColumns(revisionId, lang, viewName);
    const baseQuery = createBaseQuery(
      revisionId,
      coreView,
      locale,
      selectColumns,
      filterTableColumnQueryResult.rows,
      sortBy,
      filterBy
    );
    const cursor = cubeDBConn.query(new Cursor(baseQuery));
    let rows = await cursor.read(CURSOR_ROW_LIMIT);
    res.writeHead(200, {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Content-disposition': `attachment;filename=${revisionId}.xlsx`
    });
    res.flushHeaders();
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      filename: `${revisionId}.xlsx`,
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
  } catch (error) {
    logger.error(error, 'Something went wrong trying to read from the view of the cube');
  } finally {
    cubeDBConn.release();
  }
};

function createSQLStandardPivotQuery(
  revisionId: string,
  viewName: string,
  lang: string,
  xAxis: FactTableToDimensionName,
  yAxis: FactTableToDimensionName,
  xAxisValues: string[],
  filters: { col: string; val: string }[]
): string {
  const cols = xAxisValues.map((xVal) => {
    const filtersString: string[] = [];
    if (filters.length > 0) {
      filters.forEach((val) => {
        filtersString.push(pgformat('%I = %L', `${val.col}_${t('column_headers.reference', { lng: lang })}`, val.val));
      });
    }
    return pgformat(
      'array_agg("Data values") FILTER (WHERE %I = %L %s) AS %I',
      xAxis.dimension_name,
      xVal,
      filtersString.length > 0 ? `AND ${filtersString.join(' AND ')}` : '',
      xVal
    );
  });
  return pgformat(
    'SELECT %I,\n%s\nFROM %I.%I GROUP BY 1 ORDER BY 1;',
    yAxis.dimension_name,
    cols.join(',\n'),
    revisionId,
    viewName
  );
}

export const createStreamingPostgresPivotView = async (
  res: Response,
  revisionId: string,
  locale: string,
  xAxis: string,
  yAxis: string,
  filterBy?: FilterInterface[]
): Promise<void> => {
  const start = new Date();
  const startTime = performance.now();
  const lang = locale.split('-')[0];
  const factTableColToDimensionRunner = dbManager.getCubeDataSource().createQueryRunner();
  const factTableColToDimensionQuery = pgformat(
    'SELECT DISTINCT fact_table_column, dimension_name, language FROM %I.filter_table;',
    revisionId
  );
  let filterTableColumnQueryResult: FactTableToDimensionName[];
  try {
    logger.trace(`Running fact table to dimension query:\n\n${factTableColToDimensionQuery}\n\n`);
    filterTableColumnQueryResult = await factTableColToDimensionRunner.query(factTableColToDimensionQuery);
  } catch (err) {
    logger.error(err, 'Something went wrong trying to query the filter table for fact table column and dimension name');
    throw err;
  } finally {
    void factTableColToDimensionRunner.release();
  }

  const xAxisField = filterTableColumnQueryResult.find((col) => col.dimension_name === xAxis);
  const yAxisField = filterTableColumnQueryResult.find((col) => col.dimension_name === yAxis);

  if (!xAxisField) {
    res.status(400);
    res.json({ messages: 'X axis not found in cube.' });
    return;
  }

  if (!yAxisField) {
    res.status(400);
    res.json({ messages: 'Y axis not found in cube.' });
    return;
  }

  logger.debug(`Lang = ${locale.toLowerCase()}`);
  const dimensionCols = filterTableColumnQueryResult
    .filter((col) => col.language.includes(lang.toLowerCase()))
    .filter((col) => col.dimension_name === xAxis || col.dimension_name === yAxis);

  const notPresent = dimensionCols.filter((col) =>
    filterBy?.map((filter) => filter.columnName).includes(col.dimension_name)
  );

  if (notPresent && notPresent.length > 0) {
    logger.trace(`Dimensions not present for query: ${JSON.stringify(notPresent)}`);
    res.status(400);
    res.json({ messages: 'Not all dimension columns found in filter' });
    return;
  }

  const multiValues = filterBy?.filter((filter) => filter.values.length > 1);
  if (multiValues && multiValues.length > 0) {
    res.status(400);
    res.json({ messages: 'Filter found containing multiple values.' });
    return;
  }

  const xAxisValuesQuery = pgformat(
    'SELECT description FROM %I.filter_table WHERE language LIKE %L AND dimension_name = %L',
    revisionId,
    `${locale.toLowerCase()}%`,
    xAxis
  );
  let xAxisValues: { description: string }[];
  const filterTableXAxisValuesRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    logger.trace(`Running query to get dimension values from filter table with query:\n\n${xAxisValuesQuery}\n\n`);
    xAxisValues = await filterTableXAxisValuesRunner.query(xAxisValuesQuery);
  } catch (err) {
    logger.error(err, 'Something went wrong trying to get the X Axis values');
    throw err;
  } finally {
    void filterTableXAxisValuesRunner.release();
  }

  // queryRunner.query() does not support Cursor so we need to obtain underlying PostgreSQL connection
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  try {
    const coreView = await coreViewChooser(lang, revisionId);
    const pivotQuery = createSQLStandardPivotQuery(
      revisionId,
      coreView,
      lang,
      xAxisField,
      yAxisField,
      xAxisValues.map((val) => val.description),
      filterBy?.map((val) => {
        return { col: val.columnName, val: val.values[0] };
      }) || []
    );
    logger.trace(`Running Postgres Pivot query:\n\n${pivotQuery}\n\n`);
    const cursor = cubeDBConn.query(new Cursor(pivotQuery));
    let rows = await cursor.read(CURSOR_ROW_LIMIT);
    res.setHeader('content-type', 'application/json');
    res.flushHeaders();
    res.write('{"pivot": [');
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
    const performanceObject = {
      start: start,
      finish: new Date(),
      total_time_ms: performance.now() - startTime
    };
    res.write('],');
    res.write(`"Performance" : ${JSON.stringify(performanceObject)}`);
    res.write('}');
    res.end();
  } catch (error) {
    logger.error(error, 'Something went wrong trying to read from the view of the cube');
  } finally {
    cubeDBConn.release();
  }
};
