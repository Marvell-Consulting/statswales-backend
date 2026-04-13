import { Response } from 'express';

import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { QueryStore } from '../entities/query-store';
import { PageOptions } from '../interfaces/page-options';
import { acquireDuckDB } from './duckdb';
import { t } from '../middleware/translation';
import { DuckDBResult, DuckDBValue } from '@duckdb/node-api';
import { logger } from '../utils/logger';
import { OutputFormats } from '../enums/output-formats';
import { format as csvFormat } from '@fast-csv/format';
import ExcelJS from 'exceljs';
import { dbManager } from '../db/database-manager';
import { DatasetRepository } from '../repositories/dataset';
import { getColumnHeaders } from '../utils/column-headers';
import { ConsumerDatasetDTO } from '../dtos/consumer-dataset-dto';
import { getFilterTable, resolveDimensionToFactTableColumn, resolveFactColumnToDimension } from '../utils/consumer';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { FilterRow } from '../interfaces/filter-row';
import { UnknownException } from '../exceptions/unknown.exception';
import { makeCubeSafeString } from './cube-builder';
import { DimensionType, LookupTableTypes } from '../enums/dimension-type';

const EXCEL_ROW_LIMIT = 1048576 - 76; // Excel Limit is 1,048,576 but removed 76 rows because ?

/**
 * Query the lookup table for the x dimension to get the correct sort order
 * for pivot column headers. Falls back to the original column order when
 * sort information is unavailable (e.g. numeric/text dimensions without
 * a lookup table, or multi-dimensional pivots).
 */
export async function getSortedPivotColumns(
  rawColumnOrder: string[],
  pageOptions: PageOptions,
  queryStore: QueryStore,
  lang: string
): Promise<string[]> {
  if (!pageOptions.x || Array.isArray(pageOptions.x)) {
    return rawColumnOrder;
  }

  const yCount = Array.isArray(pageOptions.y) ? pageOptions.y.length : 1;
  const yColumns = rawColumnOrder.slice(0, yCount);
  const xValueColumns = rawColumnOrder.slice(yCount);

  if (xValueColumns.length <= 1) {
    return rawColumnOrder;
  }

  try {
    const dataset = await DatasetRepository.getById(queryStore.datasetId, { factTable: true, dimensions: true });
    const filterTable = await getFilterTable(queryStore.revisionId);

    // pageOptions.x may be a translated dimension name (e.g. "Date") or a raw
    // fact table column name (e.g. "DateRef"). Try resolving as a dimension name
    // first, then fall back to checking fact_table_column directly.
    let factCol: string;
    try {
      factCol = resolveDimensionToFactTableColumn(pageOptions.x, filterTable);
    } catch {
      const byFactCol = filterTable.find(
        (f) => f.fact_table_column.toLowerCase() === pageOptions.x!.toString().toLowerCase()
      );
      if (!byFactCol) {
        return rawColumnOrder;
      }
      factCol = byFactCol.fact_table_column;
    }
    const dimension = dataset.dimensions?.find((d) => d.factTableColumn === factCol);

    if (!dimension || !LookupTableTypes.includes(dimension.type)) {
      return rawColumnOrder;
    }

    const dimTable = `${makeCubeSafeString(factCol)}_lookup`;
    const isDateDimension = dimension.type === DimensionType.DatePeriod || dimension.type === DimensionType.Date;
    const sortDirection = isDateDimension ? 'DESC' : 'ASC';

    const cubeDataSource = dbManager.getCubeDataSource();
    const sortRows: { description: string }[] = await cubeDataSource.query(
      pgformat(
        `SELECT description FROM (SELECT DISTINCT description, sort_order FROM %I.%I WHERE language = %L) sub ORDER BY sort_order %s, description`,
        queryStore.revisionId,
        dimTable,
        lang.toLowerCase(),
        sortDirection
      )
    );

    if (sortRows.length === 0) {
      return rawColumnOrder;
    }

    const sortPosition = new Map<string, number>();
    sortRows.forEach((row, idx) => sortPosition.set(row.description, idx));

    const sorted = [...xValueColumns].sort((a, b) => {
      const posA = sortPosition.get(a) ?? Number.MAX_SAFE_INTEGER;
      const posB = sortPosition.get(b) ?? Number.MAX_SAFE_INTEGER;
      return posA - posB;
    });

    return [...yColumns, ...sorted];
  } catch (err) {
    logger.warn(err, 'Failed to determine pivot column sort order, using default order');
    return rawColumnOrder;
  }
}

function reorderRow(row: DuckDBValue[], columnMapping: number[]): DuckDBValue[] {
  return columnMapping.map((srcIdx) => row[srcIdx]);
}

async function pivotToJson(
  res: Response,
  pivot: DuckDBResult,
  columnOrder: string[],
  columnMapping: number[]
): Promise<void> {
  res.setHeader('content-type', 'application/json');
  res.flushHeaders();
  res.write('{ "pivot": [');
  let first = true;
  for await (const rows of pivot.yieldRows()) {
    for (const row of rows) {
      if (first) {
        first = false;
      } else {
        res.write(',\n');
      }
      const jsonRow =
        '{' +
        columnOrder.map((col, i) => `${JSON.stringify(col)}:${JSON.stringify(row[columnMapping[i]])}`).join(',') +
        '}';
      res.write(jsonRow);
    }
  }
  res.write(']');
  res.write('}');
  res.end();
}

async function pivotToFrontend(
  res: Response,
  lang: string,
  pivot: DuckDBResult,
  columnOrder: string[],
  columnMapping: number[],
  queryStore: QueryStore,
  pageOptions: PageOptions
): Promise<void> {
  const { pageNumber = 1, pageSize = 100 } = pageOptions;
  const startRecord = pageSize * (pageNumber - 1);
  const dataset = await DatasetRepository.getById(queryStore.datasetId, { factTable: true, dimensions: true });
  let note_codes: string[] = [];
  let filters: { fact_table_column: string; dimension_name: string }[] = [];
  const queryRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    const noteCodeRows = await queryRunner.query(
      pgformat(
        `SELECT DISTINCT UNNEST(STRING_TO_ARRAY(code, ',')) AS code FROM %I.all_notes ORDER BY code ASC`,
        queryStore.revisionId
      )
    );
    note_codes = noteCodeRows?.map((row: { code: string }) => row.code) ?? [];
    filters = await queryRunner.query(
      pgformat(
        'SELECT DISTINCT fact_table_column, dimension_name FROM %I.filter_table WHERE language = %L;',
        queryStore.revisionId,
        lang
      )
    );
  } catch (error) {
    logger.error(error, `Failed to fetch note codes for revisionId ${queryStore.revisionId}`);
    note_codes = [];
  } finally {
    await queryRunner.release();
  }
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/json',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${queryStore.datasetId}.json`
  });
  res.write('{');
  res.write(`"dataset": ${JSON.stringify(ConsumerDatasetDTO.fromDataset(dataset))},`);
  res.write(`"filters": ${JSON.stringify(queryStore.requestObject.filters || [])},`);
  res.write(`"pivot": ${JSON.stringify(queryStore.requestObject.pivot || {})},`);
  res.write(`"note_codes": ${JSON.stringify(note_codes || [])},`);

  const headers = getColumnHeaders(dataset, columnOrder, filters);
  res.write(`"headers": ${JSON.stringify(headers)},`);

  res.write('"data": [');
  let firstRow = true;
  let rowCount = 0;
  for await (const rows of pivot.yieldRows()) {
    for (const row of rows) {
      if (firstRow) {
        firstRow = false;
      } else {
        res.write(',\n');
      }
      const reorderedRow = reorderRow(row, columnMapping);
      res.write(JSON.stringify(reorderedRow));
      rowCount++;
    }
  }
  res.write('],');
  const totalPages = queryStore.totalPivotLines ? Math.max(1, Math.ceil(queryStore.totalPivotLines / pageSize)) : 0;
  const page_info = {
    current_page: pageNumber,
    page_size: pageSize,
    total_pages: totalPages,
    total_records: queryStore.totalPivotLines,
    start_record: startRecord,
    end_record: startRecord + rowCount
  };
  res.write(`"page_info": ${JSON.stringify(page_info)}`);

  res.write(`}`);
  res.end();
}

async function pivotToCsv(
  res: Response,
  pivot: DuckDBResult,
  columnOrder: string[],
  columnMapping: number[]
): Promise<void> {
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'text/csv',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=pivot-${Date.now()}.csv`
  });
  const stream = csvFormat({ delimiter: ',', headers: columnOrder });
  stream.pipe(res);

  for await (const rows of pivot.yieldRows()) {
    for (const row of rows) {
      const reorderedRow = reorderRow(row, columnMapping);
      stream.write(reorderedRow);
    }
  }
  res.write('\n');
  res.end();
  stream.end();
}

async function pivotToExcel(
  res: Response,
  pivot: DuckDBResult,
  columnOrder: string[],
  columnMapping: number[]
): Promise<void> {
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=pivot-${Date.now()}.xlsx`
  });
  res.flushHeaders();
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
    useStyles: true,
    useSharedStrings: true,
    stream: res
  });
  let sheetCount = 1;
  let totalRows = 0;
  let worksheet = workbook.addWorksheet(`Sheet-${sheetCount}`);

  worksheet.addRow(columnOrder);
  for await (const rows of pivot.yieldRows()) {
    for (const row of rows) {
      if (row === null) break;
      const data = columnMapping.map((srcIdx) => {
        const val = row[srcIdx];
        if (!val) return null;
        return isNaN(Number(val)) ? val : Number(val);
      });
      worksheet.addRow(data).commit();
    }
    totalRows += rows.length;
    if (totalRows > EXCEL_ROW_LIMIT) {
      worksheet.commit();
      sheetCount++;
      totalRows = 0;
      worksheet = workbook.addWorksheet(`Sheet-${sheetCount}`);
    }
  }
  worksheet.commit();
  await workbook.commit();
}

async function pivotToHtml(
  res: Response,
  pivot: DuckDBResult,
  columnOrder: string[],
  columnMapping: number[]
): Promise<void> {
  res.setHeader('content-type', 'text/html');
  res.flushHeaders();
  res.write(
    '<!DOCTYPE html>\n' +
      '<html lang="en">\n' +
      '<head>\n' +
      '    <meta charset="utf-8">\n' +
      '    <title>HTML Document Title</title>\n' +
      '</head>\n' +
      '<body>\n' +
      '<table>\n' +
      '<thead><tr>'
  );

  if (columnOrder.length === 0) {
    res.write('</tr>\n</thead>\n<tbody>\n</tbody>\n' + '</table>\n' + '</body>\n' + '</html>\n');
    res.end();
    return;
  }
  columnOrder.forEach((key) => {
    res.write(`<th>${key}</th>`);
  });
  res.write('</tr></thead><tbody>');
  for await (const rows of pivot.yieldRows()) {
    for (const row of rows) {
      res.write('<tr>');
      columnMapping.forEach((srcIdx, i) => {
        const value = row[srcIdx];
        if (i === 0) {
          res.write(`<th>${value === null ? '' : value}</th>`);
        } else {
          res.write(`<td>${value === null ? '' : value}</td>`);
        }
      });
      res.write('</tr>');
    }
  }
  res.write('</tbody>\n' + '</table>\n' + '</body>\n' + '</html>\n');
  res.end();
}

export async function createPivotOutputUsingDuckDB(
  res: Response,
  lang: string,
  pivotQuery: string,
  pageOptions: PageOptions,
  queryStore: QueryStore
): Promise<void> {
  const { duckdb, releaseDuckDB } = await acquireDuckDB();
  try {
    await duckdb.run('CALL pg_clear_cache();');
    const pivot = await duckdb.stream(pivotQuery);
    const rawColumns = pivot.columnNames();
    const columnOrder = await getSortedPivotColumns(rawColumns, pageOptions, queryStore, lang);

    // Build a mapping from sorted column positions to original row indices.
    // When no reordering occurred this is the identity [0, 1, 2, ...].
    const rawIndexMap = new Map(rawColumns.map((col, i) => [col, i]));
    const columnMapping = columnOrder.map((col) => rawIndexMap.get(col)!);

    switch (pageOptions.format) {
      case OutputFormats.Json:
        await pivotToJson(res, pivot, columnOrder, columnMapping);
        break;
      case OutputFormats.Csv:
        await pivotToCsv(res, pivot, columnOrder, columnMapping);
        break;
      case OutputFormats.Excel:
        await pivotToExcel(res, pivot, columnOrder, columnMapping);
        break;
      case OutputFormats.Html:
        await pivotToHtml(res, pivot, columnOrder, columnMapping);
        break;
      case OutputFormats.Frontend:
        await pivotToFrontend(res, lang, pivot, columnOrder, columnMapping, queryStore, pageOptions);
        break;
    }
  } catch (err) {
    if (err instanceof Error && err.message.includes('Binder Error')) {
      throw new BadRequestException('Invalid sort by column');
    }
    logger.error(err, 'Error creating pivot from query');
    throw new UnknownException('Pivot query failed to run');
  } finally {
    releaseDuckDB();
  }
}

export function langToLocale(lang: string): string {
  if (!lang) return 'en-GB';
  if (lang.length === 5) lang = lang.substring(0, 2);
  switch (lang) {
    case 'en':
      return 'en-GB';
    case 'cy':
      return 'cy-GB';
    default:
      return 'en-GB';
  }
}

export function validateColOnly(columnName: string, locale: string, filterTable: FactTableToDimensionName[]): string {
  resolveDimensionToFactTableColumn(columnName, filterTable);
  return columnName;
}

export async function getPivotRowCount(query: string): Promise<number> {
  const { duckdb, releaseDuckDB } = await acquireDuckDB();
  try {
    await duckdb.run('CALL pg_clear_cache();');
    const result = await duckdb.run(query);
    return result.rowCount;
  } catch (err) {
    logger.error(err, 'Something went wrong trying to run the pivot query to get the line count');
    throw new UnknownException('Pivot line count failed to run the pivot query');
  } finally {
    releaseDuckDB();
  }
}

export async function createPivotQuery(
  lang: string,
  queryStore: QueryStore,
  pageOptions: PageOptions
): Promise<string> {
  const query = queryStore.query[lang].replaceAll(
    pgformat('%I', queryStore.revisionId),
    pgformat('%I.%I', 'cube_db', queryStore.revisionId)
  );
  const filterTable: FilterRow[] = await getFilterTable(queryStore.revisionId);
  let columnFinderValidator = validateColOnly;
  if (queryStore.requestObject.options?.use_raw_column_names) {
    columnFinderValidator = resolveFactColumnToDimension;
  }

  const dataValuesCol = t('column_headers.data_values', { lng: pageOptions.locale });

  let pagingQuery = '';
  if (pageOptions.pageSize) {
    pagingQuery = `LIMIT ${pageOptions.pageSize} OFFSET ${pageOptions.pageSize * (pageOptions.pageNumber - 1)}`;
  }

  let x = pageOptions.x;
  if (!x) {
    throw new BadRequestException('X is required for pivot creation');
  }
  if (Array.isArray(x)) {
    x = x.map((val) => pgformat('%I', columnFinderValidator(val, lang, filterTable)));
    x.forEach((val) => {
      if (!query.includes(val)) {
        throw new BadRequestException(`X value ${val} is not present in the query`);
      }
    });
    x = x.join(` || ' & ' || `);
  } else {
    x = pgformat('%I', columnFinderValidator(x, lang, filterTable));
  }

  let y = pageOptions.y;
  if (!y) {
    throw new BadRequestException('Y is required for pivot creation');
  }
  if (Array.isArray(y)) {
    y = y.map((val) => pgformat('%I', columnFinderValidator(val, lang, filterTable)));
    y.forEach((val) => {
      if (!query.includes(val)) {
        throw new BadRequestException(`Y value ${val} is not present in the query`);
      }
    });
    y = y.join(', ');
  } else {
    y = pgformat('%I', columnFinderValidator(y, lang, filterTable));
  }

  let sortQuery = '';
  if (pageOptions.sort.length > 0) {
    const sortParts: string[] = [];
    for (const col of pageOptions.sort) {
      const sortCol = col.split('|');
      const direction = (sortCol[1] || '').toUpperCase();
      if (direction !== 'ASC' && direction !== 'DESC') {
        throw new BadRequestException(`Invalid sort direction: ${sortCol[1]}`);
      }
      sortParts.push(`${pgformat('%I', sortCol[0])} ${direction}`);
    }
    if (sortParts.length > 0) {
      sortQuery = 'ORDER BY ' + sortParts.join(', ');
    }
  }

  return pgformat(
    'PIVOT (%s) ON %s USING first(%I) GROUP BY %s %s %s',
    query,
    x,
    dataValuesCol,
    y,
    sortQuery,
    pagingQuery
  );
}
