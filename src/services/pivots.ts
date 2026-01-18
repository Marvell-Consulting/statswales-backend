import { Response } from 'express';

import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { QueryStore } from '../entities/query-store';
import { PageOptions } from '../interfaces/page-options';
import { duckdb } from './duckdb';
import { t } from '../middleware/translation';
import { DuckDBResult } from '@duckdb/node-api';
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

const EXCEL_ROW_LIMIT = 1048576 - 76; // Excel Limit is 1,048,576 but removed 76 rows because ?

async function pivotToJson(res: Response, pivot: DuckDBResult): Promise<void> {
  res.setHeader('content-type', 'application/json');
  res.flushHeaders();
  res.write('{ "pivot": [');
  let rows = await pivot.getRowObjects();
  while (rows.length > 0) {
    const lastRowIndex = rows.length - 1;
    rows.forEach((row: unknown, index: number) => {
      if (index < lastRowIndex) {
        res.write(`${JSON.stringify(row)},\n`);
      } else {
        res.write(`${JSON.stringify(row)}`);
      }
    });
    rows = await pivot.getRowObjects();
    if (rows.length > 0) {
      res.write(',\n');
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
  queryStore: QueryStore,
  pageOptions: PageOptions
): Promise<void> {
  const { pageNumber = 1, pageSize = queryStore.totalLines } = pageOptions;
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
  res.write(`"note_codes": ${JSON.stringify(note_codes || [])},`);

  let rows = await pivot.getRowObjects();
  if (rows.length > 0) {
    const tableHeaders = Object.keys(rows[0]);
    const headers = getColumnHeaders(dataset, tableHeaders, filters);
    res.write(`"headers": ${JSON.stringify(headers)},`);
  }

  res.write('"data": [');
  let firstRow = true;
  let rowCount = 0;
  while (rows.length > 0) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    rows.forEach((row: any) => {
      if (firstRow) {
        firstRow = false;
      } else {
        res.write(',\n');
      }
      res.write(JSON.stringify(Object.values(row)));
      rowCount++;
    });
    rows = await pivot.getRowObjects();
  }
  while (rows.length > 0) {
    const lastRowIndex = rows.length - 1;
    rows.forEach((row: unknown, index: number) => {
      if (index < lastRowIndex) {
        res.write(`${JSON.stringify(row)},\n`);
      } else {
        res.write(`${JSON.stringify(row)}`);
      }
    });
    rows = await pivot.getRowObjects();
    if (rows.length > 0) {
      res.write(',\n');
    }
  }
  res.write('],');
  const page_info = {
    current_page: pageNumber,
    page_size: pageSize,
    total_pages: Math.max(1, Math.ceil(queryStore.totalLines / pageSize)),
    total_records: queryStore.totalLines,
    start_record: startRecord,
    end_record: startRecord + rowCount
  };
  res.write(`"page_info": ${JSON.stringify(page_info)}`);

  res.write(`}`);
  res.end();
}

async function pivotToCsv(res: Response, pivot: DuckDBResult): Promise<void> {
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'text/csv',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${pivot} ${Date.now()}.csv`
  });
  const stream = csvFormat({ delimiter: ',', headers: true });
  stream.pipe(res);

  let rows = await pivot.getRowObjects();
  while (rows.length > 0) {
    rows.map((row: unknown) => {
      stream.write(row);
    });
    rows = await pivot.getRowObjects();
  }
  res.write('\n');
  res.end();
  stream.end();
}

async function pivotToExcel(res: Response, pivot: DuckDBResult): Promise<void> {
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${pivot} ${Date.now()}.xlsx`
  });
  res.flushHeaders();
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
    filename: `${pivot} ${Date.now()}.xlsx`,
    useStyles: true,
    useSharedStrings: true,
    stream: res
  });
  let sheetCount = 1;
  let totalRows = 0;
  let worksheet = workbook.addWorksheet(`Sheet-${sheetCount}`);

  let rows = await pivot.getRowObjects();
  worksheet.addRow(Object.keys(rows[0]));
  while (rows.length > 0) {
    for (const row of rows) {
      if (row === null) break;
      const data = Object.values(row).map((val) => {
        if (!val) return null;
        return isNaN(Number(val)) ? val : Number(val);
      });
      worksheet.addRow(Object.values(data)).commit();
    }
    totalRows += rows.length;
    if (totalRows > EXCEL_ROW_LIMIT) {
      worksheet.commit();
      sheetCount++;
      totalRows = 0;
      worksheet = workbook.addWorksheet(`Sheet-${sheetCount}`);
    }
    rows = await pivot.getRowObjects();
  }
  worksheet.commit();
  await workbook.commit();
}

async function pivotToHtml(res: Response, pivot: DuckDBResult): Promise<void> {
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

  let rows = await pivot.getRowObjects();
  Object.keys(rows[0]).forEach((key) => {
    res.write(`<th>${key}</th>`);
  });
  res.write('</tr></thead><tbody>');
  while (rows.length > 0) {
    for (const row of rows) {
      res.write('<tr>');
      Object.values(row).forEach((value, idx) => {
        if (idx === 0) {
          res.write(`<th>${value === null ? '' : value}</th>`);
        } else {
          res.write(`<td>${value === null ? '' : value}</td>`);
        }
      });
      res.write('</tr>');
    }
    rows = await pivot.getRowObjects();
  }
  res.write('</tbody>\n' + '</table>\n' + '</body>\n' + '</html>\n');
  res.end();
}

async function formatChooser(
  res: Response,
  lang: string,
  pivot: DuckDBResult,
  pageOptions: PageOptions,
  queryStore: QueryStore
): Promise<void> {
  switch (pageOptions.format) {
    case OutputFormats.Json:
      await pivotToJson(res, pivot);
      break;
    case OutputFormats.Csv:
      await pivotToCsv(res, pivot);
      break;
    case OutputFormats.Excel:
      await pivotToExcel(res, pivot);
      break;
    case OutputFormats.Html:
      await pivotToHtml(res, pivot);
      break;
    case OutputFormats.Frontend:
      await pivotToFrontend(res, lang, pivot, queryStore, pageOptions);
      break;
  }
}

export function langToLocale(lang: string): string {
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

export async function createPivotFromQuery(
  res: Response,
  queryStore: QueryStore,
  pageOptions: PageOptions
): Promise<void> {
  const quack = await duckdb();
  const lang = langToLocale(pageOptions.locale);
  const query = queryStore.query[lang].replaceAll(
    pgformat('%I', queryStore.revisionId),
    pgformat('%I.%I', 'cube_db', queryStore.revisionId)
  );
  const filterTable: FactTableToDimensionName[] = await getFilterTable(queryStore.revisionId);
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

  const pivotQuery = pgformat(
    'PIVOT (%s) ON %s USING first(%I) GROUP BY %s %s;',
    query,
    x,
    dataValuesCol,
    y,
    pagingQuery
  );
  logger.trace(`Pivot Query = ${pivotQuery}`);
  try {
    const pivot = await quack.stream(pivotQuery);
    await formatChooser(res, lang, pivot, pageOptions, queryStore);
  } catch (err) {
    logger.error(err, 'Error creating pivot from query');
  } finally {
    quack.closeSync();
  }
}

// async function createPivotFromPost(): Promise<void> {}
