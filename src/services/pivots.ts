import { Response } from 'express';

import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { QueryStore } from '../entities/query-store';
import { PageOptions } from '../interfaces/page-options';
import { duckdb } from './duckdb';
import { t } from '../middleware/translation';
import { DuckDBResultReader } from '@duckdb/node-api';
import { logger } from '../utils/logger';
import { OutputFormats } from '../enums/output-formats';
import { format as csvFormat } from '@fast-csv/format';
import ExcelJS from 'exceljs';

const EXCEL_ROW_LIMIT = 1048576 - 76; // Excel Limit is 1,048,576 but removed 76 rows because ?

async function pivotToJson(res: Response, pivot: DuckDBResultReader): Promise<void> {
  res.setHeader('content-type', 'application/json');
  res.flushHeaders();
  res.write('{ "pivot": [');
  await pivot.readUntil(100);

  while (!pivot.done) {
    const rows = pivot.getRows();
    const lastRowIndex = rows.length - 1;
    rows.forEach((row: unknown, index: number) => {
      res.write(JSON.stringify(row));
      if (!pivot.done && index !== lastRowIndex) {
        res.write(',');
      }
    });
    await pivot.readUntil(100);
  }
  res.write(']');
  res.write('}');
  res.end();
}

async function pivotToCsv(res: Response, pivot: DuckDBResultReader): Promise<void> {
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'text/csv',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${pivot} ${Date.now()}.csv`
  });
  const stream = csvFormat({ delimiter: ',', headers: true });
  stream.pipe(res);
  await pivot.readUntil(100);

  while (!pivot.done) {
    const rows = pivot.getRows();
    rows.map((row: unknown) => {
      stream.write(row);
    });
    await pivot.readUntil(100);
  }
  res.write('\n');
  res.end();
  stream.end();
}

async function pivotToExcel(res: Response, pivot: DuckDBResultReader): Promise<void> {
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
  await pivot.readUntil(100);

  while (!pivot.done) {
    const rows = pivot.getRows();
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
    }
    await pivot.readUntil(100);
  }
  worksheet.commit();
  await workbook.commit();
}

async function pivotToHtml(res: Response, pivot: DuckDBResultReader): Promise<void> {
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

  await pivot.readUntil(100);
  let rows = pivot.getRows();
  Object.keys(rows[0]).forEach((key) => {
    res.write(`<th>${key}</th>`);
  });
  res.write('</tr></thead><tbody>');
  while (!pivot.done) {
    for (const row of rows) {
      res.write('<tr>');
      Object.values(row).forEach((value) => {
        res.write(`<td>${value}</td>`);
      });
      res.write('</tr>');
    }
    await pivot.readUntil(100);
    rows = pivot.getRows();
  }
  res.write('</tbody>\n' + '</table>\n' + '</body>\n' + '</html>\n');
}

async function formatChooser(res: Response, pivot: DuckDBResultReader, pageOptions: PageOptions): Promise<void> {
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
    default:
      logger.error('Invalid format specified');
  }
}

export async function createPivotFromQuery(
  res: Response,
  queryStore: QueryStore,
  pageOptions: PageOptions
): Promise<void> {
  const quack = await duckdb();
  const query = queryStore.query[pageOptions.locale];
  const dataValuesCol = t('columns.data_values', { lng: pageOptions.locale });
  const pivotQuery = pgformat(
    'PIVOT (%s) ON %I USING first(%I) GROUP BY %s;',
    query,
    pageOptions.x,
    dataValuesCol,
    pageOptions.y
  );
  try {
    const pivot = await quack.streamAndRead(pivotQuery);
    await formatChooser(res, pivot, pageOptions);
  } catch (err) {
    logger.error(err, 'Error creating pivot from query');
  } finally {
    quack.closeSync();
  }
}

// async function createPivotFromPost(): Promise<void> {}
