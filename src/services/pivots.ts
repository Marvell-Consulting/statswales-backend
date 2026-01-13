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
import { Dataset } from '../entities/dataset/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { dbManager } from '../db/database-manager';

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

async function pivotToFrontend(res: Response, pivot: DuckDBResult, queryStore: QueryStore): Promise<void> {
  const dataset = await Dataset.findOneOrFail({ id: queryStore.datasetId });
  const datasetDto = DatasetDTO.fromDataset(dataset);
  let note_codes: string[] = [];
  const queryRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    const noteCodeRows = await queryRunner.query(
      pgformat(
        `SELECT DISTINCT UNNEST(STRING_TO_ARRAY(code, ',')) AS code FROM %I.all_notes ORDER BY code ASC`,
        queryStore.revisionId
      )
    );
    note_codes = noteCodeRows?.map((row: { code: string }) => row.code) ?? [];
  } catch (error) {
    logger.error(error, `Failed to fetch note codes for revisionId ${queryStore.revisionId}`);
    note_codes = [];
  } finally {
    await queryRunner.release();
  }
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
      await pivotToFrontend(res, pivot, queryStore);
      break;
  }
}

export async function createPivotFromQuery(
  res: Response,
  queryStore: QueryStore,
  pageOptions: PageOptions
): Promise<void> {
  const quack = await duckdb();
  const lang = pageOptions.locale === 'en' ? 'en-GB' : pageOptions.locale;
  const query = queryStore.query[lang].replaceAll(
    pgformat('%I', queryStore.revisionId),
    pgformat('%I.%I', 'cube_db', queryStore.revisionId)
  );
  const dataValuesCol = t('column_headers.data_values', { lng: pageOptions.locale });
  const pivotQuery = pgformat(
    'PIVOT (%s) ON %I USING first(%I) GROUP BY %I;',
    query,
    pageOptions.x,
    dataValuesCol,
    pageOptions.y
  );
  logger.debug(`Pivot Query = ${pivotQuery}`);
  try {
    const pivot = await quack.stream(pivotQuery);
    await formatChooser(res, pivot, pageOptions, queryStore);
  } catch (err) {
    logger.error(err, 'Error creating pivot from query');
  } finally {
    quack.closeSync();
  }
}

// async function createPivotFromPost(): Promise<void> {}
