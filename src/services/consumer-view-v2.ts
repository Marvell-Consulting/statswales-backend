import { Response } from 'express';
import { pipeline } from 'node:stream/promises';
import { escape } from 'lodash';
import { PoolClient } from 'pg';
import QueryStream from 'pg-query-stream';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { format as csvFormat } from '@fast-csv/format';
import ExcelJS from 'exceljs';
import { t } from 'i18next';

import { FilterRow } from '../interfaces/filter-row';
import { FilterTable } from '../interfaces/filter-table';
import { dbManager } from '../db/database-manager';
import { QueryStore } from '../entities/query-store';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { flattenHierarchy, transformHierarchy } from '../utils/consumer';
import { DatasetRepository } from '../repositories/dataset';
import { PageOptions } from '../interfaces/page-options';
import { logger } from '../utils/logger';
import { ConsumerDatasetDTO } from '../dtos/consumer-dataset-dto';
import { getColumnHeaders } from '../utils/column-headers';
import { QueryStoreRepository } from '../repositories/query-store';
import { DataSource } from 'typeorm';

const EXCEL_ROW_LIMIT = 1048576 - 76; // Excel Limit is 1,048,576 but removed 76 rows because ?
const HIGH_WATER_MARK = 500; // max rows to buffer in memory at once when streaming from the database

export async function sendCsv(query: string, queryStore: QueryStore, res: Response): Promise<void> {
  logger.debug(`Sending CSV for query id ${queryStore.id}...`);
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  let dbStream: QueryStream | null = null;
  let hasData = false;

  try {
    await cubeDBConn.query('BEGIN');
    dbStream = cubeDBConn.query(new QueryStream(query, [], { highWaterMark: HIGH_WATER_MARK }));
    dbStream.on('data', () => (hasData = true));

    const csvStream = csvFormat({ delimiter: ',', headers: true });

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment;filename=${queryStore.datasetId}.csv`);

    await pipeline(dbStream, csvStream, res, { end: false });

    await cubeDBConn.query('COMMIT');
    if (!hasData) {
      res.write('\n'); // Write a newline for empty CSV to avoid zero-byte file issues in some clients
    }
    res.end();
  } catch (err) {
    await cubeDBConn.query('ROLLBACK').catch(() => {});
    logger.error(err, `Error sending CSV for query id ${queryStore.id}`);
    dbStream?.destroy();
    if (!res.headersSent) {
      res.status(500).end();
    }
    throw err;
  } finally {
    await cubeDBConn.release();
  }
}

export async function sendExcel(query: string, queryStore: QueryStore, res: Response): Promise<void> {
  logger.debug(`Sending Excel for query id ${queryStore.id}...`);
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  let dbStream: QueryStream | null = null;

  try {
    await cubeDBConn.query('BEGIN');
    dbStream = cubeDBConn.query(new QueryStream(query, [], { highWaterMark: HIGH_WATER_MARK }));

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment;filename=${queryStore.datasetId}.xlsx`);
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
    let isFirstRow = true;

    for await (const row of dbStream as AsyncIterable<Record<string, unknown>>) {
      if (row === null) continue;

      // Add headers from first row
      if (isFirstRow) {
        worksheet.addRow(Object.keys(row));
        isFirstRow = false;
      }

      const data = Object.values(row).map((val) => {
        if (!val) return null;
        return isNaN(Number(val)) ? val : Number(val);
      });
      worksheet.addRow(data).commit();

      totalRows++;
      if (totalRows > EXCEL_ROW_LIMIT) {
        worksheet.commit();
        sheetCount++;
        totalRows = 0;
        worksheet = workbook.addWorksheet(`Sheet-${sheetCount}`);
      }
    }

    worksheet.commit();
    await workbook.commit();
    await cubeDBConn.query('COMMIT');
  } catch (err) {
    await cubeDBConn.query('ROLLBACK').catch(() => {});
    logger.error(err, `Error sending Excel for query id ${queryStore.id}`);
    dbStream?.destroy();
    if (!res.headersSent) {
      res.status(500).end();
    }
    throw err;
  } finally {
    await cubeDBConn.release();
  }
}

export async function sendJson(query: string, queryStore: QueryStore, res: Response): Promise<void> {
  logger.debug(`Sending JSON for query id ${queryStore.id}...`);
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  let dbStream: QueryStream | null = null;

  try {
    await cubeDBConn.query('BEGIN');
    dbStream = cubeDBConn.query(new QueryStream(query, [], { highWaterMark: HIGH_WATER_MARK }));

    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment;filename=${queryStore.datasetId}.json`);

    res.write('[');
    let isFirstRow = true;

    for await (const row of dbStream as AsyncIterable<unknown>) {
      if (isFirstRow) {
        isFirstRow = false;
      } else {
        res.write(',\n');
      }
      res.write(JSON.stringify(row));
    }

    res.write(']');
    await cubeDBConn.query('COMMIT');
    res.end();
  } catch (err) {
    await cubeDBConn.query('ROLLBACK').catch(() => {});
    logger.error(err, `Error sending JSON for query id ${queryStore.id}`);
    dbStream?.destroy();
    if (!res.headersSent) {
      res.status(500).end();
    }
    throw err;
  } finally {
    await cubeDBConn.release();
  }
}

export async function sendHtml(query: string, queryStore: QueryStore, res: Response): Promise<void> {
  logger.debug(`Sending HTML for query id ${queryStore.id}...`);
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  let dbStream: QueryStream | null = null;

  try {
    await cubeDBConn.query('BEGIN');
    dbStream = cubeDBConn.query(new QueryStream(query, [], { highWaterMark: HIGH_WATER_MARK }));

    res.setHeader('Content-Type', 'text/html');
    res.flushHeaders();

    res.write(`
      <!DOCTYPE html>
      <html lang="en">
      <head>
          <meta charset="utf-8">
          <title>${escape(String(queryStore.datasetId))}</title>
      </head>
      <body>
      <table>
      <thead><tr>
    `);

    let isFirstRow = true;
    let hasData = false;

    for await (const row of dbStream as AsyncIterable<Record<string, unknown>>) {
      hasData = true;

      // Add headers from first row
      if (isFirstRow) {
        Object.keys(row).forEach((key) => {
          res.write(`<th>${escape(key)}</th>`);
        });
        res.write('</tr></thead><tbody>');
        isFirstRow = false;
      }

      // Add data row
      res.write('<tr>');
      Object.values(row).forEach((value) => {
        res.write(`<td>${value === null ? '' : escape(value as string)}</td>`);
      });
      res.write('</tr>');
    }

    if (!hasData) {
      res.write(`</tr></thead><tbody></tbody>`);
    } else {
      res.write(`</tbody>`);
    }
    res.write(`</table></body></html>`);
    await cubeDBConn.query('COMMIT');
    res.end();
  } catch (err) {
    await cubeDBConn.query('ROLLBACK').catch(() => {});
    logger.error(err, `Error sending HTML for query id ${queryStore.id}`);
    dbStream?.destroy();
    if (!res.headersSent) {
      res.status(500).end();
    }
    throw err;
  } finally {
    await cubeDBConn.release();
  }
}

export async function sendFilters(query: string, res: Response): Promise<void> {
  logger.debug('Sending filters...');
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];

  try {
    const result = await cubeDBConn.query(query);
    const rows = result.rows as FilterRow[];
    const columnData = new Map<string, FilterRow[]>();

    rows.forEach((row: FilterRow) => {
      let data = columnData.get(row.fact_table_column);
      if (data) {
        data.push(row);
      } else {
        data = [row];
      }
      columnData.set(row.fact_table_column, data);
    });

    const filterData: FilterTable[] = [];
    for (const col of columnData.keys()) {
      const data = columnData.get(col);
      if (!data) {
        continue;
      }
      const hierarchy = transformHierarchy(data[0].fact_table_column, data[0].dimension_name, data);
      const flattenedHierarchy = flattenHierarchy(hierarchy.values);
      // If there's a problem with the hierarchy, bin it off
      if (data.length != flattenedHierarchy.length) {
        hierarchy.values = data.map((val) => {
          return {
            reference: val.reference,
            description: val.description
          };
        });
      }
      filterData.push(hierarchy);
    }
    res.json(filterData);
  } catch (err) {
    logger.error(err, 'Error sending filters');
    throw err;
  } finally {
    await cubeDBConn.release();
  }
}

async function runAndRetryQuery(query: string, queryStore: QueryStore, cubeDataSource: DataSource): Promise<never[]> {
  logger.debug(`Fetching view data for query id ${queryStore.id}...`);
  let rows: never[];
  try {
    rows = await cubeDataSource.query(query);
  } catch (error) {
    logger.warn(error, 'Query failed trying alternative view name');
    let retryQuery = query;
    if (query.includes('core_view_mat')) {
      retryQuery = query.replace('core_view_mat', 'core_view');
    } else {
      retryQuery = query.replace('core_view', 'core_view_mat');
    }
    try {
      rows = await cubeDataSource.query(retryQuery);
    } catch (retryError) {
      logger.warn(retryError, 'Query still failed after retrying');
      throw retryError;
    }
    void QueryStoreRepository.rebuildQueriesForRevision(queryStore.revisionId).catch((err) => {
      logger.warn(err, `Rebuild query store entries for revision ${queryStore.revisionId} failed.`);
    });
  }
  return rows;
}

export async function sendFrontendView(
  query: string,
  queryStore: QueryStore,
  pageOptions: PageOptions,
  res: Response
): Promise<void> {
  logger.info(`Sending Frontend View for query id ${queryStore.id}...`);
  const cubeDataSource = dbManager.getCubeDataSource();

  try {
    const { pageNumber = 1, pageSize = queryStore.totalLines, locale } = pageOptions;
    const lang = locale.includes('en') ? 'en-gb' : 'cy-gb';

    const filters = await cubeDataSource.query(
      pgformat(
        'SELECT DISTINCT fact_table_column, dimension_name FROM %I.filter_table WHERE language = %L;',
        queryStore.revisionId,
        lang
      )
    );

    let note_codes: string[] = [];
    try {
      const noteCodeRows = await cubeDataSource.query(
        pgformat(
          `SELECT DISTINCT UNNEST(STRING_TO_ARRAY(code, ',')) AS code FROM %I.all_notes ORDER BY code ASC`,
          queryStore.revisionId
        )
      );
      note_codes = noteCodeRows?.map((row: { code: string }) => row.code) ?? [];
    } catch (error) {
      logger.error(error, `Failed to fetch note codes for revisionId ${queryStore.revisionId}`);
      note_codes = [];
    }

    logger.debug(`Fetching dataset ${queryStore.datasetId}...`);
    const dataset = await DatasetRepository.getById(queryStore.datasetId, { factTable: true, dimensions: true });

    const rows = await runAndRetryQuery(query, queryStore, cubeDataSource);
    logger.debug(`Fetched ${rows.length} rows`);

    // Build headers from the first row if available
    let headers: ReturnType<typeof getColumnHeaders> | undefined;
    if (rows.length > 0) {
      const tableHeaders = Object.keys(rows[0]);
      headers = getColumnHeaders(dataset, tableHeaders, filters);
    }

    // Transform rows to arrays of values
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const data = rows.map((row: any) => Object.values(row));

    const start_record = pageSize * (pageNumber - 1);
    const end_record = start_record + rows.length;
    const total_pages = Math.max(1, Math.ceil(queryStore.totalLines / pageSize));

    const response = {
      dataset: ConsumerDatasetDTO.fromDataset(dataset),
      filters: queryStore.requestObject.filters || [],
      note_codes: note_codes || [],
      ...(headers && { headers }),
      data,
      page_info: {
        current_page: pageNumber,
        page_size: pageSize,
        total_pages,
        total_records: queryStore.totalLines,
        start_record,
        end_record
      }
    };

    res.json(response);
    logger.debug(`Frontend view sent successfully for query id ${queryStore.id}`);
  } catch (err) {
    logger.error(err, `Error sending Frontend View for query id ${queryStore.id}`);
    throw err;
  }
}

export async function buildDataQuery(queryStore: QueryStore, pageOptions: PageOptions): Promise<string> {
  logger.debug(`Building data query from query store id ${queryStore.id}...`);
  const { locale, pageNumber, pageSize, sort } = pageOptions;
  const lang = locale.includes('en') ? 'en-GB' : 'cy-GB';
  let query = queryStore.query[lang] || queryStore.query['en-GB'];

  if (!query) {
    throw new Error(`No query found for language ${lang} or fallback en-GB`);
  }

  if (sort && sort.length > 0) {
    const sortBy: string[] = [];
    const sortColumnPostfix = `_${t('column_headers.sort', { lng: locale })}`;
    const validColumns = queryStore.columnMapping
      .filter((m) => m.language === lang.toLowerCase())
      .map((m) => m.dimension_name);
    validColumns.push(t('column_headers.data_values', { lng: locale }));

    for (const sortOption of sort) {
      const [colName, direction = 'asc'] = sortOption.split('|');
      if (!validColumns.includes(colName)) {
        throw new BadRequestException('errors.invalid_sort_by');
      }
      sortBy.push(pgformat('%I %s', `${colName}${sortColumnPostfix}`, direction.toUpperCase()));
    }

    query = pgformat('%s ORDER BY %s', query, sortBy.join(', '));
  }

  // if no page size is provided we return all rows (which may be zero for the current query)
  const limit = pageSize || queryStore.totalLines;
  const offset = (pageNumber - 1) * limit;

  // prevent div by zero
  const totalPages = limit <= 0 ? 0 : Math.ceil(queryStore.totalLines / limit);

  if (totalPages > 0 && pageNumber > totalPages) {
    throw new BadRequestException('errors.page_number_too_high');
  }

  if (pageNumber) {
    query = pgformat(`%s LIMIT %L OFFSET %L;`, query, limit, offset);
  }

  logger.debug(`Query = ${query}`);

  return query;
}
