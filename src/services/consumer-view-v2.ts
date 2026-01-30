import { Response } from 'express';
import { escape } from 'lodash';
import { PoolClient } from 'pg';
import Cursor from 'pg-cursor';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { format as csvFormat } from '@fast-csv/format';
import ExcelJS from 'exceljs';
import { t } from 'i18next';

import { FilterRow } from '../interfaces/filter-row';
import { FilterTable } from '../interfaces/filter-table';
import { dbManager } from '../db/database-manager';
import { QueryStore } from '../entities/query-store';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { transformHierarchy } from '../utils/consumer';
import { DatasetRepository } from '../repositories/dataset';
import { PageOptions } from '../interfaces/page-options';
import { logger } from '../utils/logger';
import { ConsumerDatasetDTO } from '../dtos/consumer-dataset-dto';
import { getColumnHeaders } from '../utils/column-headers';

const EXCEL_ROW_LIMIT = 1048576 - 76; // Excel Limit is 1,048,576 but removed 76 rows because ?
const CURSOR_ROW_LIMIT = 500;

export async function sendCsv(query: string, queryStore: QueryStore, res: Response): Promise<void> {
  logger.debug(`Sending CSV for query id ${queryStore.id}...`);
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  let cursor: Cursor | null = null;

  try {
    cursor = cubeDBConn.query(new Cursor(query));
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
      stream.end();
    } else {
      res.write('\n');
    }
  } catch (err) {
    logger.error(err, `Error sending CSV for query id ${queryStore.id}`);
    throw err;
  } finally {
    if (res.headersSent) res.end();
    if (cursor) {
      try {
        await cursor.close();
      } catch (cursorErr) {
        logger.warn(cursorErr, 'Failed to close cursor');
      }
    }
    await cubeDBConn.release();
  }
}

export async function sendExcel(query: string, queryStore: QueryStore, res: Response): Promise<void> {
  logger.debug(`Sending Excel for query id ${queryStore.id}...`);
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  let cursor: Cursor | null = null;

  try {
    cursor = cubeDBConn.query(new Cursor(query));
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
          const data = Object.values(row).map((val) => {
            if (!val) return null;
            return isNaN(Number(val)) ? val : Number(val);
          });
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
  } catch (err) {
    logger.error(err, `Error sending Excel for query id ${queryStore.id}`);
    throw err;
  } finally {
    if (res.headersSent) res.end();
    if (cursor) {
      try {
        await cursor.close();
      } catch (cursorErr) {
        logger.warn(cursorErr, 'Failed to close cursor');
      }
    }
    await cubeDBConn.release();
  }
}

export async function sendJson(query: string, queryStore: QueryStore, res: Response): Promise<void> {
  logger.debug(`Sending JSON for query id ${queryStore.id}...`);
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  let cursor: Cursor | null = null;

  try {
    cursor = cubeDBConn.query(new Cursor(query));
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
  } catch (err) {
    logger.error(err, `Error sending JSON for query id ${queryStore.id}`);
    throw err;
  } finally {
    if (res.headersSent) res.end();
    if (cursor) {
      try {
        await cursor.close();
      } catch (cursorErr) {
        logger.warn(cursorErr, 'Failed to close cursor');
      }
    }
    await cubeDBConn.release();
  }
}

export async function sendHtml(query: string, queryStore: QueryStore, res: Response): Promise<void> {
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  let cursor: Cursor | null = null;

  try {
    cursor = cubeDBConn.query(new Cursor(query));
    res.setHeader('content-type', 'text/html');
    res.flushHeaders();
    res.write(
      '<!DOCTYPE html>\n' +
        '<html lang="en">\n' +
        '<head>\n' +
        '    <meta charset="utf-8">\n' +
        `    <title>${queryStore.datasetId}</title>\n` +
        '</head>\n' +
        '<body>\n' +
        '<table>\n' +
        '<thead><tr>'
    );

    let rows = await cursor.read(CURSOR_ROW_LIMIT);
    if (rows.length === 0) {
      // No rows returned; close the table and document without headers or body rows.
      res.write('</tr></thead><tbody></tbody>\n' + '</table>\n' + '</body>\n' + '</html>\n');
      return;
    }
    Object.keys(rows[0]).forEach((key) => {
      res.write(`<th>${escape(key)}</th>`);
    });
    res.write('</tr></thead><tbody>');
    while (rows.length > 0) {
      for (const row of rows) {
        res.write('<tr>');
        Object.values(row).forEach((value) => {
          res.write(`<td>${value === null ? '' : escape(value as string)}</td>`);
        });
        res.write('</tr>');
      }
      rows = await cursor.read(CURSOR_ROW_LIMIT);
    }
    res.write('</tbody>\n' + '</table>\n' + '</body>\n' + '</html>\n');
  } catch (err) {
    logger.error(err, `Error sending HTML for query id ${queryStore.id}`);
    throw err;
  } finally {
    if (res.headersSent) res.end();
    if (cursor) {
      try {
        await cursor.close();
      } catch (cursorErr) {
        logger.warn(cursorErr, 'Failed to close cursor');
      }
    }
    await cubeDBConn.release();
  }
}

export async function sendFilters(query: string, res: Response): Promise<void> {
  logger.debug('Sending filters...');
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];
  let cursor: Cursor | null = null;

  try {
    cursor = cubeDBConn.query(new Cursor(query));
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
    res.json(filterData);
  } catch (err) {
    logger.error(err, 'Error sending filters');
    throw err;
  } finally {
    if (cursor) {
      try {
        await cursor.close();
      } catch (cursorErr) {
        logger.warn(cursorErr, 'Failed to close cursor');
      }
    }
    await cubeDBConn.release();
  }
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

    logger.debug(`Fetching view data for query id ${queryStore.id}...`);
    const rows = await cubeDataSource.query(query);
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

    for (const sortOption of sort) {
      const [colName, direction = 'asc'] = sortOption.split('|');
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
