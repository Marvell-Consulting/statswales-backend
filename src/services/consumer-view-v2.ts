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
import { Dataset } from '../entities/dataset/dataset';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { flattenHierarchy, sortFilterRows, transformHierarchy } from '../utils/consumer';
import { PageOptions } from '../interfaces/page-options';
import { logger } from '../utils/logger';
import { ConsumerDatasetDTO } from '../dtos/consumer-dataset-dto';
import { getColumnHeaders } from '../utils/column-headers';
import { QueryStoreRepository } from '../repositories/query-store';
import { DataSource } from 'typeorm';
import { DatasetLoader } from './consumer-view';
import {
  CursorDirection,
  CursorKeyValue,
  CursorPayload,
  CURSOR_VERSION,
  computeSortHash,
  decodeCursor,
  encodeCursor
} from '../utils/cursor-codec';
import { KeysetSortColumn, buildKeysetWhere } from './keyset-where-builder';
import { resolveDefaultSort } from './default-sort-resolver';

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

export async function sendFilters(query: string, res: Response, dateColumns: Set<string> = new Set()): Promise<void> {
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
      const unsorted = columnData.get(col);
      if (!unsorted) {
        continue;
      }
      const data = sortFilterRows(unsorted, dateColumns.has(col));
      const hierarchy = transformHierarchy(data[0].fact_table_column, data[0].dimension_name, data);
      const flattenedHierarchy = flattenHierarchy(hierarchy.values);
      // If there's a problem with the hierarchy, bin it off
      if (data.length != flattenedHierarchy.length) {
        hierarchy.values = data.map((val) => {
          return {
            reference: String(val.reference),
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

async function runAndRetryQuery(
  query: string,
  queryStore: QueryStore,
  cubeDataSource: DataSource
): Promise<Record<string, unknown>[]> {
  logger.debug(`Fetching view data for query id ${queryStore.id}...`);
  let rows: Record<string, unknown>[];
  try {
    rows = await cubeDataSource.query(query);
  } catch (error) {
    logger.warn(error, 'Query failed trying alternative view name');
    const retryQuery = query.includes('core_view_mat')
      ? query.replace('core_view_mat', 'core_view')
      : query.replace('core_view', 'core_view_mat');
    try {
      rows = await cubeDataSource.query(retryQuery);
    } catch (retryError) {
      logger.warn(retryError, 'Query still failed after retrying');
      throw retryError;
    }
    void QueryStoreRepository.rebuildQueryEntry(queryStore.id).catch((rebuildError) => {
      logger.warn(rebuildError, `Failed to rebuild query entry for query id ${queryStore.id}`);
    });
  }
  return rows;
}

export async function sendFrontendView(
  buildResult: BuildDataQueryResult,
  queryStore: QueryStore,
  pageOptions: PageOptions,
  res: Response,
  loader: DatasetLoader
): Promise<void> {
  logger.info(`Sending Frontend View for query id ${queryStore.id}...`);
  const cubeDataSource = dbManager.getCubeDataSource();
  const query = buildResult.sql;

  try {
    const { pageNumber = 1, pageSize = queryStore.totalLines, locale, cursor } = pageOptions;
    const lang = locale.includes('en') ? 'en-gb' : 'cy-gb';
    const langForHash = locale.includes('en') ? 'en-GB' : 'cy-GB';

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
    const dataset = await loader(queryStore.datasetId, { factTable: true, dimensions: true });

    let rows = await runAndRetryQuery(query, queryStore, cubeDataSource);
    logger.debug(`Fetched ${rows.length} rows`);

    // Cursor mode over-fetches one row to detect whether more rows exist;
    // pop the trailing row before shaping the response, then reverse if we
    // were walking backwards.
    let hasMore = false;
    if (buildResult.mode === 'cursor' && rows.length > pageSize) {
      hasMore = true;
      rows = rows.slice(0, pageSize);
    }
    if (buildResult.mode === 'cursor' && buildResult.direction === 'b') {
      rows = rows.slice().reverse();
    }

    // Capture sort-key values per row before we drop the `_sort` columns —
    // we need them later when building next_cursor / prev_cursor. In offset
    // mode we also harvest the keys so the response carries an initial cursor
    // for the frontend's switch into cursor mode at the page-cap boundary.
    const sortIdents = buildResult.sortPlan?.map((s) => s.sortIdent) ?? [];
    const rowKeyValues: Record<string, unknown>[] = rows.map((row) => {
      const captured: Record<string, unknown> = {};
      for (const ident of sortIdents) captured[ident] = (row as Record<string, unknown>)[ident];
      return captured;
    });

    // Strip injected `_sort` columns so the client only sees display columns.
    if (sortIdents.length > 0) {
      const dropSet = new Set(sortIdents);
      rows = rows.map((row) => {
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(row as Record<string, unknown>)) {
          if (!dropSet.has(k)) out[k] = v;
        }
        return out;
      });
    }

    // Build headers from the first row if available
    let headers: ReturnType<typeof getColumnHeaders> | undefined;
    if (rows.length > 0) {
      const tableHeaders = Object.keys(rows[0]);
      headers = getColumnHeaders(dataset, tableHeaders, filters);
    }

    // Transform rows to arrays of values
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const data = rows.map((row: any) => Object.values(row));

    const total_pages = Math.max(1, Math.ceil(queryStore.totalLines / pageSize));

    // current_page / start_record / end_record are meaningless under cursor
    // pagination — the response just carries totals + cursors there.
    const isCursorMode = buildResult.mode === 'cursor';
    const start_record = isCursorMode ? null : pageSize * (pageNumber - 1);
    const end_record = isCursorMode ? null : (start_record ?? 0) + rows.length;
    const current_page = isCursorMode ? null : pageNumber;

    let next_cursor: string | null = null;
    let prev_cursor: string | null = null;
    if (buildResult.sortPlan && buildResult.sortPlan.length > 0 && buildResult.sortHash && rowKeyValues.length > 0) {
      // Offset mode emits next_cursor whenever there are more rows after the
      // current page — it's the seam the frontend uses to swap from
      // page_number paging into cursor paging. Cursor mode emits next_cursor
      // only when there is genuinely more data ahead (the over-fetch row).
      const lastKeys = rowKeyValues[rowKeyValues.length - 1];
      const firstKeys = rowKeyValues[0];
      const offsetHasMore = !isCursorMode && pageSize * pageNumber < queryStore.totalLines;

      if ((!isCursorMode && offsetHasMore) || (isCursorMode && hasMore)) {
        next_cursor = buildCursorFromRow(
          lastKeys,
          buildResult.sortPlan,
          'f',
          queryStore,
          langForHash,
          buildResult.sortHash
        );
      }
      if (isCursorMode && cursor) {
        prev_cursor = buildCursorFromRow(
          firstKeys,
          buildResult.sortPlan,
          'b',
          queryStore,
          langForHash,
          buildResult.sortHash
        );
      }
    }

    const response = {
      dataset: ConsumerDatasetDTO.fromDataset(dataset),
      filters: queryStore.requestObject.filters || [],
      note_codes: note_codes || [],
      ...(headers && { headers }),
      data,
      page_info: {
        current_page,
        page_size: pageSize,
        total_pages,
        total_records: queryStore.totalLines,
        start_record,
        end_record,
        next_cursor,
        prev_cursor
      }
    };

    res.json(response);
    logger.debug(`Frontend view sent successfully for query id ${queryStore.id}`);
  } catch (err) {
    logger.error(err, `Error sending Frontend View for query id ${queryStore.id}`);
    throw err;
  }
}

export interface SortColumnPlan {
  // Display name as it appears in the core view (e.g. 'Year', 'Ardal').
  displayName: string;
  // The SQL identifier used in ORDER BY and the keyset WHERE — display
  // name plus the translated `_sort` postfix (e.g. 'Year_sort').
  sortIdent: string;
  direction: 'asc' | 'desc';
}

export type BuildDataMode = 'bulk' | 'offset' | 'cursor';

export interface BuildDataQueryResult {
  sql: string;
  mode: BuildDataMode;
  // Populated whenever the query is paginated. sendFrontendView consumes
  // this to build next_cursor / prev_cursor from the returned rows.
  sortPlan?: SortColumnPlan[];
  // Forward or backward traversal — meaningful in cursor mode only.
  direction?: CursorDirection;
  // Hash used to bind cursors to a specific sort spec + language.
  sortHash?: string;
}

export async function buildDataQuery(
  queryStore: QueryStore,
  pageOptions: PageOptions,
  dataset?: Dataset
): Promise<BuildDataQueryResult> {
  logger.debug(`Building data query from query store id ${queryStore.id}...`);
  const { locale, pageNumber, pageSize, sort, cursor } = pageOptions;
  const lang = locale.includes('en') ? 'en-GB' : 'cy-GB';
  const baseQuery = queryStore.query[lang] || queryStore.query['en-GB'];

  if (!baseQuery) {
    throw new Error(`No query found for language ${lang} or fallback en-GB`);
  }

  // Bulk export path (no pagination, no sort coercion, no cursor support).
  if (pageSize === undefined) {
    if (cursor) throw new BadRequestException('errors.cursor_unsupported_for_format');
    logger.debug(`Query = ${baseQuery}`);
    return { sql: baseQuery, mode: 'bulk' };
  }

  const sortPlan = resolveSortPlan(sort, queryStore, dataset, locale, lang);
  const orderByClause = sortPlan.length > 0 ? buildOrderByClause(sortPlan) : '';
  const sortHash = sortPlan.length > 0 ? computeSortHashForPlan(sortPlan, lang) : undefined;

  if (cursor) {
    if (sortPlan.length === 0) {
      // Without a resolvable order there is nothing to key off.
      throw new BadRequestException('errors.cursor_requires_sort');
    }

    const payload = decodeCursor(cursor, {
      queryStoreId: queryStore.id,
      revisionId: queryStore.revisionId,
      language: lang,
      sortHash: sortHash!,
      keyArity: sortPlan.length
    });

    const sql = buildCursorSql(baseQuery, sortPlan, payload, pageSize, payload.d);
    logger.debug(`Query = ${sql}`);
    return { sql, mode: 'cursor', sortPlan, direction: payload.d, sortHash };
  }

  // Offset path.
  const offset = (pageNumber - 1) * pageSize;
  const totalPages = pageSize <= 0 ? 0 : Math.ceil(queryStore.totalLines / pageSize);
  if (totalPages > 0 && pageNumber > totalPages) {
    throw new BadRequestException('errors.page_number_too_high');
  }

  // Inject `_sort` columns so sendFrontendView can build a usable next_cursor
  // from the returned rows (offset mode is the entry point the frontend uses
  // before swapping into cursor mode at the page-cap boundary).
  const queryWithSortIdents =
    sortPlan.length > 0
      ? injectSortIdentsIntoSelect(
          baseQuery,
          sortPlan.map((s) => s.sortIdent)
        )
      : baseQuery;

  const orderedQuery = orderByClause ? pgformat('%s %s', queryWithSortIdents, orderByClause) : queryWithSortIdents;
  const sql = pgformat(`%s LIMIT %L OFFSET %L;`, orderedQuery, pageSize, offset);
  logger.debug(`Query = ${sql}`);
  return { sql, mode: 'offset', sortPlan: sortPlan.length > 0 ? sortPlan : undefined, sortHash };
}

function resolveSortPlan(
  userSort: string[],
  queryStore: QueryStore,
  dataset: Dataset | undefined,
  locale: string,
  lang: string
): SortColumnPlan[] {
  const sortPostfix = `_${t('column_headers.sort', { lng: locale })}`;
  const langKey = lang.toLowerCase();
  const validColumns = queryStore.columnMapping.filter((m) => m.language === langKey).map((m) => m.dimension_name);
  validColumns.push(t('column_headers.data_values', { lng: locale }));
  const validSet = new Set(validColumns);

  const entries: Array<{ displayName: string; direction: 'asc' | 'desc' }> = [];
  const seen = new Set<string>();

  // 1. User-supplied sort_by first, with their chosen direction.
  for (const sortOption of userSort ?? []) {
    const [colName, direction = 'asc'] = sortOption.split('|');
    if (!validSet.has(colName)) {
      throw new BadRequestException('errors.invalid_sort_by');
    }
    if (seen.has(colName)) continue;
    seen.add(colName);
    entries.push({ displayName: colName, direction: direction.toLowerCase() === 'desc' ? 'desc' : 'asc' });
  }

  // 2. Append the default sort / PK tie-breakers in ASC for determinism.
  //    resolveDefaultSort picks the time column first, falls back to first dim,
  //    then appends remaining PK columns. We skip anything already in entries.
  if (dataset) {
    const defaults = resolveDefaultSort(dataset.factTable, queryStore.columnMapping, lang);
    for (const d of defaults) {
      if (seen.has(d.columnName)) continue;
      seen.add(d.columnName);
      entries.push({ displayName: d.columnName, direction: d.direction });
    }
  }

  return entries.map((e) => ({
    displayName: e.displayName,
    sortIdent: `${e.displayName}${sortPostfix}`,
    direction: e.direction
  }));
}

function buildOrderByClause(sortPlan: SortColumnPlan[]): string {
  const parts = sortPlan.map((s) => pgformat('%I %s', s.sortIdent, s.direction.toUpperCase()));
  return `ORDER BY ${parts.join(', ')}`;
}

function computeSortHashForPlan(sortPlan: SortColumnPlan[], lang: string): string {
  return computeSortHash(
    sortPlan.map((s) => ({ columnName: s.displayName, direction: s.direction })),
    lang
  );
}

function buildCursorSql(
  baseQuery: string,
  sortPlan: SortColumnPlan[],
  payload: CursorPayload,
  pageSize: number,
  direction: CursorDirection
): string {
  // For backward traversal we flip every ORDER BY direction so the LIMIT
  // grabs rows immediately preceding the cursor; sendFrontendView reverses
  // them client-side before emitting.
  const orderedPlan: SortColumnPlan[] =
    direction === 'b' ? sortPlan.map((s) => ({ ...s, direction: s.direction === 'asc' ? 'desc' : 'asc' })) : sortPlan;

  const orderBy = buildOrderByClause(orderedPlan);
  const keysetColumns: KeysetSortColumn[] = sortPlan.map((s) => ({
    sqlIdent: s.sortIdent,
    direction: s.direction
  }));
  const where = buildKeysetWhere(keysetColumns, payload.k, direction);

  // The base query's SELECT projects display columns only (e.g. "Year",
  // "Area"). Keyset comparisons need the `_sort` variants of those columns,
  // so we amend the inner SELECT to also project them. sendFrontendView
  // strips them before returning rows to the client.
  const sortIdents = sortPlan.map((s) => s.sortIdent);
  const innerQuery = injectSortIdentsIntoSelect(baseQuery, sortIdents);

  // Wrap as a subquery so we don't have to reason about whether the inner
  // query already has WHERE / ORDER BY clauses.
  return pgformat(`SELECT * FROM (%s) AS t WHERE %s %s LIMIT %L;`, innerQuery, where, orderBy, pageSize + 1);
}

// Add `_sort` identifiers to the SELECT list of `baseQuery` so a wrapping
// subquery can reference them. No-op when the base SELECT is `*` or already
// projects every required identifier. Conservatively falls through if the
// SELECT clause doesn't match our expected shape — the caller will then
// surface a clear SQL error.
function injectSortIdentsIntoSelect(baseQuery: string, sortIdents: string[]): string {
  const match = baseQuery.match(/^(\s*SELECT\s+)(.*?)(\s+FROM\s+[\s\S]+)$/i);
  if (!match) return baseQuery;
  const [, prefix, cols, rest] = match;
  const trimmed = cols.trim();
  if (trimmed === '*') return baseQuery;

  const missing = sortIdents.filter((id) => !columnAlreadyProjected(cols, id));
  if (missing.length === 0) return baseQuery;

  const extras = missing.map((id) => pgformat('%I', id)).join(', ');
  return `${prefix}${cols}, ${extras}${rest}`;
}

function columnAlreadyProjected(selectList: string, ident: string): boolean {
  // Cheap presence check — the base SELECT only ever quotes identifiers
  // ("Area_sort") so a substring test catches the existing projections
  // without parsing the SQL.
  return selectList.includes(`"${ident}"`);
}

// Build the cursor payload for the row at `index` in the returned dataset,
// given the resolved sort plan. The key tuple is composed from the row's
// sort-postfixed columns so the next keyset query can resume past it.
export function buildCursorFromRow(
  row: Record<string, unknown>,
  sortPlan: SortColumnPlan[],
  direction: CursorDirection,
  queryStore: QueryStore,
  language: string,
  sortHash: string
): string {
  const key: CursorKeyValue[] = sortPlan.map((s) => normaliseKeyValue(row[s.sortIdent]));
  const payload: CursorPayload = {
    v: CURSOR_VERSION,
    q: queryStore.id,
    r: queryStore.revisionId,
    l: language,
    h: sortHash,
    d: direction,
    k: key
  };
  return encodeCursor(payload);
}

function normaliseKeyValue(v: unknown): CursorKeyValue {
  if (v === null || v === undefined) return null;
  const t = typeof v;
  if (t === 'string' || t === 'number' || t === 'boolean') return v as string | number | boolean;
  // BIGINT comes back from node-postgres as a string by default; Date objects
  // stringify safely. Fall back to JSON-roundtrip via String() for anything
  // else exotic — the keyset comparison happens server-side via pgformat %L,
  // which handles the implicit cast back to the column's Postgres type.
  return String(v);
}
