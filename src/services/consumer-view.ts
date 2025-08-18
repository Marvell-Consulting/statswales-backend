import { Response } from 'express';
import ExcelJS from 'exceljs';
import { PoolClient, QueryResult } from 'pg';
import Cursor from 'pg-cursor';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { format as csvFormat } from '@fast-csv/format';

import { Revision } from '../entities/dataset/revision';
import { validateParams } from '../validators/preview-validator';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DatasetDTO } from '../dtos/dataset-dto';
import { Dataset } from '../entities/dataset/dataset';
import { logger } from '../utils/logger';
import { DatasetRepository } from '../repositories/dataset';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { FilterInterface } from '../interfaces/filterInterface';
import { dbManager } from '../db/database-manager';
import { CORE_VIEW_NAME } from './cube-handler';
import { getColumnHeaders } from '../utils/column-headers';
import { t } from 'i18next';

const EXCEL_ROW_LIMIT = 1048576;
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

export const getFilters = async (revision: Revision, language: string): Promise<FilterTable[]> => {
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    const filterTableQuery = pgformat('SELECT * FROM %I.filter_table WHERE language = %L;', revision.id, language);
    const filterTable: FilterRow[] = await cubeDB.query(filterTableQuery);
    const columnData = new Map<string, FilterRow[]>();

    for (const row of filterTable) {
      let data = columnData.get(row.fact_table_column);
      if (data) {
        data.push(row);
      } else {
        data = [row];
      }
      columnData.set(row.fact_table_column, data);
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

    return filterData;
  } catch (err) {
    logger.error(err, 'Something went wrong trying to get the filter table from the database server');
    throw err;
  } finally {
    cubeDB.release();
  }
};

function createBaseQuery(
  revision: Revision,
  view: string,
  lang: string,
  columns: string[],
  sortBy?: SortByInterface[],
  filterBy?: FilterInterface[]
): string {
  let sortByQuery: string | undefined;
  const sortColumnPostfix = `_${t('column_headers.sort', { lng: lang })}`;

  try {
    if (sortBy && sortBy.length > 0) {
      logger.debug('Multiple sort by columns are present. Creating sort by query');
      sortByQuery = sortBy
        .map((sort) =>
          pgformat(
            `%I %s, %I %s`,
            `${sort.columnName}${sortColumnPostfix}`,
            sort.direction ? sort.direction : 'ASC',
            sort.columnName,
            sort.direction ? sort.direction : 'ASC'
          )
        )
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
        .map((whereClause) => pgformat('%I in (%L)', whereClause.columnName, whereClause.values))
        .join(' and ');
    }
  } catch (err) {
    logger.error(err, `Something went wrong trying to filter: ${JSON.stringify(filterBy)}`);
    throw err;
  }

  if (columns[0] === '*') {
    return pgformat(
      'SELECT * FROM %I.%I %s %s',
      revision.id,
      view,
      filterQuery ? `WHERE ${filterQuery}` : '',
      sortByQuery ? `ORDER BY ${sortByQuery}` : ''
    );
  } else {
    return pgformat(
      'SELECT %s FROM %I.%I %s %s',
      columns.join(', '),
      revision.id,
      view,
      filterQuery ? `WHERE ${filterQuery}` : '',
      sortByQuery ? `ORDER BY ${sortByQuery}` : ''
    );
  }
}

async function coreViewChooser(cubeDBConn: PoolClient, lang: string, revision: Revision): Promise<string> {
  const availableMaterializedView: QueryResult<{ matviewname: string }> = await cubeDBConn.query(
    pgformat(
      `SELECT * FROM pg_matviews WHERE matviewname = %L AND schemaname = %L;`,
      `${CORE_VIEW_NAME}_mat_${lang}`,
      revision.id
    )
  );

  if (availableMaterializedView.rows.length > 0) {
    return `${CORE_VIEW_NAME}_mat_${lang}`;
  } else {
    return `${CORE_VIEW_NAME}_${lang}`;
  }
}

async function getColumns(cubeDBConn: PoolClient, revisionId: string, lang: string, view: string): Promise<string[]> {
  const columnsMetadata: QueryResult<{ value: string }> = await cubeDBConn.query(
    pgformat(`SELECT value FROM %I.metadata WHERE key = %L`, revisionId, `${view}_${lang}_columns`)
  );
  let columns = ['*'];
  if (columnsMetadata.rows.length > 0) {
    columns = JSON.parse(columnsMetadata.rows[0].value) as string[];
  }
  return columns;
}

export const createFrontendView = async (
  dataset: Dataset,
  revision: Revision,
  lang: string,
  pageNumber: number,
  pageSize: number,
  sortBy?: SortByInterface[],
  filterBy?: FilterInterface[]
): Promise<ViewDTO | ViewErrDTO> => {
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];

  try {
    await cubeDBConn.query(pgformat(`SET search_path TO %I;`, revision.id));
    const baseQuery = createBaseQuery(
      revision,
      await coreViewChooser(cubeDBConn, lang, revision),
      lang,
      await getColumns(cubeDBConn, revision.id, lang, 'frontend'),
      sortBy,
      filterBy
    );
    logger.debug(baseQuery);
    const totalsQuery = pgformat('SELECT count(*) as "totalLines" from (%s);', baseQuery);
    const totals = await cubeDBConn.query(totalsQuery);
    const totalLines = Number(totals.rows[0].totalLines);
    const totalPages = Math.max(1, Math.ceil(totalLines / pageSize));
    const errors = validateParams(pageNumber, totalPages, pageSize);

    if (errors.length > 0) {
      return { status: 400, errors, dataset_id: dataset.id };
    }

    const dataQuery = pgformat('%s LIMIT %L OFFSET %L', baseQuery, pageSize, (pageNumber - 1) * pageSize);
    const preview = await cubeDBConn.query(dataQuery);
    const startLine = pageSize * (pageNumber - 1) + 1;

    // PATCH: Handle empty preview result
    if (!preview || preview.rows.length === 0) {
      const currentDataset = await DatasetRepository.getById(dataset.id);
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

    const filterTable = await cubeDBConn.query(
      pgformat(
        `SELECT DISTINCT fact_table_column, dimension_name FROM "filter_table" WHERE language = %L`,
        `${lang}-gb`
      )
    );

    const tableHeaders = Object.keys(preview.rows[0]);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const data = preview.rows.map((row: any) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id, { factTable: true, dimensions: true });
    const lastLine = startLine + data.length - 1;
    const headers = getColumnHeaders(currentDataset, tableHeaders, filterTable.rows);
    let note_codes: string[] = [];

    try {
      note_codes = (
        await cubeDBConn.query(
          `SELECT DISTINCT UNNEST(STRING_TO_ARRAY(code, ',')) AS code
            FROM "all_notes"
            ORDER BY code ASC`
        )
      ).rows?.map((row) => row.code);
    } catch (err) {
      logger.error(err, `Something went wrong trying to fetch the used note codes`);
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
  } catch (err) {
    logger.error(err, `Something went wrong trying to create the cube preview`);
    return { status: 500, errors: [], dataset_id: dataset.id };
  } finally {
    cubeDBConn.release();
  }
};

export const createStreamingJSONFilteredView = async (
  res: Response,
  revision: Revision,
  lang: string,
  sortBy?: SortByInterface[],
  filterBy?: FilterInterface[]
): Promise<void> => {
  // queryRunner.query() does not support Cursor so we need to obtain underlying PostgreSQL connection
  const [cubeDBConn] = (await dbManager.getCubeDataSource().driver.obtainMasterConnection()) as [PoolClient];

  try {
    await cubeDBConn.query(pgformat(`SET search_path TO %I;`, revision.id));
    const view = await coreViewChooser(cubeDBConn, lang, revision);
    const columns = await getColumns(cubeDBConn, lang, 'raw');
    const baseQuery = createBaseQuery(revision, view, columns, sortBy, filterBy);
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
  revision: Revision,
  lang: string,
  sortBy?: SortByInterface[],
  filterBy?: FilterInterface[]
): Promise<void> => {
  // queryRunner.query() does not support Cursor so we need to obtain underlying PostgreSQL connection
  const [cubeDBConn] = await dbManager.getCubeDataSource().driver.obtainMasterConnection();

  try {
    await cubeDBConn.query(pgformat(`SET search_path TO %I;`, revision.id));
    const view = await coreViewChooser(cubeDBConn, 'raw', lang, revision);
    const columns = await getColumns(cubeDBConn, lang);
    const baseQuery = createBaseQuery(revision, view, columns, sortBy, filterBy);
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
  revision: Revision,
  lang: string,
  sortBy?: SortByInterface[],
  filterBy?: FilterInterface[]
): Promise<void> => {
  // queryRunner.query() does not support Cursor so we need to obtain underlying PostgreSQL connection
  const [cubeDBConn] = await dbManager.getCubeDataSource().driver.obtainMasterConnection();

  try {
    await cubeDBConn.query(pgformat(`SET search_path TO %I;`, revision.id));
    const view = await coreViewChooser(cubeDBConn, 'raw', lang, revision);
    const columns = await getColumns(cubeDBConn, lang);
    const baseQuery = createBaseQuery(revision, view, columns, sortBy, filterBy);
    const cursor = cubeDBConn.query(new Cursor(baseQuery));
    let rows = await cursor.read(CURSOR_ROW_LIMIT);
    res.writeHead(200, {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Content-disposition': `attachment;filename=${revision.id}.xlsx`
    });
    res.flushHeaders();
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      filename: `${revision.id}.xlsx`,
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
