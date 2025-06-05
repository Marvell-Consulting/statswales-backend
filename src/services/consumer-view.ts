import { Revision } from '../entities/dataset/revision';
import { validateParams } from '../validators/preview-validator';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetDTO } from '../dtos/dataset-dto';
import { Dataset } from '../entities/dataset/dataset';
import { logger } from '../utils/logger';
import { QueryResult } from 'pg';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { DatasetRepository } from '../repositories/dataset';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { FilterInterface } from '../interfaces/filterInterface';
import { getCubeDB } from '../db/cube-db';

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
  const cubeDB = getCubeDB();
  const filterTableQuery = pgformat('SELECT * FROM %I.filter_table WHERE language = %L;', revision.id, language);
  const filterTable: QueryResult<FilterRow> = await cubeDB.query(filterTableQuery);
  const columnData = new Map<string, FilterRow[]>();
  for (const row of filterTable.rows) {
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
};

export const createView = async (
  dataset: Dataset,
  revision: Revision,
  lang: string,
  pageNumber: number,
  pageSize: number,
  sortBy?: SortByInterface[],
  filter?: FilterInterface[]
): Promise<ViewDTO | ViewErrDTO> => {
  let sortByQuery = '';
  if (sortBy && sortBy.length > 0) {
    logger.debug('Multiple sort by columns are present. Creating sort by query');
    sortByQuery = sortBy
      .map((sort) => pgformat(`%I %s`, sort.column, sort.direction ? sort.direction : 'ASC'))
      .join(', ');
  }
  let filterQuery = '';
  if (filter && filter.length > 0) {
    logger.debug('Filters are present. Creating filter query');
    filterQuery = filter
      .map((whereClause) => pgformat('%I in (%L)', whereClause.columnName, whereClause.values))
      .join(' and ');
  }

  const baseQuery = pgformat(
    'SELECT * FROM %I.%I %s %s',
    revision.id,
    `default_view_${lang}`,
    filterQuery ? `WHERE ${filterQuery}` : '',
    sortByQuery ? `ORDER BY ${sortByQuery}` : ''
  );
  // logger.debug(`Base query: ${baseQuery}`);

  try {
    const cubeDB = getCubeDB();
    const totalsQuery = pgformat(
      'SELECT count(*) as "totalLines", ceil(count(*)/%L) as "totalPages" from (%s);',
      pageSize,
      baseQuery
    );
    // logger.debug(`Totals query: ${totalsQuery}`);
    const totals = await cubeDB.query(totalsQuery);
    const totalPages = Number(totals.rows[0].totalPages) > 0 ? Number(totals.rows[0].totalPages) : 1;
    const totalLines = Number(totals.rows[0].totalLines);
    const errors = validateParams(pageNumber, totalPages, pageSize);

    if (errors.length > 0) {
      return { status: 400, errors, dataset_id: dataset.id };
    }

    const dataQuery = pgformat('%s LIMIT %L OFFSET %L', baseQuery, pageSize, (pageNumber - 1) * pageSize);
    // logger.debug(`Data query: ${dataQuery}`);
    const queryResult: QueryResult<unknown[]> = await cubeDB.query(dataQuery);
    const preview = queryResult.rows;

    const startLine = pageSize * (pageNumber - 1) + 1;
    const lastLine = pageNumber * pageSize + pageSize;

    // PATCH: Handle empty preview result
    if (!preview || preview.length === 0) {
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

    const tableHeaders = Object.keys(preview[0]);
    const dataArray = preview.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);

    const headers: CSVHeader[] = tableHeaders.map((header, idx) => ({
      index: idx - 1,
      name: header,
      source_type: header === 'int_line_number' ? FactTableColumnType.LineNumber : FactTableColumnType.Unknown
    }));
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
      data: dataArray
    };
  } catch (err) {
    logger.error(err, `Something went wrong trying to create the cube preview`);
    return { status: 500, errors: [], dataset_id: dataset.id };
  }
};
