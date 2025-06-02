import { Revision } from '../entities/dataset/revision';
import { validateParams } from '../validators/preview-validator';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetDTO } from '../dtos/dataset-dto';
import { Dataset } from '../entities/dataset/dataset';
import { pool } from '../app';
import { logger } from '../utils/logger';
import { QueryResult } from 'pg';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { DatasetRepository } from '../repositories/dataset';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { FilterInterface } from '../interfaces/filterInterface';

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
  if (sortBy && sortBy.length > 1) {
    sortByQuery = sortBy
      .map((sort) => pgformat(`%I %s`, sort.column, sort.direction ? sort.direction : 'DESC'))
      .join(', ');
  }
  let filterQuery = '';
  if (filter && filter.length > 0) {
    filterQuery = filter
      .map((whereClause) => pgformat('%I in (%L)', whereClause.columnName, whereClause.values))
      .join(' and ');
  }
  logger.debug(`revision ID: ${revision.id}, view: default_view_${lang}`);
  const baseQuery = pgformat(
    'SELECT * FROM %I.%I %s %s LIMIT %L OFFSET %L',
    revision.id,
    `default_view_${lang}`,
    filterQuery ? `WHERE ${filterQuery}` : '',
    sortByQuery ? `ORDER BY ${sortByQuery}` : '',
    pageSize,
    (pageNumber - 1) * pageSize
  );
  logger.debug(`Base query: ${baseQuery}`);

  try {
    const totalsQuery = pgformat(
      'SELECT count(*) as "totalLines", ceil(count(*)/%L) as "totalPages" from (%s);',
      pageSize,
      baseQuery
    );
    logger.debug(`Totals query: ${totalsQuery}`);
    const totals = await pool.query(totalsQuery);
    const totalPages = Number(totals.rows[0].totalPages);
    const totalLines = Number(totals.rows[0].totalLines);
    const errors = validateParams(pageNumber, totalPages, pageSize);

    if (errors.length > 0) {
      return { status: 400, errors, dataset_id: dataset.id };
    }

    const queryResult: QueryResult<unknown[]> = await pool.query(baseQuery);
    const preview = queryResult.rows;

    const startLine = pageSize * (pageNumber - 1) + 1;
    const lastLine = pageNumber * pageSize + pageSize;
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
