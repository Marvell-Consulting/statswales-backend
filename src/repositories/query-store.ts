import { createHash } from 'node:crypto';

import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { customAlphabet } from 'nanoid';

import { dataSource } from '../db/data-source';
import { dbManager } from '../db/database-manager';
import { DataOptionsDTO, DEFAULT_DATA_OPTIONS } from '../dtos/data-options-dto';
import { QueryStore } from '../entities/query-store';
import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';
import { SUPPORTED_LOCALES, t } from '../middleware/translation';
import {
  checkAvailableViews,
  getFilterTable,
  coreViewChooser,
  getColumns,
  resolveDimensionToFactTableColumn,
  resolveFactColumnToDimension,
  resolveFactDescriptionToReference
} from '../utils/consumer';
import { FilterRow } from '../interfaces/filter-row';

const nanoId = customAlphabet('1234567890abcdefghjklmnpqrstuvwxy', 12);

function generateHash(datasetId: string, revisionId: string, options: DataOptionsDTO): string {
  return createHash('sha256')
    .update(`${datasetId}:${revisionId}:${JSON.stringify(options)}`)
    .digest('hex');
}

function createBaseQuery(
  revisionId: string,
  view: string,
  locale: string,
  columns: string[],
  filterTable: FilterRow[],
  dataOptions?: DataOptionsDTO
): string {
  const refColumnPostfix = `_${t('column_headers.reference', { lng: locale })}`;
  const useRefValues = dataOptions?.options?.use_reference_values ?? true;
  const useRawColumns = dataOptions?.options?.use_raw_column_names ?? true;
  const filters: string[] = [];

  if (dataOptions?.filters && dataOptions?.filters.length > 0) {
    for (const filter of dataOptions.filters) {
      let colName = Object.keys(filter)[0];
      let filterValues = Object.values(filter)[0];
      if (useRawColumns) {
        colName = resolveFactColumnToDimension(colName, locale, filterTable);
      } else {
        resolveDimensionToFactTableColumn(colName, filterTable);
      }
      if (!useRefValues) {
        const filterTableValues = filterTable.filter(
          (row) => row.fact_table_column.toLowerCase() === colName.toLowerCase()
        );
        filterValues = resolveFactDescriptionToReference(filterValues, filterTableValues);
      }
      colName = `${colName}${refColumnPostfix}`;
      filters.push(pgformat('%I in (%L)', colName, filterValues));
    }
  }

  if (columns[0] === '*') {
    return pgformat(
      'SELECT * FROM %I.%I %s',
      revisionId,
      view,
      filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : ''
    );
  } else {
    return pgformat(
      'SELECT %s FROM %I.%I %s',
      columns.join(', '),
      revisionId,
      view,
      filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : ''
    );
  }
}

export const QueryStoreRepository = dataSource.getRepository(QueryStore).extend({
  async getById(id: string): Promise<QueryStore> {
    logger.debug(`Loading query store by id ${id}...`);
    return this.findOneByOrFail({ id });
  },

  async getByFullHash(hash: string): Promise<QueryStore> {
    logger.debug(`Loading query store by full hash ${hash}...`);
    return this.findOneByOrFail({ hash });
  },

  async getByRequest(datasetId: string, revisionId: string, dataOptions?: DataOptionsDTO): Promise<QueryStore> {
    logger.debug(`Looking for query store entry for dataset ${datasetId}, revision ${revisionId}...`);
    if (!dataOptions) dataOptions = DEFAULT_DATA_OPTIONS;
    const hash = generateHash(datasetId, revisionId, dataOptions);

    try {
      return await this.getByFullHash(hash);
    } catch (_err) {
      logger.debug(`No query store entry found for hash ${hash}, generating new entry...`);
      return await this.generate(datasetId, revisionId, dataOptions);
    }
  },

  async generate(datasetId: string, revisionId: string, dataOptions: DataOptionsDTO): Promise<QueryStore> {
    logger.debug(`Generating new query store entry for dataset ${datasetId}, revision ${revisionId}...`);
    const hash = generateHash(datasetId, revisionId, dataOptions);
    let id = nanoId();

    while (true) {
      const existing = await this.getById(id);
      if (!existing) break;
      logger.warn(`Collision detected for query store ${id}, regenerating...`);
      id = nanoId();
    }

    const queryStore = QueryStore.create({
      id,
      datasetId,
      revisionId,
      requestObject: dataOptions,
      hash
    });

    const dataValueType = dataOptions.options?.data_value_type;
    const viewName = checkAvailableViews(dataValueType);
    const queryMap = new Map<Locale, string>();
    const filterTable = await getFilterTable(revisionId);
    const totals: { total_lines: number }[] = [];
    const queryRunner = dbManager.getCubeDataSource().createQueryRunner();

    try {
      for (const locale of SUPPORTED_LOCALES) {
        const lang = locale.split('-')[0];
        const coreView = await coreViewChooser(lang, revisionId);
        const selectColumns = await getColumns(revisionId, lang, viewName);
        const baseQuery = createBaseQuery(revisionId, coreView, locale, selectColumns, filterTable, dataOptions);
        queryMap.set(locale, baseQuery);
        const query = pgformat('SELECT COUNT(*) as total_lines FROM (%s);', baseQuery);
        logger.trace(`Testing query and getting a line count: ${query}`);
        totals.push(...(await queryRunner.query(query)));
      }
    } catch (err) {
      logger.error(err, 'Failed to run generated base query');
      throw err;
    } finally {
      void queryRunner.release();
    }

    const totalLines = totals[0].total_lines;

    for (const line of totals) {
      if (line.total_lines != totalLines) {
        logger.warn(`Base query for query store ${id} is producing inconsistent result`);
        break;
      }
    }

    const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
    let factTableToDimensionName: FactTableToDimensionName[];
    try {
      factTableToDimensionName = await cubeDB.query(
        pgformat('SELECT DISTINCT fact_table_column, dimension_name, language FROM %I.filter_table;', revisionId)
      );
    } catch (err) {
      logger.warn(err, `Failed to query the filter table for cube ${revisionId}`);
      throw err;
    } finally {
      void cubeDB.release();
    }

    queryStore.query = Object.fromEntries(queryMap);
    queryStore.totalLines = totalLines;
    queryStore.columnMapping = factTableToDimensionName;

    logger.debug(`Saving new query store entry ${id}...`);
    return this.save(queryStore);
  }
});
