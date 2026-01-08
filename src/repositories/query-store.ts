import { createHash } from 'node:crypto';

import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { customAlphabet } from 'nanoid';

import { dataSource } from '../db/data-source';
import { dbManager } from '../db/database-manager';
import { DataOptionsDTO } from '../dtos/data-options-dto';
import { QueryStore } from '../entities/query-store';
import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { checkAvailableViews, getFilterTable, coreViewChooser, getColumns, createBaseQuery } from '../utils/consumer';

const nanoId = customAlphabet('1234567890abcdefghjklmnpqrstuvwxy', 12);

function generateHash(datasetId: string, revisionId: string, options: DataOptionsDTO): string {
  return createHash('sha256')
    .update(`${datasetId}:${revisionId}:${JSON.stringify(options)}`)
    .digest('hex');
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

  async getByRequest(datasetId: string, revisionId: string, dataOptions: DataOptionsDTO): Promise<QueryStore> {
    logger.debug(`Looking for query store entry for dataset ${datasetId}, revision ${revisionId}...`);
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
    let remainingAttempts = 10;
    let id = nanoId();

    while (remainingAttempts > 0) {
      const existing = await this.findOneBy({ id });
      if (!existing) break;
      remainingAttempts--;
      logger.warn(`Collision detected for query store ${id}, regenerating...`);
      id = nanoId();
    }

    if (remainingAttempts === 0) {
      throw new Error('Failed to generate unique id for query store entry after multiple attempts');
    }

    const queryStore = QueryStore.create({
      id,
      datasetId,
      revisionId,
      requestObject: dataOptions,
      hash
    });

    logger.debug(`Creating base queries for all supported locales for query store ${id}...`);

    const dataValueType = dataOptions.options?.data_value_type;
    const viewName = checkAvailableViews(dataValueType);
    const queryMap = new Map<Locale, string>();
    const totals: { total_lines: number }[] = [];
    const filterTable = await getFilterTable(revisionId);
    const cubeDataSource = dbManager.getCubeDataSource();

    try {
      for (const locale of SUPPORTED_LOCALES) {
        const lang = locale.split('-')[0];
        const coreView = await coreViewChooser(lang, revisionId);
        const selectColumns = await getColumns(revisionId, lang, viewName);
        const baseQuery = createBaseQuery(revisionId, coreView, locale, selectColumns, filterTable, dataOptions);
        queryMap.set(locale, baseQuery);
        const lineCountQuery = pgformat('SELECT COUNT(*) as total_lines FROM (%s);', baseQuery);
        const lineCountResult = await cubeDataSource.query(lineCountQuery);
        totals.push(...lineCountResult);
      }
    } catch (err) {
      logger.error(err, 'Failed to run generated base query');
      throw err;
    }

    const totalLines = totals[0].total_lines;

    for (const line of totals) {
      if (line.total_lines !== totalLines) {
        logger.warn(`Base query for query store ${id} is producing inconsistent result`);
        break;
      }
    }

    let factTableToDimensionName: FactTableToDimensionName[];
    try {
      factTableToDimensionName = await cubeDataSource.query(
        pgformat('SELECT DISTINCT fact_table_column, dimension_name, language FROM %I.filter_table;', revisionId)
      );
    } catch (err) {
      logger.warn(err, `Failed to query the filter table for cube ${revisionId}`);
      throw err;
    }

    queryStore.query = Object.fromEntries(queryMap);
    queryStore.totalLines = totalLines;
    queryStore.columnMapping = factTableToDimensionName;

    logger.debug(`Saving new query store entry ${id}...`);
    return this.save(queryStore);
  }
});
