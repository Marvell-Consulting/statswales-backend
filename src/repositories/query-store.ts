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

interface QueryStoreUpdate {
  queryMap: Map<Locale, string>;
  totalLines: number;
  columnMapping: FactTableToDimensionName[];
}

async function generateQuery(
  storeID: string,
  dataOptions: DataOptionsDTO,
  revisionId: string
): Promise<QueryStoreUpdate> {
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
      logger.warn(`Base query for query store ${storeID} is producing inconsistent result`);
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

  return {
    totalLines,
    columnMapping: factTableToDimensionName,
    queryMap
  };
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
    logger.info(`Looking for query store entry for dataset ${datasetId}, revision ${revisionId}...`);
    const hash = generateHash(datasetId, revisionId, dataOptions);

    try {
      return await this.getByFullHash(hash);
    } catch (_err) {
      logger.debug(`No query store entry found for hash ${hash}, generating new entry...`);
      return await this.generate(datasetId, revisionId, dataOptions);
    }
  },

  async generate(datasetId: string, revisionId: string, dataOptions: DataOptionsDTO): Promise<QueryStore> {
    logger.info(`Generating new query store entry for dataset ${datasetId}, revision ${revisionId}...`);
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

    const { queryMap, totalLines, columnMapping } = await generateQuery(id, dataOptions, revisionId);

    queryStore.query = Object.fromEntries(queryMap);
    queryStore.totalLines = totalLines;
    queryStore.columnMapping = columnMapping;

    logger.debug(`Saving new query store entry ${id}...`);
    return this.save(queryStore);
  },

  async rebuildAll(): Promise<void> {
    await dataSource.query(
      'DELETE FROM query_store qs USING revision r WHERE qs.revision_id = r.id AND r.publish_at IS NULL OR r.publish_at > NOW();'
    );

    const publishedRevisionQueries: { id: string }[] = await this.createQueryBuilder('qs')
      .select('qs.id AS id')
      .innerJoin('revision', 'r', 'r.id = qs.revision_id')
      .where('r.publish_at IS NOT NULL')
      .andWhere('r.publish_at <= NOW()')
      .andWhere('r.approved_at IS NOT NULL')
      .andWhere('r.approved_at <= NOW()')
      .getRawMany(); // only need the id, not the full object

    // rebuild qs for published revisions
    for (const queryStore of publishedRevisionQueries) {
      await this.rebuildQueryEntry(queryStore.id);
    }
  },

  async rebuildQueriesForRevision(revisionId: string): Promise<void> {
    const revisionQueryStoreEntries = await QueryStore.findBy({ revisionId });
    for (const entry of revisionQueryStoreEntries) {
      try {
        await this.updateEntry(entry);
      } catch (error) {
        logger.warn(error, `Entry with ID ${entry.id} could not be regenerated.  Removing entry`);
        await QueryStore.remove(entry);
      }
    }
  },

  async rebuildQueriesForDataset(datasetId: string): Promise<void> {
    const datasetQueryStoreEntries = await QueryStore.findBy({ datasetId });
    for (const entry of datasetQueryStoreEntries) {
      try {
        await this.updateEntry(entry);
      } catch (error) {
        logger.warn(error, `Entry with ID ${entry.id} could not be regenerated.  Removing entry`);
        await QueryStore.remove(entry);
      }
    }
  },

  async rebuildQueryEntry(id: string): Promise<void> {
    const entry = await QueryStore.findOneOrFail({ where: { id } });
    try {
      await this.updateEntry(entry);
    } catch (error) {
      logger.warn(error, `Entry with ID ${entry.id} could not be regenerated.  Removing entry`);
      await QueryStore.remove(entry);
    }
  },

  async updateEntry(entry: QueryStore): Promise<void> {
    const { queryMap, totalLines, columnMapping } = await generateQuery(
      entry.id,
      entry.requestObject,
      entry.revisionId
    );
    entry.totalLines = totalLines;
    entry.columnMapping = columnMapping;
    entry.query = Object.fromEntries(queryMap);
    entry.updatedAt = new Date();
    await entry.save();
  }
});
