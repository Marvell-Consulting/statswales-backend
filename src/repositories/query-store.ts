import { createHash } from 'node:crypto';

import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { customAlphabet } from 'nanoid';

import { consumerDataSource } from '../db/consumer-source';
import { dbManager } from '../db/database-manager';
import { DataOptionsDTO, FRONTEND_DATA_OPTIONS } from '../dtos/data-options-dto';
import { QueryStore } from '../entities/query-store';
import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';
import { FilterInterface, FilterV2 } from '../interfaces/filterInterface';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { checkAvailableViews, getFilterTable, coreViewChooser, getColumns, createBaseQuery } from '../utils/consumer';

const nanoId = customAlphabet('1234567890abcdefghjklmnpqrstuvwxy', 12);

function generateHash(datasetId: string, revisionId: string, options: DataOptionsDTO, namespace?: string): string {
  const prefix = namespace ? `${namespace}:` : '';
  return createHash('sha256')
    .update(`${prefix}${datasetId}:${revisionId}:${JSON.stringify(options)}`)
    .digest('hex');
}

// Marker prefix for v1 cache entries so their hashes can never collide with v2.
const V1_HASH_NAMESPACE = 'v1';

// Translates a v1 filter list into a DataOptionsDTO whose hash is stable across
// client-side reorderings. Sort matters here, not for SQL correctness, but to
// guarantee that two v1 requests with the same filter set produce the same
// query_store hash regardless of how the client laid out the query string.
//
// `use_raw_column_names: true` is required because v1's `filter` query param
// uses fact-table column names; v2's `createBaseQuery` only resolves them
// correctly when this flag is set. View selection is irrelevant to the count.
export function v1FilterToDataOptions(filter?: FilterInterface[]): DataOptionsDTO {
  const filters: FilterV2[] = (filter ?? [])
    .map((f) => ({ columnName: f.columnName, values: [...f.values].sort() }))
    .sort((a, b) => a.columnName.localeCompare(b.columnName))
    .map(({ columnName, values }) => ({ [columnName]: values }));

  return {
    filters,
    options: {
      ...FRONTEND_DATA_OPTIONS.options,
      use_raw_column_names: true
    }
  };
}

interface QueryStoreUpdate {
  queryMap: Map<Locale, string>;
  totalLines: number;
  columnMapping: FactTableToDimensionName[];
}

async function generateQuery(dataOptions: DataOptionsDTO, revisionId: string): Promise<QueryStoreUpdate> {
  const dataValueType = dataOptions.options?.data_value_type;
  const viewName = checkAvailableViews(dataValueType);
  const queryMap = new Map<Locale, string>();
  let totalLines = 0;
  const filterTable = await getFilterTable(revisionId);
  const cubeDataSource = dbManager.getCubeDataSource();

  try {
    for (const locale of SUPPORTED_LOCALES) {
      const lang = locale.split('-')[0];
      const coreView = await coreViewChooser(lang, revisionId);
      const selectColumns = await getColumns(revisionId, lang, viewName);
      const baseQuery = createBaseQuery(revisionId, coreView, locale, selectColumns, filterTable, dataOptions);
      queryMap.set(locale, baseQuery);

      // Row count is locale-independent — only count once using the first locale
      if (totalLines === 0 && queryMap.size === 1) {
        const lineCountQuery = pgformat('SELECT COUNT(*) as total_lines FROM (%s);', baseQuery);
        const lineCountResult = await cubeDataSource.query(lineCountQuery);
        totalLines = Number(lineCountResult[0].total_lines);
      }
    }
  } catch (err) {
    logger.error(err, 'Failed to run generated base query');
    throw err;
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

async function runCountAgainstCube(baseQuery: string): Promise<number> {
  const totalsQuery = pgformat('SELECT count(*) as "totalLines" from (%s);', baseQuery);
  const connection = dbManager.getCubeDataSource().createQueryRunner();
  try {
    const totals: { totalLines: string }[] = await connection.query(totalsQuery);
    return Number(totals[0].totalLines);
  } finally {
    void connection.release();
  }
}

export const QueryStoreRepository = consumerDataSource.getRepository(QueryStore).extend({
  async getById(id: string): Promise<QueryStore> {
    logger.debug(`Loading query store by id ${id}...`);
    return this.findOneByOrFail({ id });
  },

  async getByFullHash(hash: string): Promise<QueryStore> {
    logger.debug(`Loading query store by full hash ${hash}...`);
    return this.findOneByOrFail({ hash });
  },

  // Cache-or-compute helper for the v1 datatable count. v1's filter contract
  // is broader than v2 (it permits any fact-table column, dimensioned or not),
  // so we deliberately don't run the v1 request through v2's generateQuery —
  // we wrap the COUNT directly here using v1's already-built baseQuery.
  //
  // The entry is persisted to query_store with placeholder `query`/`columnMapping`
  // fields. On cube rebuild, `rebuildQueriesForRevision` will attempt to
  // regenerate via generateQuery; if the filter references non-dimensioned
  // columns that path will throw and the entry is removed (existing fallback
  // in `rebuildQueriesForRevision`). The cache then repopulates on the next
  // v1 request. Dimensioned filters survive the rebuild cleanly.
  async getTotalLinesForV1(
    datasetId: string,
    revisionId: string,
    filter: FilterInterface[] | undefined,
    baseQuery: string
  ): Promise<number> {
    const dataOptions = v1FilterToDataOptions(filter);
    const hash = generateHash(datasetId, revisionId, dataOptions, V1_HASH_NAMESPACE);

    const cached = await QueryStore.findOneBy({ hash });
    if (cached) {
      return cached.totalLines;
    }

    const totalLines = await runCountAgainstCube(baseQuery);

    let id = nanoId();
    let remainingAttempts = 10;
    while (remainingAttempts > 0) {
      const existing = await QueryStore.findOneBy({ id });
      if (!existing) break;
      remainingAttempts--;
      id = nanoId();
    }
    if (remainingAttempts === 0) {
      logger.warn(`Failed to generate unique id for v1 query_store entry — skipping persistence`);
      return totalLines;
    }

    const entry = QueryStore.create({
      id,
      datasetId,
      revisionId,
      requestObject: dataOptions,
      hash,
      query: {},
      totalLines,
      columnMapping: []
    });

    try {
      await entry.save();
    } catch (err) {
      // Concurrent writers may have inserted the same hash between our lookup
      // and save — that's fine, the cache is still warm. Log and continue.
      logger.debug(err, `Failed to persist v1 query_store entry for hash ${hash} (likely a concurrent write)`);
    }

    return totalLines;
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

    const { queryMap, totalLines, columnMapping } = await generateQuery(dataOptions, revisionId);

    queryStore.query = Object.fromEntries(queryMap);
    queryStore.totalLines = totalLines;
    queryStore.columnMapping = columnMapping;

    logger.debug(`Saving new query store entry ${id}...`);
    return this.save(queryStore);
  },

  async rebuildAll(): Promise<void> {
    await consumerDataSource.query(
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
    const { queryMap, totalLines, columnMapping } = await generateQuery(entry.requestObject, entry.revisionId);
    entry.totalLines = totalLines;
    entry.columnMapping = columnMapping;
    entry.query = Object.fromEntries(queryMap);
    entry.updatedAt = new Date();
    await entry.save();
  }
});
