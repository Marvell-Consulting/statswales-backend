import { dbManager } from '../../../src/db/database-manager';

import { BuildLogRepository } from '../../../src/repositories/build-log';
import { DatasetRepository } from '../../../src/repositories/dataset';
import { DatasetStatsRepository } from '../../../src/repositories/dataset-stats';
import { DataTableRepository } from '../../../src/repositories/data-table';
import { DimensionRepository } from '../../../src/repositories/dimension';
import { OrganisationRepository } from '../../../src/repositories/organisation';
import { ProviderRepository } from '../../../src/repositories/provider';
import { RevisionRepository } from '../../../src/repositories/revision';
import { TopicRepository } from '../../../src/repositories/topic';
import { UserRepository } from '../../../src/repositories/user';
import { UserGroupRepository } from '../../../src/repositories/user-group';

import { PublishedDatasetRepository } from '../../../src/repositories/published-dataset';
import { PublishedRevisionRepository } from '../../../src/repositories/published-revision';
import { PublishedTopicRepository } from '../../../src/repositories/published-topic';
import { QueryStoreRepository } from '../../../src/repositories/query-store';
import { SearchLogRepository } from '../../../src/repositories/search-log';

// Fail fast if a repo gets accidentally re-bound to the wrong pool. SW-1265 isolation depends on
// publisher routes never serving from the consumer pool (and vice versa). The check round-trips
// through Postgres so we verify what the server actually sees in pg_stat_activity.application_name,
// not just the TypeORM config object.

const PUBLISHER_APP = 'sw3-backend-publisher';
const CONSUMER_APP = 'sw3-backend-consumer';

interface AppNameRow {
  name: string;
}

// `Repository<T>.query` is what each repo inherits via `dataSource.getRepository(...).extend(...)`.
// Calling it returns the application_name Postgres sees for the connection that served the query.
const appNameViaRepo = async (repo: { query: (sql: string) => Promise<AppNameRow[]> }): Promise<string> => {
  const rows = await repo.query("SELECT current_setting('application_name') AS name");
  return rows[0].name;
};

describe('TypeORM pool binding (SW-1265)', () => {
  beforeAll(async () => {
    await dbManager.initDataSources();
  });

  afterAll(async () => {
    await dbManager.destroyDataSources();
  });

  describe.each([
    ['BuildLogRepository', BuildLogRepository],
    ['DatasetRepository', DatasetRepository],
    ['DatasetStatsRepository', DatasetStatsRepository],
    ['DataTableRepository', DataTableRepository],
    ['DimensionRepository', DimensionRepository],
    ['OrganisationRepository', OrganisationRepository],
    ['ProviderRepository', ProviderRepository],
    ['RevisionRepository', RevisionRepository],
    ['TopicRepository', TopicRepository],
    ['UserRepository', UserRepository],
    ['UserGroupRepository', UserGroupRepository]
  ])('publisher pool', (name, repo) => {
    it(`${name} queries against ${PUBLISHER_APP}`, async () => {
      expect(await appNameViaRepo(repo)).toBe(PUBLISHER_APP);
    });
  });

  describe.each([
    ['PublishedDatasetRepository', PublishedDatasetRepository],
    ['PublishedRevisionRepository', PublishedRevisionRepository],
    ['PublishedTopicRepository', PublishedTopicRepository],
    ['QueryStoreRepository', QueryStoreRepository],
    ['SearchLogRepository', SearchLogRepository]
  ])('consumer pool', (name, repo) => {
    it(`${name} queries against ${CONSUMER_APP}`, async () => {
      expect(await appNameViaRepo(repo)).toBe(CONSUMER_APP);
    });
  });
});
