import { ensureWorkerDataSources } from '../../helpers/reset-database';
import { dbManager } from '../../../src/db/database-manager';

describe('dbManager.runMigrations', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
  });

  test('runs nothing and resolves when every migration has already run', async () => {
    await expect(dbManager.runMigrations()).resolves.toEqual([]);
    await expect(dbManager.getPublisherDataSource().showMigrations()).resolves.toBe(false);
  });
});
