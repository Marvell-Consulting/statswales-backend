import 'dotenv/config';
import { DataSource } from 'typeorm';

import { dataSource } from '../../db/data-source';
import { User } from '../../entities/user/user';
import { config } from '../../config';
import { AppEnv } from '../../config/env.enum';
import { logger } from '../../utils/logger';

import { testUsers } from './fixtures/users';
import { UserGroup } from '../../entities/user/user-group';
import { UserGroupRole } from '../../entities/user/user-group-role';
import { testGroups } from './fixtures/group';

// This seeder loads test fixtures used by the e2e tests on the frontend. This needs to be run before
// the frontend tests so that the test users are available in the database.
export class TestSeeder {
  constructor(private ds: DataSource) {
    this.ds = ds;
  }

  async run(): Promise<void> {
    if (![AppEnv.Local, AppEnv.Ci].includes(config.env)) {
      throw new Error('SeedTestFixtures is only intended to be run in local or test environments');
    }

    logger.info('Starting TestSeeder...');
    await this.seedTestGroup();
    await this.seedUsers();
    logger.info('TestSeeder finished.');
  }

  async seedTestGroup(): Promise<void> {
    logger.info(`Seeding ${testGroups.length} test groups...`);
    const entityManager = this.ds.createEntityManager();
    const groups = entityManager.create(UserGroup, testGroups);
    await this.ds.getRepository(UserGroup).save(groups);
  }

  async seedUsers(): Promise<void> {
    logger.info(`Seeding ${testUsers.length} test users...`);

    // Remove existing group roles for test users so re-runs don't hit unique constraint violations
    const userIds = testUsers.map((u) => u.id).filter(Boolean) as string[];
    if (userIds.length > 0) {
      await this.ds
        .createQueryBuilder()
        .delete()
        .from(UserGroupRole)
        .where('user_id IN (:...userIds)', { userIds })
        .execute();
    }

    const entityManager = this.ds.createEntityManager();
    const users = entityManager.create(User, testUsers);
    await this.ds.getRepository(User).save(users);
  }
}

Promise.resolve()
  .then(async () => {
    if (!dataSource.isInitialized) await dataSource.initialize();
    await new TestSeeder(dataSource).run();
  })
  .catch(async (err) => {
    logger.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (dataSource.isInitialized) await dataSource.destroy();
  });
