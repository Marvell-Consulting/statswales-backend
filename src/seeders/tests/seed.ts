import 'dotenv/config';
import { DataSource } from 'typeorm';

import { dataSource } from '../../db/data-source';
import { User } from '../../entities/user/user';
import { config } from '../../config';
import { AppEnv } from '../../config/env.enum';
import { logger } from '../../utils/logger';

import { testUsers } from './fixtures/users';
import { UserGroup } from '../../entities/user/user-group';
import { testGroups } from './fixtures/group';

// This seeder loads test fixtures used by the e2e tests on the frontend. This needs to be run before
// the frontend tests so that the test users are available in the database.
export default class TestSeeder {
  async run(): Promise<void> {
    if (![AppEnv.Local, AppEnv.Ci].includes(config.env)) {
      throw new Error('SeedTestFixtures is only intended to be run in local or test environments');
    }

    await dataSource.initialize();
    await this.seedTestGroup(dataSource);
    await this.seedUsers(dataSource);
    await dataSource.destroy();
  }

  async seedTestGroup(dataSource: DataSource): Promise<void> {
    logger.info(`Seeding test groups...`);
    const entityManager = dataSource.createEntityManager();
    const groups = entityManager.create(UserGroup, testGroups);
    await dataSource.getRepository(UserGroup).save(groups);
  }

  async seedUsers(dataSource: DataSource): Promise<void> {
    logger.info(`Seeding ${testUsers.length} test users...`);
    const entityManager = dataSource.createEntityManager();
    const users = entityManager.create(User, testUsers);
    await dataSource.getRepository(User).save(users);
  }
}

Promise.resolve()
  .then(async () => {
    const seeder = new TestSeeder();
    await seeder.run();
  })
  .catch(async (err) => {
    logger.error(err);
    await dataSource.destroy();
    process.exit(1);
  });
