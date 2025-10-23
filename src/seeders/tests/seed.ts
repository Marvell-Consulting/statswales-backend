/* eslint-disable no-console */

import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource } from 'typeorm';

import { User } from '../../entities/user/user';
import { config } from '../../config';
import { AppEnv } from '../../config/env.enum';

import { testUsers } from './fixtures/users';
import { UserGroup } from '../../entities/user/user-group';
import { testGroups } from './fixtures/group';

// This seeder loads test fixtures used by the e2e tests on the frontend. This needs to be run before
// the frontend tests so that the test users are available in the database.
export default class SeedTestFixtures extends Seeder {
  async run(dataSource: DataSource): Promise<void> {
    if (![AppEnv.Local, AppEnv.Ci].includes(config.env)) {
      throw new Error('SeedTestFixtures is only intended to be run in local or test environments');
    }
    await this.seedTestGroup(dataSource);
    await this.seedUsers(dataSource);
  }

  async seedTestGroup(dataSource: DataSource): Promise<void> {
    console.log(`Seeding test groups...`);
    const entityManager = dataSource.createEntityManager();
    const groups = entityManager.create(UserGroup, testGroups);
    await dataSource.getRepository(UserGroup).save(groups);
  }

  async seedUsers(dataSource: DataSource): Promise<void> {
    console.log(`Seeding ${testUsers.length} test users...`);
    const entityManager = dataSource.createEntityManager();
    const users = entityManager.create(User, testUsers);
    await dataSource.getRepository(User).save(users);
  }
}
