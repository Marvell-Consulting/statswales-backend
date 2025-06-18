/* eslint-disable no-console */

import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource } from 'typeorm';
import { omit } from 'lodash';

import { User } from '../../src/entities/user/user';
import { Dataset } from '../../src/entities/dataset/dataset';
import { appConfig } from '../../src/config';
import { AppEnv } from '../../src/config/env.enum';
import { validateAndUpload } from '../../src/services/csv-processor';

import { testUsers } from './users';
import { testDatasets } from './datasets';
import { Revision } from '../../src/entities/dataset/revision';
import { UserGroup } from '../../src/entities/user/user-group';
import { testGroup } from './group';

const config = appConfig();

// This seeder loads test fixtures used by the e2e tests on the frontend. This needs to be run before the frontend tests
// so that the test users and starting datasets are available in the database.
export default class SeedTestFixtures extends Seeder {
  async run(dataSource: DataSource): Promise<void> {
    if (![AppEnv.Local, AppEnv.Ci].includes(config.env)) {
      throw new Error('SeedTestFixtures is only intended to be run in local or test environments');
    }
    await this.seedTestGroup(dataSource);
    await this.seedUsers(dataSource);
    await this.seedDatasets(dataSource);
  }

  async seedTestGroup(dataSource: DataSource) {
    console.log(`seeding test group...`);
    const entityManager = dataSource.createEntityManager();
    const group = entityManager.create(UserGroup, testGroup);
    await dataSource.getRepository(UserGroup).save(group);
  }

  async seedUsers(dataSource: DataSource): Promise<void> {
    console.log(`Seeding ${testUsers.length} test users...`);
    const entityManager = dataSource.createEntityManager();
    const users = entityManager.create(User, testUsers);
    await dataSource.getRepository(User).save(users);
  }

  async seedDatasets(dataSource: DataSource): Promise<void> {
    console.log(`Seeding ${testDatasets.length} test datasets...`);
    const entityManager = dataSource.createEntityManager();

    for (const testDataset of testDatasets) {
      try {
        let revision = testDataset.dataset.draftRevision || testDataset.dataset.publishedRevision;
        const partialDataset = omit(testDataset.dataset, ['draftRevision', 'publishedRevision']);
        const dataset = await entityManager.getRepository(Dataset).create(partialDataset).save();

        if (revision && testDataset.csvPath) {
          const file = {
            mimetype: 'text/csv',
            originalname: 'test-fixture.csv',
            path: testDataset.csvPath
          } as any;
          const dataTable = await validateAndUpload(file, dataset.id, 'data_table');

          revision = await entityManager.getRepository(Revision).save({
            ...revision,
            dataset,
            dataTable,
            approvedBy: dataset.createdBy,
            approvedAt: dataset.live || undefined,
            publishAt: dataset.live || undefined
          });

          await entityManager.getRepository(Dataset).save({
            ...dataset,
            startRevision: revision,
            endRevision: revision,
            draftRevision: dataset.live ? undefined : revision,
            publishedRevision: dataset.live ? revision : undefined
          });
        }
      } catch (err) {
        console.error(err, `Error seeding dataset ${testDataset.dataset.id}`);
        process.exit(1);
      }
    }
  }
}
