/* eslint-disable no-console */

import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource } from 'typeorm';
import { omit } from 'lodash';

import { User } from '../../entities/user/user';
import { Dataset } from '../../entities/dataset/dataset';
import { config } from '../../config';
import { AppEnv } from '../../config/env.enum';
import { validateAndUpload } from '../../services/csv-processor';

import { testUsers } from './fixtures/users';
import { setupTmpCsv, testDatasets } from './fixtures/datasets';
import { Revision } from '../../entities/dataset/revision';
import { UserGroup } from '../../entities/user/user-group';
import { testGroups } from './fixtures/group';
import { TempFile } from '../../interfaces/temp-file';

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

  async seedDatasets(dataSource: DataSource): Promise<void> {
    console.log(`Seeding ${testDatasets.length} test datasets...`);
    const entityManager = dataSource.createEntityManager();

    setupTmpCsv();

    for (const testDataset of testDatasets) {
      try {
        let revision = testDataset.dataset.draftRevision || testDataset.dataset.publishedRevision;
        const partialDataset = omit(testDataset.dataset, ['draftRevision', 'publishedRevision']);
        const dataset = await entityManager.getRepository(Dataset).create(partialDataset).save();

        if (revision && testDataset.csvPath) {
          const file: TempFile = {
            mimetype: 'text/csv',
            originalname: 'test-fixture.csv',
            path: testDataset.csvPath
          };
          const dataTable = await validateAndUpload(file, dataset.id, 'data_table');

          revision = await entityManager.getRepository(Revision).save({
            ...revision,
            dataset,
            dataTable,
            approvedBy: dataset.createdBy,
            approvedAt: dataset.firstPublishedAt || undefined,
            publishAt: dataset.firstPublishedAt || undefined
          });

          await entityManager.getRepository(Dataset).save({
            ...dataset,
            startRevision: revision,
            endRevision: revision,
            draftRevision: dataset.firstPublishedAt ? undefined : revision,
            publishedRevision: dataset.firstPublishedAt ? revision : undefined
          });
        }
      } catch (err) {
        console.error(err, `Error seeding dataset ${testDataset.dataset.id}`);
        process.exit(1);
      }
    }
  }
}
