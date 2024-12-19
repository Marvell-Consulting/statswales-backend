/* eslint-disable no-console */
import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource } from 'typeorm';

import { User } from '../../src/entities/user/user';
import { Dataset } from '../../src/entities/dataset/dataset';
import { appConfig } from '../../src/config';
import { AppEnv } from '../../src/config/env.enum';

import { testUsers } from './users';
import { testDatasets } from './datasets';

const config = appConfig();

// This seeder loads test fixtures used by the e2e tests on the frontend. This needs to be run before the frontend tests
// so that the test users and starting datasets are available in the database.
export default class SeedTestFixtures extends Seeder {
    async run(dataSource: DataSource): Promise<void> {
        if (![AppEnv.Local, AppEnv.Ci].includes(config.env)) {
            throw new Error('SeedTestFixtures is only intended to be run in local or test environments');
        }

        await this.seedUsers(dataSource);
        await this.seedDatasets(dataSource);
    }

    async seedUsers(dataSource: DataSource): Promise<void> {
        console.log(`Seeding ${testUsers.length} test users...`);
        const entityManager = dataSource.createEntityManager();
        const users = await entityManager.create(User, testUsers);
        await dataSource.getRepository(User).save(users);
    }

    async seedDatasets(dataSource: DataSource): Promise<void> {
        console.log(`Seeding ${testDatasets.length} test datasets...`);
        const entityManager = dataSource.createEntityManager();

        for (const testDataset of testDatasets) {
            try {
                const entity = await entityManager.create(Dataset, testDataset.dataset);
                const dataset = await dataSource.getRepository(Dataset).save(entity);
            } catch (err) {
                console.error(`Error seeding dataset ${testDataset.dataset.id}`, err);
            }
        }
    }
}
