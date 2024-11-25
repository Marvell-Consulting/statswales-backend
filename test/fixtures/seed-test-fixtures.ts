/* eslint-disable no-console */
import fs from 'node:fs/promises';

import { Seeder } from '@jorgebodega/typeorm-seeding';
import { DataSource } from 'typeorm';

import { User } from '../../src/entities/user/user';
import { Dataset } from '../../src/entities/dataset/dataset';
import { appConfig } from '../../src/config';
import { AppEnv } from '../../src/config/env.enum';
import { createSources, moveFileToDataLake, uploadCSVBufferToBlobStorage } from '../../src/controllers/csv-processor';
import {
    validateSourceAssignment,
    createDimensionsFromSourceAssignment
} from '../../src/controllers/dimension-processor';
import { RevisionRepository } from '../../src/repositories/revision';
import { SourceAssignmentDTO } from '../../src/dtos/source-assignment-dto';

import { testUsers } from './users';
import { testDatasets } from './datasets';

const config = appConfig();

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

// const seedDataFile = async (testDataset, dataset) => {
//     const csvBuffer = await fs.readFile(testDataset.csvPath);
//     const fileImport = await uploadCSVBufferToBlobStorage(csvBuffer, 'text/csv');
//     const revision = await RevisionRepository.createFromImport(dataset, fileImport, dataset.createdBy);
//     fileImport.revision = revision;
//     await fileImport.save();
//     await moveFileToDataLake(fileImport);
//     await createSources(fileImport);

//     const sourceAssignment: SourceAssignmentDTO[] = fileImport.sources.map((source, idx) => ({
//         sourceId: source.id,
//         sourceType: testDataset.sourceTypes[idx]
//     }));

//     const validatedSourceAssignment = await validateSourceAssignment(fileImport, sourceAssignment);
//     await createDimensionsFromSourceAssignment(dataset, revision, validatedSourceAssignment);

//     console.log(`Uploaded ${testDataset.csvPath} to create revision ${revision.id} for dataset ${dataset.id}`);
// };
