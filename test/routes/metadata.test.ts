import path from 'node:path';
import { createReadStream } from 'node:fs';
import { readFile } from 'node:fs/promises';

import request from 'supertest';
import { t } from 'i18next';

import app from '../../src/app';
import { initDb } from '../../src/db/init';
import DatabaseManager from '../../src/db/database-manager';
import { initPassport } from '../../src/middleware/passport-auth';
import { Dataset } from '../../src/entities/dataset/dataset';
import { Revision } from '../../src/entities/dataset/revision';
import { User } from '../../src/entities/user/user';
import { DatasetDTO } from '../../src/dtos/dataset-dto';
import { DimensionDTO } from '../../src/dtos/dimension-dto';
import { RevisionDTO } from '../../src/dtos/revision-dto';
import { DatasetRepository } from '../../src/repositories/dataset';
import { DataTableRepository } from '../../src/repositories/data-table';
import { DataTableDto } from '../../src/dtos/data-table-dto';
import { Locale } from '../../src/enums/locale';
import { logger } from '../../src/utils/logger';
import { withMetadataAndProviders } from '../../src/repositories/revision';

import { createFullDataset } from '../helpers/test-helper';
import { getTestUser, getTestUserGroup } from '../helpers/get-test-user';
import { getAuthHeader } from '../helpers/auth-header';
import BlobStorage from '../../src/services/blob-storage';
import { UserGroup } from '../../src/entities/user/user-group';
import { UserGroupRole } from '../../src/entities/user/user-group-role';
import { GroupRole } from '../../src/enums/group-role';
import { QueryRunner } from 'typeorm';

jest.mock('../../src/services/blob-storage');

const dataset1Id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
const revision1Id = '85f0e416-8bd1-4946-9e2c-1c958897c6ef';
const dataTableId = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
const user: User = getTestUser('test', 'user');
let userGroup = getTestUserGroup('Test Group');
let queryRunner: QueryRunner;

describe('API Endpoints for viewing dataset objects', () => {
  let dbManager: DatabaseManager;

  beforeAll(async () => {
    try {
      dbManager = await initDb();
      queryRunner = dbManager.getDataSource().createQueryRunner();
      await queryRunner.dropSchema('data_tables', true, true);
      await queryRunner.dropSchema(revision1Id, true, true);
      await queryRunner.createSchema('data_tables', true);
      await initPassport(dbManager.getDataSource());
      userGroup = await dbManager.getDataSource().getRepository(UserGroup).save(userGroup);
      user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
      await user.save();
      await createFullDataset(dataset1Id, revision1Id, dataTableId, user);
    } catch (error) {
      logger.error(error, 'Could not initialise test database');
      await dbManager.getDataSource().dropDatabase();
      await dbManager.getDataSource().destroy();
      process.exit(1);
    }
  });

  test('Check fixtures loaded successfully', async () => {
    const dataset1 = await Dataset.findOneBy({ id: dataset1Id });
    if (!dataset1) {
      throw new Error('Dataset not found');
    }
    const dto = DatasetDTO.fromDataset(dataset1);
    expect(dto).toBeInstanceOf(DatasetDTO);
  });

  describe('List all datasets', () => {
    test('returns 401 if no auth header is sent (JWT auth)', async () => {
      const res = await request(app).get('/dataset');
      expect(res.status).toBe(401);
      expect(res.body).toEqual({});
    });

    test('Get a list of all datasets returns 200 with a file list', async () => {
      const res = await request(app).get('/dataset').set(getAuthHeader(user));
      const today = new Date().toISOString().split('T')[0];
      expect(res.status).toBe(200);
      expect(res.body).toEqual({
        data: [
          {
            id: dataset1Id,
            title: 'Test Dataset 1',
            title_alt: 'Test Dataset 1',
            group_name: 'Test Group EN',
            last_updated: expect.stringContaining(today),
            status: 'new',
            publishing_status: 'incomplete'
          }
        ],
        count: 1
      });
    });
  });

  describe('Display dataset object endpoints', () => {
    test('returns 401 if no auth header is sent (JWT auth)', async () => {
      const res = await request(app).get(`/dataset/${dataset1Id}`);
      expect(res.status).toBe(401);
      expect(res.body).toEqual({});
    });

    test('Get a dataset returns 200', async () => {
      const dataset1 = await DatasetRepository.getById(dataset1Id);
      if (!dataset1) {
        throw new Error('Dataset not found');
      }
      const dto = await DatasetDTO.fromDataset(dataset1);
      const res = await request(app).get(`/dataset/${dataset1Id}`).set(getAuthHeader(user));
      expect(res.status).toBe(200);
      expect(res.body).toEqual(dto);
    });

    test('Get a dataset returns 404 if an invalid ID is given', async () => {
      const res = await request(app).get(`/dataset/INVALID-ID`).set(getAuthHeader(user));
      expect(res.status).toBe(404);
      expect(res.body).toEqual({ error: 'Dataset id is invalid or missing' });
    });

    test('Get a dataset returns 404 if a non-existant ID is given', async () => {
      const res = await request(app).get(`/dataset/8B9434D1-4807-41CD-8E81-228769671A07`).set(getAuthHeader(user));
      expect(res.status).toBe(404);
    });
  });

  describe('Display dimension metadata endpoints', () => {
    test('returns 401 if no auth header is sent (JWT auth)', async () => {
      const res = await request(app).get(`/dataset/${dataset1Id}/dimension/by-id/06b60fc5-93c9-4bd8-ac6f-3cc60ea538c4`);
      expect(res.status).toBe(401);
      expect(res.body).toEqual({});
    });

    test('Get a dimension returns 200 with a shallow object', async () => {
      const dataset1 = await DatasetRepository.getById(dataset1Id, {
        dimensions: { metadata: true, lookupTable: true }
      });
      if (!dataset1) {
        throw new Error('Dataset not found');
      }
      const dimension = dataset1.dimensions.pop();
      if (!dimension) {
        throw new Error('No dimension found on test dataset');
      }
      const dto = await DimensionDTO.fromDimension(dimension);
      const res = await request(app)
        .get(`/dataset/${dataset1Id}/dimension/by-id/${dimension.id}`)
        .set(getAuthHeader(user));
      expect(res.status).toBe(200);
      expect(res.body).toEqual(dto);
    });

    test('Get a dimension returns 404 if an invalid ID is given', async () => {
      const res = await request(app).get(`/dataset/${dataset1Id}/dimension/by-id/INVALID-ID`).set(getAuthHeader(user));
      expect(res.status).toBe(404);
      expect(res.body).toEqual({ error: 'Dimension id is invalid or missing' });
    });

    test('Get a dimension returns 404 if a non-existant ID is given', async () => {
      const res = await request(app)
        .get(`/dataset/${dataset1Id}/dimension/by-id/8B9434D1-4807-41CD-8E81-228769671A07`)
        .set(getAuthHeader(user));
      expect(res.status).toBe(404);
    });
  });

  describe('Get revision metadata endpoints', () => {
    test('returns 401 if no auth header is sent (JWT auth)', async () => {
      const res = await request(app).get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}`);
      expect(res.status).toBe(401);
      expect(res.body).toEqual({});
    });

    test('Get a revision returns 200', async () => {
      const revision = await Revision.findOne({ where: { id: revision1Id }, relations: withMetadataAndProviders });
      if (!revision) {
        throw new Error('Revision not found');
      }
      const res = await request(app)
        .get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}`)
        .set(getAuthHeader(user));
      expect(res.status).toBe(200);
      const dto = await RevisionDTO.fromRevision(revision);
      expect(res.body).toEqual(dto);
    });

    test('Get revision returns 404 if an invalid ID is given', async () => {
      const res = await request(app).get(`/dataset/${dataset1Id}/revision/by-id/INVALID-ID`).set(getAuthHeader(user));
      expect(res.status).toBe(404);
      expect(res.body).toEqual({ error: 'Revision id is invalid or missing' });
    });

    test('Get revision returns 404 if a ID is given', async () => {
      const res = await request(app)
        .get(`/dataset/${dataset1Id}/revision/by-id/8B9434D1-4807-41CD-8E81-228769671A07`)
        .set(getAuthHeader(user));
      expect(res.status).toBe(404);
    });
  });

  describe('DataTable endpoints', () => {
    test('returns 401 if no auth header is sent (JWT auth)', async () => {
      const res = await request(app).get(
        `/dataset/${dataset1Id}/revision/by-id/${revision1Id}/data-table/by-id/${dataTableId}/preview`
      );

      expect(res.status).toBe(401);
      expect(res.body).toEqual({});
    });

    test('Get data-table returns 200 with object', async () => {
      const dataTable = await DataTableRepository.getDataTableById(dataset1Id, revision1Id, dataTableId);
      if (!dataTable) {
        throw new Error('Data table not found');
      }
      const res = await request(app)
        .get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}/data-table`)
        .set(getAuthHeader(user));

      const expectedDTO = DataTableDto.fromDataTable(dataTable);
      expect(res.status).toBe(200);
      expect(res.body).toEqual(expectedDTO);
    });

    describe('Getting a raw file out of a file import', () => {
      const loadStreamMock = jest.fn();

      beforeEach(() => {
        BlobStorage.prototype.loadStream = loadStreamMock;
      });

      test('Get file from a revision and import returns 200 and complete file data if stored in the Data Lake', async () => {
        const testFile2 = path.resolve(__dirname, `../sample-files/csv/test-data-2.csv`);
        const testFileStream = createReadStream(testFile2);
        const testFile2Buffer = await readFile(testFile2);
        loadStreamMock.mockImplementation(() => {
          return Promise.resolve(testFileStream);
        });

        const res = await request(app)
          .get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}/data-table/raw`)
          .set(getAuthHeader(user));

        expect(res.status).toBe(200);
        expect(res.text).toEqual(testFile2Buffer.toString());
      });

      test('Get file from a revision and import returns 500 if an error with the Data Lake occurs', async () => {
        loadStreamMock.mockImplementation(() => {
          return Promise.reject(Error('Unknown Data Lake Error'));
        });

        const res = await request(app)
          .get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}/data-table/raw`)
          .set(getAuthHeader(user));

        expect(res.status).toBe(500);
        expect(res.body).toEqual({
          status: 500,
          errors: [
            {
              field: 'csv',
              message: [
                {
                  lang: Locale.English,
                  message: t('errors.download_from_filestore', { lng: Locale.English })
                },
                {
                  lang: Locale.Welsh,
                  message: t('errors.download_from_filestore', { lng: Locale.Welsh })
                }
              ],
              tag: { name: 'errors.download_from_filestore', params: {} }
            }
          ],
          dataset_id: dataset1Id
        });
      });
    });
  });

  afterAll(async () => {
    await queryRunner.dropSchema('data_tables', true, true);
    await dbManager.getDataSource().dropDatabase();
    await dbManager.getDataSource().destroy();
  });
});
