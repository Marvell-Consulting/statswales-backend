import path from 'node:path';
import * as fs from 'node:fs';

import request from 'supertest';

import app from '../../src/app';
import { initDb } from '../../src/db/init';
import DatabaseManager from '../../src/db/database-manager';
import { initPassport } from '../../src/middleware/passport-auth';
import { User } from '../../src/entities/user/user';
import { logger } from '../../src/utils/logger';
import { DatasetRepository } from '../../src/repositories/dataset';
import { RevisionRepository } from '../../src/repositories/revision';

import { createFullDataset } from '../helpers/test-helper';
import { getTestUser } from '../helpers/get-user';
import { getAuthHeader } from '../helpers/auth-header';
import BlobStorage from '../../src/services/blob-storage';

jest.mock('../../src/services/blob-storage');

BlobStorage.prototype.listFiles = jest
  .fn()
  .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

BlobStorage.prototype.loadBuffer = jest.fn();

const dataset1Id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
const revision1Id = '85f0e416-8bd1-4946-9e2c-1c958897c6ef';
const import1Id = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
const user: User = getTestUser('test', 'user');

describe('API Endpoints for viewing the contents of a dataset', () => {
  let dbManager: DatabaseManager;
  beforeAll(async () => {
    try {
      dbManager = await initDb();
      await initPassport(dbManager.getDataSource());
      await user.save();
      await createFullDataset(dataset1Id, revision1Id, import1Id, user);
    } catch (error) {
      logger.error(error, 'Could not initialise test database');
      await dbManager.getDataSource().dropDatabase();
      await dbManager.getDataSource().destroy();
      process.exit(1);
    }
  });

  test('Get file from a dataset, stored in data lake, returns 200 and complete file data', async () => {
    const testFile2 = path.resolve(__dirname, `../sample-files/csv/sure-start-short.csv`);
    const lookupTable = path.resolve(__dirname, `../sample-files/csv/rowref-sw2-lookup.csv`);
    const testFile1Buffer = fs.readFileSync(testFile2);
    const lookupTableBuffer = fs.readFileSync(lookupTable);

    BlobStorage.prototype.loadBuffer = jest.fn().mockImplementation((filename: string, _directory: string) => {
      if (filename === 'RowRefLookupTable.csv') return lookupTableBuffer;
      else return testFile1Buffer;
    });

    const res = await request(app)
      .get(`/dataset/${dataset1Id}/view`)
      .set(getAuthHeader(user))
      .query({ page_number: 1, page_size: 100 });
    expect(res.status).toBe(200);
    expect(res.body.current_page).toBe(1);
    expect(res.body.total_pages).toBe(1);
    expect(res.body.page_size).toBe(100);
    expect(res.body.headers).toEqual([
      { index: -1, name: 'int_line_number', source_type: 'line_number' },
      { index: 0, name: 'Data Values', source_type: 'unknown' },
      { index: 1, name: 'YearCode', source_type: 'unknown' },
      { index: 2, name: 'Start Date', source_type: 'unknown' },
      { index: 3, name: 'End Date', source_type: 'unknown' },
      { index: 4, name: 'AreaCode', source_type: 'unknown' },
      { index: 5, name: 'RowRef', source_type: 'unknown' },
      { index: 6, name: 'Notes', source_type: 'unknown' }
    ]);
    expect(res.body.data[0]).toEqual([
      1,
      4.030567686,
      '2021-22',
      '01/04/2021',
      '31/03/2022',
      'Wales',
      'Health Visitor',
      'Average'
    ]);
    expect(res.body.data[23]).toEqual([
      24,
      780,
      '2021-22',
      '01/04/2021',
      '31/03/2022',
      'Isle of Anglesey',
      'Other Staff',
      null
    ]);
  });

  test('Get a dataset view returns 500 if there is no revision on the dataset', async () => {
    const dataset = await DatasetRepository.create({ createdBy: user }).save();

    const res = await request(app)
      .get(`/dataset/${dataset.id}/view`)
      .set(getAuthHeader(user))
      .query({ page_number: 2, page_size: 100 });

    expect(res.status).toBe(500);
    expect(res.body).toEqual({ error: 'No revision found for dataset' });
  });

  test('Get a dataset view returns 500 if there is no fact table on the dataset', async () => {
    const dataset = await DatasetRepository.create({ createdBy: user }).save();
    const revision = await RevisionRepository.create({ createdBy: user, dataset, revisionIndex: 1 }).save();
    await DatasetRepository.update(
      { id: dataset.id },
      { draftRevision: revision, startRevision: revision, endRevision: revision }
    );

    const res = await request(app)
      .get(`/dataset/${dataset.id}/view`)
      .set(getAuthHeader(user))
      .query({ page_number: 2, page_size: 100 });
    expect(res.status).toBe(500);
    expect(res.body).toEqual({ error: 'errors.cube_create_error' });
  });

  test('Get file view returns 404 when a not valid UUID is supplied', async () => {
    const res = await request(app).get(`/dataset/NOT-VALID-ID`).set(getAuthHeader(user));
    expect(res.status).toBe(404);
    expect(res.body).toEqual({ error: 'Dataset id is invalid or missing' });
  });

  afterAll(async () => {
    await dbManager.getDataSource().dropDatabase();
    await dbManager.getDataSource().destroy();
  });
});
