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

import { createFullDataset } from '../helpers/test-helper';
import { getTestUser, getTestUserGroup } from '../helpers/get-test-user';
import { getAuthHeader } from '../helpers/auth-header';
import BlobStorage from '../../src/services/blob-storage';
import { UserGroup } from '../../src/entities/user/user-group';
import { UserGroupRole } from '../../src/entities/user/user-group-role';
import { GroupRole } from '../../src/enums/group-role';
import { QueryRunner } from 'typeorm';

jest.mock('../../src/services/blob-storage');

BlobStorage.prototype.listFiles = jest
  .fn()
  .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

BlobStorage.prototype.loadBuffer = jest.fn();

const dataset1Id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
const revision1Id = '85f0e416-8bd1-4946-9e2c-1c958897c6ef';
const import1Id = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
const user: User = getTestUser('test', 'user');
let userGroup = getTestUserGroup('Test Group');
let queryRunner: QueryRunner;

describe('API Endpoints for viewing the contents of a dataset', () => {
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
    expect(res.body.total_pages).toBe(15);
    expect(res.body.page_size).toBe(100);
    expect(res.body.headers).toEqual([
      { index: -1, name: 'Data Values', source_type: 'unknown' },
      { index: 0, name: 'Measure', source_type: 'unknown' },
      { index: 1, name: 'Year', source_type: 'unknown' },
      { index: 2, name: 'Start Date', source_type: 'unknown' },
      { index: 3, name: 'End Date', source_type: 'unknown' },
      { index: 4, name: 'Local Authority', source_type: 'unknown' },
      { index: 5, name: 'Staff Type', source_type: 'unknown' },
      { index: 6, name: 'Notes', source_type: 'unknown' }
    ]);
    expect(res.body.data[0]).toEqual([
      '3,697',
      'Number of contacts',
      '2013-14',
      '01/04/2013',
      '31/03/2014',
      'Merthyr Tydfil',
      'Health Visitor',
      null
    ]);
    // If this test fails don't just change the output to match.  It's failure implies something in the cube builder
    // has changed the view significantly.  Probably a broken join statement.
    expect(res.body.data[23]).toEqual([
      '1,247',
      'Number of contacts',
      '2013-14',
      '01/04/2013',
      '31/03/2014',
      'Merthyr Tydfil',
      'Wider Health Team',
      null
    ]);
  });

  test('Get a dataset view returns 500 if there is no revision on the dataset', async () => {
    const dataset = await DatasetRepository.create({ createdBy: user, userGroupId: userGroup.id }).save();

    const res = await request(app)
      .get(`/dataset/${dataset.id}/view`)
      .set(getAuthHeader(user))
      .query({ page_number: 2, page_size: 100 });

    expect(res.status).toBe(500);
    expect(res.body).toEqual({ error: 'No revision found for dataset' });
  });

  test('Get file view returns 404 when a not valid UUID is supplied', async () => {
    const res = await request(app).get(`/dataset/NOT-VALID-ID`).set(getAuthHeader(user));
    expect(res.status).toBe(404);
    expect(res.body).toEqual({ error: 'Dataset id is invalid or missing' });
  });

  afterAll(async () => {
    await queryRunner.dropSchema('data_tables', true, true);
    await queryRunner.dropSchema(revision1Id, true, true);
    await dbManager.getDataSource().dropDatabase();
    await dbManager.getDataSource().destroy();
  });
});
