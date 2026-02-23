import path from 'node:path';

import request from 'supertest';

import app from '../../src/app';
import { dbManager } from '../../src/db/database-manager';
import { initPassport } from '../../src/middleware/passport-auth';
import { SourceAssignmentDTO } from '../../src/dtos/source-assignment-dto';
import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
import { User } from '../../src/entities/user/user';
import { UserGroup } from '../../src/entities/user/user-group';
import { UserGroupRole } from '../../src/entities/user/user-group-role';
import { GroupRole } from '../../src/enums/group-role';
import { Revision } from '../../src/entities/dataset/revision';
import { logger } from '../../src/utils/logger';
import { getTestUser, getTestUserGroup } from '../helpers/get-test-user';
import { getAuthHeader } from '../helpers/auth-header';
import BlobStorage from '../../src/services/blob-storage';
import { QueryRunner } from 'typeorm';
import { FactTableValidationExceptionType } from '../../src/enums/fact-table-validation-exception-type';
import { DatasetRepository } from '../../src/repositories/dataset';

jest.mock('../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.saveBuffer = jest.fn();

const CSV_DIR = path.resolve(__dirname, '../sample-files/csv');

const user: User = getTestUser('validation-test-user');
let userGroup = getTestUserGroup('Validation Group');
let queryRunner: QueryRunner;
const createdRevisions: string[] = [];

// Helper to create a dataset, upload a CSV, and assign sources in one go
async function createDatasetAndUploadCsv(csvPath: string): Promise<{ datasetId: string; revisionId: string }> {
  const data = { title: 'Validation Test Dataset', user_group_id: userGroup.id };
  const createRes = await request(app).post('/dataset').set(getAuthHeader(user)).send(data);
  expect(createRes.status).toBe(201);
  const datasetId = createRes.body.id;

  const uploadRes = await request(app)
    .post(`/dataset/${datasetId}/data`)
    .set(getAuthHeader(user))
    .attach('csv', csvPath);
  expect(uploadRes.status).toBe(201);
  const revisionId = uploadRes.body.start_revision_id;
  createdRevisions.push(revisionId);
  return { datasetId, revisionId };
}

// Builds a minimal source assignment for CSVs with columns: date, data, measure, notes
function minimalSourceAssignment(): SourceAssignmentDTO[] {
  return [
    { column_index: 0, column_name: 'date', column_type: FactTableColumnType.Dimension },
    { column_index: 1, column_name: 'data', column_type: FactTableColumnType.DataValues },
    { column_index: 2, column_name: 'measure', column_type: FactTableColumnType.Measure },
    { column_index: 3, column_name: 'notes', column_type: FactTableColumnType.NoteCodes }
  ];
}

describe('Fact table validation (integration)', () => {
  beforeAll(async () => {
    try {
      await dbManager.initDataSources();
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.getAppDataSource().runMigrations();
      await initPassport(dbManager.getAppDataSource());
      queryRunner = dbManager.getAppDataSource().createQueryRunner();
      await queryRunner.dropSchema('data_tables', true, true);
      await queryRunner.dropSchema('lookup_tables', true, true);
      await queryRunner.createSchema('data_tables', true);
      await queryRunner.createSchema('lookup_tables', true);
      userGroup = (await dbManager.getAppDataSource().getRepository(UserGroup).save(userGroup)) as UserGroup;
      user.groupRoles = [UserGroupRole.create({ group: userGroup as UserGroup, roles: [GroupRole.Editor] })];
      await user.save();
    } catch (error) {
      logger.error(error, 'Could not initialise test database for validation tests');
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.destroyDataSources();
      process.exit(1);
    } finally {
      await queryRunner.release();
    }
  });

  afterAll(async () => {
    const qr = dbManager.getAppDataSource().createQueryRunner();
    await qr.dropSchema('data_tables', true, true);
    await qr.dropSchema('lookup_tables', true, true);
    for (const revId of createdRevisions) {
      try {
        await qr.dropSchema(revId, true, true);
      } catch (_) {
        /* empty */
      }
    }
    await qr.release();
    await dbManager.getAppDataSource().dropDatabase();
    await dbManager.destroyDataSources();
  });

  describe('First revision - valid data', () => {
    test('Assigning sources succeeds for a valid minimal CSV', async () => {
      const { datasetId } = await createDatasetAndUploadCsv(path.join(CSV_DIR, 'minimal/data.csv'));
      const res = await request(app)
        .patch(`/dataset/${datasetId}/sources`)
        .set(getAuthHeader(user))
        .send(minimalSourceAssignment());
      expect(res.status).toBe(200);
      expect(res.body.dataset).toBeDefined();
      expect(res.body.build_id).toBeDefined();
    });
  });

  describe('First revision - duplicate facts', () => {
    test('Assigning sources returns 400 with duplicate_fact error for CSV with duplicate rows', async () => {
      const { datasetId } = await createDatasetAndUploadCsv(path.join(CSV_DIR, 'invalid/dupe-fact.csv'));
      const res = await request(app)
        .patch(`/dataset/${datasetId}/sources`)
        .set(getAuthHeader(user))
        .send(minimalSourceAssignment());
      expect(res.status).toBe(400);
      expect(res.body.errors).toBeDefined();
      expect(res.body.errors[0].message.key).toBe(
        `errors.fact_table_validation.${FactTableValidationExceptionType.DuplicateFact}`
      );
      expect(res.body.data).toBeDefined();
      expect(res.body.headers).toBeDefined();
    });
  });

  describe('First revision - non-numeric data values', () => {
    test('Assigning sources returns 400 with non_numeric_data_value error', async () => {
      const { datasetId } = await createDatasetAndUploadCsv(path.join(CSV_DIR, 'invalid/non-numeric-data.csv'));
      const res = await request(app)
        .patch(`/dataset/${datasetId}/sources`)
        .set(getAuthHeader(user))
        .send(minimalSourceAssignment());
      expect(res.status).toBe(400);
      expect(res.body.errors).toBeDefined();
      expect(res.body.errors[0].message.key).toBe(
        `errors.fact_table_validation.${FactTableValidationExceptionType.NonNumericDataValueColumn}`
      );
      expect(res.body.data).toBeDefined();
      expect(res.body.headers).toBeDefined();
    });
  });

  describe('First revision - incomplete facts', () => {
    test('Assigning sources returns 400 with incomplete_fact error for CSV with null dimension', async () => {
      const { datasetId } = await createDatasetAndUploadCsv(path.join(CSV_DIR, 'invalid/incomplete-fact.csv'));
      const res = await request(app)
        .patch(`/dataset/${datasetId}/sources`)
        .set(getAuthHeader(user))
        .send(minimalSourceAssignment());
      expect(res.status).toBe(400);
      expect(res.body.errors).toBeDefined();
      expect(res.body.errors[0].message.key).toBe(
        `errors.fact_table_validation.${FactTableValidationExceptionType.IncompleteFact}`
      );
    });
  });

  describe('First revision - bad note codes', () => {
    test('Assigning sources returns 400 with bad_note_codes error', async () => {
      const { datasetId } = await createDatasetAndUploadCsv(path.join(CSV_DIR, 'invalid/bad-note-codes.csv'));
      const res = await request(app)
        .patch(`/dataset/${datasetId}/sources`)
        .set(getAuthHeader(user))
        .send(minimalSourceAssignment());
      expect(res.status).toBe(400);
      expect(res.body.errors).toBeDefined();
      expect(res.body.errors[0].message.key).toBe(
        `errors.fact_table_validation.${FactTableValidationExceptionType.BadNoteCodes}`
      );
      expect(res.body.data).toBeDefined();
      expect(res.body.headers).toBeDefined();
    });
  });

  describe('Subsequent revision - duplicate facts on update', () => {
    test('Uploading a CSV with duplicates to revision 2 returns 400 with duplicate_fact error', async () => {
      // Step 1: Create dataset with valid data and assign sources
      const { datasetId, revisionId } = await createDatasetAndUploadCsv(path.join(CSV_DIR, 'minimal/data.csv'));
      const assignRes = await request(app)
        .patch(`/dataset/${datasetId}/sources`)
        .set(getAuthHeader(user))
        .send(minimalSourceAssignment());
      expect(assignRes.status).toBe(200);

      // Step 2: Simulate publication by directly updating the database
      // Mark the revision as approved/published
      const revision = await Revision.findOneOrFail({ where: { id: revisionId } });
      revision.approvedAt = new Date();
      revision.approvedBy = user;
      revision.publishAt = new Date();
      revision.onlineCubeFilename = `${revisionId}.duckdb`;
      await revision.save();

      // Set publishedRevision on dataset, clear draftRevision
      await DatasetRepository.save({
        id: datasetId,
        publishedRevision: revision,
        draftRevision: null
      });

      // Step 3: Create revision 2 via the API
      const newRevRes = await request(app).post(`/dataset/${datasetId}/revision/`).set(getAuthHeader(user));
      expect(newRevRes.status).toBe(201);
      const rev2Id = newRevRes.body.id;
      createdRevisions.push(rev2Id);

      // Step 4: Upload a CSV with a duplicate row (2015,152,2 exists in original data.csv)
      const uploadRes = await request(app)
        .post(`/dataset/${datasetId}/revision/by-id/${rev2Id}/data-table`)
        .set(getAuthHeader(user))
        .attach('csv', path.join(CSV_DIR, 'invalid/update-dupe-fact.csv'));

      // This should return 400 with a specific duplicate_fact error, not a generic 500
      expect(uploadRes.status).toBe(400);
      expect(uploadRes.body.errors).toBeDefined();
      expect(uploadRes.body.errors[0].message.key).toBe(
        `errors.fact_table_validation.${FactTableValidationExceptionType.DuplicateFact}`
      );
    });
  });
});
