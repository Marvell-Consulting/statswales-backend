import request from 'supertest';
// eslint-disable-next-line import/no-unresolved
import { parse } from 'csv-parse/sync';
import { format as pgformat } from '@scaleleap/pg-format';

import app from '../../../src/app';
import { dbManager } from '../../../src/db/database-manager';
import { initPassport } from '../../../src/middleware/passport-auth';
import { User } from '../../../src/entities/user/user';
import { UserGroup } from '../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../src/entities/user/user-group-role';
import { GroupRole } from '../../../src/enums/group-role';
import { Dataset } from '../../../src/entities/dataset/dataset';
import { Revision } from '../../../src/entities/dataset/revision';
import { RevisionMetadata } from '../../../src/entities/dataset/revision-metadata';
import { DataTable } from '../../../src/entities/dataset/data-table';
import { FactTableColumn } from '../../../src/entities/dataset/fact-table-column';
import { FactTableColumnType } from '../../../src/enums/fact-table-column-type';
import { DataTableAction } from '../../../src/enums/data-table-action';
import { FileType } from '../../../src/enums/file-type';
import { ensureWorkerDataSources, resetDatabase } from '../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../helpers/get-test-user';
import { cubeDataSource } from '../../../src/db/cube-source';
import { createAllCubeFiles } from '../../../src/services/cube-builder';
import { MAX_PAGE_SIZE } from '../../../src/utils/page-defaults';
import BlobStorage from '../../../src/services/blob-storage';

jest.mock('../../../src/services/blob-storage');

BlobStorage.prototype.listFiles = jest
  .fn()
  .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);
BlobStorage.prototype.loadBuffer = jest.fn();

const TOTAL_ROWS = 12_000;
const datasetId = '839110bd-8c5e-46bf-9b89-b7eb90957c12';
const revisionId = 'b472dfc4-fbd0-4545-971b-844a0bdd4cc7';
const dataTableId = 'd67ee3c4-a63b-4aad-98a0-b0e0ee541bc3';

const user: User = getTestUser();
let userGroup = getTestUserGroup('Download Test Group');

/**
 * Creates a large test dataset with TOTAL_ROWS rows in the cube database,
 * marks it as published, and builds the cube views so it is accessible
 * via the v2 public consumer API.
 */
async function createLargePublishedDataset(): Promise<void> {
  const pastDate = new Date(Date.now() - 24 * 60 * 60 * 1000);

  // Create the dataset entity with fact table columns
  await Dataset.create({
    id: datasetId,
    createdBy: user,
    userGroupId: user?.groupRoles[0]?.groupId,
    firstPublishedAt: pastDate,
    factTable: [
      FactTableColumn.create({
        columnName: 'RowNum',
        columnIndex: 0,
        columnType: FactTableColumnType.Unknown,
        columnDatatype: 'BIGINT'
      }),
      FactTableColumn.create({
        columnName: 'Value',
        columnIndex: 1,
        columnType: FactTableColumnType.Unknown,
        columnDatatype: 'DOUBLE PRECISION'
      })
    ]
  }).save();

  // Create the revision with metadata, marked as published
  const revision = await Revision.create({
    id: revisionId,
    datasetId,
    createdBy: user,
    revisionIndex: 1,
    approvedAt: pastDate,
    publishAt: pastDate,
    metadata: ['en-GB', 'cy-GB'].map((lang) =>
      RevisionMetadata.create({
        language: lang,
        title: 'Large Download Test Dataset',
        summary: 'Test dataset for download integration tests'
      })
    ),
    dataTable: DataTable.create({
      id: dataTableId,
      filename: `${dataTableId}.csv`,
      originalFilename: 'large-test-data.csv',
      hash: 'test-hash-large-download',
      action: DataTableAction.Add,
      fileType: FileType.Csv,
      mimeType: 'text/csv',
      dataTableDescriptions: [
        { columnName: 'RowNum', columnIndex: 0, columnDatatype: 'BIGINT', factTableColumn: 'RowNum' },
        { columnName: 'Value', columnIndex: 1, columnDatatype: 'DOUBLE PRECISION', factTableColumn: 'Value' }
      ]
    })
  }).save();

  // Point dataset to its published revision
  await Dataset.update(datasetId, {
    startRevisionId: revision.id,
    endRevisionId: revision.id,
    publishedRevisionId: revision.id
  });

  // Create the fact table in the cube database and bulk-insert rows
  const cubeDB = await cubeDataSource.createQueryRunner();
  try {
    await cubeDB.query(pgformat('CREATE SCHEMA IF NOT EXISTS %I;', revisionId));
    await cubeDB.query(`
      CREATE TABLE data_tables."${dataTableId}" (
        "RowNum" BIGINT,
        "Value"  DOUBLE PRECISION
      );
    `);

    // Bulk-insert rows in batches for speed
    const BATCH_SIZE = 1000;
    for (let i = 0; i < TOTAL_ROWS; i += BATCH_SIZE) {
      const values: string[] = [];
      for (let j = i; j < Math.min(i + BATCH_SIZE, TOTAL_ROWS); j++) {
        values.push(`(${j + 1}, ${(j + 1) * 0.1})`);
      }
      await cubeDB.query(`INSERT INTO data_tables."${dataTableId}" VALUES ${values.join(',')};`);
    }
  } finally {
    await cubeDB.release();
  }

  // Build cube views so the consumer API can query the data
  await createAllCubeFiles(datasetId, revisionId);
}

describe('Consumer V2 download format page_size tests', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport(dbManager.getAppDataSource());
    userGroup = await dbManager.getAppDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();
    await createLargePublishedDataset();
  }, 60_000);

  describe('download formats return all rows without page_size cap', () => {
    it('CSV download returns all rows', async () => {
      const res = await request(app).get(`/v2/${datasetId}/data`).query({ format: 'csv' });

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/text\/csv/);
      expect(res.headers['content-disposition']).toMatch(/attachment/);

      const rows = parse(res.text, { columns: true });
      expect(rows.length).toBe(TOTAL_ROWS);
    });

    it('JSON download returns all rows', async () => {
      const res = await request(app).get(`/v2/${datasetId}/data`).query({ format: 'json' });

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      expect(res.headers['content-disposition']).toMatch(/attachment/);

      const data = JSON.parse(res.text);
      expect(Array.isArray(data)).toBe(true);
      expect(data.length).toBe(TOTAL_ROWS);
    });

    it('Excel download succeeds with correct content type', async () => {
      const res = await request(app)
        .get(`/v2/${datasetId}/data`)
        .query({ format: 'xlsx' })
        .buffer(true)
        .parse((res, callback) => {
          const chunks: Buffer[] = [];
          res.on('data', (chunk: Buffer) => chunks.push(chunk));
          res.on('end', () => callback(null, Buffer.concat(chunks)));
        });

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/spreadsheetml/);
      expect(res.headers['content-disposition']).toMatch(/attachment/);
    });
  });

  describe('explicit large page_size accepted for download formats', () => {
    it('accepts page_size above MAX_PAGE_SIZE for csv', async () => {
      const res = await request(app).get(`/v2/${datasetId}/data`).query({ format: 'csv', page_size: 50_000 });

      expect(res.status).toBe(200);

      const rows = parse(res.text, { columns: true });
      expect(rows.length).toBe(TOTAL_ROWS);
    });

    it('accepts page_size above MAX_PAGE_SIZE for json', async () => {
      const res = await request(app).get(`/v2/${datasetId}/data`).query({ format: 'json', page_size: 50_000 });

      expect(res.status).toBe(200);

      const data = JSON.parse(res.text);
      expect(data.length).toBe(TOTAL_ROWS);
    });
  });

  describe('non-download formats respect page_size cap', () => {
    it('frontend format returns paginated data', async () => {
      const res = await request(app)
        .get(`/v2/${datasetId}/data`)
        .query({ format: 'frontend', page_size: 100, page_number: 1 });

      expect(res.status).toBe(200);
      expect(res.body.data.length).toBe(100);
      expect(res.body.page_info.page_size).toBe(100);
      expect(Number(res.body.page_info.total_records)).toBe(TOTAL_ROWS);
    });

    it('rejects page_size above MAX_PAGE_SIZE for frontend format', async () => {
      const res = await request(app)
        .get(`/v2/${datasetId}/data`)
        .query({ format: 'frontend', page_size: MAX_PAGE_SIZE + 1 });

      expect(res.status).toBe(400);
    });

    it('accepts page_size at exactly MAX_PAGE_SIZE for frontend format', async () => {
      const res = await request(app)
        .get(`/v2/${datasetId}/data`)
        .query({ format: 'frontend', page_size: MAX_PAGE_SIZE, page_number: 1 });

      expect(res.status).toBe(200);
      expect(res.body.page_info.page_size).toBe(MAX_PAGE_SIZE);
    });
  });
});
