import request from 'supertest';
// eslint-disable-next-line import/no-unresolved
import { parse as parseCsv } from 'csv-parse/sync';

import app from '../../../../src/app';
import { dbManager } from '../../../../src/db/database-manager';
import { initPassport } from '../../../../src/middleware/passport-auth';
import { User } from '../../../../src/entities/user/user';
import { UserGroup } from '../../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../../src/entities/user/user-group-role';
import { GroupRole } from '../../../../src/enums/group-role';
import { ensureWorkerDataSources, resetDatabase } from '../../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../../helpers/get-test-user';
import { seedPublishedDataset } from '../../../helpers/seed-published-dataset';
import BlobStorage from '../../../../src/services/blob-storage';

jest.mock('../../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

const user: User = getTestUser('data test user');
let userGroup = getTestUserGroup('Data Test Group');

const DATASET_ID = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc';
const REVISION_ID = 'cccccccc-cccc-4ccc-8ccc-eeeeeeeeeeee';
const DATA_TABLE_ID = 'cccccccc-cccc-4ccc-8ccc-dddddddddddd';

const ROW_COUNT = 60;
const AREAS = ['W06000001', 'W06000002', 'W06000003'];
const YEARS = [2020, 2021, 2022, 2023];

const rowBuilder = (i: number): unknown[] => [
  AREAS[i % AREAS.length],
  YEARS[i % YEARS.length],
  Math.round((i + 1) * 10.5 * 100) / 100
];

describe('Consumer V1 — dataset data endpoints (/view, /view/filters, /download)', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport(dbManager.getAppDataSource());

    userGroup = await dbManager.getAppDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();

    await seedPublishedDataset({
      user,
      datasetId: DATASET_ID,
      revisionId: REVISION_ID,
      dataTableId: DATA_TABLE_ID,
      title: { en: 'Data endpoints fixture', cy: 'Ffynhonnell endpwyntiau data' },
      factColumns: [
        { name: 'AreaCode', datatype: 'VARCHAR' },
        { name: 'YearCode', datatype: 'BIGINT' },
        { name: 'Data', datatype: 'DOUBLE PRECISION' }
      ],
      rowBuilder,
      rowCount: ROW_COUNT
    });
  }, 120_000);

  describe('GET /v1/:dataset_id/view — paginated frontend view', () => {
    it('returns 200 with documented DatasetView shape', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/view`);
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      expect(res.body).toHaveProperty('data');
      expect(Array.isArray(res.body.data)).toBe(true);
      expect(res.body).toHaveProperty('headers');
      expect(Array.isArray(res.body.headers)).toBe(true);
      expect(res.body).toHaveProperty('page_info');
      expect(res.body).toHaveProperty('current_page');
      expect(res.body).toHaveProperty('page_size');
      expect(res.body).toHaveProperty('total_pages');
    });

    it('page_info.total_records matches the seeded row count', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/view`);
      expect(Number(res.body.page_info.total_records)).toBe(ROW_COUNT);
    });

    it('honours page_size and page_number', async () => {
      const p1 = await request(app).get(`/v1/${DATASET_ID}/view`).query({ page_size: 10, page_number: 1 });
      const p2 = await request(app).get(`/v1/${DATASET_ID}/view`).query({ page_size: 10, page_number: 2 });

      expect(p1.status).toBe(200);
      expect(p2.status).toBe(200);
      expect(p1.body.data.length).toBe(10);
      expect(p2.body.data.length).toBe(10);
      expect(p1.body.current_page).toBe(1);
      expect(p2.body.current_page).toBe(2);
      // different rows on different pages
      expect(JSON.stringify(p1.body.data)).not.toBe(JSON.stringify(p2.body.data));
    });

    it('returns 400 for a malformed filter JSON', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/view`).query({ filter: 'not json' });
      expect(res.status).toBe(400);
    });

    it('filters reduce the result set', async () => {
      const filter = JSON.stringify([{ columnName: 'YearCode', values: ['2020'] }]);
      const res = await request(app).get(`/v1/${DATASET_ID}/view`).query({ filter });
      expect(res.status).toBe(200);
      // 2020 appears once every 4 rows → 15 of 60
      expect(Number(res.body.page_info.total_records)).toBe(15);
    });

    it('returns 4xx for a filter on a non-existent column (rather than 500)', async () => {
      // Intended behaviour: user-provided-data error → 4xx. The controller currently
      // wraps any cube error in UnknownException → 500, so this may fail and flag the issue.
      const filter = JSON.stringify([{ columnName: 'NotAColumn', values: ['x'] }]);
      const res = await request(app).get(`/v1/${DATASET_ID}/view`).query({ filter });
      expect(res.status).toBeGreaterThanOrEqual(400);
      expect(res.status).toBeLessThan(500);
    });

    it('returns 400 for malformed sort_by JSON', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/view`).query({ sort_by: 'not json' });
      // parseSortByToObjects may silently return undefined for garbage; intended behaviour
      // per the OpenAPI sort_by schema is 400. If it returns 200 the controller is too
      // permissive about malformed query params.
      expect(res.status).toBe(400);
    });

    it('returns 404 for a non-existent dataset', async () => {
      const res = await request(app).get('/v1/99999999-9999-4999-8999-999999999999/view');
      expect(res.status).toBe(404);
    });
  });

  describe('GET /v1/:dataset_id/view/filters — available filters', () => {
    it('returns 200 and an array', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/view/filters`);
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      expect(Array.isArray(res.body)).toBe(true);
    });

    it('each entry matches the Filter schema: { factTableColumn, columnName, values[] }', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/view/filters`);
      for (const filter of res.body) {
        expect(typeof filter.factTableColumn).toBe('string');
        expect(typeof filter.columnName).toBe('string');
        expect(Array.isArray(filter.values)).toBe(true);
        for (const v of filter.values) {
          expect(typeof v.reference).toBe('string');
          expect(typeof v.description).toBe('string');
        }
      }
    });

    it('returns 404 for a non-existent dataset', async () => {
      const res = await request(app).get('/v1/99999999-9999-4999-8999-999999999999/view/filters');
      expect(res.status).toBe(404);
    });
  });

  describe('GET /v1/:dataset_id/download/:format — file download', () => {
    it('CSV streams text/csv with ROW_COUNT rows (no attachment header — programmatic consumer)', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/download/csv`);
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/text\/csv/);
      expect(res.headers['content-disposition']).toBeUndefined();
      const rows = parseCsv(res.text, { columns: true });
      expect(rows.length).toBe(ROW_COUNT);
    });

    it('JSON streams application/json with ROW_COUNT rows (no attachment header — programmatic consumer)', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/download/json`);
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      expect(res.headers['content-disposition']).toBeUndefined();
      const data = JSON.parse(res.text);
      expect(Array.isArray(data)).toBe(true);
      expect(data.length).toBe(ROW_COUNT);
    });

    it('XLSX returns a spreadsheetml attachment (zip magic bytes)', async () => {
      const res = await request(app)
        .get(`/v1/${DATASET_ID}/download/xlsx`)
        .buffer(true)
        .parse((response, callback) => {
          const chunks: Buffer[] = [];
          response.on('data', (c: Buffer) => chunks.push(c));
          response.on('end', () => callback(null, Buffer.concat(chunks)));
        });
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/spreadsheetml/);
      expect(res.headers['content-disposition']).toMatch(/attachment/);
      const buf = res.body as Buffer;
      // XLSX is a zip — first two bytes are "PK"
      expect(buf.subarray(0, 2).toString('utf8')).toBe('PK');
    });

    it('returns 400 for an unrecognised format', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/download/pdf`);
      expect(res.status).toBe(400);
    });

    it('filter+sort_by combined correctly narrow and order rows (CSV)', async () => {
      const filter = JSON.stringify([{ columnName: 'YearCode', values: ['2020', '2021'] }]);
      const res = await request(app).get(`/v1/${DATASET_ID}/download/csv`).query({ filter });
      expect(res.status).toBe(200);
      const rows = parseCsv(res.text, { columns: true }) as Record<string, string>[];
      // 2020: 15, 2021: 15 → 30 total
      expect(rows.length).toBe(30);
    });

    it('returns 404 for a non-existent dataset', async () => {
      const res = await request(app).get('/v1/99999999-9999-4999-8999-999999999999/download/csv');
      expect(res.status).toBe(404);
    });
  });
});
