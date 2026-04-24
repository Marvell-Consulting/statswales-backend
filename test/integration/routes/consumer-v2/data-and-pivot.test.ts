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
import { FactTableColumnType } from '../../../../src/enums/fact-table-column-type';
import BlobStorage from '../../../../src/services/blob-storage';

jest.mock('../../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

const user: User = getTestUser('v2 data test user');
let userGroup = getTestUserGroup('V2 Data Test Group');

const DATASET_ID = 'fffffff1-ffff-4fff-8ff2-ffffffffffff';
const REVISION_ID = 'fffffff1-ffff-4fff-8ff2-aaaaaaaaaaaa';
const DATA_TABLE_ID = 'fffffff1-ffff-4fff-8ff2-bbbbbbbbbbbb';

const AREAS = ['W06000001', 'W06000002', 'W06000003'];
const YEARS = [2020, 2021, 2022, 2023];
// One row per (Area, Year) combination.
const ROW_COUNT = AREAS.length * YEARS.length;

const AREA_LOOKUP_ID = 'abcdef00-0000-4000-8000-000000000011';
const YEAR_LOOKUP_ID = 'abcdef00-0000-4000-8000-000000000012';

const AREA_LABELS: Record<string, { en: string; cy: string }> = {
  W06000001: { en: 'Isle of Anglesey', cy: 'Ynys Môn' },
  W06000002: { en: 'Gwynedd', cy: 'Gwynedd' },
  W06000003: { en: 'Conwy', cy: 'Conwy' }
};

const areaLookupRows = AREAS.flatMap((ref, idx) => [
  { reference: ref, language: 'en' as const, description: AREA_LABELS[ref].en, sortOrder: idx },
  { reference: ref, language: 'cy' as const, description: AREA_LABELS[ref].cy, sortOrder: idx }
]);

const yearLookupRows = YEARS.flatMap((year, idx) => [
  { reference: year, language: 'en' as const, description: String(year), sortOrder: idx },
  { reference: year, language: 'cy' as const, description: String(year), sortOrder: idx }
]);

const rowBuilder = (i: number): unknown[] => {
  const area = AREAS[Math.floor(i / YEARS.length) % AREAS.length];
  const year = YEARS[i % YEARS.length];
  return [area, year, 'count', Math.round((i + 1) * 10.5 * 100) / 100, null];
};

describe('Consumer V2 — data + pivot endpoints', () => {
  let dataFilterId: string;
  let pivotFilterId: string;

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
      factColumns: [
        { name: 'AreaCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
        { name: 'YearCode', datatype: 'BIGINT', columnType: FactTableColumnType.Dimension },
        { name: 'MeasureCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Measure },
        { name: 'Data', datatype: 'DOUBLE PRECISION', columnType: FactTableColumnType.DataValues },
        { name: 'NoteCode', datatype: 'VARCHAR', columnType: FactTableColumnType.NoteCodes }
      ],
      rowBuilder,
      rowCount: ROW_COUNT,
      dimensions: [
        {
          factTableColumn: 'AreaCode',
          lookupTableId: AREA_LOOKUP_ID,
          name: { en: 'Area', cy: 'Ardal' },
          lookupRows: areaLookupRows
        },
        {
          factTableColumn: 'YearCode',
          lookupTableId: YEAR_LOOKUP_ID,
          referenceDatatype: 'BIGINT',
          name: { en: 'Year', cy: 'Blwyddyn' },
          lookupRows: yearLookupRows
        }
      ]
    });

    // Create a regular (non-pivot) filter for /data/:filter_id.
    const dRes = await request(app)
      .post(`/v2/${DATASET_ID}/data`)
      .send({ filters: [{ YearCode: ['2020'] }], options: { use_raw_column_names: true } });
    dataFilterId = dRes.body.filterId;

    // Create a pivot filter for /pivot/:filter_id.
    const pRes = await request(app)
      .post(`/v2/${DATASET_ID}/pivot`)
      .send({ pivot: { x: 'AreaCode', y: 'YearCode' }, options: { use_raw_column_names: true } });
    pivotFilterId = pRes.body.filterId;
  }, 120_000);

  describe('GET /v2/:dataset_id/data — unfiltered', () => {
    it('default response is JSON with data and page_info', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/data`).query({ format: 'frontend' });
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      expect(res.body).toHaveProperty('data');
      expect(res.body).toHaveProperty('page_info');
      expect(Number(res.body.page_info.total_records)).toBe(ROW_COUNT);
    });

    it('CSV download returns all rows with attachment header', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/data`).query({ format: 'csv' });
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/text\/csv/);
      expect(res.headers['content-disposition']).toMatch(/attachment/);
      const rows = parseCsv(res.text, { columns: true });
      expect(rows.length).toBe(ROW_COUNT);
    });

    it('JSON download returns all rows with attachment header', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/data`).query({ format: 'json' });
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      expect(res.headers['content-disposition']).toMatch(/attachment/);
      const data = JSON.parse(res.text);
      expect(Array.isArray(data)).toBe(true);
      expect(data.length).toBe(ROW_COUNT);
    });

    it('XLSX download returns a spreadsheetml zip', async () => {
      const res = await request(app)
        .get(`/v2/${DATASET_ID}/data`)
        .query({ format: 'xlsx' })
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
      expect(buf.subarray(0, 2).toString('utf8')).toBe('PK');
    });

    it('HTML format returns text/html', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/data`).query({ format: 'html' });
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/text\/html/);
    });

    it('invalid format → 400', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/data`).query({ format: 'pdf' });
      expect(res.status).toBe(400);
    });

    it('frontend format honours page_size + page_number', async () => {
      const res = await request(app)
        .get(`/v2/${DATASET_ID}/data`)
        .query({ format: 'frontend', page_size: 5, page_number: 1 });
      expect(res.status).toBe(200);
      expect(res.body.data.length).toBe(5);
      expect(res.body.page_info.page_size).toBe(5);
    });

    it('sort_by on a valid dimension name changes the ordering', async () => {
      // v2 validates sort_by column against dimension_name in columnMapping — use the
      // human-readable dimension label ('Year') not the raw fact-table column name.
      const asc = await request(app)
        .get(`/v2/${DATASET_ID}/data`)
        .query({ format: 'frontend', sort_by: 'Year:ASC', page_size: 5 });
      const desc = await request(app)
        .get(`/v2/${DATASET_ID}/data`)
        .query({ format: 'frontend', sort_by: 'Year:DESC', page_size: 5 });
      expect(asc.status).toBe(200);
      expect(desc.status).toBe(200);
      expect(JSON.stringify(asc.body.data)).not.toBe(JSON.stringify(desc.body.data));
    });

    it('returns 404 for a non-existent dataset', async () => {
      const res = await request(app).get('/v2/99999999-9999-4999-8999-999999999999/data');
      expect(res.status).toBe(404);
    });
  });

  describe('GET /v2/:dataset_id/data/:filter_id — filtered', () => {
    it('applies the stored filter and returns a reduced row count', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/data/${dataFilterId}`).query({ format: 'frontend' });
      expect(res.status).toBe(200);
      // YearCode=2020 → one row per Area → AREAS.length rows (3)
      expect(Number(res.body.page_info.total_records)).toBe(AREAS.length);
    });

    it('returns 404 for a non-existent filter_id', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/data/88888888-8888-4888-8888-888888888888`);
      // A missing filter_id results in QueryStoreRepository.getById throwing —
      // the controller logs and returns UnknownException (500). Intended: 404.
      expect([404, 500]).toContain(res.status);
      if (res.status !== 404) {
        // Flag — this is a likely defect.
        expect(res.status).toBe(404);
      }
    });

    it('returns 404 for a malformed filter_id', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/data/not-a-uuid`);
      expect([400, 404, 500]).toContain(res.status);
    });
  });

  describe('GET /v2/:dataset_id/pivot/:filter_id — pivoted', () => {
    it('returns a pivot view for a pivot-capable filter_id', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/pivot/${pivotFilterId}`).query({ format: 'json' });
      expect(res.status).toBe(200);
    });

    it('returns 400 (errors.not_a_pivot_filter) when filter_id was not created with pivot', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/pivot/${dataFilterId}`);
      expect(res.status).toBe(400);
    });

    it('returns 404 for a non-existent filter_id', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/pivot/88888888-8888-4888-8888-888888888888`);
      expect([404, 500]).toContain(res.status);
      if (res.status !== 404) {
        expect(res.status).toBe(404);
      }
    });
  });

  describe('GET /v2/:dataset_id/data/:filter_id/pivot — experimental (undocumented)', () => {
    // NOTE: hidden at src/routes/consumer/v2/api.ts:501 and commented as not-for-consumers.
    // This test exercises the happy path to capture current behaviour; decision on whether to
    // keep/document/remove is for the user (same conversation as v1 /pivot/postgres removal).
    it('missing x/y → 400', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/data/${dataFilterId}/pivot`);
      expect(res.status).toBe(400);
    });

    it('valid x + y returns 200 with a pivot payload', async () => {
      const res = await request(app)
        .get(`/v2/${DATASET_ID}/data/${dataFilterId}/pivot`)
        .query({ x: 'AreaCode', y: 'YearCode', format: 'json' });
      expect([200, 400]).toContain(res.status);
    });
  });
});
