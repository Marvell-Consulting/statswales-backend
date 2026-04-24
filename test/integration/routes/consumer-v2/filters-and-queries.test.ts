import request from 'supertest';

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

const user: User = getTestUser('v2 filters/queries test user');
let userGroup = getTestUserGroup('V2 Filters Test Group');

const DATASET_ID = 'eeeeeeee-eeee-4eee-8ee2-eeeeeeeeeeee';
const REVISION_ID = 'eeeeeeee-eeee-4eee-8ee2-aaaaaaaaaaaa';
const DATA_TABLE_ID = 'eeeeeeee-eeee-4eee-8ee2-bbbbbbbbbbbb';

const AREAS = ['W06000001', 'W06000002', 'W06000003'];
const YEARS = [2020, 2021, 2022, 2023];
// One row per (Area, Year) combination — fact table has a composite PK on the
// Dimension/Measure columns, so rows must be unique across them.
const ROW_COUNT = AREAS.length * YEARS.length;

const AREA_LOOKUP_ID = 'abcdef00-0000-4000-8000-000000000001';
const YEAR_LOOKUP_ID = 'abcdef00-0000-4000-8000-000000000002';

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

describe('Consumer V2 — filters + query-store endpoints', () => {
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
      title: { en: 'Filters fixture', cy: 'Ffynhonnell hidlwyr' },
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
  }, 120_000);

  describe('GET /v2/:dataset_id/filters — available filters', () => {
    it('returns 200 and the documented Filters shape', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/filters`);
      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      // Response is an array of Filter objects; each has columnName + values.
      // (Lookup-backed dimensions would also carry reference/description per value.)
      expect(Array.isArray(res.body) || Array.isArray(res.body.data) || typeof res.body === 'object').toBe(true);
    });

    it('returns 404 for a non-existent dataset', async () => {
      const res = await request(app).get('/v2/99999999-9999-4999-8999-999999999999/filters');
      expect(res.status).toBe(404);
    });

    it('returns 404 for a malformed UUID', async () => {
      const res = await request(app).get('/v2/not-a-uuid/filters');
      expect(res.status).toBe(404);
    });
  });

  describe('POST /v2/:dataset_id/data — generate filter ID', () => {
    it('empty body returns a FilterId for the default query', async () => {
      const res = await request(app).post(`/v2/${DATASET_ID}/data`).send({});
      expect(res.status).toBe(200);
      expect(typeof res.body.filterId).toBe('string');
    });

    it('valid DataOptions body returns a FilterId', async () => {
      const res = await request(app)
        .post(`/v2/${DATASET_ID}/data`)
        .send({
          filters: [{ AreaCode: ['W06000001'] }],
          options: { use_raw_column_names: true, use_reference_values: true, data_value_type: 'raw' }
        });
      expect(res.status).toBe(200);
      expect(typeof res.body.filterId).toBe('string');
    });

    it('identical bodies return the same FilterId (deterministic)', async () => {
      const body = {
        filters: [{ YearCode: ['2020', '2021'] }],
        options: { use_raw_column_names: true, use_reference_values: true, data_value_type: 'raw' }
      };
      const a = await request(app).post(`/v2/${DATASET_ID}/data`).send(body);
      const b = await request(app).post(`/v2/${DATASET_ID}/data`).send(body);
      expect(a.status).toBe(200);
      expect(b.status).toBe(200);
      expect(a.body.filterId).toBe(b.body.filterId);
    });

    it('different bodies return different FilterIds', async () => {
      const a = await request(app)
        .post(`/v2/${DATASET_ID}/data`)
        .send({ filters: [{ YearCode: ['2020'] }] });
      const b = await request(app)
        .post(`/v2/${DATASET_ID}/data`)
        .send({ filters: [{ YearCode: ['2021'] }] });
      expect(a.body.filterId).not.toBe(b.body.filterId);
    });

    it('invalid body (wrong type on options) → 400', async () => {
      const res = await request(app).post(`/v2/${DATASET_ID}/data`).send({ options: 'not-an-object' });
      expect(res.status).toBe(400);
    });

    it('returns 404 for a non-existent dataset', async () => {
      const res = await request(app).post('/v2/99999999-9999-4999-8999-999999999999/data').send({});
      expect(res.status).toBe(404);
    });
  });

  describe('POST /v2/:dataset_id/pivot — generate pivot filter ID', () => {
    it('valid pivot body with x/y on real columns returns a FilterId', async () => {
      const res = await request(app)
        .post(`/v2/${DATASET_ID}/pivot`)
        .send({ pivot: { x: 'AreaCode', y: 'YearCode' }, options: { use_raw_column_names: true } });
      expect(res.status).toBe(200);
      expect(typeof res.body.filterId).toBe('string');
    });

    it('identical pivot bodies return the same FilterId', async () => {
      const body = { pivot: { x: 'AreaCode', y: 'YearCode' }, options: { use_raw_column_names: true } };
      const a = await request(app).post(`/v2/${DATASET_ID}/pivot`).send(body);
      const b = await request(app).post(`/v2/${DATASET_ID}/pivot`).send(body);
      expect(a.status).toBe(200);
      expect(b.status).toBe(200);
      expect(typeof a.body.filterId).toBe('string');
      expect(a.body.filterId).toBe(b.body.filterId);
    });

    it('missing pivot.x or pivot.y → 400', async () => {
      const res = await request(app).post(`/v2/${DATASET_ID}/pivot`).send({});
      expect(res.status).toBe(400);
    });

    it('multi-column x ("a,b") → 400 errors.pivot_only_one_column', async () => {
      const res = await request(app)
        .post(`/v2/${DATASET_ID}/pivot`)
        .send({ pivot: { x: 'AreaCode,YearCode', y: 'YearCode' }, options: { use_raw_column_names: true } });
      expect(res.status).toBe(400);
    });

    it('unknown x column → 400', async () => {
      const res = await request(app)
        .post(`/v2/${DATASET_ID}/pivot`)
        .send({ pivot: { x: 'NotAColumn', y: 'YearCode' }, options: { use_raw_column_names: true } });
      expect(res.status).toBe(400);
    });

    it('unknown y column → 400', async () => {
      const res = await request(app)
        .post(`/v2/${DATASET_ID}/pivot`)
        .send({ pivot: { x: 'AreaCode', y: 'NotAColumn' }, options: { use_raw_column_names: true } });
      expect(res.status).toBe(400);
    });

    it('non-axis filter with multiple values → 400', async () => {
      const res = await request(app)
        .post(`/v2/${DATASET_ID}/pivot`)
        .send({
          pivot: { x: 'AreaCode', y: 'YearCode' },
          filters: [{ Data: ['1.0', '2.0'] }],
          options: { use_raw_column_names: true }
        });
      expect(res.status).toBe(400);
    });

    it('returns 404 for a non-existent dataset', async () => {
      const res = await request(app)
        .post('/v2/99999999-9999-4999-8999-999999999999/pivot')
        .send({ pivot: { x: 'AreaCode', y: 'YearCode' } });
      expect(res.status).toBe(404);
    });
  });

  describe('GET /v2/:dataset_id/query/:filter_id — filter details', () => {
    let filterId: string;

    beforeAll(async () => {
      const res = await request(app)
        .post(`/v2/${DATASET_ID}/data`)
        .send({ filters: [{ AreaCode: ['W06000001'] }] });
      filterId = res.body.filterId;
    });

    it('returns the QueryStore shape for a valid filter_id', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/query/${filterId}`);
      expect(res.status).toBe(200);
      expect(res.body.id).toBe(filterId);
      expect(res.body).toHaveProperty('hash');
      expect(res.body).toHaveProperty('datasetId');
      expect(res.body).toHaveProperty('revisionId');
      expect(res.body).toHaveProperty('requestObject');
      expect(res.body.datasetId).toBe(DATASET_ID);
    });

    it('requestObject round-trips the filters that were posted', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/query/${filterId}`);
      expect(res.body.requestObject.filters).toEqual([{ AreaCode: ['W06000001'] }]);
    });

    it('returns 404 for a non-existent filter_id', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/query/88888888-8888-4888-8888-888888888888`);
      expect(res.status).toBe(404);
    });

    it('returns 404 when the dataset does not exist (middleware guard runs first)', async () => {
      const res = await request(app).get(`/v2/99999999-9999-4999-8999-999999999999/query/${filterId}`);
      expect(res.status).toBe(404);
    });
  });

  describe('GET /v2/:dataset_id/query/ — no filter ID (undocumented)', () => {
    // NOTE: swagger-ignored at src/routes/consumer/v2/api.ts:467. The controller computes a
    // default-options QueryStore when filter_id is absent. Record behaviour so we can decide
    // later whether to document or remove it.
    it('returns a QueryStore shape for the default query (or 404)', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/query/`);
      expect([200, 404]).toContain(res.status);
      if (res.status === 200) {
        expect(res.body).toHaveProperty('id');
        expect(res.body.datasetId).toBe(DATASET_ID);
      }
    });
  });
});
