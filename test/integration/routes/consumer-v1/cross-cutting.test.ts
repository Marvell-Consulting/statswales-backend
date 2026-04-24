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
import BlobStorage from '../../../../src/services/blob-storage';

jest.mock('../../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

const user: User = getTestUser('cross-cutting test user');
let userGroup = getTestUserGroup('Cross Cutting Test Group');

const DATASET_ID = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd';
const REVISION_ID = 'dddddddd-dddd-4ddd-8ddd-eeeeeeeeeeee';
const DATA_TABLE_ID = 'dddddddd-dddd-4ddd-8ddd-ffffffffffff';

// Endpoints that accept a :dataset_id path param — used for the "invalid UUID" and
// "non-existent dataset" sweeps.
const DATASET_ENDPOINTS = [
  (id: string) => `/v1/${id}`,
  (id: string) => `/v1/${id}/history`,
  (id: string) => `/v1/${id}/view`,
  (id: string) => `/v1/${id}/view/filters`,
  (id: string) => `/v1/${id}/download/csv`
];

describe('Consumer V1 — cross-cutting behaviour (CORS, method guard, Vary, 404 sweeps)', () => {
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
      dataTableId: DATA_TABLE_ID
    });
  }, 120_000);

  describe('Method guard (api.ts:51-55 — only GET allowed)', () => {
    it('POST /v1/ → 405', async () => {
      const res = await request(app).post('/v1/');
      expect(res.status).toBe(405);
    });

    it('PUT /v1/:dataset_id → 405', async () => {
      const res = await request(app).put(`/v1/${DATASET_ID}`);
      expect(res.status).toBe(405);
    });

    it('DELETE /v1/:dataset_id → 405', async () => {
      const res = await request(app).delete(`/v1/${DATASET_ID}`);
      expect(res.status).toBe(405);
    });

    it('PATCH /v1/topic → 405', async () => {
      const res = await request(app).patch('/v1/topic');
      expect(res.status).toBe(405);
    });
  });

  describe('CORS (api.ts:49)', () => {
    it('OPTIONS preflight sets Access-Control-Allow-Origin: *', async () => {
      const res = await request(app).options('/v1/').set('Origin', 'https://example.com');
      // Preflight should succeed with permissive CORS
      expect(res.status).toBeLessThan(300);
      expect(res.headers['access-control-allow-origin']).toBe('*');
    });

    it('GET responses include Access-Control-Allow-Origin for cross-origin callers', async () => {
      const res = await request(app).get('/v1/').set('Origin', 'https://example.com');
      expect(res.headers['access-control-allow-origin']).toBe('*');
    });
  });

  describe('Accept-Language caching (api.ts:57)', () => {
    it('GET /v1/ sets Vary: Accept-Language', async () => {
      const res = await request(app).get('/v1/');
      // `Vary` can contain multiple comma-separated values. Assert Accept-Language is among them.
      const vary = (res.headers.vary ?? '').split(',').map((v) => v.trim().toLowerCase());
      expect(vary).toContain('accept-language');
    });

    it('GET /v1/topic sets Vary: Accept-Language', async () => {
      const res = await request(app).get('/v1/topic');
      const vary = (res.headers.vary ?? '').split(',').map((v) => v.trim().toLowerCase());
      expect(vary).toContain('accept-language');
    });
  });

  describe('Dataset ID validation — malformed UUID returns 404 across every /:dataset_id endpoint', () => {
    for (const build of DATASET_ENDPOINTS) {
      const url = build('not-a-uuid');
      it(`${url} → 404`, async () => {
        const res = await request(app).get(url);
        expect(res.status).toBe(404);
      });
    }
  });

  describe('Dataset ID validation — valid UUID of a non-existent dataset returns 404 across every /:dataset_id endpoint', () => {
    const missingId = '99999999-9999-4999-8999-999999999999';
    for (const build of DATASET_ENDPOINTS) {
      const url = build(missingId);
      it(`${url} → 404`, async () => {
        const res = await request(app).get(url);
        expect(res.status).toBe(404);
      });
    }
  });

  describe('Error response shape', () => {
    it('404 responses for malformed dataset UUIDs return an error body, not an HTML page', async () => {
      const res = await request(app).get('/v1/not-a-uuid');
      expect(res.status).toBe(404);
      // Intended behaviour: JSON error payload for a JSON API. If this returns an HTML
      // 404 page, the v1 error handler isn't content-negotiating.
      expect(res.headers['content-type']).toMatch(/application\/json/);
    });
  });
});
