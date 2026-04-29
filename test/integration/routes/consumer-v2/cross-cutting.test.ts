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
import { PublishedDatasetRepository } from '../../../../src/repositories/published-dataset';

jest.mock('../../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

const user: User = getTestUser('v2 cross-cutting test user');
let userGroup = getTestUserGroup('V2 Cross Cutting Test Group');

const DATASET_ID = 'dddddddd-dddd-4ddd-8dd2-dddddddddddd';
const REVISION_ID = 'dddddddd-dddd-4ddd-8dd2-eeeeeeeeeeee';
const DATA_TABLE_ID = 'dddddddd-dddd-4ddd-8dd2-ffffffffffff';

const MISSING_ID = '99999999-9999-4999-8999-999999999999';
const MISSING_FILTER = '88888888-8888-4888-8888-888888888888';

// Endpoints that accept a :dataset_id path param — used for the "invalid UUID" and
// "non-existent dataset" sweeps. For POST endpoints we exercise them via request.post().
const GET_DATASET_ENDPOINTS = [
  (id: string) => `/v2/${id}`,
  (id: string) => `/v2/${id}/filters`,
  (id: string) => `/v2/${id}/data`,
  (id: string) => `/v2/${id}/data/${MISSING_FILTER}`,
  (id: string) => `/v2/${id}/pivot/${MISSING_FILTER}`,
  (id: string) => `/v2/${id}/query/${MISSING_FILTER}`
];

const POST_DATASET_ENDPOINTS = [(id: string) => `/v2/${id}/data`, (id: string) => `/v2/${id}/pivot`];

describe('Consumer V2 — cross-cutting behaviour (CORS, Vary, 404 sweeps)', () => {
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

  describe('CORS (api.ts:80)', () => {
    it('OPTIONS preflight sets Access-Control-Allow-Origin: *', async () => {
      const res = await request(app).options('/v2/').set('Origin', 'https://example.com');
      expect(res.status).toBeLessThan(300);
      expect(res.headers['access-control-allow-origin']).toBe('*');
    });

    it('GET responses include Access-Control-Allow-Origin for cross-origin callers', async () => {
      const res = await request(app).get('/v2/').set('Origin', 'https://example.com');
      expect(res.headers['access-control-allow-origin']).toBe('*');
    });
  });

  describe('Accept-Language caching (api.ts:82-85)', () => {
    it('GET /v2/ sets Vary: Accept-Language', async () => {
      const res = await request(app).get('/v2/');
      const vary = (res.headers.vary ?? '').split(',').map((v) => v.trim().toLowerCase());
      expect(vary).toContain('accept-language');
    });

    it('GET /v2/topic sets Vary: Accept-Language', async () => {
      const res = await request(app).get('/v2/topic');
      const vary = (res.headers.vary ?? '').split(',').map((v) => v.trim().toLowerCase());
      expect(vary).toContain('accept-language');
    });
  });

  describe('Dataset ID validation — malformed UUID returns 404 across every /:dataset_id endpoint', () => {
    for (const build of GET_DATASET_ENDPOINTS) {
      const url = build('not-a-uuid');
      it(`GET ${url} → 404`, async () => {
        const res = await request(app).get(url);
        expect(res.status).toBe(404);
      });
    }

    for (const build of POST_DATASET_ENDPOINTS) {
      const url = build('not-a-uuid');
      it(`POST ${url} → 404`, async () => {
        const res = await request(app).post(url).send({});
        expect(res.status).toBe(404);
      });
    }
  });

  describe('Dataset ID validation — well-formed UUID of a non-existent dataset returns 404 across every /:dataset_id endpoint', () => {
    for (const build of GET_DATASET_ENDPOINTS) {
      const url = build(MISSING_ID);
      it(`GET ${url} → 404`, async () => {
        const res = await request(app).get(url);
        expect(res.status).toBe(404);
      });
    }

    for (const build of POST_DATASET_ENDPOINTS) {
      const url = build(MISSING_ID);
      it(`POST ${url} → 404`, async () => {
        const res = await request(app).post(url).send({});
        expect(res.status).toBe(404);
      });
    }
  });

  describe('DB error during dataset load returns 500 (SW-1253)', () => {
    afterEach(() => {
      jest.restoreAllMocks();
    });

    it('non-EntityNotFoundError from PublishedDatasetRepository.getById surfaces as a sanitized JSON 500', async () => {
      jest.spyOn(PublishedDatasetRepository, 'getById').mockRejectedValueOnce(new Error('connection terminated'));

      const res = await request(app).get(`/v2/${DATASET_ID}`);
      expect(res.status).toBe(500);
      expect(res.headers['content-type']).toMatch(/application\/json/);
      expect(res.text).not.toContain('connection terminated');
    });
  });

  describe('Error response shape', () => {
    it('404 responses for malformed dataset UUIDs return JSON, not HTML', async () => {
      const res = await request(app).get('/v2/not-a-uuid');
      expect(res.status).toBe(404);
      expect(res.headers['content-type']).toMatch(/application\/json/);
    });
  });

  describe('Method guard (missing in v2 — recorded for comparison with v1)', () => {
    // v1 has an explicit 405 guard at src/routes/consumer/v1/api.ts:51-55.
    // v2 has no equivalent — unmapped methods fall through to Express default 404.
    // Flag for the user: is 405 desired here, for consistency with v1?
    it('POST /v2/ has no handler so Express returns 404 (no 405 guard in v2)', async () => {
      const res = await request(app).post('/v2/');
      expect(res.status).toBe(404);
    });

    it('PUT /v2/:dataset_id has no handler → 404', async () => {
      const res = await request(app).put(`/v2/${DATASET_ID}`);
      expect(res.status).toBe(404);
    });

    it('DELETE /v2/:dataset_id has no handler → 404', async () => {
      const res = await request(app).delete(`/v2/${DATASET_ID}`);
      expect(res.status).toBe(404);
    });
  });
});
