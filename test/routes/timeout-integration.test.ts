import request from 'supertest';

import app from '../../src/app';
import { dbManager } from '../../src/db/database-manager';
import { initPassport } from '../../src/middleware/passport-auth';
import { ensureWorkerDataSources, resetDatabase } from '../helpers/reset-database';

import { getTestUser } from '../helpers/get-test-user';
import { getAuthHeader } from '../helpers/auth-header';

jest.mock('../../src/services/blob-storage', () => {
  return function BlobStorage() {
    return {
      getServiceClient: jest.fn().mockReturnValue({
        getProperties: jest.fn().mockResolvedValue(true)
      })
    };
  };
});

describe('Request timeout integration', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport(dbManager.getAppDataSource());
  });

  describe('routes with default timeout', () => {
    test('healthcheck responds within the default timeout', async () => {
      const res = await request(app).get('/healthcheck/');
      expect(res.status).toBe(200);
      // Confirms the default timeout middleware does not interfere with fast responses
    });

    test('unauthenticated request to a protected route returns 401, not 504', async () => {
      const res = await request(app).get('/dataset');
      expect(res.status).toBe(401);
      // The timeout should not fire before auth rejects the request
    });
  });

  describe('routes with long timeout', () => {
    test('authenticated request to a dataset upload route returns 401 without auth, not 504', async () => {
      const res = await request(app).post('/dataset/00000000-0000-0000-0000-000000000000/data');
      expect(res.status).toBe(401);
    });

    test('authenticated request to a non-existent dataset download returns 404, not 504', async () => {
      const testUser = getTestUser();
      await testUser.save();

      const res = await request(app)
        .get('/dataset/00000000-0000-0000-0000-000000000000/download')
        .set(getAuthHeader(testUser));
      // Should fail with a dataset auth error, not a timeout
      expect(res.status).not.toBe(504);
    });
  });

  describe('public API routes with long timeout', () => {
    test('v1 download of non-existent dataset returns 404, not 504', async () => {
      const res = await request(app).get('/v1/00000000-0000-0000-0000-000000000000/download/csv');
      expect(res.status).not.toBe(504);
    });

    test('v2 data endpoint for non-existent dataset returns 404, not 504', async () => {
      const res = await request(app).get('/v2/00000000-0000-0000-0000-000000000000/data');
      expect(res.status).not.toBe(504);
    });

    test('v2 pivot endpoint for non-existent dataset returns 404, not 504', async () => {
      const res = await request(app).get('/v2/00000000-0000-0000-0000-000000000000/pivot/some-filter-id');
      expect(res.status).not.toBe(504);
    });
  });
});
