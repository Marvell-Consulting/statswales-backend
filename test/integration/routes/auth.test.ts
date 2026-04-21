import request from 'supertest';

import app from '../../../src/app';
import { dbManager } from '../../../src/db/database-manager';
import { initPassport } from '../../../src/middleware/passport-auth';
import { ensureWorkerDataSources, resetDatabase } from '../../helpers/reset-database';
import { config } from '../../../src/config';

// Need to mock blob storage as it is included in services middleware for every route
// avoids the "Jest did not exit one second after the test run has completed"
jest.mock('../../../src/services/blob-storage', () => {
  return function BlobStorage() {
    return {
      getContainerClient: jest.fn().mockReturnValue({
        createIfNotExists: jest.fn().mockResolvedValue(true)
      })
    };
  };
});

describe('Auth routes', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport(dbManager.getAppDataSource());
  });

  test('/auth/providers returns a list of enabled providers', async () => {
    const expectedProviders = config.auth.providers;
    const res = await request(app).get('/auth/providers');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ enabled: expectedProviders });
  });
});
