import request from 'supertest';

import app from '../../src/app';
import { dbManager } from '../../src/db/database-manager';
import { initPassport } from '../../src/middleware/passport-auth';
import { logger } from '../../src/utils/logger';

import { appConfig } from '../../src/config';

// Need to mock blob storage as it is included in services middleware for every route
// avoids the "Jest did not exit one second after the test run has completed"
jest.mock('../../src/services/blob-storage', () => {
  return function BlobStorage() {
    return {
      getContainerClient: jest.fn().mockReturnValue({
        createIfNotExists: jest.fn().mockResolvedValue(true)
      })
    };
  };
});

describe('Healthcheck', () => {
  beforeAll(async () => {
    try {
      await dbManager.initDataSources();
      await initPassport(dbManager.getAppDataSource());
    } catch (error) {
      logger.error(error, 'Could not initialise test database');
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.destroyDataSources();
      process.exit(1);
    }
  });

  test('/auth/providers returns a list of enabled providers', async () => {
    const expectedProviders = appConfig().auth.providers;
    const res = await request(app).get('/auth/providers');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ enabled: expectedProviders });
  });

  afterAll(async () => {
    await dbManager.getAppDataSource().dropDatabase();
    await dbManager.destroyDataSources();
  });
});
