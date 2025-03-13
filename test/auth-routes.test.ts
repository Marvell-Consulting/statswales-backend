import request from 'supertest';

import app from '../src/app';
import { initDb } from '../src/db/init';
import DatabaseManager from '../src/db/database-manager';
import { initPassport } from '../src/middleware/passport-auth';
import { logger } from '../src/utils/logger';

import { appConfig } from '../src/config';

describe('Healthcheck', () => {
  let dbManager: DatabaseManager;

  beforeAll(async () => {
    try {
      dbManager = await initDb();
      await initPassport(dbManager.getDataSource());
    } catch (error) {
      logger.error(error, 'Could not initialise test database');
      await dbManager.getDataSource().dropDatabase();
      await dbManager.getDataSource().destroy();
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
    await dbManager.getDataSource().dropDatabase();
    await dbManager.getDataSource().destroy();
  });
});
