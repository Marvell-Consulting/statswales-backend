import request from 'supertest';

import app from '../../../src/app';
import { initPassport } from '../../../src/middleware/passport-auth';
import { SUPPORTED_LOCALES } from '../../../src/middleware/translation';
import { Locale } from '../../../src/enums/locale';
import { ensureWorkerDataSources, resetDatabase } from '../../helpers/reset-database';

import { getTestUser } from '../../helpers/get-test-user';
import { getAuthHeader } from '../../helpers/auth-header';
import { UserDTO } from '../../../src/dtos/user/user-dto';

jest.mock('../../../src/services/blob-storage', () => {
  return function BlobStorage() {
    return {
      getServiceClient: jest.fn().mockReturnValue({
        getProperties: jest.fn().mockResolvedValue(true)
      })
    };
  };
});

describe('Healthcheck', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport();
  });

  describe('Server up', () => {
    test('/healthcheck/ returns success', async () => {
      const res = await request(app).get('/healthcheck/');
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ message: 'success' });
    });
  });

  describe('Server readiness', () => {
    test('/healthcheck/ready returns success with session store status', async () => {
      const res = await request(app).get('/healthcheck/ready');
      expect(res.status).toBe(200);
      expect(res.body.message).toBe('success');
      expect(res.body.sessionStore).toEqual({ type: 'memory', connected: true });
    });
  });

  describe('Server liveness', () => {
    test('/healthcheck/live returns success', async () => {
      const res = await request(app).get('/healthcheck/live');
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ message: 'success' });
    });
  });

  describe('Language detection', () => {
    test('/healthcheck/language detects language as en if no header sent', async () => {
      const res = await request(app).get('/healthcheck/language');
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ lang: Locale.English, supported: SUPPORTED_LOCALES });
    });

    test('/healthcheck/language detects language as en if en header sent', async () => {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      const res = await request(app).get('/healthcheck/language').set({ 'accept-language': Locale.English });
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ lang: Locale.English, supported: SUPPORTED_LOCALES });
    });

    test('/healthcheck/language detects language as en-gb if en-gb header sent', async () => {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      const res = await request(app).get('/healthcheck/language').set({ 'accept-language': Locale.EnglishGb });
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ lang: Locale.EnglishGb, supported: SUPPORTED_LOCALES });
    });

    test('/healthcheck/language detects language as cy if cy header sent', async () => {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      const res = await request(app).get('/healthcheck/language').set({ 'accept-language': Locale.Welsh });
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ lang: Locale.Welsh, supported: SUPPORTED_LOCALES });
    });

    test('/healthcheck/language detects language as cy-gb if CY-GB header sent', async () => {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      const res = await request(app).get('/healthcheck/language').set({ 'accept-language': Locale.WelshGb });
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ lang: Locale.WelshGb, supported: SUPPORTED_LOCALES });
    });
  });

  describe('Database pools', () => {
    test('/healthcheck/db returns an array of pool stats', async () => {
      const res = await request(app).get('/healthcheck/db');
      expect(res.status).toBe(200);
      expect(Array.isArray(res.body.pools)).toBe(true);
      expect(res.body.pools).toHaveLength(3);

      for (const pool of res.body.pools) {
        expect(pool).toEqual(
          expect.objectContaining({
            name: expect.any(String),
            connectionTimeout: expect.stringMatching(/^\d+ms$/),
            idleTimeout: expect.stringMatching(/^\d+ms$/),
            clients: expect.objectContaining({
              min: expect.any(Number),
              max: expect.any(Number),
              idle: expect.any(Number),
              waiting: expect.any(Number),
              expired: expect.any(Number),
              total: expect.any(Number),
              isFull: expect.any(Boolean)
            })
          })
        );
      }
    });
  });

  describe('Authentication', () => {
    test('/heathcheck/jwt returns 401 without a bearer token', async () => {
      const res = await request(app).get('/healthcheck/jwt');
      expect(res.status).toBe(401);
    });

    test('/heathcheck/jwt returns 401 with an invalid bearer token', async () => {
      const res = await request(app).get('/healthcheck/jwt').set({ Authorization: 'Bearer this-is-not-a-token' });
      expect(res.status).toBe(401);
    });

    test('/heathcheck/jwt returns 401 with a valid bearer token but inactive user', async () => {
      const inactiveUser = getTestUser('Inactive User');
      const res = await request(app).get('/healthcheck/jwt').set(getAuthHeader(inactiveUser));
      expect(res.status).toBe(401);
    });

    test('/heathcheck/jwt returns 200 with a valid bearer token', async () => {
      const testUser = getTestUser();
      await testUser.save();

      const res = await request(app).get('/healthcheck/jwt').set(getAuthHeader(testUser));

      expect(res.status).toBe(200);
      expect(res.body).toEqual({
        message: 'success',
        user: UserDTO.fromUser(testUser, Locale.English)
      });
    });
  });
});
