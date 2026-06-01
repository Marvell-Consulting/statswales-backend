import request from 'supertest';
import jwt from 'jsonwebtoken';

import app from '../../../src/app';
import { initPassport } from '../../../src/middleware/passport-auth';
import { ensureWorkerDataSources, resetDatabase } from '../../helpers/reset-database';
import { config } from '../../../src/config';
import { dbManager } from '../../../src/db/database-manager';
import { UserDTO } from '../../../src/dtos/user/user-dto';
import { UserGroup } from '../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../src/entities/user/user-group-role';
import { GroupRole } from '../../../src/enums/group-role';
import { Locale } from '../../../src/enums/locale';
import { getTestUser, getTestUserGroup } from '../../helpers/get-test-user';
import { getAuthHeader } from '../../helpers/auth-header';

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
  const callbackURL = `${config.frontend.url}/auth/callback`;

  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport();
  });

  test('/auth/providers returns a list of enabled providers', async () => {
    const expectedProviders = config.auth.providers;
    const res = await request(app).get('/auth/providers');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ enabled: expectedProviders });
  });

  describe('GET /auth/local (loginLocal)', () => {
    test('redirects to the frontend callback and sets a jwt cookie for a known user', async () => {
      const user = getTestUser('Local Success');
      await user.save();

      const res = await request(app).get('/auth/local').query({ username: user.providerUserId });

      expect(res.status).toBe(302);
      expect(res.headers.location).toBe(callbackURL);

      const cookies = res.headers['set-cookie'] as unknown as string[];
      const jwtCookie = cookies.find((cookie) => cookie.startsWith('jwt='));
      expect(jwtCookie).toBeDefined();
      expect(jwtCookie).toContain('HttpOnly');
    });

    test('redirects with error=login when no username is provided', async () => {
      const res = await request(app).get('/auth/local');
      expect(res.status).toBe(302);
      expect(res.headers.location).toBe(`${callbackURL}?error=login`);
      expect(res.headers['set-cookie']).toBeUndefined();
    });

    test('redirects with error=login when the user does not exist', async () => {
      const res = await request(app).get('/auth/local').query({ username: 'no-such-user' });
      expect(res.status).toBe(302);
      expect(res.headers.location).toBe(`${callbackURL}?error=login`);
      expect(res.headers['set-cookie']).toBeUndefined();
    });
  });

  // The happy path and the no-token / invalid-token / unknown-user cases live in healthcheck.test.ts.
  // These cover the remaining JWT strategy branches in passport-auth.ts.
  describe('JWT auth middleware (passport-auth)', () => {
    test('/healthcheck/jwt returns 401 for an expired token', async () => {
      const user = getTestUser('Expired Token User');
      await user.save();

      const token = jwt.sign({ user: UserDTO.fromUser(user, Locale.English) }, config.auth.jwt.secret, {
        expiresIn: '-1s'
      });

      const res = await request(app)
        .get('/healthcheck/jwt')
        .set({ Authorization: `Bearer ${token}` });
      expect(res.status).toBe(401);
    });

    test('/healthcheck/jwt returns 401 when the user permissions have changed since the token was issued', async () => {
      const group = await dbManager
        .getPublisherDataSource()
        .getRepository(UserGroup)
        .save(getTestUserGroup('Perm Change Group'));

      const user = getTestUser('Perm Change User');
      user.groupRoles = [UserGroupRole.create({ group, roles: [GroupRole.Editor] })];
      await user.save();

      // token captures the user while they hold the Editor role
      const authHeader = getAuthHeader(user);

      // revoke the role in the database so the live permissions no longer match the token
      await dbManager.getPublisherDataSource().getRepository(UserGroupRole).delete({ userId: user.id });

      const res = await request(app).get('/healthcheck/jwt').set(authHeader);
      expect(res.status).toBe(401);
    });
  });
});
