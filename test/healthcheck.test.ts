import request from 'supertest';

import app, { initDb } from '../src/app';
import DatabaseManager from '../src/db/database-manager';
import { sanitiseUser } from '../src/utils/sanitise-user';

import { getTestUser } from './helpers/get-user';
import { getAuthHeader } from './helpers/auth-header';

describe('Healthcheck routes', () => {
    let dbManager: DatabaseManager;

    beforeAll(async () => {
        dbManager = await initDb();
    });

    test('/healthcheck/basic returns success', async () => {
        const res = await request(app).get('/healthcheck/basic');
        expect(res.status).toBe(200);
        expect(res.body).toEqual({ message: 'success' });
    });

    test('/heathcheck/jwt returns 401 without a bearer token', async () => {
        const res = await request(app).get('/healthcheck/jwt');
        expect(res.status).toBe(401);
    });

    test('/heathcheck/jwt returns 401 with an invalid bearer token', async () => {
        const res = await request(app).get('/healthcheck/jwt').set({ Authorization: 'Bearer this-is-not-a-token' });
        expect(res.status).toBe(401);
    });

    test('/heathcheck/jwt returns 200 with a valid bearer token', async () => {
        const testUser = getTestUser();
        await testUser.save();

        const res = await request(app).get('/healthcheck/jwt').set(getAuthHeader(testUser));

        expect(res.status).toBe(200);
        expect(res.body).toEqual({
            message: 'success',
            user: sanitiseUser(testUser)
        });
    });

    test('/healthcheck/datalake returns success', async () => {
        const res = await request(app).get('/healthcheck/datalake');
        expect(res.status).toBe(200);
        expect(res.body).toEqual({ message: 'success' });
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
        await dbManager.getDataSource().destroy();
    });
});
