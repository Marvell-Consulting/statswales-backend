import request from 'supertest';

import app, { initDb } from '../src/app';
import DatabaseManager from '../src/db/database-manager';
import { sanitiseUser } from '../src/utils/sanitise-user';
import { SUPPORTED_LOCALES } from '../src/middleware/translation';

import { getTestUser } from './helpers/get-user';
import { getAuthHeader } from './helpers/auth-header';

describe('Healthcheck', () => {
    let dbManager: DatabaseManager;

    beforeAll(async () => {
        dbManager = await initDb();
    });

    test('/healthcheck/ returns success', async () => {
        const res = await request(app).get('/healthcheck/');
        expect(res.status).toBe(200);
        expect(res.body).toEqual({ message: 'success' });
    });

    test('/healthcheck/basic returns success', async () => {
        const res = await request(app).get('/healthcheck/basic');
        expect(res.status).toBe(200);
        expect(res.body).toEqual({ message: 'success' });
    });

    describe('Language detection', () => {
        test('/healthcheck/language detects language as en if no header sent', async () => {
            const res = await request(app).get('/healthcheck/language');
            expect(res.status).toBe(200);
            expect(res.body).toEqual({ lang: 'en', supported: SUPPORTED_LOCALES });
        });

        test('/healthcheck/language detects language as en if en header sent', async () => {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            const res = await request(app).get('/healthcheck/language').set({ 'accept-language': 'en' });
            expect(res.status).toBe(200);
            expect(res.body).toEqual({ lang: 'en', supported: SUPPORTED_LOCALES });
        });

        test('/healthcheck/language detects language as en-gb if en-gb header sent', async () => {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            const res = await request(app).get('/healthcheck/language').set({ 'accept-language': 'en-gb' });
            expect(res.status).toBe(200);
            expect(res.body).toEqual({ lang: 'en-GB', supported: SUPPORTED_LOCALES });
        });

        test('/healthcheck/language detects language as cy if cy header sent', async () => {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            const res = await request(app).get('/healthcheck/language').set({ 'accept-language': 'cy' });
            expect(res.status).toBe(200);
            expect(res.body).toEqual({ lang: 'cy', supported: SUPPORTED_LOCALES });
        });

        test('/healthcheck/language detects language as cy-gb if CY-GB header sent', async () => {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            const res = await request(app).get('/healthcheck/language').set({ 'accept-language': 'cy-GB' });
            expect(res.status).toBe(200);
            expect(res.body).toEqual({ lang: 'cy-GB', supported: SUPPORTED_LOCALES });
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
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
        await dbManager.getDataSource().destroy();
    });
});
