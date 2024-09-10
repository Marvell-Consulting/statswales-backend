import request from 'supertest';

import app, { initDb } from '../src/app';
import DatabaseManager from '../src/db/database-manager';

describe('Language switcher routes', () => {
    let dbManager: DatabaseManager;

    beforeAll(async () => {
        dbManager = await initDb();
    });

    test('/ redirects to english when no language header sent', async () => {
        const res = await request(app).get('/');
        expect(res.status).toBe(302);
        expect(res.header.location).toBe('/en-GB/api');
    });

    test('/ redirects to welsh when accept-language header is sent', async () => {
        const res = await request(app).get('/').set('Accept-Language', 'cy-GB');
        expect(res.status).toBe(302);
        expect(res.header.location).toBe('/cy-GB/api');
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
        await dbManager.getDataSource().destroy();
    });
});
