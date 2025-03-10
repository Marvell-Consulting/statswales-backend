import request from 'supertest';
import { v4 as uuid } from 'uuid';

import app from '../src/app';
import { initDb } from '../src/db/init';
import DatabaseManager from '../src/db/database-manager';
import { initPassport } from '../src/middleware/passport-auth';
import { User } from '../src/entities/user/user';
import { Provider } from '../src/entities/dataset/provider';
import { ProviderSource } from '../src/entities/dataset/provider-source';
import { ProviderSourceDTO } from '../src/dtos/provider-source-dto';

import { getTestUser } from './helpers/get-user';
import { getAuthHeader } from './helpers/auth-header';

const user: User = getTestUser('test', 'user');

const providerId1 = uuid();
const providerId2 = uuid();
const providerId3 = uuid();
const providerId4 = uuid();

const providers: Partial<Provider>[] = [
    { id: providerId1, name: 'Provider 1', language: 'en-gb' },
    { id: providerId2, name: 'Provider 2', language: 'en-gb' },
    { id: providerId3, name: 'Provider 3', language: 'en-gb' },
    { id: providerId4, name: 'Provider 4', language: 'en-gb' }
];

const sources: Partial<ProviderSource>[] = [
    { id: uuid(), name: 'Source 1a', providerId: providerId1, language: 'en-gb' },
    { id: uuid(), name: 'Source 1b', providerId: providerId1, language: 'en-gb' },
    { id: uuid(), name: 'Source 1c', providerId: providerId1, language: 'en-gb' },
    { id: uuid(), name: 'Source 1d', providerId: providerId1, language: 'en-gb' },
    { id: uuid(), name: 'Source 2', providerId: providerId2, language: 'en-gb' },
    { id: uuid(), name: 'Source 3', providerId: providerId3, language: 'en-gb' },
    { id: uuid(), name: 'Source 4', providerId: providerId4, language: 'en-gb' }
];

describe('Providers', () => {
    let dbManager: DatabaseManager;

    beforeAll(async () => {
        try {
            dbManager = await initDb();
            await initPassport(dbManager.getDataSource());
            await user.save();
            await dbManager.getEntityManager().save(Provider, providers);
            await dbManager.getEntityManager().save(ProviderSource, sources);
        } catch (_err) {
            await dbManager.getDataSource().dropDatabase();
            await dbManager.getDataSource().destroy();
            process.exit(1);
        }
    });

    test('Get all providers', async () => {
        const res = await request(app).get('/provider').set(getAuthHeader(user));
        expect(res.status).toBe(200);
        expect(res.body).toEqual(providers);
    });

    test('Get sources for a provider', async () => {
        const res = await request(app).get(`/provider/${providerId1}/sources`).set(getAuthHeader(user));

        const expected = sources
            .filter((source) => source.providerId === providerId1)
            .map((source) => ProviderSourceDTO.fromProviderSource(source as ProviderSource));

        expect(res.status).toBe(200);
        expect(res.body).toEqual(expected);
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
        await dbManager.getDataSource().destroy();
    });
});
