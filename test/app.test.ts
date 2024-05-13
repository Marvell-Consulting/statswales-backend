import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import app, { connectToDb } from '../src/app';
import { Datafile } from '../src/entity/Datafile';

import { datasourceOptions } from './test-data-source';

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

DataLakeService.prototype.uploadFile = jest.fn();

beforeAll(async () => {
    await connectToDb(datasourceOptions);
    const datafile1 = new Datafile();
    datafile1.name = 'test-data-1.csv';
    datafile1.description = 'Test Data File 1';
    datafile1.id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
    await datafile1.save();
    const datafile2 = new Datafile();
    datafile2.name = 'test-data-2.csv';
    datafile2.description = 'Test Data File 2';
    datafile2.id = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
    await datafile2.save();
    console.log(`Datafile created: ${Datafile.find()}`);
});

describe('Test app.ts', () => {
    test('Redirects to language when going to /', async () => {
        const res = await request(app).get('/');
        expect(res.status).toBe(302);
        expect(res.header.location).toBe('/en-GB/api');
    });

    test('Redirects to welsh when accept-header is present when going to /', async () => {
        const res = await request(app).get('/').set('Accept-Language', 'cy-GB');
        expect(res.status).toBe(302);
        expect(res.header.location).toBe('/cy-GB/api');
    });

    test('Check inital healthcheck endpoint works', async () => {
        const res = await request(app).get('/healthcheck');
        expect(res.status).toBe(200);
        expect(res.body).toEqual({
            status: 'App is running',
            notes: 'Expand endpoint to check for database connection and other services.'
        });
    });
});
