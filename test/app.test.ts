import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import app, { dbManager, connectToDb } from '../src/app';
import { Dataset } from '../src/entity/dataset';

import { datasourceOptions } from './test-data-source';

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

DataLakeService.prototype.uploadFile = jest.fn();

describe('Test app.ts', () => {
    beforeAll(async () => {
        await connectToDb(datasourceOptions);
        await dbManager.initializeDataSource();

        const dataset1 = Dataset.createDataset('Test Data 1', 'test', 'bdc40218-af89-424b-b86e-d21710bc92f1');
        await dataset1.save();
        dataset1.live = true;
        dataset1.code = 'tst0001';
        dataset1.addTitleByString('Test Dataset 1', 'EN');
        dataset1.addDescriptionByString('I am the first test dataset', 'EN');

        const dataset2 = Dataset.createDataset('Test Data 2', 'test', 'fa07be9d-3495-432d-8c1f-d0fc6daae359');
        await dataset2.save();
        dataset2.live = true;
        dataset2.code = 'tst0002';
        dataset2.createdBy = 'test';
        dataset2.addTitleByString('Test Dataset 2', 'EN');
        dataset2.addDescriptionByString('I am the second test dataset', 'EN');
    });

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
