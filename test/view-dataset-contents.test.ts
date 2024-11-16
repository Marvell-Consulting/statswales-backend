import path from 'path';
import * as fs from 'fs';

import request from 'supertest';

import { DataLakeService } from '../src/services/datalake';
import app, { initDb } from '../src/app';
import { Revision } from '../src/entities/dataset/revision';
import { FileImport } from '../src/entities/dataset/file-import';
import { t } from '../src/middleware/translation';
import DatabaseManager from '../src/db/database-manager';
import { User } from '../src/entities/user/user';
import { DataLocation } from '../src/enums/data-location';
import { Locale } from '../src/enums/locale';

import { createFullDataset, createSmallDataset } from './helpers/test-helper';
import { getTestUser } from './helpers/get-user';
import { getAuthHeader } from './helpers/auth-header';

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

DataLakeService.prototype.getFileBuffer = jest.fn();

const dataset1Id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
const revision1Id = '85f0e416-8bd1-4946-9e2c-1c958897c6ef';
const import1Id = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
const user: User = getTestUser('test', 'user');

describe('API Endpoints for viewing the contents of a dataset', () => {
    let dbManager: DatabaseManager;
    beforeAll(async () => {
        dbManager = await initDb();
        await user.save();
        await createFullDataset(dataset1Id, revision1Id, import1Id, user);
    });

    test('Get file from a dataset, stored in data lake, returns 200 and complete file data', async () => {
        const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
        const testFile1Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.getFileBuffer = jest.fn().mockReturnValue(testFile1Buffer.toString());

        const res = await request(app)
            .get(`/dataset/${dataset1Id}/view`)
            .set(getAuthHeader(user))
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(200);
        expect(res.body.current_page).toBe(2);
        expect(res.body.total_pages).toBe(6);
        expect(res.body.page_size).toBe(100);
        expect(res.body.headers).toEqual([
            { index: -1, name: 'int_line_number', source_type: 'line_number' },
            { index: 0, name: 'ID', source_type: 'ignore' },
            { index: 1, name: 'Text', source_type: 'dimension' },
            { index: 2, name: 'Number', source_type: 'data_values' },
            { index: 3, name: 'Date', source_type: 'dimension' }
        ]);
        expect(res.body.data[0]).toEqual([101, 101, 'GEYiRzLIFM', 774477, '2002-03-13']);
        expect(res.body.data[99]).toEqual([200, 200, 'QhBxdmrUPb', 3256099, '2026-12-17']);
    });

    test('Get a dataset view returns 500 if there is no revision on the dataset', async () => {
        const removeRevisionDatasetID = crypto.randomUUID().toLowerCase();
        const removeRevisionRevisionID = crypto.randomUUID().toLowerCase();
        await createSmallDataset(
            removeRevisionDatasetID,
            removeRevisionRevisionID,
            crypto.randomUUID().toLowerCase(),
            user
        );
        const revision = await Revision.findOneBy({ id: removeRevisionRevisionID });
        if (!revision) {
            throw new Error('Revision not found... Either it was not created or the test is broken');
        }
        await Revision.remove(revision);
        const res = await request(app)
            .get(`/dataset/${removeRevisionDatasetID}/view`)
            .set(getAuthHeader(user))
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(500);
        expect(res.body).toEqual({ error: 'No revision found for dataset' });
    });

    test('Get a dataset view returns 500 if there is no import on the dataset', async () => {
        const removeRevisionDatasetID = crypto.randomUUID().toLowerCase();
        const importId = crypto.randomUUID().toLowerCase();
        await createSmallDataset(removeRevisionDatasetID, crypto.randomUUID(), importId, user);
        const fileImport = await FileImport.findOneBy({ id: importId });
        if (!fileImport) {
            throw new Error('Revision not found... Either it was not created or the test is broken');
        }
        await FileImport.remove(fileImport);
        const res = await request(app)
            .get(`/dataset/${removeRevisionDatasetID}/view`)
            .set(getAuthHeader(user))
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(500);
        expect(res.body).toEqual({ error: 'No import found for dataset' });
    });

    test('Get file view returns 404 when a not valid UUID is supplied', async () => {
        const res = await request(app).get(`/dataset/NOT-VALID-ID`).set(getAuthHeader(user));
        expect(res.status).toBe(404);
        expect(res.body).toEqual({ error: 'Dataset id is invalid or missing' });
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
        await dbManager.getDataSource().destroy();
    });
});
