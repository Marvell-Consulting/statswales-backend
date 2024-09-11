import path from 'path';
import * as fs from 'fs';

import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import { BlobStorageService } from '../src/controllers/blob-storage';
import app, { initDb } from '../src/app';
import { t, ENGLISH, WELSH } from '../src/middleware/translation';
import { Revision } from '../src/entities/revision';
import { FileImport } from '../src/entities/file-import';
import DatabaseManager from '../src/db/database-manager';

import { createFullDataset, createSmallDataset } from './helpers/test-helper';
import { User } from "../src/entities/user";
import { getTestUser } from "./helpers/get-user";
import { getAuthHeader } from "./helpers/auth-header";

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

BlobStorageService.prototype.uploadFile = jest.fn();

DataLakeService.prototype.uploadFile = jest.fn();

const dataset1Id = 'BDC40218-AF89-424B-B86E-D21710BC92F1'.toLowerCase();
const revision1Id = '85F0E416-8BD1-4946-9E2C-1C958897C6EF'.toLowerCase();
const import1Id = 'FA07BE9D-3495-432D-8C1F-D0FC6DAAE359'.toLowerCase();
const user: User = getTestUser('test', 'user');

describe('API Endpoints for viewing the contents of a dataset', () => {
    let dbManager: DatabaseManager;
    beforeAll(async () => {
        dbManager = await initDb();
        await user.save();
        await createFullDataset(dataset1Id, revision1Id, import1Id, user);
    });

    test('Get file from a dataset, stored in blobStorage, returns 200 and complete file data', async () => {
        const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
        const testFile1Buffer = fs.readFileSync(testFile2);
        BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile1Buffer.toString());

        const res = await request(app)
            .get(`/en-GB/dataset/${dataset1Id}/view`)
            .set(getAuthHeader(user))
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(200);
        expect(res.body.current_page).toBe(2);
        expect(res.body.total_pages).toBe(6);
        expect(res.body.page_size).toBe(100);
        expect(res.body.headers).toEqual([
            { index: 0, name: 'ID' },
            { index: 1, name: 'Text' },
            { index: 2, name: 'Number' },
            { index: 3, name: 'Date' }
        ]);
        expect(res.body.data[0]).toEqual(['101', 'GEYiRzLIFM', '774477', '2002-03-13']);
        expect(res.body.data[99]).toEqual(['200', 'QhBxdmrUPb', '3256099', '2026-12-17']);
    });

    test('Get file from a dataset, stored in datalake, returns 200 and complete file data', async () => {
        const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
        const testFile1Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile1Buffer.toString());
        const fileImport = await FileImport.findOneBy({ id: import1Id });
        if (!fileImport) {
            throw new Error('Import not found');
        }
        fileImport.location = 'Datalake';
        await FileImport.save(fileImport);

        const res = await request(app)
            .get(`/en-GB/dataset/${dataset1Id}/view`)
            .set(getAuthHeader(user))
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(200);
        expect(res.body.current_page).toBe(2);
        expect(res.body.total_pages).toBe(6);
        expect(res.body.page_size).toBe(100);
        expect(res.body.headers).toEqual([
            { index: 0, name: 'ID' },
            { index: 1, name: 'Text' },
            { index: 2, name: 'Number' },
            { index: 3, name: 'Date' }
        ]);
        expect(res.body.data[0]).toEqual(['101', 'GEYiRzLIFM', '774477', '2002-03-13']);
        expect(res.body.data[99]).toEqual(['200', 'QhBxdmrUPb', '3256099', '2026-12-17']);
    });

    test('Get file from a dataset, stored in an unknown location, returns 500 and an error message', async () => {
        const fileImport = await FileImport.findOneBy({ id: import1Id });
        if (!fileImport) {
            throw new Error('Import not found');
        }
        fileImport.location = 'Unknown';
        await FileImport.save(fileImport);

        const res = await request(app)
            .get(`/en-GB/dataset/${dataset1Id}/view`)
            .set(getAuthHeader(user))
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(500);
        expect(res.body).toEqual({ message: 'Import location not supported.' });
    });

    test('Get file from a dataset, stored in blob storage, returns 500 if the file is empty and an error message', async () => {
        const fileImport = await FileImport.findOneBy({ id: import1Id });
        if (!fileImport) {
            throw new Error('Import not found');
        }
        fileImport.location = 'BlobStorage';
        await FileImport.save(fileImport);
        BlobStorageService.prototype.readFile = jest.fn().mockRejectedValue(new Error('File is empty'));

        const res = await request(app)
            .get(`/en-GB/dataset/${dataset1Id}/view`)
            .set(getAuthHeader(user))
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(500);
        expect(res.body).toEqual({
            success: false,
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: ENGLISH, message: t('errors.download_from_blobstorage', { lng: ENGLISH }) },
                        { lang: WELSH, message: t('errors.download_from_blobstorage', { lng: WELSH }) }
                    ],
                    tag: { name: 'errors.download_from_blobstorage', params: {} }
                }
            ],
            dataset_id: dataset1Id
        });
    });

    test('Get a dataset view returns 500 if there is no revision on the dataset', async () => {
        const removeRevisionDatasetID = crypto.randomUUID();
        const revisionID = crypto.randomUUID();
        await createSmallDataset(removeRevisionDatasetID, revisionID, crypto.randomUUID(), user);
        const revision = await Revision.findOneBy({ id: revisionID });
        if (!revision) {
            throw new Error('Revision not found... Either it was not created or the test is broken');
        }
        await Revision.remove(revision);
        const res = await request(app)
            .get(`/en-GB/dataset/${removeRevisionDatasetID}/view`)
            .set(getAuthHeader(user))
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(500);
        expect(res.body).toEqual({ message: 'No revision found for dataset' });
    });

    test('Get a dataset view returns 500 if there is no import on the dataset', async () => {
        const removeRevisionDatasetID = crypto.randomUUID();
        const importId = crypto.randomUUID();
        await createSmallDataset(removeRevisionDatasetID, crypto.randomUUID(), importId, user);
        const fileImport = await FileImport.findOneBy({ id: importId });
        if (!fileImport) {
            throw new Error('Revision not found... Either it was not created or the test is broken');
        }
        await FileImport.remove(fileImport);
        const res = await request(app)
            .get(`/en-GB/dataset/${removeRevisionDatasetID}/view`)
            .set(getAuthHeader(user))
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(500);
        expect(res.body).toEqual({ message: 'No import record found for dataset' });
    });

    test('Get file view returns 400 when a not valid UUID is supplied', async () => {
        const res = await request(app).get(`/en-GB/dataset/NOT-VALID-ID`).set(getAuthHeader(user));
        expect(res.status).toBe(400);
        expect(res.body).toEqual({ message: 'Dataset ID is not valid' });
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
        await dbManager.getDataSource().destroy();
    });
});
