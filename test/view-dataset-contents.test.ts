import path from 'path';
import * as fs from 'fs';

import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import { BlobStorageService } from '../src/controllers/blob-storage';
import app, { ENGLISH, WELSH, i18n, dbManager, databaseManager } from '../src/app';
import { Revision } from '../src/entities/revision';
import { FileImport } from '../src/entities/import_file';

import { createFullDataset, createSmallDataset } from './helpers/test-helper';
import { datasourceOptions } from './helpers/test-data-source';

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

BlobStorageService.prototype.uploadFile = jest.fn();

DataLakeService.prototype.uploadFile = jest.fn();

const dataset1Id = 'BDC40218-AF89-424B-B86E-D21710BC92F1';
const revision1Id = '85F0E416-8BD1-4946-9E2C-1C958897C6EF';
const import1Id = 'FA07BE9D-3495-432D-8C1F-D0FC6DAAE359';
const dimension1Id = '2D7ACD0B-A46A-43F7-8A88-224CE97FC8B9';

describe('API Endpoints for viewing the contents of a dataset', () => {
    beforeAll(async () => {
        await databaseManager(datasourceOptions);
        await dbManager.initializeDataSource();
        await createFullDataset(dataset1Id, revision1Id, import1Id, dimension1Id);
    });

    test('Get file from a dataset, stored in blobStorage, returns 200 and complete file data', async () => {
        const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
        const testFile1Buffer = fs.readFileSync(testFile2);
        BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile1Buffer.toString());

        const res = await request(app)
            .get(`/en-GB/dataset/${dataset1Id}/view`)
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
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(500);
        expect(res.body).toEqual({
            success: false,
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: ENGLISH, message: i18n.t('errors.download_from_blobstorage', { lng: ENGLISH }) },
                        { lang: WELSH, message: i18n.t('errors.download_from_blobstorage', { lng: WELSH }) }
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
        await createSmallDataset(removeRevisionDatasetID, revisionID, crypto.randomUUID());
        const revision = await Revision.findOneBy({ id: revisionID });
        if (!revision) {
            throw new Error('Revision not found... Either it was not created or the test is broken');
        }
        await Revision.remove(revision);
        const res = await request(app)
            .get(`/en-GB/dataset/${removeRevisionDatasetID}/view`)
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(500);
        expect(res.body).toEqual({ message: 'No revision found for dataset' });
    });

    test('Get a dataset view returns 500 if there is no import on the dataset', async () => {
        const removeRevisionDatasetID = crypto.randomUUID();
        const importId = crypto.randomUUID();
        await createSmallDataset(removeRevisionDatasetID, crypto.randomUUID(), importId);
        const fileImport = await FileImport.findOneBy({ id: importId });
        if (!fileImport) {
            throw new Error('Revision not found... Either it was not created or the test is broken');
        }
        await FileImport.remove(fileImport);
        const res = await request(app)
            .get(`/en-GB/dataset/${removeRevisionDatasetID}/view`)
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(500);
        expect(res.body).toEqual({ message: 'No import record found for dataset' });
    });

    test('Get file view returns 400 when a not valid UUID is supplied', async () => {
        const res = await request(app).get(`/en-GB/dataset/NOT-VALID-ID`);
        expect(res.status).toBe(400);
        expect(res.body).toEqual({ message: 'Dataset ID is not valid' });
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
    });
});
