import path from 'path';
import * as fs from 'fs';

import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import { BlobStorageService } from '../src/controllers/blob-storage';
import app, { initDb } from '../src/app';
import { t, ENGLISH, WELSH } from '../src/middleware/translation';
import { Dataset } from '../src/entities/dataset';
import { DatasetInfo } from '../src/entities/dataset-info';
import { DatasetDTO } from '../src/dtos/dataset-dto';
import { ViewErrDTO } from '../src/dtos/view-dto';
import { MAX_PAGE_SIZE, MIN_PAGE_SIZE } from '../src/controllers/csv-processor';
import DatabaseManager from '../src/db/database-manager';
import { User } from '../src/entities/user';
import { FileImport } from '../src/entities/file-import';
import { DataLocation } from '../src/enums/data-location';

import { createFullDataset, createSmallDataset } from './helpers/test-helper';
import { getTestUser } from './helpers/get-user';
import { getAuthHeader } from './helpers/auth-header';

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

BlobStorageService.prototype.uploadFile = jest.fn();

DataLakeService.prototype.uploadFile = jest.fn();

const dataset1Id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
const revision1Id = '85f0e416-8bd1-4946-9e2c-1c958897c6ef';
const import1Id = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
const user: User = getTestUser('test', 'user');

describe('API Endpoints', () => {
    let dbManager: DatabaseManager;
    beforeAll(async () => {
        dbManager = await initDb();
        await user.save();
        await createFullDataset(dataset1Id, revision1Id, import1Id, user);
    });

    test('Return true test', async () => {
        const dataset1 = await Dataset.findOneBy({ id: dataset1Id });
        if (!dataset1) {
            throw new Error('Dataset not found');
        }
        const dto = await DatasetDTO.fromDatasetComplete(dataset1);
        expect(dto).toBe(dto);
    });

    describe('Step 1 - initial title and file upload', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).post('/en-GB/dataset').query({ filename: 'test-data-1.csv' });
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Upload returns 400 if no file attached', async () => {
            const err: ViewErrDTO = {
                success: false,
                status: 400,
                dataset_id: undefined,
                errors: [
                    {
                        field: 'csv',
                        message: [
                            {
                                lang: ENGLISH,
                                message: t('errors.no_csv_data', { lng: ENGLISH })
                            },
                            {
                                lang: WELSH,
                                message: t('errors.no_csv_data', { lng: WELSH })
                            }
                        ],
                        tag: {
                            name: 'errors.no_csv_data',
                            params: {}
                        }
                    }
                ]
            };
            const res = await request(app)
                .post('/en-GB/dataset')
                .set(getAuthHeader(user))
                .query({ filename: 'test-data-1.csv' });
            expect(res.status).toBe(400);
            expect(res.body).toEqual(err);
        });

        test('Upload returns 400 if no title is given', async () => {
            const err: ViewErrDTO = {
                success: false,
                status: 400,
                dataset_id: undefined,
                errors: [
                    {
                        field: 'title',
                        message: [
                            {
                                lang: ENGLISH,
                                message: t('errors.no_title', { lng: ENGLISH })
                            },
                            {
                                lang: WELSH,
                                message: t('errors.no_title', { lng: WELSH })
                            }
                        ],
                        tag: {
                            name: 'errors.no_title',
                            params: {}
                        }
                    }
                ]
            };
            const csvFile = path.resolve(__dirname, `sample-csvs/test-data-1.csv`);
            const res = await request(app).post('/en-GB/dataset').set(getAuthHeader(user)).attach('csv', csvFile);
            expect(res.status).toBe(400);
            expect(res.body).toEqual(err);
        });

        test('Upload returns 201 if a file is attached', async () => {
            const csvFile = path.resolve(__dirname, `sample-csvs/test-data-1.csv`);

            const res = await request(app)
                .post('/en-GB/dataset')
                .set(getAuthHeader(user))
                .attach('csv', csvFile)
                .field('title', 'Test Dataset 3')
                .field('lang', 'en-GB');
            const datasetInfo = await DatasetInfo.findOneBy({ title: 'Test Dataset 3' });
            if (!datasetInfo) {
                expect(datasetInfo).not.toBeNull();
                return;
            }
            const dataset = await datasetInfo.dataset;
            const datasetDTO = await DatasetDTO.fromDatasetWithRevisionsAndImports(dataset);
            expect(res.status).toBe(201);
            expect(res.body).toEqual(datasetDTO);
            await Dataset.remove(dataset);
        });

        test('Upload returns 500 if an error occurs with blob storage', async () => {
            BlobStorageService.prototype.uploadFile = jest.fn().mockImplementation(() => {
                throw new Error('Test error');
            });
            const csvFile = path.resolve(__dirname, `sample-csvs/test-data-1.csv`);
            const res = await request(app)
                .post('/en-GB/dataset')
                .set(getAuthHeader(user))
                .attach('csv', csvFile)
                .field('title', 'Test Dataset 3')
                .field('lang', 'en-GB');
            expect(res.status).toBe(500);
            expect(res.body).toEqual({ message: 'Error uploading file' });
        });
    });

    describe('Step 2 - Get a preview of the uploaded file with a View Object', () => {
        test('Get file preview returns 400 if page_number is too high', async () => {
            const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
            const testFile2Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
                .set(getAuthHeader(user))
                .query({ page_number: 20 });
            expect(res.status).toBe(400);
            expect(res.body).toEqual({
                success: false,
                status: 400,
                dataset_id: dataset1Id,
                errors: [
                    {
                        field: 'page_number',
                        message: [
                            {
                                lang: ENGLISH,
                                message: t('errors.page_number_to_high', { lng: ENGLISH, page_number: 6 })
                            },
                            { lang: WELSH, message: t('errors.page_number_to_high', { lng: WELSH, page_number: 6 }) }
                        ],
                        tag: {
                            name: 'errors.page_number_to_high',
                            params: { page_number: 6 }
                        }
                    }
                ]
            });
        });

        test('Get file preview returns 400 if page_size is too high', async () => {
            const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
            const testFile2Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);

            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
                .set(getAuthHeader(user))
                .query({ page_size: 1000 });
            expect(res.status).toBe(400);
            expect(res.body).toEqual({
                success: false,
                status: 400,
                dataset_id: dataset1Id,
                errors: [
                    {
                        field: 'page_size',
                        message: [
                            {
                                lang: ENGLISH,
                                message: t('errors.page_size', {
                                    lng: ENGLISH,
                                    max_page_size: MAX_PAGE_SIZE,
                                    min_page_size: MIN_PAGE_SIZE
                                })
                            },
                            {
                                lang: WELSH,
                                message: t('errors.page_size', {
                                    lng: WELSH,
                                    max_page_size: MAX_PAGE_SIZE,
                                    min_page_size: MIN_PAGE_SIZE
                                })
                            }
                        ],
                        tag: {
                            name: 'errors.page_size',
                            params: { max_page_size: MAX_PAGE_SIZE, min_page_size: MIN_PAGE_SIZE }
                        }
                    }
                ]
            });
        });

        test('Get file preview returns 400 if page_size is too low', async () => {
            const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
            const testFile2Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);

            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
                .set(getAuthHeader(user))
                .query({ page_size: 1 });
            expect(res.status).toBe(400);
            expect(res.body).toEqual({
                success: false,
                status: 400,
                dataset_id: dataset1Id,
                errors: [
                    {
                        field: 'page_size',
                        message: [
                            {
                                lang: ENGLISH,
                                message: t('errors.page_size', {
                                    lng: ENGLISH,
                                    max_page_size: MAX_PAGE_SIZE,
                                    min_page_size: MIN_PAGE_SIZE
                                })
                            },
                            {
                                lang: WELSH,
                                message: t('errors.page_size', {
                                    lng: WELSH,
                                    max_page_size: MAX_PAGE_SIZE,
                                    min_page_size: MIN_PAGE_SIZE
                                })
                            }
                        ],
                        tag: {
                            name: 'errors.page_size',
                            params: { max_page_size: MAX_PAGE_SIZE, min_page_size: MIN_PAGE_SIZE }
                        }
                    }
                ]
            });
        });
        test('Get preview of an import returns 200 and correct page data if the file is stored in BlobStorage (Default)', async () => {
            const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
            const testFile1Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile1Buffer.toString());

            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
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

        test('Get preview of an import returns 200 and correct page data if the file is stored in a Datalake', async () => {
            const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
            const testFile1Buffer = fs.readFileSync(testFile2);
            DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile1Buffer.toString());
            const fileImport = await FileImport.findOneBy({ id: import1Id });
            if (!fileImport) {
                throw new Error('Import not found');
            }
            fileImport.location = DataLocation.DATA_LAKE;
            await fileImport.save();

            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
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

        test('Get preview of an import returns 500 if the import location is not supported', async () => {
            const fileImport = await FileImport.findOneBy({ id: import1Id });
            if (!fileImport) {
                throw new Error('Import not found');
            }
            fileImport.location = DataLocation.UNKNOWN;
            await fileImport.save();

            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
                .set(getAuthHeader(user))
                .query({ page_number: 2, page_size: 100 });
            expect(res.status).toBe(500);
            expect(res.body).toEqual({ message: 'Import location not supported.' });
        });

        test('Get preview of an import returns 500 if a Datalake error occurs', async () => {
            DataLakeService.prototype.downloadFile = jest.fn().mockRejectedValue(new Error('A datalake error occured'));
            const fileImport = await FileImport.findOneBy({ id: import1Id });
            if (!fileImport) {
                throw new Error('Import not found');
            }
            fileImport.location = DataLocation.DATA_LAKE;
            await fileImport.save();

            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
                .set(getAuthHeader(user))
                .query({ page_number: 2, page_size: 100 });
            expect(res.status).toBe(500);
            expect(res.body).toEqual({
                success: false,
                status: 500,
                errors: [
                    {
                        field: 'csv',
                        message: [
                            { lang: ENGLISH, message: t('errors.download_from_datalake', { lng: ENGLISH }) },
                            { lang: WELSH, message: t('errors.download_from_datalake', { lng: WELSH }) }
                        ],
                        tag: { name: 'errors.download_from_datalake', params: {} }
                    }
                ],
                dataset_id: dataset1Id
            });
        });

        test('Get preview of an import returns 404 when a non-existant import is requested', async () => {
            const res = await request(app)
                .get(
                    `/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/97C3F48F-127C-4317-B39C-87350F222310/preview`
                )
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
            expect(res.body).toEqual({ message: 'Import not found.' });
        });
    });

    test('Ensure Delete a dataset really deletes the dataset', async () => {
        const datasetID = crypto.randomUUID().toLowerCase();
        const testDataset = await createSmallDataset(datasetID, crypto.randomUUID(), crypto.randomUUID(), user);
        expect(testDataset).not.toBeNull();
        expect(testDataset.id).toBe(datasetID);
        const datesetFromDb = await Dataset.findOneBy({ id: datasetID });
        expect(datesetFromDb).not.toBeNull();
        expect(datesetFromDb?.id).toBe(datasetID);

        const res = await request(app).delete(`/en-GB/dataset/${datasetID}`).set(getAuthHeader(user));
        expect(res.status).toBe(204);
        const dataset = await Dataset.findOneBy({ id: datasetID });
        expect(dataset).toBeNull();
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
        await dbManager.getDataSource().destroy();
    });
});
