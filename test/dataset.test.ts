import path from 'path';
import * as fs from 'fs';

import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import { BlobStorageService } from '../src/controllers/blob-storage';
import app, { initDb } from '../src/app';
import { ENGLISH, WELSH, i18next } from '../src/middleware/translation';
import { Dataset } from '../src/entities/dataset';
import { DatasetInfo } from '../src/entities/dataset-info';
import { Revision } from '../src/entities/revision';
import { Dimension } from '../src/entities/dimension';
import { User } from '../src/entities/user';
import { DatasetDTO, DimensionDTO, RevisionDTO } from '../src/dtos/dataset-dto';
import { ViewErrDTO } from '../src/dtos/view-dto';
import { MAX_PAGE_SIZE, MIN_PAGE_SIZE } from '../src/controllers/csv-processor';
import DatabaseManager from '../src/db/database-manager';

import { createFullDataset } from './helpers/test-helper';
import { getTestUser } from './helpers/get-user';
import { getAuthHeader } from './helpers/auth-header';

const t = i18next.t;

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

BlobStorageService.prototype.uploadFile = jest.fn();

DataLakeService.prototype.uploadFile = jest.fn();

const dataset1Id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
const revision1Id = '85f0e416-8bd1-4946-9e2c-1c958897c6ef';
const import1Id = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
const dimension1Id = '2d7acd0b-a46a-43f7-8a88-224ce97fc8b9';

let dbManager: DatabaseManager;
const user: User = getTestUser('test', 'user');

describe('Dataset routes', () => {
    beforeAll(async () => {
        dbManager = await initDb();
        await user.save();
        await createFullDataset(dataset1Id, revision1Id, import1Id, dimension1Id);
    });

    test('Check fixtures loaded successfully', async () => {
        const dataset1 = await Dataset.findOneBy({ id: dataset1Id });
        if (!dataset1) {
            throw new Error('Dataset not found');
        }
        const dto = await DatasetDTO.fromDatasetComplete(dataset1);
        expect(dto).toBeInstanceOf(DatasetDTO);
    });

    describe('Upload dataset', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).post('/en-GB/dataset').query({ filename: 'test-data-1.csv' });
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('returns 400 if no file attached', async () => {
            const err: ViewErrDTO = {
                success: false,
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

        test('returns 400 if no title is given', async () => {
            const err: ViewErrDTO = {
                success: false,
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
            const csvfile = path.resolve(__dirname, `./test-data-1.csv`);
            const res = await request(app).post('/en-GB/dataset').set(getAuthHeader(user)).attach('csv', csvfile);

            expect(res.status).toBe(400);
            expect(res.body).toEqual(err);
        });

        test('returns 201 if a file is attached', async () => {
            const csvfile = path.resolve(__dirname, `./test-data-1.csv`);

            const res = await request(app)
                .post('/en-GB/dataset')
                .set(getAuthHeader(user))
                .attach('csv', csvfile)
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
    });

    describe('List datasets', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get('/en-GB/dataset');
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('returns 200 with a file list', async () => {
            const res = await request(app).get('/en-GB/dataset').set(getAuthHeader(user));
            expect(res.status).toBe(200);
            expect(res.body).toEqual({
                filelist: [
                    {
                        titles: [{ language: 'en-GB', title: 'Test Dataset 1' }],
                        dataset_id: 'bdc40218-af89-424b-b86e-d21710bc92f1'
                    }
                ]
            });
        });
    });

    describe('Fetch dataset', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(`/en-GB/dataset/${dataset1Id}`);
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('returns 400 when a not valid UUID is supplied', async () => {
            const res = await request(app).get(`/en-GB/dataset/NOT-VALID-ID`).set(getAuthHeader(user));
            expect(res.status).toBe(400);
            expect(res.body).toEqual({ message: 'Dataset ID is not valid' });
        });

        test('returns 200 with a shallow object', async () => {
            const dataset1 = await Dataset.findOneBy({ id: dataset1Id });
            if (!dataset1) {
                throw new Error('Dataset not found');
            }
            const dto = await DatasetDTO.fromDatasetComplete(dataset1);
            const res = await request(app).get(`/en-GB/dataset/${dataset1Id}`).set(getAuthHeader(user));
            expect(res.status).toBe(200);
            expect(res.body).toEqual(dto);
        });

        test('returns 200 and complete file data', async () => {
            const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
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
            expect(res.body.headers).toEqual(['ID', 'Text', 'Number', 'Date']);
            expect(res.body.data[0]).toEqual(['101', 'GEYiRzLIFM', '774477', '2002-03-13']);
            expect(res.body.data[99]).toEqual(['200', 'QhBxdmrUPb', '3256099', '2026-12-17']);
        });
    });

    describe('Fetch dimension', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(`/en-GB/dataset/${dataset1Id}/dimension/by-id/${dimension1Id}`);
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('returns 200 with a shallow object', async () => {
            const dimension = await Dimension.findOneBy({ id: dimension1Id });
            if (!dimension) {
                throw new Error('Dataset not found');
            }
            const dto = await DimensionDTO.fromDimension(dimension);
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/dimension/by-id/${dimension1Id}`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(200);
            expect(res.body).toEqual(dto);
        });
    });

    describe('Fetch revision', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}`);
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('returns 200 with a shallow object', async () => {
            const revision = await Revision.findOneBy({ id: revision1Id });
            if (!revision) {
                throw new Error('Dataset not found');
            }
            const dto = await RevisionDTO.fromRevision(revision);
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(200);
            expect(res.body).toEqual(dto);
        });
    });

    describe('Fetch import', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(
                `/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`
            );
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('returns 400 if page_number is too high', async () => {
            const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
            const testFile2Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
                .set(getAuthHeader(user))
                .query({ page_number: 20 });
            expect(res.status).toBe(400);
            expect(res.body).toEqual({
                success: false,
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

        test('returns 400 if page_size is too high', async () => {
            const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
            const testFile2Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);

            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
                .set(getAuthHeader(user))
                .query({ page_size: 1000 });
            expect(res.status).toBe(400);
            expect(res.body).toEqual({
                success: false,
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

        test('returns 400 if page_size is too low', async () => {
            const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
            const testFile2Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
                .set(getAuthHeader(user))
                .query({ page_size: 1 });
            expect(res.status).toBe(400);
            expect(res.body).toEqual({
                success: false,
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

        describe('raw', () => {
            test('returns 200 and complete file data', async () => {
                const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
                const testFileStream = fs.createReadStream(testFile2);
                const testFile2Buffer = fs.readFileSync(testFile2);
                BlobStorageService.prototype.getReadableStream = jest.fn().mockReturnValue(testFileStream);
                const res = await request(app)
                    .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/raw`)
                    .set(getAuthHeader(user));
                expect(res.status).toBe(200);
                expect(res.text).toEqual(testFile2Buffer.toString());
            });
        });

        describe('preview', () => {
            test('returns 200 and correct page data', async () => {
                const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
                const testFile1Buffer = fs.readFileSync(testFile2);
                BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile1Buffer.toString());
                const res = await request(app)
                    .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
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

            test('returns 404 when a non-existant import is requested', async () => {
                const res = await request(app)
                    .get(
                        `/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/97C3F48F-127C-4317-B39C-87350F222310/preview`
                    )
                    .set(getAuthHeader(user));
                expect(res.status).toBe(404);
                expect(res.body).toEqual({ message: 'Import not found.' });
            });
        });
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
        await dbManager.getDataSource().destroy();
    });
});
