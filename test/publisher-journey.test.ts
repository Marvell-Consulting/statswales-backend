import path from 'path';
import * as fs from 'fs';

import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import { BlobStorageService } from '../src/controllers/blob-storage';
import app, { ENGLISH, WELSH, t, dbManager, databaseManager } from '../src/app';
import { Dataset } from '../src/entities/dataset';
import { DatasetInfo } from '../src/entities/dataset_info';
import { DatasetDTO } from '../src/dtos/dataset-dto';
import { ViewErrDTO } from '../src/dtos/view-dto';
import { MAX_PAGE_SIZE, MIN_PAGE_SIZE } from '../src/controllers/csv-processor';

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

describe('API Endpoints', () => {
    beforeAll(async () => {
        await databaseManager(datasourceOptions);
        await dbManager.initializeDataSource();
        await createFullDataset(dataset1Id, revision1Id, import1Id, dimension1Id);
    });

    test('Return true test', async () => {
        const dataset1 = await Dataset.findOneBy({ id: dataset1Id });
        if (!dataset1) {
            throw new Error('Dataset not found');
        }
        const dto = await DatasetDTO.fromDatasetComplete(dataset1);
        expect(dto).toBe(dto);
    });

    test('Upload returns 400 if no file attached', async () => {
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
        const res = await request(app).post('/en-GB/dataset').query({ filename: 'test-data-1.csv' });
        expect(res.status).toBe(400);
        expect(res.body).toEqual(err);
    });

    test('Upload returns 400 if no title is given', async () => {
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
        const csvfile = path.resolve(__dirname, `sample-csvs/test-data-1.csv`);
        const res = await request(app).post('/en-GB/dataset').attach('csv', csvfile);
        expect(res.status).toBe(400);
        expect(res.body).toEqual(err);
    });

    test('Upload returns 201 if a file is attached', async () => {
        const csvfile = path.resolve(__dirname, `sample-csvs/test-data-1.csv`);

        const res = await request(app)
            .post('/en-GB/dataset')
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

    test('Upload returns 500 if an error occurs with blob storage', async () => {
        BlobStorageService.prototype.uploadFile = jest.fn().mockImplementation(() => {
            throw new Error('Test error');
        });
        const csvfile = path.resolve(__dirname, `sample-csvs/test-data-1.csv`);
        const res = await request(app)
            .post('/en-GB/dataset')
            .attach('csv', csvfile)
            .field('title', 'Test Dataset 3')
            .field('lang', 'en-GB');
        expect(res.status).toBe(500);
        expect(res.body).toEqual({ message: 'Error uploading file' });
    });

    test('Get file view returns 400 if page_number is too high', async () => {
        const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);
        const res = await request(app)
            .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
            .query({ page_number: 20 });
        expect(res.status).toBe(400);
        expect(res.body).toEqual({
            success: false,
            dataset_id: dataset1Id,
            errors: [
                {
                    field: 'page_number',
                    message: [
                        { lang: ENGLISH, message: t('errors.page_number_to_high', { lng: ENGLISH, page_number: 6 }) },
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

    test('Get file view returns 400 if page_size is too high', async () => {
        const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);

        const res = await request(app)
            .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
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

    test('Get file view returns 400 if page_size is too low', async () => {
        const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);

        const res = await request(app)
            .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`)
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

    test('Get file from a revision and import rertunrs 200 and complete file data', async () => {
        const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
        const testFileStream = fs.createReadStream(testFile2);
        const testFile2Buffer = fs.readFileSync(testFile2);
        BlobStorageService.prototype.getReadableStream = jest.fn().mockReturnValue(testFileStream);
        const res = await request(app).get(
            `/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/raw`
        );
        expect(res.status).toBe(200);
        expect(res.text).toEqual(testFile2Buffer.toString());
    });

    test('Get preview of an import returns 200 and correct page data', async () => {
        const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
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

    test('Get preview of an import returns 404 when a non-existant import is requested', async () => {
        const res = await request(app).get(
            `/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/97C3F48F-127C-4317-B39C-87350F222310/preview`
        );
        expect(res.status).toBe(404);
        expect(res.body).toEqual({ message: 'Import not found.' });
    });

    test('Delete a dataset actaully deletes the dataset', async () => {
        const datasetID = crypto.randomUUID();
        const testDataset = await createSmallDataset(datasetID, crypto.randomUUID(), crypto.randomUUID());
        expect(testDataset).not.toBeNull();
        expect(testDataset.id).toBe(datasetID);
        const datesetFromDb = await Dataset.findOneBy({ id: datasetID });
        expect(datesetFromDb).not.toBeNull();
        expect(datesetFromDb?.id).toBe(datasetID);

        const res = await request(app).delete(`/en-GB/dataset/${datasetID}`);
        expect(res.status).toBe(204);
        const dataset = await Dataset.findOneBy({ id: datasetID });
        expect(dataset).toBeNull();
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
    });
});
