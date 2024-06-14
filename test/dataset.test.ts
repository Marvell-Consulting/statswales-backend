import path from 'path';
import * as fs from 'fs';
import { createHash } from 'crypto';

import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import app, { ENGLISH, WELSH, t, dbManager, connectToDb } from '../src/app';
import { Dataset } from '../src/entity/dataset';
import { Datafile } from '../src/entity/datafile';
import { datasetToDatasetDTO } from '../src/dtos/dataset-dto';
import { ViewErrDTO } from '../src/dtos/view-dto';
import { MAX_PAGE_SIZE, MIN_PAGE_SIZE } from '../src/controllers/csv-processor';

import { datasourceOptions } from './test-data-source';

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

DataLakeService.prototype.uploadFile = jest.fn();

describe('API Endpoints', () => {
    beforeAll(async () => {
        await connectToDb(datasourceOptions);
        await dbManager.initializeDataSource();

        const dataset1 = Dataset.createDataset('Test Data 1', 'test', 'bdc40218-af89-424b-b86e-d21710bc92f1');
        dataset1.live = true;
        dataset1.code = 'tst0001';
        await dataset1.save();
        dataset1.addTitleByString('Test Dataset 1', 'EN');
        dataset1.addDescriptionByString('I am the first test dataset', 'EN');

        const dataset2 = Dataset.createDataset('Test Data 2', 'test', 'fa07be9d-3495-432d-8c1f-d0fc6daae359');
        dataset2.live = true;
        dataset2.code = 'tst0002';
        dataset2.createdBy = 'test';
        await dataset2.save();
        const datafile2 = new Datafile();
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        datafile2.sha256hash = createHash('sha256').update(testFile2Buffer).digest('hex');
        datafile2.createdBy = 'test';
        await dataset2.addDatafile(datafile2);
        dataset2.addTitleByString('Test Dataset 2', 'EN');
        dataset2.addDescriptionByString('I am the second test dataset', 'EN');
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

    test('Upload returns 400 if no internal name is given', async () => {
        const err: ViewErrDTO = {
            success: false,
            dataset_id: undefined,
            errors: [
                {
                    field: 'internal_name',
                    message: [
                        {
                            lang: ENGLISH,
                            message: t('errors.internal_name', { lng: ENGLISH })
                        },
                        {
                            lang: WELSH,
                            message: t('errors.internal_name', { lng: WELSH })
                        }
                    ],
                    tag: {
                        name: 'errors.internal_name',
                        params: {}
                    }
                }
            ]
        };
        const csvfile = path.resolve(__dirname, `./test-data-1.csv`);
        const res = await request(app).post('/en-GB/dataset').attach('csv', csvfile);
        expect(res.status).toBe(400);
        expect(res.body).toEqual(err);
    });

    test('Upload returns 200 if a file is attached', async () => {
        const csvfile = path.resolve(__dirname, `./test-data-1.csv`);

        const res = await request(app)
            .post('/en-GB/dataset')
            .attach('csv', csvfile)
            .field('internal_name', 'Test Dataset 3');
        const dataset = await Dataset.findOneBy({ internalName: 'Test Dataset 3' });
        if (!dataset) {
            expect(dataset).not.toBeNull();
            return;
        }
        const datasetDTO = await datasetToDatasetDTO(dataset);
        expect(res.status).toBe(200);
        expect(res.body).toEqual({
            success: true,
            dataset: datasetDTO
        });
        await dataset.remove();
    });

    test('Get a filelist list returns 200 with a file list', async () => {
        const res = await request(app).get('/en-GB/dataset');
        expect(res.status).toBe(200);
        expect(res.body).toEqual({
            filelist: [
                {
                    internal_name: 'Test Data 1',
                    id: 'bdc40218-af89-424b-b86e-d21710bc92f1'
                },
                {
                    internal_name: 'Test Data 2',
                    id: 'fa07be9d-3495-432d-8c1f-d0fc6daae359'
                }
            ]
        });
    });

    test('Get file view returns 400 if page_number is too high', async () => {
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile2Buffer);
        const res = await request(app)
            .get('/en-GB/dataset/fa07be9d-3495-432d-8c1f-d0fc6daae359/view')
            .query({ page_number: 20 });
        expect(res.status).toBe(400);
        expect(res.body).toEqual({
            success: false,
            dataset_id: 'fa07be9d-3495-432d-8c1f-d0fc6daae359',
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
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile2Buffer);

        const res = await request(app)
            .get('/en-GB/dataset/fa07be9d-3495-432d-8c1f-d0fc6daae359/view')
            .query({ page_size: 1000 });
        expect(res.status).toBe(400);
        expect(res.body).toEqual({
            success: false,
            dataset_id: 'fa07be9d-3495-432d-8c1f-d0fc6daae359',
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
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile2Buffer);

        const res = await request(app)
            .get('/en-GB/dataset/fa07be9d-3495-432d-8c1f-d0fc6daae359/view')
            .query({ page_size: 1 });
        expect(res.status).toBe(400);
        expect(res.body).toEqual({
            success: false,
            dataset_id: 'fa07be9d-3495-432d-8c1f-d0fc6daae359',
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

    test('Get file rertunrs 200 and complete file data', async () => {
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile2Buffer.toString());
        const dataset = await Dataset.findOneBy({ id: 'fa07be9d-3495-432d-8c1f-d0fc6daae359' });
        if (!dataset) {
            expect(dataset).not.toBeNull();
            return;
        }

        const res = await request(app).get('/en-GB/dataset/fa07be9d-3495-432d-8c1f-d0fc6daae359/');
        expect(res.status).toBe(200);
        const expectedDTO = await datasetToDatasetDTO(dataset);
        expect(res.body).toEqual(expectedDTO);
    });

    test('Get csv file rertunrs 200 and complete file data', async () => {
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile2Buffer.toString());

        const res = await request(app).get('/en-GB/dataset/fa07be9d-3495-432d-8c1f-d0fc6daae359/csv');
        expect(res.status).toBe(200);
        expect(res.text).toEqual(testFile2Buffer.toString());
    });

    test('Get xlsx file rertunrs 200 and complete file data', async () => {
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile2Buffer.toString());

        const res = await request(app).get('/en-GB/dataset/fa07be9d-3495-432d-8c1f-d0fc6daae359/xlsx');
        expect(res.status).toBe(200);
        expect(res.body).toEqual({ message: 'Not implmented yet' });
    });

    test('Get file view returns 200 and correct page data', async () => {
        const testFile2 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile1Buffer = fs.readFileSync(testFile2);
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile1Buffer.toString());

        const res = await request(app)
            .get('/en-GB/dataset/fa07be9d-3495-432d-8c1f-d0fc6daae359/view')
            .query({ page_number: 2, page_size: 100 });
        expect(res.status).toBe(200);
        expect(res.body.current_page).toBe(2);
        expect(res.body.total_pages).toBe(6);
        expect(res.body.page_size).toBe(100);
        expect(res.body.headers).toEqual(['ID', 'Text', 'Number', 'Date']);
        expect(res.body.data[0]).toEqual(['101', 'GEYiRzLIFM', '774477', '2002-03-13']);
        expect(res.body.data[99]).toEqual(['200', 'QhBxdmrUPb', '3256099', '2026-12-17']);
    });

    test('Get file view returns 404 when a non-existant file is requested', async () => {
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(null);

        const res = await request(app).get('/en-GB/dataset/test-data-4.csv/csv');
        expect(res.status).toBe(404);
        expect(res.body).toEqual({ message: 'Dataset not found... Dataset ID not found in Database' });
    });

    test('Get file view returns 404 when a non-existant file view is requested', async () => {
        DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(null);

        const res = await request(app).get('/en-GB/dataset/test-data-4.csv/view');
        expect(res.status).toBe(404);
        expect(res.body).toEqual({ message: 'Dataset not found... Dataset ID not found in Database' });
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
    });
});
