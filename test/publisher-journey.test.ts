import path from 'path';
import * as fs from 'fs';

import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import { BlobStorageService } from '../src/controllers/blob-storage';
import app, { initDb } from '../src/app';
import { ENGLISH, t, WELSH } from '../src/middleware/translation';
import { Dataset } from '../src/entities/dataset';
import { DatasetInfo } from '../src/entities/dataset-info';
import { DatasetDTO, ImportDTO } from '../src/dtos/dataset-dto';
import { ViewErrDTO } from '../src/dtos/view-dto';
import { MAX_PAGE_SIZE, MIN_PAGE_SIZE } from '../src/controllers/csv-processor';
import DatabaseManager from '../src/db/database-manager';
import { User } from '../src/entities/user';
import { FileImport } from '../src/entities/file-import';
import { DataLocation } from '../src/enums/data-location';
import { DimensionCreationDTO } from '../src/dtos/dimension-creation-dto';
import { SourceType } from '../src/enums/source-type';
import { Revision } from '../src/entities/revision';

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
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
            const testFile2Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);
            const res = await request(app)
                .get(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/preview`
                )
                .set(getAuthHeader(user))
                .query({ page_number: 20 });
            expect(res.status).toBe(400);
            expect(res.body).toEqual({
                success: false,
                status: 400,
                dataset_id: testDatasetId,
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
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
            const testFile2Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);

            const res = await request(app)
                .get(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/preview`
                )
                .set(getAuthHeader(user))
                .query({ page_size: 1000 });
            expect(res.status).toBe(400);
            expect(res.body).toEqual({
                success: false,
                status: 400,
                dataset_id: testDatasetId,
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
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
            const testFile2Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile2Buffer);

            const res = await request(app)
                .get(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/preview`
                )
                .set(getAuthHeader(user))
                .query({ page_size: 1 });
            expect(res.status).toBe(400);
            expect(res.body).toEqual({
                success: false,
                status: 400,
                dataset_id: testDatasetId,
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
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
            const testFile1Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.readFile = jest.fn().mockReturnValue(testFile1Buffer.toString());

            const res = await request(app)
                .get(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/preview`
                )
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

    describe('Step 2b - Unhappy path of the user uploading the wrong file', () => {
        test('Returns 200 when the user requests to delete the import', async () => {
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            BlobStorageService.prototype.deleteFile = jest.fn().mockReturnValue(true);
            const res = await request(app)
                .delete(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}`
                )
                .set(getAuthHeader(user));
            expect(res.status).toBe(200);
            const updatedRevision = await Revision.findOneBy({ id: testRevisionId });
            if (!updatedRevision) {
                throw new Error('Revision not found');
            }
            const imports = await updatedRevision.imports;
            expect(imports).toBeInstanceOf(Array);
            expect(imports.length).toBe(0);
            const updatedDataset = await Dataset.findOneBy({ id: testDatasetId });
            if (!updatedDataset) {
                throw new Error('Dataset not found');
            }
            const dto = await DatasetDTO.fromDatasetWithRevisionsAndImports(updatedDataset);
            expect(res.body).toEqual(dto);
        });

        test('Returns 500 when the user requests to delete the import and there is an error with BlobStorage', async () => {
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            BlobStorageService.prototype.deleteFile = jest.fn().mockRejectedValue(new Error('File not found'));
            const res = await request(app)
                .delete(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}`
                )
                .set(getAuthHeader(user));
            expect(res.status).toBe(500);
            const updatedRevision = await Revision.findOneBy({ id: testRevisionId });
            if (!updatedRevision) {
                throw new Error('Revision not found');
            }
            const imports = await updatedRevision.imports;
            expect(imports).toBeInstanceOf(Array);
            expect(imports.length).toBe(1);
            expect(res.body).toEqual({
                message: 'Error removing file from temporary blob storage.  Please try again.'
            });
        });

        test('Upload returns 400 if no file attached', async () => {
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            const fileImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!fileImport) {
                throw new Error('File Import not found');
            }
            await fileImport.remove();
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
                .post(`/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import`)
                .set(getAuthHeader(user))
                .query({ title: 'Failure Test' });
            expect(res.status).toBe(400);
            expect(res.body).toEqual(err);
        });

        test('Upload returns 201 if a file is attached', async () => {
            BlobStorageService.prototype.uploadFile = jest.fn().mockReturnValue({});
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            const fileImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!fileImport) {
                throw new Error('File Import not found');
            }
            await fileImport.remove();
            const revision = await Revision.findOneBy({ id: testRevisionId });
            if (!revision) {
                expect(revision).not.toBeNull();
                return;
            }
            expect((await revision.imports).length).toBe(0);
            const csvFile = path.resolve(__dirname, `sample-csvs/test-data-1.csv`);

            const res = await request(app)
                .post(`/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import`)
                .set(getAuthHeader(user))
                .attach('csv', csvFile)
                .field('title', 'Test Dataset 3')
                .field('lang', 'en-GB');
            const updatedRevision = await Revision.findOneBy({ id: testRevisionId });
            if (!updatedRevision) {
                expect(updatedRevision).not.toBeNull();
                return;
            }
            expect((await updatedRevision.imports).length).toBe(1);
            const dataset = await revision.dataset;
            const datasetDTO = await DatasetDTO.fromDatasetWithRevisionsAndImports(dataset);
            expect(res.status).toBe(201);
            expect(res.body).toEqual(datasetDTO);
            await Dataset.remove(dataset);
        });

        test('Upload returns 500 if an error occurs with blob storage', async () => {
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            const fileImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!fileImport) {
                throw new Error('File Import not found');
            }
            await fileImport.remove();
            BlobStorageService.prototype.uploadFile = jest.fn().mockImplementation(() => {
                throw new Error('Test error');
            });
            const csvFile = path.resolve(__dirname, `sample-csvs/test-data-1.csv`);
            const res = await request(app)
                .post(`/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import`)
                .set(getAuthHeader(user))
                .attach('csv', csvFile)
                .field('lang', 'en-GB');
            expect(res.status).toBe(500);
            expect(res.body).toEqual({ message: 'Error uploading file' });
        });
    });

    describe('Step 3 - Confirming the datafile', () => {
        test('Returns 200 with an import dto listing the new sources which have been created', async () => {
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            const preRunFilImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!preRunFilImport) {
                throw new Error('Import not found');
            }
            preRunFilImport.location = DataLocation.BLOB_STORAGE;
            await preRunFilImport.save();
            const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
            const testFile1Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.getReadableStream = jest.fn();
            DataLakeService.prototype.uploadFileStream = jest.fn();
            DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile1Buffer.toString());
            BlobStorageService.prototype.deleteFile = jest.fn().mockReturnValue(true);
            const res = await request(app)
                .patch(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/confirm`
                )
                .set(getAuthHeader(user));
            const postRunFileImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!postRunFileImport) {
                throw new Error('Import not found');
            }
            expect(postRunFileImport.location).toBe(DataLocation.DATA_LAKE);
            const sources = await postRunFileImport.sources;
            expect(sources.length).toBe(4);
            const dto = await ImportDTO.fromImport(postRunFileImport);
            expect(res.status).toBe(200);
            expect(res.body).toEqual(dto);
        });

        test('Returns 200 with an import dto listing the no additional sources are created if sources are already present', async () => {
            const res = await request(app)
                .patch(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/confirm`)
                .set(getAuthHeader(user));
            const postRunFileImport = await FileImport.findOneBy({ id: import1Id });
            if (!postRunFileImport) {
                throw new Error('Import not found');
            }
            expect(postRunFileImport.location).toBe(DataLocation.DATA_LAKE);
            const sources = await postRunFileImport.sources;
            expect(sources.length).toBe(4);
            const dto = await ImportDTO.fromImport(postRunFileImport);
            expect(res.status).toBe(200);
            expect(res.body).toEqual(dto);
        });

        test('Returns 500 if an error occurs moving the file between BlobStorage and the Datalake', async () => {
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            const preRunFilImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!preRunFilImport) {
                throw new Error('Import not found');
            }
            preRunFilImport.location = DataLocation.BLOB_STORAGE;
            await preRunFilImport.save();
            BlobStorageService.prototype.getReadableStream = jest.fn().mockRejectedValue(new Error('File not found'));
            const res = await request(app)
                .patch(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/confirm`
                )
                .set(getAuthHeader(user));
            const postRunFileImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!postRunFileImport) {
                throw new Error('Import not found');
            }
            expect(postRunFileImport.location).toBe(DataLocation.BLOB_STORAGE);
            expect(res.status).toBe(500);
            expect(res.body).toEqual({
                message: 'Error moving file from temporary blob storage to Data Lake.  Please try again.'
            });
        });

        test('Returns 500 if an error occurs processing sources from Datalake', async () => {
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            const preRunFilImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!preRunFilImport) {
                throw new Error('Import not found');
            }
            preRunFilImport.location = DataLocation.BLOB_STORAGE;
            await preRunFilImport.save();
            BlobStorageService.prototype.getReadableStream = jest.fn();
            DataLakeService.prototype.uploadFileStream = jest.fn();
            BlobStorageService.prototype.deleteFile = jest.fn().mockReturnValue(true);
            DataLakeService.prototype.downloadFile = jest.fn().mockRejectedValue(new Error('File not found'));
            const res = await request(app)
                .patch(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/confirm`
                )
                .set(getAuthHeader(user));
            const postRunFileImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!postRunFileImport) {
                throw new Error('Import not found');
            }
            expect(postRunFileImport.location).toBe(DataLocation.DATA_LAKE);
            expect(res.status).toBe(500);
            expect(res.body).toEqual({ message: 'Error creating sources from the uploaded file.  Please try again.' });
        });
    });

    describe('Step 4 - Create dimensions', () => {
        async function createDatasetWithSources(
            testDatasetId: string,
            testRevisionId: string,
            testFileImportId: string
        ): Promise<Dataset> {
            await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
            const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
            const testFile1Buffer = fs.readFileSync(testFile2);
            BlobStorageService.prototype.getReadableStream = jest.fn();
            DataLakeService.prototype.uploadFileStream = jest.fn();
            DataLakeService.prototype.downloadFile = jest.fn().mockReturnValue(testFile1Buffer.toString());
            BlobStorageService.prototype.deleteFile = jest.fn().mockReturnValue(true);
            // Create sources in the database
            await request(app)
                .patch(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/confirm`
                )
                .set(getAuthHeader(user));
            const updatedDataset = await Dataset.findOneBy({ id: testDatasetId });
            if (!updatedDataset) {
                throw new Error('Dataset not found... Was it ever created?');
            }
            return updatedDataset;
        }

        test('Create dimensions from user supplied JSON returns 200 and updated dataset with dimensions attached', async () => {
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createDatasetWithSources(testDatasetId, testRevisionId, testFileImportId);
            const postProcessedImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!postProcessedImport) {
                throw new Error('Import not found');
            }
            const sources = await postProcessedImport.sources;
            const dimensionCreationJson: DimensionCreationDTO[] = sources.map((source) => {
                return {
                    sourceId: source.id,
                    sourceType: SourceType.DIMENSION
                };
            });
            dimensionCreationJson[0].sourceType = SourceType.DATAVALUES;
            dimensionCreationJson[1].sourceType = SourceType.FOOTNOTES;
            const res = await request(app)
                .patch(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/sources`
                )
                .send(dimensionCreationJson)
                .set(getAuthHeader(user));
            expect(res.status).toBe(200);
            const updatedDataset = await Dataset.findOneBy({ id: testDatasetId });
            if (!updatedDataset) {
                throw new Error('Dataset not found');
            }
            const dimensions = await updatedDataset.dimensions;
            expect(dimensions.length).toBe(3);
            const dto = await DatasetDTO.fromDatasetComplete(updatedDataset);
            expect(res.body).toEqual(dto);
        });

        test('Create dimensions from user supplied JSON returns 400 if the body is empty', async () => {
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createDatasetWithSources(testDatasetId, testRevisionId, testFileImportId);
            const res = await request(app)
                .patch(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/sources`
                )
                .send()
                .set(getAuthHeader(user));
            expect(res.status).toBe(400);
            expect(res.body).toEqual({
                message:
                    'Error processing the supplied JSON with the following error TypeError: dimensionCreationDTO.map is not a function'
            });
        });

        test('Create dimensions from user supplied JSON returns 400 if there is more than one set of Data Values', async () => {
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createDatasetWithSources(testDatasetId, testRevisionId, testFileImportId);
            const postProcessedImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!postProcessedImport) {
                throw new Error('Import not found');
            }
            const sources = await postProcessedImport.sources;
            const dimensionCreationJson: DimensionCreationDTO[] = sources.map((source) => {
                return {
                    sourceId: source.id,
                    sourceType: SourceType.DIMENSION
                };
            });
            dimensionCreationJson[0].sourceType = SourceType.DATAVALUES;
            dimensionCreationJson[1].sourceType = SourceType.DATAVALUES;
            const res = await request(app)
                .patch(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/sources`
                )
                .send(dimensionCreationJson)
                .set(getAuthHeader(user));
            expect(res.status).toBe(400);
            const updatedDataset = await Dataset.findOneBy({ id: testDatasetId });
            if (!updatedDataset) {
                throw new Error('Dataset not found');
            }
            const dimensions = await updatedDataset.dimensions;
            expect(dimensions.length).toBe(0);
            expect(res.body).toEqual({
                message:
                    'Error processing the supplied JSON with the following error Error: Only one DataValues source can be specified'
            });
        });

        test('Create dimensions from user supplied JSON returns 400 if there is more than one set of Footnotes', async () => {
            const testDatasetId = crypto.randomUUID().toLowerCase();
            const testRevisionId = crypto.randomUUID().toLowerCase();
            const testFileImportId = crypto.randomUUID().toLowerCase();
            await createDatasetWithSources(testDatasetId, testRevisionId, testFileImportId);
            const postProcessedImport = await FileImport.findOneBy({ id: testFileImportId });
            if (!postProcessedImport) {
                throw new Error('Import not found');
            }
            const sources = await postProcessedImport.sources;
            const dimensionCreationJson: DimensionCreationDTO[] = sources.map((source) => {
                return {
                    sourceId: source.id,
                    sourceType: SourceType.DIMENSION
                };
            });
            dimensionCreationJson[0].sourceType = SourceType.FOOTNOTES;
            dimensionCreationJson[1].sourceType = SourceType.FOOTNOTES;
            const res = await request(app)
                .patch(
                    `/en-GB/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/import/by-id/${testFileImportId}/sources`
                )
                .send(dimensionCreationJson)
                .set(getAuthHeader(user));
            expect(res.status).toBe(400);
            const updatedDataset = await Dataset.findOneBy({ id: testDatasetId });
            if (!updatedDataset) {
                throw new Error('Dataset not found');
            }
            const dimensions = await updatedDataset.dimensions;
            expect(dimensions.length).toBe(0);
            expect(res.body).toEqual({
                message:
                    'Error processing the supplied JSON with the following error Error: Only one FootNote source can be specified'
            });
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
