import path from 'path';
import fs from 'fs';

import request from 'supertest';

import { t, ENGLISH, WELSH } from '../src/middleware/translation';
import { DataLakeService } from '../src/controllers/datalake';
import { BlobStorageService } from '../src/controllers/blob-storage';
import app, { initDb } from '../src/app';
import { Dataset } from '../src/entities/dataset';
import { Revision } from '../src/entities/revision';
import { FileImport } from '../src/entities/file-import';
import { User } from '../src/entities/user';
import { DatasetDTO, DimensionDTO, ImportDTO, RevisionDTO } from '../src/dtos/dataset-dto';
import DatabaseManager from '../src/db/database-manager';
import { DataLocation } from '../src/enums/data-location';

import { createFullDataset } from './helpers/test-helper';
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

describe('API Endpoints for viewing dataset objects', () => {
    let dbManager: DatabaseManager;
    beforeAll(async () => {
        dbManager = await initDb();
        await user.save();
        await createFullDataset(dataset1Id, revision1Id, import1Id, user);
    });

    test('Check fixtures loaded successfully', async () => {
        const dataset1 = await Dataset.findOneBy({ id: dataset1Id });
        if (!dataset1) {
            throw new Error('Dataset not found');
        }
        const dto = await DatasetDTO.fromDatasetComplete(dataset1);
        expect(dto).toBeInstanceOf(DatasetDTO);
    });

    describe('List all datasets', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get('/en-GB/dataset');
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Get a list of all datasets returns 200 with a file list', async () => {
            const res = await request(app).get('/en-GB/dataset').set(getAuthHeader(user));
            expect(res.status).toBe(200);
            expect(res.body).toEqual({
                filelist: [
                    {
                        titles: [{ language: 'en-GB', title: 'Test Dataset 1' }],
                        dataset_id: dataset1Id
                    }
                ]
            });
        });
    });

    describe('Display dataset object endpoints', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(`/en-GB/dataset/${dataset1Id}`);
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Get a dataset returns 200 with a shallow object', async () => {
            const dataset1 = await Dataset.findOneBy({ id: dataset1Id });
            if (!dataset1) {
                throw new Error('Dataset not found');
            }
            const dto = await DatasetDTO.fromDatasetComplete(dataset1);
            const res = await request(app).get(`/en-GB/dataset/${dataset1Id}`).set(getAuthHeader(user));
            expect(res.status).toBe(200);
            expect(res.body).toEqual(dto);
        });

        test('Get a dataset returns 400 if an invalid ID is given', async () => {
            const res = await request(app).get(`/en-GB/dataset/INVALID-ID`).set(getAuthHeader(user));
            expect(res.status).toBe(400);
            expect(res.body).toEqual({ message: 'Dataset ID is not valid' });
        });

        test('Get a dataset returns 404 if a non-existant ID is given', async () => {
            const res = await request(app)
                .get(`/en-GB/dataset/8B9434D1-4807-41CD-8E81-228769671A07`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
        });
    });

    describe('Display dimension metadata endpoints', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(
                `/en-GB/dataset/${dataset1Id}/dimension/by-id/06b60fc5-93c9-4bd8-ac6f-3cc60ea538c4`
            );
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Get a dimension returns 200 with a shallow object', async () => {
            const dataset1 = await Dataset.findOneBy({ id: dataset1Id });
            if (!dataset1) {
                throw new Error('Dataset not found');
            }
            const dimension = (await dataset1.dimensions).pop();
            if (!dimension) {
                throw new Error('No dimension found on test dataset');
            }
            const dto = await DimensionDTO.fromDimension(dimension);
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/dimension/by-id/${dimension.id}`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(200);
            expect(res.body).toEqual(dto);
        });

        test('Get a dimension returns 400 if an invalid ID is given', async () => {
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/dimension/by-id/INVALID-ID`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(400);
            expect(res.body).toEqual({ message: 'Dimension ID is not valid' });
        });

        test('Get a dimension returns 404 if a non-existant ID is given', async () => {
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/dimension/by-id/8B9434D1-4807-41CD-8E81-228769671A07`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
        });
    });

    describe('Get revision metadata endpoints', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}`);
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Get a revision returns 200 with a shallow object', async () => {
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

        test('Get revision returns 400 if an invalid ID is given', async () => {
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/INVALID-ID`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(400);
            expect(res.body).toEqual({ message: 'Revision ID is not valid' });
        });

        test('Get revision returns 404 if a ID is given', async () => {
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/8B9434D1-4807-41CD-8E81-228769671A07`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
        });
    });

    describe('Get FileImport metadata endpoints', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(
                `/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/preview`
            );
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Get import returns 200 with object', async () => {
            const imp = await FileImport.findOneBy({ id: import1Id });
            if (!imp) {
                throw new Error('Import not found');
            }
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}`)
                .set(getAuthHeader(user));
            const expectedDTO = await ImportDTO.fromImport(imp);
            expect(res.status).toBe(200);
            expect(res.body).toEqual(expectedDTO);
        });

        test('Get import returns 400 if given an invalid ID', async () => {
            const res = await request(app)
                .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/IN-VALID-ID`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(400);
            expect(res.body).toEqual({ message: 'Import ID is not valid' });
        });

        test('Get import returns 404 if given a missing ID', async () => {
            const res = await request(app)
                .get(
                    `/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/8B9434D1-4807-41CD-8E81-228769671A07`
                )
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
        });

        describe('Getting a raw file out of a file import', () => {
            test('Get file from a revision and import returns 200 and complete file data if stored in BlobStorage', async () => {
                const fileImport = await FileImport.findOneBy({ id: import1Id });
                if (!fileImport) {
                    throw new Error('Import not found');
                }
                fileImport.location = DataLocation.BLOB_STORAGE;
                await fileImport.save();
                const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
                const testFileStream = fs.createReadStream(testFile2);
                const testFile2Buffer = fs.readFileSync(testFile2);
                BlobStorageService.prototype.getReadableStream = jest.fn().mockReturnValue(testFileStream);
                const res = await request(app)
                    .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/raw`)
                    .set(getAuthHeader(user));
                expect(res.status).toBe(200);
                expect(res.text).toEqual(testFile2Buffer.toString());
                fileImport.location = DataLocation.DATA_LAKE;
                await fileImport.save();
            });

            test('Get file from a revision and import returns 200 and complete file data if stored in the Data Lake', async () => {
                const testFile2 = path.resolve(__dirname, `sample-csvs/test-data-2.csv`);
                const testFileStream = fs.createReadStream(testFile2);
                const testFile2Buffer = fs.readFileSync(testFile2);
                DataLakeService.prototype.downloadFileStream = jest.fn().mockReturnValue(testFileStream);

                const res = await request(app)
                    .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/raw`)
                    .set(getAuthHeader(user));
                expect(res.status).toBe(200);
                expect(res.text).toEqual(testFile2Buffer.toString());
            });

            test('Get file from a revision and import returns 500 and complete file data if stored in an unknown location', async () => {
                const fileImport = await FileImport.findOneBy({ id: import1Id });
                if (!fileImport) {
                    throw new Error('Import not found');
                }
                fileImport.location = DataLocation.UNKNOWN;
                await fileImport.save();

                const res = await request(app)
                    .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/raw`)
                    .set(getAuthHeader(user));
                expect(res.status).toBe(500);
                expect(res.body).toEqual({ message: 'Import location not supported.' });
            });

            test('Get file from a revision and import returns 500 if an error with the Data Lake occurs', async () => {
                DataLakeService.prototype.downloadFileStream = jest
                    .fn()
                    .mockRejectedValue(new Error('Unknown Data Lake Error'));
                const fileImport = await FileImport.findOneBy({ id: import1Id });
                if (!fileImport) {
                    throw new Error('Import not found');
                }
                fileImport.location = DataLocation.DATA_LAKE;
                await fileImport.save();

                const res = await request(app)
                    .get(`/en-GB/dataset/${dataset1Id}/revision/by-id/${revision1Id}/import/by-id/${import1Id}/raw`)
                    .set(getAuthHeader(user));
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
        });
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
        await dbManager.getDataSource().destroy();
    });
});
