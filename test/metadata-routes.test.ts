import path from 'path';
import fs from 'fs';

import request from 'supertest';
import { t } from 'i18next';

import { DataLakeService } from '../src/services/datalake';
import app, { initDb } from '../src/app';
import { Dataset } from '../src/entities/dataset/dataset';
import { Revision } from '../src/entities/dataset/revision';
import { User } from '../src/entities/user/user';
import { DatasetDTO } from '../src/dtos/dataset-dto';
import { DimensionDTO } from '../src/dtos/dimension-dto';
import { RevisionDTO } from '../src/dtos/revision-dto';
import DatabaseManager from '../src/db/database-manager';
import { DatasetRepository } from '../src/repositories/dataset';
import { FactTableRepository } from '../src/repositories/fact-table';
import { FactTableDTO } from '../src/dtos/fact-table-dto';
import { Locale } from '../src/enums/locale';

import { createFullDataset } from './helpers/test-helper';
import { getTestUser } from './helpers/get-user';
import { getAuthHeader } from './helpers/auth-header';

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

DataLakeService.prototype.uploadFileBuffer = jest.fn();

const dataset1Id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
const revision1Id = '85f0e416-8bd1-4946-9e2c-1c958897c6ef';
const import1Id = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
const user: User = getTestUser('test', 'user');

describe('API Endpoints for viewing dataset objects', () => {
    let dbManager: DatabaseManager;
    beforeAll(async () => {
        try {
            dbManager = await initDb();
            await user.save();
            await createFullDataset(dataset1Id, revision1Id, import1Id, user);
        } catch (error) {
            await dbManager.getDataSource().dropDatabase();
            await dbManager.getDataSource().destroy();
            process.exit(1);
        }
    });

    test('Check fixtures loaded successfully', async () => {
        const dataset1 = await Dataset.findOneBy({ id: dataset1Id });
        if (!dataset1) {
            throw new Error('Dataset not found');
        }
        const dto = DatasetDTO.fromDataset(dataset1);
        expect(dto).toBeInstanceOf(DatasetDTO);
    });

    describe('List all datasets', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get('/dataset');
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Get a list of all datasets returns 200 with a file list', async () => {
            const res = await request(app).get('/dataset').set(getAuthHeader(user));
            expect(res.status).toBe(200);
            expect(res.body).toEqual([{ id: dataset1Id, title: 'Test Dataset 1' }]);
        });
    });

    describe('List all active datasets', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get('/dataset/active');
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Get a list of all active datasets returns 200 with a file list', async () => {
            const res = await request(app).get('/dataset/active').set(getAuthHeader(user));
            const today = new Date().toISOString().split('T')[0];
            expect(res.status).toBe(200);
            expect(res.body).toEqual([
                {
                    id: dataset1Id,
                    title: 'Test Dataset 1',
                    last_updated: expect.stringContaining(today),
                    status: 'live',
                    publishing_status: 'incomplete'
                }
            ]);
        });
    });

    describe('Display dataset object endpoints', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(`/dataset/${dataset1Id}`);
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Get a dataset returns 200', async () => {
            const dataset1 = await DatasetRepository.getById(dataset1Id);
            if (!dataset1) {
                throw new Error('Dataset not found');
            }
            const dto = await DatasetDTO.fromDataset(dataset1);
            const res = await request(app).get(`/dataset/${dataset1Id}`).set(getAuthHeader(user));
            expect(res.status).toBe(200);
            expect(res.body).toEqual(dto);
        });

        test('Get a dataset returns 404 if an invalid ID is given', async () => {
            const res = await request(app).get(`/dataset/INVALID-ID`).set(getAuthHeader(user));
            expect(res.status).toBe(404);
            expect(res.body).toEqual({ error: 'Dataset id is invalid or missing' });
        });

        test('Get a dataset returns 404 if a non-existant ID is given', async () => {
            const res = await request(app)
                .get(`/dataset/8B9434D1-4807-41CD-8E81-228769671A07`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
        });
    });

    describe('Display dimension metadata endpoints', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(
                `/dataset/${dataset1Id}/dimension/by-id/06b60fc5-93c9-4bd8-ac6f-3cc60ea538c4`
            );
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Get a dimension returns 200 with a shallow object', async () => {
            const dataset1 = await DatasetRepository.getById(dataset1Id);
            if (!dataset1) {
                throw new Error('Dataset not found');
            }
            const dimension = dataset1.dimensions.pop();
            if (!dimension) {
                throw new Error('No dimension found on test dataset');
            }
            const dto = await DimensionDTO.fromDimension(dimension);
            const res = await request(app)
                .get(`/dataset/${dataset1Id}/dimension/by-id/${dimension.id}`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(200);
            expect(res.body).toEqual(dto);
        });

        test('Get a dimension returns 404 if an invalid ID is given', async () => {
            const res = await request(app)
                .get(`/dataset/${dataset1Id}/dimension/by-id/INVALID-ID`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
            expect(res.body).toEqual({ error: 'Dimension id is invalid or missing' });
        });

        test('Get a dimension returns 404 if a non-existant ID is given', async () => {
            const res = await request(app)
                .get(`/dataset/${dataset1Id}/dimension/by-id/8B9434D1-4807-41CD-8E81-228769671A07`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
        });
    });

    describe('Get revision metadata endpoints', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}`);
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Get a revision returns 200', async () => {
            const revision = await Revision.findOne({
                where: { id: revision1Id },
                relations: ['createdBy', 'factTables', 'factTables.factTableInfo']
            });
            if (!revision) {
                throw new Error('Dataset not found');
            }
            const res = await request(app)
                .get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(200);
            const dto = await RevisionDTO.fromRevision(revision);
            expect(res.body).toEqual(dto);
        });

        test('Get revision returns 404 if an invalid ID is given', async () => {
            const res = await request(app)
                .get(`/dataset/${dataset1Id}/revision/by-id/INVALID-ID`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
            expect(res.body).toEqual({ error: 'Revision id is invalid or missing' });
        });

        test('Get revision returns 404 if a ID is given', async () => {
            const res = await request(app)
                .get(`/dataset/${dataset1Id}/revision/by-id/8B9434D1-4807-41CD-8E81-228769671A07`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
        });
    });

    describe('Get FileImport metadata endpoints', () => {
        test('returns 401 if no auth header is sent (JWT auth)', async () => {
            const res = await request(app).get(
                `/dataset/${dataset1Id}/revision/by-id/${revision1Id}/fact-table/by-id/${import1Id}/preview`
            );
            expect(res.status).toBe(401);
            expect(res.body).toEqual({});
        });

        test('Get import returns 200 with object', async () => {
            const factTable = await FactTableRepository.getFactTableById(dataset1Id, revision1Id, import1Id);
            if (!factTable) {
                throw new Error('Import not found');
            }
            const res = await request(app)
                .get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}/fact-table/by-id/${import1Id}`)
                .set(getAuthHeader(user));
            const expectedDTO = FactTableDTO.fromFactTable(factTable);
            expect(res.status).toBe(200);
            expect(res.body).toEqual(expectedDTO);
        });

        test('Get import returns 404 if given an invalid ID', async () => {
            const res = await request(app)
                .get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}/fact-table/by-id/IN-VALID-ID`)
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
            expect(res.body).toEqual({ error: 'Import id is invalid or missing' });
        });

        test('Get import returns 404 if given a missing ID', async () => {
            const res = await request(app)
                .get(
                    `/dataset/${dataset1Id}/revision/by-id/${revision1Id}/fact-table/by-id/8B9434D1-4807-41CD-8E81-228769671A07`
                )
                .set(getAuthHeader(user));
            expect(res.status).toBe(404);
        });

        describe('Getting a raw file out of a file import', () => {
            test('Get file from a revision and import returns 200 and complete file data if stored in the Data Lake', async () => {
                const testFile2 = path.resolve(__dirname, `sample-files/csv/test-data-2.csv`);
                const testFileStream = fs.createReadStream(testFile2);
                const testFile2Buffer = fs.readFileSync(testFile2);
                DataLakeService.prototype.getFileStream = jest.fn().mockReturnValue(Promise.resolve(testFileStream));

                const res = await request(app)
                    .get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}/fact-table/by-id/${import1Id}/raw`)
                    .set(getAuthHeader(user));
                expect(res.status).toBe(200);
                expect(res.text).toEqual(testFile2Buffer.toString());
            });

            test('Get file from a revision and import returns 500 if an error with the Data Lake occurs', async () => {
                DataLakeService.prototype.getFileStream = jest.fn().mockRejectedValue(Error('Unknown Data Lake Error'));
                const res = await request(app)
                    .get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}/fact-table/by-id/${import1Id}/raw`)
                    .set(getAuthHeader(user));
                expect(res.status).toBe(500);
                expect(res.body).toEqual({
                    status: 500,
                    errors: [
                        {
                            field: 'csv',
                            message: [
                                {
                                    lang: Locale.English,
                                    message: t('errors.download_from_datalake', { lng: Locale.English })
                                },
                                {
                                    lang: Locale.Welsh,
                                    message: t('errors.download_from_datalake', { lng: Locale.Welsh })
                                }
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
