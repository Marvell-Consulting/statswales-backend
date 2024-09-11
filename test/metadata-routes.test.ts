import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import { BlobStorageService } from '../src/controllers/blob-storage';
import app, { initDb } from '../src/app';
import { Dataset } from '../src/entities/dataset';
import { Revision } from '../src/entities/revision';
import { Dimension } from '../src/entities/dimension';
import { FileImport } from '../src/entities/file-import';
import { User } from '../src/entities/user';
import { DatasetDTO, DimensionDTO, ImportDTO, RevisionDTO } from '../src/dtos/dataset-dto';
import DatabaseManager from '../src/db/database-manager';

import { createFullDataset, createSmallDataset } from "./helpers/test-helper";
import { getTestUser } from './helpers/get-user';
import { getAuthHeader } from './helpers/auth-header';

DataLakeService.prototype.listFiles = jest
    .fn()
    .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

BlobStorageService.prototype.uploadFile = jest.fn();

DataLakeService.prototype.uploadFile = jest.fn();

const dataset1Id = 'BDC40218-AF89-424B-B86E-D21710BC92F1'.toLowerCase();
const revision1Id = '85F0E416-8BD1-4946-9E2C-1C958897C6EF'.toLowerCase();
const import1Id = 'FA07BE9D-3495-432D-8C1F-D0FC6DAAE359'.toLowerCase();
const user: User = getTestUser('test', 'user');

describe('API Endpoints for viewing dataset objects', () => {
    let dbManager: DatabaseManager;
    beforeAll(async () => {
        dbManager = await initDb();
        await user.save();
        await createFullDataset(dataset1Id, revision1Id, import1Id, user);
    });

    describe('List all datasets', () => {
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
    });

    afterAll(async () => {
        await dbManager.getDataSource().dropDatabase();
        await dbManager.getDataSource().destroy();
    });
});
