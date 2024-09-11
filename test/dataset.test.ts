import path from 'path';
import * as fs from 'fs';
import { createHash } from 'crypto';

import request from 'supertest';

import { DataLakeService } from '../src/controllers/datalake';
import { BlobStorageService } from '../src/controllers/blob-storage';
import app, { initDb } from '../src/app';
import { ENGLISH, WELSH, i18next } from '../src/middleware/translation';
import { Dataset } from '../src/entities/dataset';
import { DatasetInfo } from '../src/entities/dataset-info';
import { Revision } from '../src/entities/revision';
import { Import } from '../src/entities/import';
import { CsvInfo } from '../src/entities/csv-info';
import { Source } from '../src/entities/source';
import { Dimension } from '../src/entities/dimension';
import { DimensionType } from '../src/enums/dimension-type';
import { DimensionInfo } from '../src/entities/dimension-info';
import { User } from '../src/entities/user';
import { DatasetDTO, DimensionDTO, RevisionDTO } from '../src/dtos/dataset-dto';
import { ViewErrDTO } from '../src/dtos/view-dto';
import { MAX_PAGE_SIZE, MIN_PAGE_SIZE } from '../src/controllers/csv-processor';
import DatabaseManager from '../src/db/database-manager';

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

        // First create a dataset
        const dataset1 = new Dataset();
        dataset1.id = dataset1Id;
        dataset1.createdBy = Promise.resolve(user);
        dataset1.live = new Date(Date.now());

        // Give it some info
        const datasetInfo1 = new DatasetInfo();
        datasetInfo1.dataset = Promise.resolve(dataset1);
        datasetInfo1.title = 'Test Dataset 1';
        datasetInfo1.description = 'I am the first test dataset';
        datasetInfo1.language = 'en-GB';
        dataset1.datasetInfo = Promise.resolve([datasetInfo1]);

        // At the sametime we also always create a first revision
        const revision1 = new Revision();
        revision1.id = revision1Id;
        revision1.dataset = Promise.resolve(dataset1);
        revision1.createdBy = Promise.resolve(user);
        revision1.revisionIndex = 1;
        dataset1.revisions = Promise.resolve([revision1]);

        // Attach an import e.g. a file to the revision
        const import1 = new Import();
        import1.revision = Promise.resolve(revision1);
        import1.id = import1Id;
        import1.filename = 'fa07be9d-3495-432d-8c1f-d0fc6daae359.csv';
        const testFile1 = path.resolve(__dirname, `./test-data-2.csv`);
        const testFile2Buffer = fs.readFileSync(testFile1);
        import1.hash = createHash('sha256').update(testFile2Buffer).digest('hex');

        // First is a draft import and a first upload so everything is in blob storage
        import1.location = 'BlobStorage';
        import1.type = 'Draft';
        import1.mimeType = 'text/csv';

        // Its a CSV file so we need to know how to parse it
        const csvInfo1 = new CsvInfo();
        csvInfo1.import = Promise.resolve(import1);
        csvInfo1.delimiter = ',';
        csvInfo1.quote = '"';
        csvInfo1.linebreak = '\n';
        import1.csvInfo = Promise.resolve([csvInfo1]);
        revision1.imports = Promise.resolve([import1]);
        await dataset1.save();

        // Create some sources for each of the columns in the CSV
        const sources: Source[] = [];
        const source1 = new Source();
        source1.import = Promise.resolve(import1);
        source1.revision = Promise.resolve(revision1);
        source1.csvField = 'ID';
        source1.columnIndex = 0;
        source1.action = 'ignore';
        sources.push(source1);

        const source2 = new Source();
        source2.import = Promise.resolve(import1);
        source2.revision = Promise.resolve(revision1);
        source2.csvField = 'Text';
        source2.columnIndex = 1;
        source2.action = 'create';
        sources.push(source2);

        const source3 = new Source();
        source3.import = Promise.resolve(import1);
        source3.revision = Promise.resolve(revision1);
        source3.csvField = 'Number';
        source3.columnIndex = 2;
        source3.action = 'create';
        sources.push(source3);

        const source4 = new Source();
        source4.import = Promise.resolve(import1);
        source4.revision = Promise.resolve(revision1);
        source4.csvField = 'Date';
        source4.columnIndex = 3;
        source4.action = 'create';
        sources.push(source4);

        import1.sources = Promise.resolve(sources);
        await import1.save();

        // Next create some dimensions
        const dimensions: Dimension[] = [];
        const dimension1 = new Dimension();
        dimension1.id = dimension1Id;
        dimension1.dataset = Promise.resolve(dataset1);
        dimension1.startRevision = Promise.resolve(revision1);
        dimension1.type = DimensionType.RAW;
        await dimension1.save();

        const dimension1Info = new DimensionInfo();
        dimension1Info.dimension = Promise.resolve(dimension1);
        dimension1Info.name = 'ID';
        dimension1Info.description = 'Unique identifier';
        dimension1Info.language = 'en-GB';
        dimension1.dimensionInfo = Promise.resolve([dimension1Info]);
        dimension1.sources = Promise.resolve([source1]);
        source1.dimension = Promise.resolve(dimension1);
        dimensions.push(dimension1);

        // Dimension 2
        const dimension2 = new Dimension();
        dimension2.id = '61d51f82-0771-4c90-849e-55ffa7a4d802';
        dimension2.dataset = Promise.resolve(dataset1);
        dimension2.startRevision = Promise.resolve(revision1);
        dimension2.type = DimensionType.TEXT;
        await dimension2.save();

        const dimension2Info = new DimensionInfo();
        dimension2Info.dimension = Promise.resolve(dimension2);
        dimension2Info.name = 'Text';
        dimension2Info.description = 'Sample text strings';
        dimension2Info.language = 'en-GB';
        dimension2.dimensionInfo = Promise.resolve([dimension2Info]);
        dimension2.sources = Promise.resolve([source2]);
        source2.dimension = Promise.resolve(dimension2);
        dimensions.push(dimension2);

        // Dimension 3
        const dimension3 = new Dimension();
        dimension3.id = 'f4d5b0f4-180e-4020-aad5-9300b673d92b';
        dimension3.dataset = Promise.resolve(dataset1);
        dimension3.startRevision = Promise.resolve(revision1);
        dimension3.type = DimensionType.NUMERIC;
        await dimension3.save();

        const dimension3Info = new DimensionInfo();
        dimension3Info.dimension = Promise.resolve(dimension3);
        dimension3Info.name = 'Value';
        dimension3Info.description = 'Sample numeric values';
        dimension3Info.language = 'en-GB';
        dimension3.dimensionInfo = Promise.resolve([dimension3Info]);
        dimension3.sources = Promise.resolve([source3]);
        source3.dimension = Promise.resolve(dimension3);
        dimensions.push(dimension3);

        // Dimension 4
        const dimension4 = new Dimension();
        dimension4.id = 'c24962f4-f395-40ef-b4dd-270e90e10972';
        dimension4.dataset = Promise.resolve(dataset1);
        dimension4.startRevision = Promise.resolve(revision1);
        dimension4.type = DimensionType.TIME_POINT;
        await dimension4.save();

        const dimension4Info = new DimensionInfo();
        dimension4Info.dimension = Promise.resolve(dimension4);
        dimension4Info.name = 'Date';
        dimension4Info.description = 'Sample date values';
        dimension4Info.language = 'en-GB';
        dimension4.dimensionInfo = Promise.resolve([dimension4Info]);
        dimension4.sources = Promise.resolve([source4]);
        source4.dimension = Promise.resolve(dimension4);
        dimensions.push(dimension4);
        dataset1.dimensions = Promise.resolve(dimensions);
        // await dataset1.save();
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
