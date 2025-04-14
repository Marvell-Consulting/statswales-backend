import path from 'node:path';
import * as fs from 'node:fs';

import request from 'supertest';
import { addYears, subYears } from 'date-fns';

import app from '../../src/app';
import { initDb } from '../../src/db/init';
import DatabaseManager from '../../src/db/database-manager';
import { initPassport } from '../../src/middleware/passport-auth';
import { Dataset } from '../../src/entities/dataset/dataset';
import { t } from '../../src/middleware/translation';
import { DatasetDTO } from '../../src/dtos/dataset-dto';
import { DataTableDto } from '../../src/dtos/data-table-dto';
import { User } from '../../src/entities/user/user';
import { SourceAssignmentDTO } from '../../src/dtos/source-assignment-dto';
import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
import { Revision } from '../../src/entities/dataset/revision';
import { Locale } from '../../src/enums/locale';
import { DatasetRepository, withDraftAndMetadata } from '../../src/repositories/dataset';
import { DataTable } from '../../src/entities/dataset/data-table';
import { DatasetService } from '../../src/services/dataset';
import { logger } from '../../src/utils/logger';
import { RevisionRepository } from '../../src/repositories/revision';
import { RevisionMetadataDTO } from '../../src/dtos/revistion-metadata-dto';

import { createFullDataset, createSmallDataset } from '../helpers/test-helper';
import { getTestUser } from '../helpers/get-test-user';
import { getAuthHeader } from '../helpers/auth-header';
import BlobStorage from '../../src/services/blob-storage';
import { MAX_PAGE_SIZE, MIN_PAGE_SIZE } from '../../src/validators/preview-validator';
import { UserGroup } from '../../src/entities/user/user-group';
import { GroupRole } from '../../src/enums/group-role';
import { UserGroupRole } from '../../src/entities/user/user-group-role';

jest.mock('../../src/services/blob-storage');

BlobStorage.prototype.listFiles = jest
  .fn()
  .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

BlobStorage.prototype.saveBuffer = jest.fn();

const dataset1Id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
const revision1Id = '85f0e416-8bd1-4946-9e2c-1c958897c6ef';
const import1Id = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
const user: User = getTestUser('test', 'user');

let userGroup: UserGroup;

let datasetService: DatasetService;

describe('API Endpoints', () => {
  let dbManager: DatabaseManager;
  beforeAll(async () => {
    try {
      dbManager = await initDb();
      await initPassport(dbManager.getDataSource());

      userGroup = UserGroup.create({
        metadata: [
          { name: 'Test', language: Locale.EnglishGb },
          { name: 'Test CY', language: Locale.WelshGb }
        ]
      });
      await userGroup.save();
      user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
      await user.save();
      await createFullDataset(dataset1Id, revision1Id, import1Id, user);
      datasetService = new DatasetService(Locale.EnglishGb);
    } catch (error) {
      logger.error(error, 'Could not initialise test database');
      await dbManager.getDataSource().dropDatabase();
      await dbManager.getDataSource().destroy();
      process.exit(1);
    }
  });

  test('Return true test', async () => {
    const dataset1 = await Dataset.findOneBy({ id: dataset1Id });
    if (!dataset1) {
      throw new Error('Dataset not found');
    }
    const dto = DatasetDTO.fromDataset(dataset1);
    expect(dto).toBeInstanceOf(DatasetDTO);
  });

  describe('Step 1 - initial title and file upload', () => {
    test('returns 401 if no auth header is sent (JWT auth)', async () => {
      const res = await request(app).post('/dataset').query({ title: 'My test datatset' });
      expect(res.status).toBe(401);
      expect(res.body).toEqual({});
    });

    test('Creates and returns a dataset with a title', async () => {
      const data = { title: 'My test datatset', user_group_id: userGroup.id };
      const res = await request(app).post('/dataset').set(getAuthHeader(user)).send(data);
      expect(res.status).toBe(201);
      const dataset = await DatasetRepository.getById(res.body.id, withDraftAndMetadata);
      expect(res.body).toEqual(DatasetDTO.fromDataset(dataset));
    });

    test('Upload returns 400 if no file attached', async () => {
      const dataset = await datasetService.createNew('Test Dataset 1', userGroup.id, user);
      const res = await request(app).post(`/dataset/${dataset.id}/data`).set(getAuthHeader(user));
      expect(res.status).toBe(400);
      expect(res.body).toEqual({ error: 'No CSV data provided' });
      await Dataset.remove(dataset);
    });

    test('Upload returns 201 if a file is attached', async () => {
      const dataset = await datasetService.createNew('Test Dataset 2', userGroup.id, user);
      const csvFile = path.resolve(__dirname, `../sample-files/csv/sure-start-short.csv`);
      const res = await request(app)
        .post(`/dataset/${dataset.id}/data`)
        .set(getAuthHeader(user))
        .attach('csv', csvFile);

      const datasetWithUpload = await DatasetRepository.getById(dataset.id);
      const datasetDTO = DatasetDTO.fromDataset(datasetWithUpload);
      expect(res.status).toBe(201);
      expect(res.body).toEqual(datasetDTO);
      await Dataset.remove(dataset);
    });
  });

  describe('Step 2 - Get a preview of the uploaded file with a View Object', () => {
    test('Get file preview returns 400 if page_number is too high', async () => {
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
      const testFile2 = path.resolve(__dirname, `../sample-files/csv/sure-start-short.csv`);
      const testFile2Buffer = fs.readFileSync(testFile2);
      BlobStorage.prototype.loadBuffer = jest.fn().mockReturnValue(testFile2Buffer);
      const res = await request(app)
        .get(`/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/data-table/preview`)
        .set(getAuthHeader(user))
        .query({ page_number: 20 });
      expect(res.status).toBe(400);
      expect(res.body).toEqual({
        status: 400,
        dataset_id: testDatasetId,
        errors: [
          {
            field: 'page_number',
            user_message: [
              {
                lang: Locale.English,
                message: t('errors.page_number_to_high', { lng: Locale.English, page_number: 1 })
              },
              {
                lang: Locale.Welsh,
                message: t('errors.page_number_to_high', { lng: Locale.Welsh, page_number: 1 })
              }
            ],
            message: {
              key: 'errors.page_number_to_high',
              params: { page_number: 1 }
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
      const testFile2 = path.resolve(__dirname, `../sample-files/csv/sure-start-short.csv`);
      const testFile2Buffer = fs.readFileSync(testFile2);
      BlobStorage.prototype.loadBuffer = jest.fn().mockReturnValue(testFile2Buffer);

      const res = await request(app)
        .get(`/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/data-table/preview`)
        .set(getAuthHeader(user))
        .query({ page_size: 1000 });
      expect(res.status).toBe(400);
      expect(res.body).toEqual({
        status: 400,
        dataset_id: testDatasetId,
        errors: [
          {
            field: 'page_size',
            user_message: [
              {
                lang: Locale.English,
                message: t('errors.page_size', {
                  lng: Locale.English,
                  max_page_size: MAX_PAGE_SIZE,
                  min_page_size: MIN_PAGE_SIZE
                })
              },
              {
                lang: Locale.Welsh,
                message: t('errors.page_size', {
                  lng: Locale.Welsh,
                  max_page_size: MAX_PAGE_SIZE,
                  min_page_size: MIN_PAGE_SIZE
                })
              }
            ],
            message: {
              key: 'errors.page_size',
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
      const testFile2 = path.resolve(__dirname, `../sample-files/csv/sure-start-short.csv`);
      const testFile2Buffer = fs.readFileSync(testFile2);
      BlobStorage.prototype.loadBuffer = jest.fn().mockReturnValue(testFile2Buffer);

      const res = await request(app)
        .get(`/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/data-table/preview`)
        .set(getAuthHeader(user))
        .query({ page_size: 1 });
      expect(res.status).toBe(400);
      expect(res.body).toEqual({
        status: 400,
        dataset_id: testDatasetId,
        errors: [
          {
            field: 'page_size',
            user_message: [
              {
                lang: Locale.English,
                message: t('errors.page_size', {
                  lng: Locale.English,
                  max_page_size: MAX_PAGE_SIZE,
                  min_page_size: MIN_PAGE_SIZE
                })
              },
              {
                lang: Locale.Welsh,
                message: t('errors.page_size', {
                  lng: Locale.Welsh,
                  max_page_size: MAX_PAGE_SIZE,
                  min_page_size: MIN_PAGE_SIZE
                })
              }
            ],
            message: {
              key: 'errors.page_size',
              params: { max_page_size: MAX_PAGE_SIZE, min_page_size: MIN_PAGE_SIZE }
            }
          }
        ]
      });
    });

    test('Get preview of an import returns 200 and correct page data', async () => {
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);

      const testFile2 = path.resolve(__dirname, `../sample-files/csv/sure-start-short.csv`);
      const testFile1Buffer = fs.readFileSync(testFile2);
      BlobStorage.prototype.loadBuffer = jest.fn().mockReturnValue(testFile1Buffer.toString());

      const res = await request(app)
        .get(`/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/data-table/preview`)
        .set(getAuthHeader(user))
        .query({ page_number: 1, page_size: 100 });

      expect(res.status).toBe(200);
      expect(res.body.current_page).toBe(1);
      expect(res.body.total_pages).toBe(1);
      expect(res.body.page_size).toBe(100);
      expect(res.body.headers).toEqual([
        { index: -1, name: 'int_line_number', source_type: 'line_number' },
        { index: 0, name: 'YearCode', source_type: 'unknown' },
        { index: 1, name: 'AreaCode', source_type: 'unknown' },
        { index: 2, name: 'Data', source_type: 'data_values' },
        { index: 3, name: 'RowRef', source_type: 'unknown' },
        { index: 4, name: 'Measure', source_type: 'unknown' },
        { index: 5, name: 'NoteCodes', source_type: 'note_codes' }
      ]);
      expect(res.body.data[0]).toEqual([1, 202223, 512, 1.442546584, 2, 2, null]);
      expect(res.body.data[23]).toEqual([24, 202122, 596, 137527, 1, 1, 't']);
    });

    test('Get preview of an import returns 500 if a file storage error occurs', async () => {
      BlobStorage.prototype.loadBuffer = jest.fn().mockRejectedValue(new Error('A Data Lake error occurred'));

      const res = await request(app)
        .get(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}/data-table/preview`)
        .set(getAuthHeader(user))
        .query({ page_number: 2, page_size: 100 });
      expect(res.status).toBe(500);
      expect(res.body).toEqual({
        status: 500,
        errors: [
          {
            field: 'csv',
            user_message: [
              {
                lang: Locale.English,
                message: t('errors.datalake.failed_to_fetch_file', { lng: Locale.English })
              },
              { lang: Locale.Welsh, message: t('errors.datalake.failed_to_fetch_file', { lng: Locale.Welsh }) }
            ],
            message: { key: 'errors.datalake.failed_to_fetch_file', params: {} }
          }
        ],
        dataset_id: dataset1Id,
        extension: {}
      });
    });

    test('Get preview of an import returns 404 when a non-existant import is requested', async () => {
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
      const dataTable = await DataTable.findOneOrFail({ where: { id: testFileImportId } });
      await dataTable.remove();

      const res = await request(app)
        .get(`/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/data-table/preview`)
        .set(getAuthHeader(user));
      expect(res.status).toBe(404);
      expect(res.body).toEqual({ error: 'errors.no_data_table' });
    });
  });

  describe('Step 2b - Unhappy path of the user uploading the wrong file', () => {
    test('Returns 200 when the user requests to delete the import stored in the file store', async () => {
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
      BlobStorage.prototype.delete = jest.fn().mockReturnValue(true);

      const res = await request(app)
        .delete(`/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/data-table`)
        .set(getAuthHeader(user));

      expect(res.status).toBe(200);

      const updatedDataset = await DatasetRepository.getById(testDatasetId);

      if (!updatedDataset) {
        throw new Error('Dataset not found');
      }

      const dto = DatasetDTO.fromDataset(updatedDataset);
      expect(res.body).toEqual(dto);

      const revision = await RevisionRepository.findOne({
        where: { id: testRevisionId },
        relations: { dataTable: true }
      });

      if (!revision) {
        throw new Error('Revision not found');
      }

      expect(revision.dataTable).toBe(null);
    });

    test('Upload returns 400 if no file attached', async () => {
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);

      const fileImport = await DataTable.findOneBy({ id: testFileImportId });
      if (!fileImport) {
        throw new Error('File Import not found');
      }
      await fileImport.remove();

      const res = await request(app)
        .post(`/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/data-table`)
        .set(getAuthHeader(user));

      expect(res.status).toBe(400);
      expect(res.body).toEqual({ error: 'No CSV data provided' });
    });

    test('Upload returns 201 if a file is attached', async () => {
      BlobStorage.prototype.saveBuffer = jest.fn().mockReturnValue({});
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);

      const dataTable = await DataTable.findOneBy({ id: testFileImportId });
      if (!dataTable) {
        throw new Error('Data table not found');
      }
      await dataTable.remove();

      const revision = await Revision.findOne({ where: { id: testRevisionId }, relations: ['dataTable'] });
      if (!revision) {
        expect(revision).not.toBeNull();
        return;
      }

      expect(revision.dataTable).toBe(null);
      const csvFile = path.resolve(__dirname, `../sample-files/csv/sure-start-short.csv`);

      const res = await request(app)
        .post(`/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/data-table`)
        .set(getAuthHeader(user))
        .attach('csv', csvFile);

      const updatedRevision = await Revision.findOne({
        where: { id: testRevisionId },
        relations: ['dataset', 'dataTable']
      });

      if (!updatedRevision) {
        expect(updatedRevision).not.toBeNull();
        return;
      }

      const dataset = await DatasetRepository.getById(testDatasetId);
      const datasetDTO = DatasetDTO.fromDataset(dataset);
      expect(res.status).toBe(201);
      expect(res.body).toEqual(datasetDTO);
      await Dataset.remove(dataset);
    });

    test('Upload returns 500 if an error occurs with file storage', async () => {
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);

      const fileImport = await DataTable.findOneBy({ id: testFileImportId });
      if (!fileImport) {
        throw new Error('File Import not found');
      }

      await fileImport.remove();

      BlobStorage.prototype.saveBuffer = jest.fn().mockImplementation(() => {
        throw new Error('Test error');
      });

      const csvFile = path.resolve(__dirname, `../sample-files/csv/sure-start-short.csv`);
      const res = await request(app)
        .post(`/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/data-table`)
        .set(getAuthHeader(user))
        .attach('csv', csvFile);
      expect(res.status).toBe(500);
      expect(res.body).toEqual({ error: 'errors.file_validation.datalake_upload_error' });
    });
  });

  describe('Step 3 - Confirming the datafile', () => {
    test('Returns 200 with an import dto listing the new sources which have been created', async () => {
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createFullDataset(testDatasetId, testRevisionId, testFileImportId, user);
      const res = await request(app)
        .patch(`/dataset/${testDatasetId}/revision/by-id/${testRevisionId}/data-table/confirm`)
        .set(getAuthHeader(user));
      const postRunFileImport = await DataTable.findOne({
        where: { id: testFileImportId },
        relations: ['dataTableDescriptions']
      });
      if (!postRunFileImport) {
        throw new Error('Import not found');
      }
      expect(postRunFileImport.dataTableDescriptions.length).toBe(6);
      const dto = DataTableDto.fromDataTable(postRunFileImport);
      expect(res.status).toBe(200);
      expect(res.body).toEqual(dto);
    });

    test('Returns 200 with an import dto listing the no additional sources are created if sources are already present', async () => {
      const res = await request(app)
        .patch(`/dataset/${dataset1Id}/revision/by-id/${revision1Id}/data-table/confirm`)
        .set(getAuthHeader(user));
      const postRunFileImport = await DataTable.findOne({
        where: { id: import1Id },
        relations: ['dataTableDescriptions']
      });
      if (!postRunFileImport) {
        throw new Error('Import not found');
      }
      expect(postRunFileImport.dataTableDescriptions.length).toBe(6);
      const dto = DataTableDto.fromDataTable(postRunFileImport);
      expect(res.status).toBe(200);
      expect(res.body).toEqual(dto);
    });
  });

  describe('Step 4 - Create dimensions', () => {
    test('Create dimensions from user supplied JSON returns 200 and updated dataset with dimensions attached', async () => {
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);

      const testFile2 = path.resolve(__dirname, `../sample-files/csv/sure-start-short.csv`);
      const testFile2Buffer = fs.readFileSync(testFile2);
      BlobStorage.prototype.loadBuffer = jest.fn().mockReturnValue(testFile2Buffer);
      BlobStorage.prototype.saveBuffer = jest.fn().mockReturnValue({});

      const postProcessedImport = await DataTable.findOne({
        where: { id: testFileImportId },
        relations: ['dataTableDescriptions']
      });

      if (!postProcessedImport) {
        throw new Error('Import not found');
      }

      const sources = postProcessedImport.dataTableDescriptions;
      const sourceAssignment: SourceAssignmentDTO[] = sources.map((source, index) => {
        switch (source.columnName) {
          case 'YearCode':
            return {
              column_index: index,
              column_name: source.columnName,
              column_type: FactTableColumnType.Dimension
            };
          case 'AreaCode':
            return {
              column_index: index,
              column_name: source.columnName,
              column_type: FactTableColumnType.Dimension
            };
          case 'Data':
            return {
              column_index: index,
              column_name: source.columnName,
              column_type: FactTableColumnType.DataValues
            };
          case 'RowRef':
            return {
              column_index: index,
              column_name: source.columnName,
              column_type: FactTableColumnType.Dimension
            };
          case 'Measure':
            return {
              column_index: index,
              column_name: source.columnName,
              column_type: FactTableColumnType.Measure
            };
          case 'NoteCodes':
            return {
              column_index: index,
              column_name: source.columnName,
              column_type: FactTableColumnType.NoteCodes
            };
          default:
            return {
              column_index: index,
              column_name: source.columnName,
              column_type: FactTableColumnType.Ignore
            };
        }
      });
      const res = await request(app)
        .patch(`/dataset/${testDatasetId}/sources`)
        .send(sourceAssignment)
        .set(getAuthHeader(user));

      expect(res.status).toBe(200);
      const updatedDataset = await DatasetRepository.getById(testDatasetId, { dimensions: true });
      if (!updatedDataset) {
        throw new Error('Dataset not found');
      }
      const dimensions = updatedDataset.dimensions;
      expect(dimensions.length).toBe(3);
    });

    test('Create dimensions from user supplied JSON returns 400 if the body is empty', async () => {
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
      const res = await request(app).patch(`/dataset/${testDatasetId}/sources`).send().set(getAuthHeader(user));
      expect(res.status).toBe(400);
      expect(res.body).toEqual({ error: 'Could not assign source types to import' });
    });

    test('Create dimensions from user supplied JSON returns 400 if there is more than one set of Data Values', async () => {
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
      const postProcessedImport = await DataTable.findOne({
        where: { id: testFileImportId },
        relations: ['dataTableDescriptions']
      });
      if (!postProcessedImport) {
        throw new Error('Import not found');
      }
      const sourceAssignment: SourceAssignmentDTO[] = postProcessedImport.dataTableDescriptions.map(
        (factTableInfo, index) => {
          return {
            column_index: index,
            column_name: factTableInfo.columnName,
            column_type: FactTableColumnType.Dimension
          };
        }
      );
      sourceAssignment[0].column_type = FactTableColumnType.DataValues;
      sourceAssignment[1].column_type = FactTableColumnType.DataValues;
      const res = await request(app)
        .patch(`/dataset/${testDatasetId}/sources`)
        .send(sourceAssignment)
        .set(getAuthHeader(user));
      expect(res.status).toBe(400);

      const updatedDataset = await DatasetRepository.getById(testDatasetId, { dimensions: true });
      if (!updatedDataset) {
        throw new Error('Dataset not found');
      }
      expect(updatedDataset.dimensions.length).toBe(0);
      expect(res.body).toEqual({
        dataset_id: testDatasetId,
        errors: [
          {
            field: 'none',
            message: {
              key: 'errors.source_assignment.too_many_data_values'
            }
          }
        ],
        status: 400
      });
    });

    test('Create dimensions from user supplied JSON returns 400 if there is more than one set of Footnotes', async () => {
      const testDatasetId = crypto.randomUUID().toLowerCase();
      const testRevisionId = crypto.randomUUID().toLowerCase();
      const testFileImportId = crypto.randomUUID().toLowerCase();
      await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);
      const postProcessedImport = await DataTable.findOne({
        where: { id: testFileImportId },
        relations: ['dataTableDescriptions']
      });
      if (!postProcessedImport) {
        throw new Error('Import not found');
      }
      const sourceAssignment: SourceAssignmentDTO[] = postProcessedImport.dataTableDescriptions.map(
        (factTableInfo, index) => {
          return {
            column_index: index,
            column_name: factTableInfo.columnName,
            column_type: FactTableColumnType.Dimension
          };
        }
      );
      sourceAssignment[0].column_type = FactTableColumnType.NoteCodes;
      sourceAssignment[1].column_type = FactTableColumnType.NoteCodes;
      const res = await request(app)
        .patch(`/dataset/${testDatasetId}/sources`)
        .send(sourceAssignment)
        .set(getAuthHeader(user));

      expect(res.status).toBe(400);

      const updatedDataset = await DatasetRepository.getById(testDatasetId, { dimensions: true });
      if (!updatedDataset) {
        throw new Error('Dataset not found');
      }
      expect(updatedDataset.dimensions.length).toBe(0);
      expect(res.body).toEqual({
        dataset_id: testDatasetId,
        errors: [
          {
            field: 'none',
            message: {
              key: 'errors.source_assignment.too_many_footnotes'
            }
          }
        ],
        status: 400
      });
    });
  });

  describe('Metadata handling routes', () => {
    describe('Update title/description endpoint', () => {
      test('Can update the English title', async () => {
        const testDatasetId = crypto.randomUUID().toLowerCase();
        const testRevisionId = crypto.randomUUID().toLowerCase();
        const testFileImportId = crypto.randomUUID().toLowerCase();
        await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);

        const meta: RevisionMetadataDTO = {
          language: 'en-GB',
          title: 'Updated dataset title'
        };

        const res = await request(app).patch(`/dataset/${testDatasetId}/metadata`).send(meta).set(getAuthHeader(user));
        expect(res.status).toBe(201);

        const updatedDataset = await DatasetRepository.getById(testDatasetId, {
          draftRevision: { metadata: true }
        });

        const metaEN = updatedDataset.draftRevision?.metadata.find((meta) => meta.language.includes('en'));

        expect(metaEN?.title).toEqual(meta.title);
      });

      test('Can update the Welsh title', async () => {
        const testDatasetId = crypto.randomUUID().toLowerCase();
        const testRevisionId = crypto.randomUUID().toLowerCase();
        const testFileImportId = crypto.randomUUID().toLowerCase();
        await createSmallDataset(testDatasetId, testRevisionId, testFileImportId, user);

        const meta: RevisionMetadataDTO = {
          language: 'cy-GB',
          title: 'This should be a welsh title'
        };

        const res = await request(app).patch(`/dataset/${testDatasetId}/metadata`).send(meta).set(getAuthHeader(user));
        expect(res.status).toBe(201);

        const updatedDataset = await DatasetRepository.getById(testDatasetId, {
          draftRevision: { metadata: true }
        });

        const metaCY = updatedDataset.draftRevision?.metadata.find((meta) => meta.language.includes('cy'));

        expect(metaCY?.title).toEqual(meta.title);
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
    BlobStorage.prototype.deleteDirectory = jest.fn();
    const res = await request(app).delete(`/dataset/${datasetID}`).set(getAuthHeader(user));
    expect(res.status).toBe(202);
    const dataset = await Dataset.findOneBy({ id: datasetID });
    expect(dataset).toBeNull();
  });

  describe('Publishing', () => {
    describe('Schedule', () => {
      let datasetId: string;
      let revisionId: string;

      beforeEach(async () => {
        datasetId = crypto.randomUUID().toLowerCase();
        revisionId = crypto.randomUUID().toLowerCase();
        const factTableId = crypto.randomUUID().toLowerCase();
        await createSmallDataset(datasetId, revisionId, factTableId, user);
      });

      test('Set publish_at fails with 404 if revision id invalid', async () => {
        const invalidId = crypto.randomUUID().toLowerCase();
        const res = await request(app)
          .patch(`/dataset/${datasetId}/revision/by-id/${invalidId}/publish-at`)
          .send({ publish_at: addYears(new Date(), 1).toISOString() })
          .set(getAuthHeader(user));
        expect(res.status).toBe(404);
      });

      test('Set publish_at fails with 400 if revision is already approved', async () => {
        const revision = await Revision.findOneByOrFail({ id: revisionId });
        revision.approvedAt = new Date();
        await revision.save();

        const res = await request(app)
          .patch(`/dataset/${datasetId}/revision/by-id/${revisionId}/publish-at`)
          .send({ publish_at: addYears(new Date(), 1).toISOString() })
          .set(getAuthHeader(user));
        expect(res.status).toBe(400);
      });

      test('Set publish_at fails with 400 if date is invalid', async () => {
        const res = await request(app)
          .patch(`/dataset/${datasetId}/revision/by-id/${revisionId}/publish-at`)
          .send({ publish_at: 'not-a-date' })
          .set(getAuthHeader(user));
        expect(res.status).toBe(400);
      });

      test('Set publish_at fails with 400 if date is in the past', async () => {
        const res = await request(app)
          .patch(`/dataset/${datasetId}/revision/by-id/${revisionId}/publish-at`)
          .send({ publish_at: subYears(new Date(), 1).toISOString() })
          .set(getAuthHeader(user));
        expect(res.status).toBe(400);
      });

      test('Set publish_at succeeds if date is valid and in the future', async () => {
        const res = await request(app)
          .patch(`/dataset/${datasetId}/revision/by-id/${revisionId}/publish-at`)
          .send({ publish_at: addYears(new Date(), 1).toISOString() })
          .set(getAuthHeader(user));
        expect(res.status).toBe(201);
      });
    });
  });

  afterAll(async () => {
    await dbManager.getDataSource().dropDatabase();
    await dbManager.getDataSource().destroy();
  });
});
