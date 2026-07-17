import { Request, Response, NextFunction } from 'express';

import { NotFoundException } from '../../../src/exceptions/not-found.exception';
import { UnknownException } from '../../../src/exceptions/unknown.exception';
import { uuidV4 } from '../../../src/utils/uuid';

jest.mock('../../../src/utils/logger', () => ({
  logger: { debug: jest.fn(), info: jest.fn(), error: jest.fn(), warn: jest.fn(), trace: jest.fn() }
}));

const mockGetById = jest.fn();
jest.mock('../../../src/repositories/dataset', () => ({
  DatasetRepository: { getById: (...args: unknown[]) => mockGetById(...args) }
}));

const mockDatasetFindOneByOrFail = jest.fn();
jest.mock('../../../src/entities/dataset/dataset', () => ({
  Dataset: { findOneByOrFail: (...args: unknown[]) => mockDatasetFindOneByOrFail(...args) }
}));

const mockFromDataset = jest.fn();
jest.mock('../../../src/dtos/dataset-dto', () => ({
  DatasetDTO: { fromDataset: (...args: unknown[]) => mockFromDataset(...args) }
}));

const mockFromMeasure = jest.fn();
jest.mock('../../../src/dtos/measure-dto', () => ({
  MeasureDTO: { fromMeasure: (...args: unknown[]) => mockFromMeasure(...args) }
}));

const mockFromLookupTable = jest.fn();
jest.mock('../../../src/dtos/lookup-table-dto', () => ({
  LookupTableDTO: { fromLookupTable: (...args: unknown[]) => mockFromLookupTable(...args) }
}));

const mockFromDimensionMetadata = jest.fn();
jest.mock('../../../src/dtos/dimension-metadata-dto', () => ({
  DimensionMetadataDTO: { fromDimensionMetadata: (...args: unknown[]) => mockFromDimensionMetadata(...args) }
}));

const mockGetMeasurePreview = jest.fn();
const mockValidateMeasureLookupTable = jest.fn();
jest.mock('../../../src/services/measure-handler', () => ({
  getMeasurePreview: (...args: unknown[]) => mockGetMeasurePreview(...args),
  validateMeasureLookupTable: (...args: unknown[]) => mockValidateMeasureLookupTable(...args)
}));

const mockValidateAndUpload = jest.fn();
jest.mock('../../../src/services/incoming-file-processor', () => ({
  validateAndUpload: (...args: unknown[]) => mockValidateAndUpload(...args)
}));

const mockCreateAllCubeFiles = jest.fn();
jest.mock('../../../src/services/cube-builder', () => ({
  createAllCubeFiles: (...args: unknown[]) => mockCreateAllCubeFiles(...args)
}));

const mockUploadAvScan = jest.fn();
const mockCleanupTmpFile = jest.fn();
jest.mock('../../../src/services/virus-scanner', () => ({
  uploadAvScan: (...args: unknown[]) => mockUploadAvScan(...args),
  cleanupTmpFile: (...args: unknown[]) => mockCleanupTmpFile(...args)
}));

const mockUpdateRevisionTasks = jest.fn();
jest.mock('../../../src/services/revision', () => ({
  updateRevisionTasks: (...args: unknown[]) => mockUpdateRevisionTasks(...args)
}));

const mockStartBuild = jest.fn();
jest.mock('../../../src/entities/dataset/build-log', () => ({
  BuildLog: { startBuild: (...args: unknown[]) => mockStartBuild(...args) }
}));

const mockMeasureMetadataCtor = jest.fn();
jest.mock('../../../src/entities/dataset/measure-metadata', () => ({
  MeasureMetadata: jest.fn().mockImplementation(() => mockMeasureMetadataCtor())
}));

import {
  resetMeasure,
  getPreviewOfMeasure,
  updateMeasureMetadata,
  getMeasureInfo,
  getMeasureLookupTableInfo,
  downloadMeasureLookupTable,
  attachLookupTableToMeasure
} from '../../../src/controllers/measure';

function createMockRequest(overrides: Partial<Request> = {}): Request {
  return {
    params: {},
    query: {},
    body: {},
    language: 'en-GB',
    user: { id: uuidV4() },
    fileService: {
      delete: jest.fn().mockResolvedValue(undefined),
      loadStream: jest.fn()
    },
    ...overrides
  } as unknown as Request;
}

function createMockResponse(locals: Record<string, unknown> = {}): Response {
  const res = {
    locals: { datasetId: uuidV4(), ...locals },
    json: jest.fn(),
    status: jest.fn(),
    writeHead: jest.fn(),
    end: jest.fn()
  } as unknown as Response;
  (res.status as jest.Mock).mockReturnValue(res);
  (res.json as jest.Mock).mockReturnValue(res);
  return res;
}

describe('Measure controller', () => {
  let mockNext: jest.MockedFunction<NextFunction>;

  beforeEach(() => {
    jest.clearAllMocks();
    mockNext = jest.fn();
    // the controllers call createAllCubeFiles(...).catch(...), so the mock must return a promise
    mockCreateAllCubeFiles.mockResolvedValue(undefined);
  });

  describe('getMeasureInfo', () => {
    it('returns the measure DTO when a measure exists', async () => {
      const dataset = { measure: { id: uuidV4() } };
      mockGetById.mockResolvedValue(dataset);
      mockFromMeasure.mockReturnValue({ id: dataset.measure.id });

      const res = createMockResponse();
      await getMeasureInfo(createMockRequest(), res);

      expect(mockFromMeasure).toHaveBeenCalledWith(dataset.measure);
      expect(res.json).toHaveBeenCalledWith({ id: dataset.measure.id });
    });

    it('responds 404 when the dataset has no measure', async () => {
      mockGetById.mockResolvedValue({ measure: null });

      const res = createMockResponse();
      await getMeasureInfo(createMockRequest(), res);

      expect(res.status).toHaveBeenCalledWith(404);
      expect(res.json).toHaveBeenCalledWith({ message: 'No measure found' });
    });
  });

  describe('getMeasureLookupTableInfo', () => {
    it('returns the lookup table DTO when present', async () => {
      const lookupTable = { id: uuidV4() };
      mockGetById.mockResolvedValue({ measure: { lookupTable } });
      mockFromLookupTable.mockReturnValue({ id: lookupTable.id });

      const res = createMockResponse();
      await getMeasureLookupTableInfo(createMockRequest(), res);

      expect(res.json).toHaveBeenCalledWith({ id: lookupTable.id });
    });

    it('responds 404 when there is no lookup table', async () => {
      mockGetById.mockResolvedValue({ measure: { lookupTable: null } });

      const res = createMockResponse();
      await getMeasureLookupTableInfo(createMockRequest(), res);

      expect(res.status).toHaveBeenCalledWith(404);
      expect(res.json).toHaveBeenCalledWith({ message: 'No lookup table found' });
    });
  });

  describe('downloadMeasureLookupTable', () => {
    it('streams the lookup table file to the response', async () => {
      const lookupTable = { filename: 'lookup.csv', originalFilename: 'orig.csv', mimeType: 'text/csv' };
      const dataset = { id: uuidV4(), measure: { lookupTable } };
      mockGetById.mockResolvedValue(dataset);
      // a plain mock stream — the controller only calls pipe()/on(), so we don't need a real Readable
      const stream = { pipe: jest.fn(), on: jest.fn() };
      const req = createMockRequest();
      ((req as any).fileService.loadStream as jest.Mock).mockResolvedValue(stream);

      const res = createMockResponse();
      await downloadMeasureLookupTable(req, res);

      expect((req as any).fileService.loadStream).toHaveBeenCalledWith('lookup.csv', dataset.id);
      const [statusCode, headers] = (res.writeHead as jest.Mock).mock.calls[0];
      expect(statusCode).toBe(200);
      expect(headers['Content-Type']).toBe('text/csv');
      expect(stream.pipe).toHaveBeenCalledWith(res);
    });

    it('responds 404 when there is no lookup table', async () => {
      mockGetById.mockResolvedValue({ id: uuidV4(), measure: { lookupTable: null } });

      const res = createMockResponse();
      await downloadMeasureLookupTable(createMockRequest(), res);

      expect(res.status).toHaveBeenCalledWith(404);
      expect(res.json).toHaveBeenCalledWith({ message: 'No lookup table found' });
    });

    it('responds 500 when loading the file fails', async () => {
      const lookupTable = { filename: 'lookup.csv', mimeType: 'text/csv' };
      mockGetById.mockResolvedValue({ id: uuidV4(), measure: { lookupTable } });
      const req = createMockRequest();
      ((req as any).fileService.loadStream as jest.Mock).mockRejectedValue(new Error('data lake down'));

      const res = createMockResponse();
      await downloadMeasureLookupTable(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
      expect(res.json).toHaveBeenCalledWith({ message: 'An error occurred trying to load the file' });
    });
  });

  describe('getPreviewOfMeasure', () => {
    it('returns the measure preview', async () => {
      const dataset = { measure: { id: uuidV4() } };
      mockGetById.mockResolvedValue(dataset);
      mockGetMeasurePreview.mockResolvedValue({ rows: [] });

      const res = createMockResponse();
      await getPreviewOfMeasure(createMockRequest(), res, mockNext);

      expect(mockGetMeasurePreview).toHaveBeenCalledWith(dataset, 'en-gb');
      expect(res.json).toHaveBeenCalledWith({ rows: [] });
    });

    it('passes NotFound to next when there is no measure', async () => {
      mockGetById.mockResolvedValue({ measure: null });

      const res = createMockResponse();
      await getPreviewOfMeasure(createMockRequest(), res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(NotFoundException);
      expect(mockGetMeasurePreview).not.toHaveBeenCalled();
    });

    it('passes UnknownException to next when the preview throws', async () => {
      mockGetById.mockResolvedValue({ measure: { id: uuidV4() } });
      mockGetMeasurePreview.mockRejectedValue(new Error('boom'));

      const res = createMockResponse();
      await getPreviewOfMeasure(createMockRequest(), res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
    });
  });

  describe('resetMeasure', () => {
    it('clears the extractor, lookup table and measure info, rebuilds and returns the dataset', async () => {
      const lookupTableRemove = jest.fn().mockResolvedValue(undefined);
      const infoRemove = jest.fn().mockResolvedValue(undefined);
      const measureSave = jest.fn().mockResolvedValue(undefined);
      const dataset = {
        id: uuidV4(),
        draftRevision: { id: uuidV4() },
        measure: {
          extractor: { some: 'extractor' },
          lookup: { filename: 'lookup.csv' },
          lookupTable: { remove: lookupTableRemove },
          measureInfo: [{ remove: infoRemove }],
          joinColumn: 'col',
          save: measureSave
        }
      };
      const reloaded = { id: dataset.id };
      mockDatasetFindOneByOrFail.mockResolvedValue(reloaded);
      mockFromDataset.mockReturnValue({ id: dataset.id });

      const req = createMockRequest();
      const res = createMockResponse({ dataset });
      await resetMeasure(req, res, mockNext);

      expect(lookupTableRemove).toHaveBeenCalled();
      expect((req as any).fileService.delete).toHaveBeenCalledWith('lookup.csv', dataset.id);
      expect(infoRemove).toHaveBeenCalled();
      expect(measureSave).toHaveBeenCalled();
      expect(dataset.measure.extractor).toBeNull();
      expect(dataset.measure.joinColumn).toBeNull();
      expect(mockCreateAllCubeFiles).toHaveBeenCalledWith(dataset.id, dataset.draftRevision.id, (req as any).user?.id);
      expect(res.json).toHaveBeenCalledWith({ id: dataset.id });
    });

    it('passes NotFound to next when the dataset has no measure', async () => {
      const res = createMockResponse({ dataset: { measure: null } });
      await resetMeasure(createMockRequest(), res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(NotFoundException);
      expect(mockCreateAllCubeFiles).not.toHaveBeenCalled();
    });
  });

  describe('updateMeasureMetadata', () => {
    it('updates the matching language metadata, starts a build and returns 202', async () => {
      const metadataSave = jest.fn().mockResolvedValue({ id: uuidV4() });
      const existingMeta = { language: 'en-GB', save: metadataSave };
      const dataset = {
        id: uuidV4(),
        draftRevision: { id: uuidV4() },
        measure: { id: uuidV4(), metadata: [existingMeta] }
      };
      mockGetById.mockResolvedValue(dataset);
      mockStartBuild.mockResolvedValue({ id: 'build-1' });
      mockFromDimensionMetadata.mockReturnValue({ name: 'Updated' });

      const req = createMockRequest({ body: { language: 'en-GB', name: 'Updated', notes: 'note' } as never });
      const res = createMockResponse();
      await updateMeasureMetadata(req, res, mockNext);

      expect(metadataSave).toHaveBeenCalled();
      expect(mockUpdateRevisionTasks).toHaveBeenCalledWith(dataset, dataset.measure.id, 'measure');
      expect(res.status).toHaveBeenCalledWith(202);
      expect(res.json).toHaveBeenCalledWith({ dimension: { name: 'Updated' }, build_id: 'build-1' });
    });

    it('passes UnknownException to next when there is no draft revision', async () => {
      const dataset = { id: uuidV4(), draftRevision: null, measure: { id: uuidV4(), metadata: [] } };
      mockGetById.mockResolvedValue(dataset);

      const req = createMockRequest({ body: { language: 'en-GB', name: 'x' } as never });
      const res = createMockResponse();
      await updateMeasureMetadata(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
      expect(res.status).not.toHaveBeenCalledWith(202);
    });

    // Regression (#686): previously the handler called next(NotFoundException) without a `return`,
    // fell through to `measure.metadata.find(...)` and threw a TypeError on the null measure.
    // It now returns after the guard, like getPreviewOfMeasure / getMeasureInfo.
    it('forwards NotFound and does not throw when the dataset has no measure', async () => {
      const dataset = { id: uuidV4(), draftRevision: { id: uuidV4() }, measure: null };
      mockGetById.mockResolvedValue(dataset);

      const req = createMockRequest({ body: { language: 'en-GB', name: 'x' } as never });
      const res = createMockResponse();

      await expect(updateMeasureMetadata(req, res, mockNext)).resolves.toBeUndefined();
      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(NotFoundException);
      expect(res.json).not.toHaveBeenCalled();
    });
  });

  describe('attachLookupTableToMeasure', () => {
    it('forwards the av-scan error and never loads the dataset', async () => {
      const scanError = new Error('virus');
      mockUploadAvScan.mockRejectedValue(scanError);

      const res = createMockResponse();
      await attachLookupTableToMeasure(createMockRequest(), res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(scanError);
      expect(mockGetById).not.toHaveBeenCalled();
    });

    it('returns the validation error response without starting a build', async () => {
      mockUploadAvScan.mockResolvedValue({ path: '/tmp/lookup.csv' });
      mockGetById.mockResolvedValue({ id: uuidV4(), measure: { id: uuidV4() }, draftRevision: { id: uuidV4() } });
      mockValidateAndUpload.mockResolvedValue({ id: 'data-table' });
      mockValidateMeasureLookupTable.mockResolvedValue({ status: 400, errors: ['bad'] });

      const req = createMockRequest({ body: {} as never });
      const res = createMockResponse();
      await attachLookupTableToMeasure(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(400);
      expect(res.json).toHaveBeenCalledWith({ status: 400, errors: ['bad'] });
      expect(mockStartBuild).not.toHaveBeenCalled();
      expect(mockCleanupTmpFile).toHaveBeenCalled();
    });

    it('rejects a malformed table matcher body before validating the lookup table', async () => {
      mockUploadAvScan.mockResolvedValue({ path: '/tmp/lookup.csv' });
      mockGetById.mockResolvedValue({ id: uuidV4(), measure: { id: uuidV4() }, draftRevision: { id: uuidV4() } });
      mockValidateAndUpload.mockResolvedValue({ id: 'data-table' });

      // description_columns must be a string[]; sending a plain string should fail DTO validation
      const req = createMockRequest({ body: { description_columns: 'description_en' } as never });
      const res = createMockResponse();
      await attachLookupTableToMeasure(req, res, mockNext);

      expect(mockValidateMeasureLookupTable).not.toHaveBeenCalled();
      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toMatchObject({ name: 'BadRequestException', status: 400 });
      expect(mockCleanupTmpFile).toHaveBeenCalled();
    });

    it('starts a build and returns the result when validation succeeds', async () => {
      const dataset = { id: uuidV4(), measure: { id: uuidV4() }, draftRevision: { id: uuidV4() } };
      mockUploadAvScan.mockResolvedValue({ path: '/tmp/lookup.csv' });
      mockGetById.mockResolvedValue(dataset);
      mockValidateAndUpload.mockResolvedValue({ id: 'data-table' });
      mockValidateMeasureLookupTable.mockResolvedValue({ extension: {} });
      mockStartBuild.mockResolvedValue({ id: 'build-9' });
      mockCreateAllCubeFiles.mockResolvedValue(undefined);

      const req = createMockRequest({ body: {} as never });
      const res = createMockResponse();
      await attachLookupTableToMeasure(req, res, mockNext);

      expect(mockUpdateRevisionTasks).toHaveBeenCalledWith(dataset, dataset.measure.id, 'measure');
      expect(mockStartBuild).toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.json).toHaveBeenCalledWith(expect.objectContaining({ extension: { build_id: 'build-9' } }));
      expect(mockCleanupTmpFile).toHaveBeenCalled();
    });
  });
});
