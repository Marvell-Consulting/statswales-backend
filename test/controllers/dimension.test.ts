import { Request, Response, NextFunction } from 'express';
import { Readable } from 'node:stream';

import { UnknownException } from '../../src/exceptions/unknown.exception';
import { Dimension } from '../../src/entities/dataset/dimension';
import { DimensionType } from '../../src/enums/dimension-type';
import { uuidV4 } from '../../src/utils/uuid';

// Mock logger
jest.mock('../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    trace: jest.fn()
  }
}));

// Mock blob storage
jest.mock('../../src/services/blob-storage', () => {
  return function BlobStorage() {
    return {
      getContainerClient: jest.fn().mockReturnValue({
        createIfNotExists: jest.fn().mockResolvedValue(true)
      })
    };
  };
});

// Mock DatasetRepository & relation presets
const mockDatasetGetById = jest.fn();
jest.mock('../../src/repositories/dataset', () => ({
  DatasetRepository: {
    getById: (...args: unknown[]) => mockDatasetGetById(...args)
  }
}));

// Mock DimensionDTO
const mockFromDimension = jest.fn();
jest.mock('../../src/dtos/dimension-dto', () => ({
  DimensionDTO: {
    fromDimension: (...args: unknown[]) => mockFromDimension(...args)
  }
}));

// Mock DimensionMetadataDTO
jest.mock('../../src/dtos/dimension-metadata-dto', () => ({
  DimensionMetadataDTO: {
    fromDimensionMetadata: jest.fn()
  }
}));

// Mock LookupTableDTO
const mockFromLookupTable = jest.fn();
jest.mock('../../src/dtos/lookup-table-dto', () => ({
  LookupTableDTO: {
    fromLookupTable: (...args: unknown[]) => mockFromLookupTable(...args)
  }
}));

// Mock virus-scanner
const mockUploadAvScan = jest.fn();
const mockCleanupTmpFile = jest.fn();
jest.mock('../../src/services/virus-scanner', () => ({
  uploadAvScan: (...args: unknown[]) => mockUploadAvScan(...args),
  cleanupTmpFile: (...args: unknown[]) => mockCleanupTmpFile(...args)
}));

// Mock dimension-processor
const mockGetFactTableColumnPreview = jest.fn();
const mockGetDimensionPreview = jest.fn();
const mockCreateAndValidateDateDimension = jest.fn();
const mockSetupTextDimension = jest.fn();
const mockValidateNumericDimension = jest.fn();
jest.mock('../../src/services/dimension-processor', () => ({
  getFactTableColumnPreview: (...args: unknown[]) => mockGetFactTableColumnPreview(...args),
  getDimensionPreview: (...args: unknown[]) => mockGetDimensionPreview(...args),
  createAndValidateDateDimension: (...args: unknown[]) => mockCreateAndValidateDateDimension(...args),
  setupTextDimension: (...args: unknown[]) => mockSetupTextDimension(...args),
  validateNumericDimension: (...args: unknown[]) => mockValidateNumericDimension(...args)
}));

// Mock incoming-file-processor
const mockValidateAndUpload = jest.fn();
jest.mock('../../src/services/incoming-file-processor', () => ({
  validateAndUpload: (...args: unknown[]) => mockValidateAndUpload(...args)
}));

// Mock lookup-table-handler
const mockValidateLookupTable = jest.fn();
jest.mock('../../src/services/lookup-table-handler', () => ({
  validateLookupTable: (...args: unknown[]) => mockValidateLookupTable(...args)
}));

// Mock cube-builder
jest.mock('../../src/services/cube-builder', () => ({
  createAllCubeFiles: jest.fn().mockResolvedValue(undefined)
}));

// Mock view-error-generators
const mockViewErrorGenerators = jest.fn();
jest.mock('../../src/utils/view-error-generators', () => ({
  viewErrorGenerators: (...args: unknown[]) => mockViewErrorGenerators(...args)
}));

// Mock update-revision-tasks
const mockUpdateRevisionTasks = jest.fn();
jest.mock('../../src/utils/update-revision-tasks', () => ({
  updateRevisionTasks: (...args: unknown[]) => mockUpdateRevisionTasks(...args)
}));

// Mock get-file-service
const mockGetFileService = jest.fn();
jest.mock('../../src/utils/get-file-service', () => ({
  getFileService: (...args: unknown[]) => mockGetFileService(...args)
}));

// Mock Dimension entity static methods
const mockFindOneByOrFail = jest.fn();
const mockDimensionSave = jest.fn();
jest.mock('../../src/entities/dataset/dimension', () => ({
  Dimension: {
    findOneByOrFail: (...args: unknown[]) => mockFindOneByOrFail(...args)
  }
}));

// Mock DimensionMetadata entity
jest.mock('../../src/entities/dataset/dimension-metadata', () => ({
  DimensionMetadata: jest.fn().mockImplementation(() => ({
    save: jest.fn().mockResolvedValue(undefined)
  }))
}));

// Mock LookupTable entity
jest.mock('../../src/entities/dataset/lookup-table', () => ({
  LookupTable: jest.fn()
}));

// Mock i18next
jest.mock('i18next', () => ({
  t: jest.fn((key: string) => key)
}));

import {
  getDimensionInfo,
  resetDimension,
  sendDimensionPreview,
  attachLookupTableToDimension,
  updateDimension,
  updateDimensionMetadata,
  getDimensionLookupTableInfo,
  downloadDimensionLookupTable
} from '../../src/controllers/dimension';

function createMockDimension(overrides: Record<string, unknown> = {}) {
  return {
    id: uuidV4(),
    datasetId: uuidV4(),
    type: DimensionType.Raw,
    extractor: null,
    joinColumn: null,
    factTableColumn: 'col1',
    isSliceDimension: false,
    lookupTable: null,
    lookuptable: null,
    metadata: [],
    dataset: { id: uuidV4() },
    save: jest.fn().mockResolvedValue(undefined),
    ...overrides
  };
}

function createMockDataset(id?: string) {
  return {
    id: id || uuidV4(),
    draftRevision: {
      id: uuidV4(),
      revisionIndex: 1,
      tasks: null,
      dataTable: { dataTableDescriptions: [] },
      save: jest.fn().mockResolvedValue(undefined)
    },
    factTable: [{ columnName: 'col1', columnType: 'dimension' }]
  };
}

function createMockRequest(overrides: Partial<Request> = {}): Request {
  return {
    params: {},
    query: {},
    body: {},
    language: 'en',
    user: {
      id: uuidV4(),
      name: 'test-user'
    },
    fileService: {
      loadStream: jest.fn(),
      delete: jest.fn(),
      deleteDirectory: jest.fn()
    },
    ...overrides
  } as unknown as Request;
}

function createMockResponse(overrides: Partial<Response> = {}): Response {
  const res = {
    locals: {},
    json: jest.fn(),
    status: jest.fn(),
    end: jest.fn(),
    writeHead: jest.fn(),
    headersSent: false,
    pipe: jest.fn(),
    ...overrides
  } as unknown as Response;
  (res.status as jest.Mock).mockReturnValue(res);
  (res.json as jest.Mock).mockReturnValue(res);
  return res;
}

describe('Dimension controller', () => {
  let mockNext: jest.MockedFunction<NextFunction>;

  beforeEach(() => {
    jest.clearAllMocks();
    mockNext = jest.fn();
  });

  describe('getDimensionInfo', () => {
    it('should return DimensionDTO from res.locals.dimension', async () => {
      const dimension = createMockDimension();
      const mockDto = { id: dimension.id, type: DimensionType.Raw };
      mockFromDimension.mockReturnValue(mockDto);

      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension } });

      await getDimensionInfo(req, res);

      expect(mockFromDimension).toHaveBeenCalledWith(dimension);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });
  });

  describe('resetDimension', () => {
    it('should reset type to Raw and return 202 with updated dimension', async () => {
      const dimension = createMockDimension({ type: DimensionType.Text, extractor: { type: 'text' } });
      const updatedDimension = { ...dimension, type: DimensionType.Raw, extractor: null };
      const mockDto = { id: dimension.id, type: DimensionType.Raw };

      mockFindOneByOrFail.mockResolvedValue(updatedDimension);
      mockFromDimension.mockReturnValue(mockDto);

      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension } });

      await resetDimension(req, res);

      expect(dimension.type).toBe(DimensionType.Raw);
      expect(dimension.extractor).toBeNull();
      expect(dimension.save).toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(202);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should remove lookup table file and entity when present', async () => {
      const mockFileService = { delete: jest.fn().mockResolvedValue(undefined) };
      mockGetFileService.mockReturnValue(mockFileService);
      const lookupTable = {
        filename: 'lookup.csv',
        remove: jest.fn().mockResolvedValue(undefined)
      };
      const dimension = createMockDimension({
        lookuptable: lookupTable,
        lookupTable,
        dataset: { id: 'dataset-1' }
      });
      const updatedDimension = { ...dimension, type: DimensionType.Raw };
      mockFindOneByOrFail.mockResolvedValue(updatedDimension);
      mockFromDimension.mockReturnValue({ id: dimension.id });

      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension } });

      await resetDimension(req, res);

      expect(mockFileService.delete).toHaveBeenCalledWith('lookup.csv', 'dataset-1');
      expect(lookupTable.remove).toHaveBeenCalled();
    });

    it('should return updated dimension even when no lookup table', async () => {
      const dimension = createMockDimension();
      const mockDto = { id: dimension.id };
      mockFindOneByOrFail.mockResolvedValue(dimension);
      mockFromDimension.mockReturnValue(mockDto);

      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension } });

      await resetDimension(req, res);

      expect(res.status).toHaveBeenCalledWith(202);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });
  });

  describe('sendDimensionPreview', () => {
    it('should call next with UnknownException when no draft revision', async () => {
      const dataset = createMockDataset();
      dataset.draftRevision = null as any;
      mockDatasetGetById.mockResolvedValue(dataset);

      const dimension = createMockDimension();
      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension, datasetId: dataset.id } });

      await sendDimensionPreview(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });

    it('should return raw fact table column preview when dimension type is Raw', async () => {
      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);
      const previewData = { dataset: {}, current_page: 1, headers: [], data: [] };
      mockGetFactTableColumnPreview.mockResolvedValue(previewData);

      const dimension = createMockDimension({ type: DimensionType.Raw });
      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension, datasetId: dataset.id } });

      await sendDimensionPreview(req, res, mockNext);

      expect(mockGetFactTableColumnPreview).toHaveBeenCalledWith(dataset, dataset.draftRevision.id, 'col1');
      expect(res.json).toHaveBeenCalledWith(previewData);
    });

    it('should return dimension preview when dimension has extractor', async () => {
      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);
      const previewData = { dataset: {}, current_page: 1, headers: [], data: [] };
      mockGetDimensionPreview.mockResolvedValue(previewData);

      const dimension = createMockDimension({ type: DimensionType.Text, extractor: { type: 'text' } });
      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension, datasetId: dataset.id } });

      await sendDimensionPreview(req, res, mockNext);

      expect(mockGetDimensionPreview).toHaveBeenCalledWith(dataset, dimension, 'en');
      expect(res.json).toHaveBeenCalledWith(previewData);
    });

    it('should set dimension type to Raw when lookup table not updated in tasks', async () => {
      const dimensionId = uuidV4();
      const dataset = createMockDataset();
      dataset.draftRevision.tasks = {
        dimensions: [{ id: dimensionId, lookupTableUpdated: false }],
        measure: undefined
      };
      mockDatasetGetById.mockResolvedValue(dataset);
      const previewData = { dataset: {}, current_page: 1, headers: [], data: [] };
      mockGetFactTableColumnPreview.mockResolvedValue(previewData);

      const dimension = createMockDimension({ id: dimensionId, type: DimensionType.LookupTable, extractor: {} });
      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension, datasetId: dataset.id } });

      await sendDimensionPreview(req, res, mockNext);

      // When lookupTableUpdated is false, dimension type should be reset to Raw
      // and raw preview path taken
      expect(mockGetFactTableColumnPreview).toHaveBeenCalled();
      expect(res.json).toHaveBeenCalledWith(previewData);
    });

    it('should return 500 status when preview has errors', async () => {
      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);
      const errorPreview = { errors: [{ field: 'test', message: 'error' }] };
      mockGetFactTableColumnPreview.mockResolvedValue(errorPreview);

      const dimension = createMockDimension({ type: DimensionType.Raw });
      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension, datasetId: dataset.id } });

      await sendDimensionPreview(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(500);
      expect(res.json).toHaveBeenCalledWith(errorPreview);
    });
  });

  describe('attachLookupTableToDimension', () => {
    it('should pass upload error to next', async () => {
      const uploadError = new Error('upload failed');
      mockUploadAvScan.mockRejectedValue(uploadError);

      const req = createMockRequest();
      const res = createMockResponse({
        locals: { datasetId: uuidV4(), dimension: createMockDimension() }
      });

      await attachLookupTableToDimension(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(uploadError);
    });

    it('should call next with UnknownException when no draft revision', async () => {
      const tmpFile = { path: '/tmp/file.csv', originalname: 'file.csv', mimetype: 'text/csv' };
      mockUploadAvScan.mockResolvedValue(tmpFile);

      const dataset = createMockDataset();
      dataset.draftRevision = null as any;
      mockDatasetGetById.mockResolvedValue(dataset);

      const req = createMockRequest();
      const res = createMockResponse({
        locals: { datasetId: dataset.id, dimension: createMockDimension() }
      });

      await attachLookupTableToDimension(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });

    it('should return validation error status when validateLookupTable returns error', async () => {
      const tmpFile = { path: '/tmp/file.csv', originalname: 'file.csv', mimetype: 'text/csv' };
      mockUploadAvScan.mockResolvedValue(tmpFile);

      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);

      const dataTable = { id: 'dt-1' };
      mockValidateAndUpload.mockResolvedValue(dataTable);

      const validationError = { status: 422, errors: [{ field: 'col', message: 'invalid' }] };
      mockValidateLookupTable.mockResolvedValue(validationError);

      const req = createMockRequest();
      const res = createMockResponse({
        locals: { datasetId: dataset.id, dimension: createMockDimension() }
      });

      await attachLookupTableToDimension(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(422);
      expect(res.json).toHaveBeenCalledWith(validationError);
    });

    it('should return result with build_id on success and clean up tmp file', async () => {
      const tmpFile = { path: '/tmp/file.csv', originalname: 'file.csv', mimetype: 'text/csv' };
      mockUploadAvScan.mockResolvedValue(tmpFile);

      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);

      const dataTable = { id: 'dt-1' };
      mockValidateAndUpload.mockResolvedValue(dataTable);

      const result = { dataset: {}, current_page: 1, headers: [], data: [] } as any;
      mockValidateLookupTable.mockResolvedValue(result);
      mockUpdateRevisionTasks.mockResolvedValue(undefined);

      const dimension = createMockDimension();
      const req = createMockRequest();
      const res = createMockResponse({
        locals: { datasetId: dataset.id, dimension }
      });

      await attachLookupTableToDimension(req, res, mockNext);

      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({ extension: { build_id: expect.any(String) } })
      );
      expect(mockCleanupTmpFile).toHaveBeenCalledWith(tmpFile);
      expect(mockUpdateRevisionTasks).toHaveBeenCalledWith(dataset, dimension.id, 'dimension');
    });
  });

  describe('updateDimension', () => {
    it('should call next with UnknownException when no draft revision', async () => {
      const dataset = createMockDataset();
      dataset.draftRevision = null as any;
      mockDatasetGetById.mockResolvedValue(dataset);

      const dimension = createMockDimension();
      const req = createMockRequest({ body: { dimension_type: DimensionType.Text } });
      const res = createMockResponse({
        locals: { dimension, datasetId: dataset.id }
      });

      await updateDimension(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });

    it('should handle Date type via createAndValidateDateDimension', async () => {
      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);
      const previewData = { dataset: {}, current_page: 1, headers: [], data: [] } as any;
      mockCreateAndValidateDateDimension.mockResolvedValue(previewData);
      mockUpdateRevisionTasks.mockResolvedValue(undefined);

      const dimension = createMockDimension();
      const req = createMockRequest({ body: { dimension_type: DimensionType.Date } });
      const res = createMockResponse({
        locals: { dimension, datasetId: dataset.id }
      });

      await updateDimension(req, res, mockNext);

      expect(mockCreateAndValidateDateDimension).toHaveBeenCalledWith(
        { dimension_type: DimensionType.Date },
        dataset,
        dimension,
        'en'
      );
      expect(res.status).toHaveBeenCalledWith(202);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({ extension: { build_id: expect.any(String) } })
      );
    });

    it('should handle Text type via setupTextDimension + getFactTableColumnPreview', async () => {
      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);
      mockSetupTextDimension.mockResolvedValue(undefined);
      const previewData = { dataset: {}, current_page: 1, headers: [], data: [] } as any;
      mockGetFactTableColumnPreview.mockResolvedValue(previewData);
      mockUpdateRevisionTasks.mockResolvedValue(undefined);

      const dimension = createMockDimension();
      const req = createMockRequest({ body: { dimension_type: DimensionType.Text } });
      const res = createMockResponse({
        locals: { dimension, datasetId: dataset.id }
      });

      await updateDimension(req, res, mockNext);

      expect(mockSetupTextDimension).toHaveBeenCalledWith(dimension);
      expect(mockGetFactTableColumnPreview).toHaveBeenCalledWith(
        dataset,
        dataset.draftRevision.id,
        dimension.factTableColumn
      );
      expect(res.status).toHaveBeenCalledWith(202);
    });

    it('should handle Numeric type via validateNumericDimension', async () => {
      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);
      const previewData = { dataset: {}, current_page: 1, headers: [], data: [] } as any;
      mockValidateNumericDimension.mockResolvedValue(previewData);
      mockUpdateRevisionTasks.mockResolvedValue(undefined);

      const dimension = createMockDimension();
      const req = createMockRequest({ body: { dimension_type: DimensionType.Numeric } });
      const res = createMockResponse({
        locals: { dimension, datasetId: dataset.id }
      });

      await updateDimension(req, res, mockNext);

      expect(mockValidateNumericDimension).toHaveBeenCalledWith(
        { dimension_type: DimensionType.Numeric },
        dataset,
        dimension
      );
      expect(res.status).toHaveBeenCalledWith(202);
    });

    it('should return 400 error for LookupTable type', async () => {
      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);
      const errorResult = { status: 400, errors: [{ field: 'dimension_type' }] };
      mockViewErrorGenerators.mockReturnValue(errorResult);

      const dimension = createMockDimension();
      const req = createMockRequest({ body: { dimension_type: DimensionType.LookupTable } });
      const res = createMockResponse({
        locals: { dimension, datasetId: dataset.id }
      });

      await updateDimension(req, res, mockNext);

      expect(mockViewErrorGenerators).toHaveBeenCalledWith(
        400,
        dataset.id,
        'dimension_type',
        'errors.dimension_validation.lookup_not_supported',
        {}
      );
      expect(res.status).toHaveBeenCalledWith(400);
      expect(res.json).toHaveBeenCalledWith(errorResult);
    });

    it('should return error status for unknown dimension type', async () => {
      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);
      const errorResult = { status: 400, errors: [{ field: 'dimension_type' }] };
      mockViewErrorGenerators.mockReturnValue(errorResult);

      const dimension = createMockDimension();
      const req = createMockRequest({ body: { dimension_type: 'unknown_type' } });
      const res = createMockResponse({
        locals: { dimension, datasetId: dataset.id }
      });

      await updateDimension(req, res, mockNext);

      expect(mockViewErrorGenerators).toHaveBeenCalledWith(
        400,
        dataset.id,
        'dimension_type',
        'errors.dimension_validation.unknown_type',
        {}
      );
      expect(res.status).toHaveBeenCalledWith(400);
    });
  });

  describe('updateDimensionMetadata', () => {
    it('should update existing metadata and return 202 with dimension + build_id', async () => {
      const dimensionId = uuidV4();
      const existingMetadata = {
        language: 'en',
        name: 'Old Name',
        notes: 'Old Notes',
        save: jest.fn().mockResolvedValue(undefined)
      };
      const dimension = createMockDimension({
        id: dimensionId,
        metadata: [existingMetadata]
      });

      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);

      const updatedDimension = { ...dimension };
      mockFindOneByOrFail.mockResolvedValue(updatedDimension);
      mockFromDimension.mockReturnValue({ id: dimensionId });
      mockUpdateRevisionTasks.mockResolvedValue(undefined);

      const req = createMockRequest({
        body: { language: 'en', name: 'New Name', notes: 'New Notes' }
      });
      const res = createMockResponse({
        locals: { dimension, datasetId: dataset.id }
      });

      await updateDimensionMetadata(req, res);

      expect(existingMetadata.name).toBe('New Name');
      expect(existingMetadata.notes).toBe('New Notes');
      expect(existingMetadata.save).toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(202);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({
          dimension: { id: dimensionId },
          build_id: expect.any(String)
        })
      );
    });

    it('should create new metadata entry when language not found', async () => {
      const dimensionId = uuidV4();
      const dimension = createMockDimension({
        id: dimensionId,
        metadata: []
      });

      const dataset = createMockDataset();
      mockDatasetGetById.mockResolvedValue(dataset);

      const updatedDimension = { ...dimension };
      mockFindOneByOrFail.mockResolvedValue(updatedDimension);
      mockFromDimension.mockReturnValue({ id: dimensionId });
      mockUpdateRevisionTasks.mockResolvedValue(undefined);

      const req = createMockRequest({
        body: { language: 'cy', name: 'Welsh Name' }
      });
      const res = createMockResponse({
        locals: { dimension, datasetId: dataset.id }
      });

      await updateDimensionMetadata(req, res);

      expect(res.status).toHaveBeenCalledWith(202);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({
          dimension: { id: dimensionId },
          build_id: expect.any(String)
        })
      );
    });
  });

  describe('getDimensionLookupTableInfo', () => {
    it('should return 404 when no lookup table', async () => {
      const dimension = createMockDimension({ lookupTable: null });
      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension } });

      await getDimensionLookupTableInfo(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
      expect(res.json).toHaveBeenCalledWith({ message: 'No lookup table found' });
    });

    it('should return LookupTableDTO when lookup table exists', async () => {
      const lookupTable = { id: 'lt-1', filename: 'lookup.csv' };
      const dimension = createMockDimension({ lookupTable });
      const mockDto = { id: 'lt-1', filename: 'lookup.csv' };
      mockFromLookupTable.mockReturnValue(mockDto);

      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension } });

      await getDimensionLookupTableInfo(req, res);

      expect(mockFromLookupTable).toHaveBeenCalledWith(lookupTable);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });
  });

  describe('downloadDimensionLookupTable', () => {
    it('should return 404 when no lookup table', async () => {
      const dimension = createMockDimension({ lookupTable: null });
      const dataset = createMockDataset();

      const req = createMockRequest();
      const res = createMockResponse({ locals: { dimension, dataset } });

      await downloadDimensionLookupTable(req, res);

      expect(res.status).toHaveBeenCalledWith(404);
      expect(res.json).toHaveBeenCalledWith({ message: 'No lookup table found' });
    });

    it('should stream file with correct headers on success', async () => {
      const lookupTable = {
        filename: 'stored-file.csv',
        originalFilename: 'my-lookup.csv',
        mimeType: 'text/csv'
      };
      const dimension = createMockDimension({ lookupTable });
      const dataset = createMockDataset();

      const mockStream = new Readable({ read() {} });
      mockStream.pipe = jest.fn();
      mockStream.on = jest.fn().mockReturnThis();

      const req = createMockRequest();
      (req as any).fileService.loadStream.mockResolvedValue(mockStream);
      const res = createMockResponse({ locals: { dimension, dataset } });

      await downloadDimensionLookupTable(req, res);

      expect((req as any).fileService.loadStream).toHaveBeenCalledWith('stored-file.csv', dataset.id);
      expect(res.writeHead).toHaveBeenCalledWith(200, {
        'Content-Type': 'text/csv',
        'Content-Disposition': 'attachment; filename=my-lookup.csv'
      });
      expect(mockStream.pipe).toHaveBeenCalledWith(res);
    });

    it('should return 500 when file stream load fails', async () => {
      const lookupTable = {
        filename: 'stored-file.csv',
        originalFilename: 'my-lookup.csv',
        mimeType: 'text/csv'
      };
      const dimension = createMockDimension({ lookupTable });
      const dataset = createMockDataset();

      const req = createMockRequest();
      (req as any).fileService.loadStream.mockRejectedValue(new Error('blob error'));
      const res = createMockResponse({ locals: { dimension, dataset } });

      await downloadDimensionLookupTable(req, res);

      expect(res.status).toHaveBeenCalledWith(500);
      expect(res.json).toHaveBeenCalledWith({ message: 'An error occurred trying to load the file' });
    });
  });
});
