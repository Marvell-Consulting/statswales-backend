import { Request, Response, NextFunction } from 'express';

import { BadRequestException } from '../../src/exceptions/bad-request.exception';
import { NotFoundException } from '../../src/exceptions/not-found.exception';
import { NotAllowedException } from '../../src/exceptions/not-allowed.exception';
import { UnknownException } from '../../src/exceptions/unknown.exception';
import { Dataset } from '../../src/entities/dataset/dataset';
import { Revision } from '../../src/entities/dataset/revision';
import { uuidV4 } from '../../src/utils/uuid';
import { CubeBuildType } from '../../src/enums/cube-build-type';
import { CubeBuildStatus } from '../../src/enums/cube-build-status';

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

// Mock RevisionRepository
const mockUpdatePublishDate = jest.fn();
jest.mock('../../src/repositories/revision', () => ({
  RevisionRepository: {
    updatePublishDate: (...args: unknown[]) => mockUpdatePublishDate(...args),
    save: jest.fn()
  }
}));

// Mock DatasetRepository
const mockDatasetGetById = jest.fn();
jest.mock('../../src/repositories/dataset', () => ({
  DatasetRepository: {
    getById: (...args: unknown[]) => mockDatasetGetById(...args)
  }
}));

// Mock BuildLogRepository
const mockGetByRevisionId = jest.fn();
jest.mock('../../src/repositories/build-log', () => ({
  BuildLogRepository: {
    getByRevisionId: (...args: unknown[]) => mockGetByRevisionId(...args)
  }
}));

// Mock DatasetDTO
const mockFromDataset = jest.fn();
jest.mock('../../src/dtos/dataset-dto', () => ({
  DatasetDTO: {
    fromDataset: (...args: unknown[]) => mockFromDataset(...args)
  }
}));

// Mock RevisionDTO
const mockFromRevision = jest.fn();
jest.mock('../../src/dtos/revision-dto', () => ({
  RevisionDTO: {
    fromRevision: (...args: unknown[]) => mockFromRevision(...args)
  }
}));

// Mock DataTable entity
jest.mock('../../src/entities/dataset/data-table', () => ({
  DataTable: {
    findOneOrFail: jest.fn(),
    getRepository: jest.fn().mockReturnValue({ remove: jest.fn() })
  }
}));

// Mock DataTableDto
const mockFromDataTable = jest.fn();
jest.mock('../../src/dtos/data-table-dto', () => ({
  DataTableDto: {
    fromDataTable: (...args: unknown[]) => mockFromDataTable(...args)
  }
}));

// Mock incoming-file-processor
jest.mock('../../src/services/incoming-file-processor', () => ({
  getFilePreview: jest.fn(),
  validateAndUpload: jest.fn()
}));

// Mock validators
const mockHasError = jest.fn();
jest.mock('../../src/validators', () => ({
  buildTypeValidator: jest.fn().mockReturnValue('buildTypeChain'),
  buildStatusValidator: jest.fn().mockReturnValue('buildStatusChain'),
  hasError: (...args: unknown[]) => mockHasError(...args)
}));

// Mock BuiltLogEntryDto
const mockFromBuildLogLite = jest.fn();
jest.mock('../../src/dtos/build-log', () => ({
  BuiltLogEntryDto: {
    fromBuildLogLite: (...args: unknown[]) => mockFromBuildLogLite(...args)
  }
}));

// Mock cube-builder
jest.mock('../../src/services/cube-builder', () => ({
  createAllCubeFiles: jest.fn()
}));

// Mock lookup-table-utils
jest.mock('../../src/utils/lookup-table-utils', () => ({
  bootstrapCubeBuildProcess: jest.fn()
}));

// Mock virus-scanner
jest.mock('../../src/services/virus-scanner', () => ({
  uploadAvScan: jest.fn(),
  cleanupTmpFile: jest.fn()
}));

// Mock consumer-view
jest.mock('../../src/services/consumer-view', () => ({
  createFrontendView: jest.fn(),
  createStreamingCSVFilteredView: jest.fn(),
  createStreamingExcelFilteredView: jest.fn(),
  createStreamingJSONFilteredView: jest.fn(),
  getFilters: jest.fn()
}));

// Mock revision service
jest.mock('../../src/services/revision', () => ({
  attachUpdateDataTableToRevision: jest.fn()
}));

// Mock performance-reporting
jest.mock('../../src/utils/performance-reporting', () => ({
  performanceReporting: jest.fn()
}));

import {
  updateRevisionPublicationDate,
  submitForPublication,
  withdrawFromPublication,
  deleteDraftRevision,
  getDataTable,
  getRevisionInfo,
  getDataTablePreview,
  removeFactTableFromRevision,
  getRevisionBuildLog
} from '../../src/controllers/revision';
import { DataTable } from '../../src/entities/dataset/data-table';
import { getFilePreview } from '../../src/services/incoming-file-processor';

function createMockDataset(id?: string): Dataset {
  const dataset = new Dataset();
  dataset.id = id || uuidV4();
  return dataset;
}

function createMockRevision(overrides: Partial<Revision> = {}): Revision {
  const rev = new Revision();
  rev.id = uuidV4();
  rev.revisionIndex = 1;
  rev.approvedAt = null;
  rev.publishAt = null;
  rev.unpublishedAt = null;
  rev.dataTableId = undefined;
  rev.dataTable = null;
  Object.assign(rev, overrides);
  return rev;
}

function createMockRequest(overrides: Partial<Request> = {}): Request {
  return {
    params: {},
    query: {},
    body: {},
    language: 'en',
    user: { id: uuidV4(), name: 'test-user' },
    datasetService: {
      getTasklistState: jest.fn(),
      getPendingPublishTask: jest.fn(),
      submitForPublication: jest.fn(),
      withdrawFromPublication: jest.fn(),
      deleteDraftRevision: jest.fn()
    },
    fileService: {
      delete: jest.fn()
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
    headersSent: false,
    ...overrides
  } as unknown as Response;
  // Chaining: status() and json() both return the response object
  (res.status as jest.Mock).mockReturnValue(res);
  (res.json as jest.Mock).mockReturnValue(res);
  return res;
}

describe('Revision controller', () => {
  let mockNext: jest.MockedFunction<NextFunction>;

  beforeEach(() => {
    jest.clearAllMocks();
    mockNext = jest.fn();
  });

  describe('updateRevisionPublicationDate', () => {
    it('should reject when revision already approved', async () => {
      const revision = createMockRevision({ approvedAt: new Date() });
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      await updateRevisionPublicationDate(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should reject when publish_at is missing', async () => {
      const revision = createMockRevision();
      const req = createMockRequest({ body: {} });
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      await updateRevisionPublicationDate(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should reject when publish_at is invalid', async () => {
      const revision = createMockRevision();
      const req = createMockRequest({ body: { publish_at: 'not-a-date' } });
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      await updateRevisionPublicationDate(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should reject when publish_at is in the past', async () => {
      const revision = createMockRevision();
      const pastDate = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
      const req = createMockRequest({ body: { publish_at: pastDate } });
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      await updateRevisionPublicationDate(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should update publish date and return 201 with dataset on success', async () => {
      const datasetId = uuidV4();
      const revision = createMockRevision();
      const futureDate = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
      const req = createMockRequest({ body: { publish_at: futureDate } });
      const res = createMockResponse({ locals: { datasetId, revision } });

      const mockDataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };
      mockDatasetGetById.mockResolvedValue(mockDataset);
      mockFromDataset.mockReturnValue(mockDto);
      mockUpdatePublishDate.mockResolvedValue(undefined);

      await updateRevisionPublicationDate(req, res, mockNext);

      expect(mockUpdatePublishDate).toHaveBeenCalledWith(revision, futureDate);
      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should call next with UnknownException on internal error', async () => {
      const revision = createMockRevision();
      const futureDate = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
      const req = createMockRequest({ body: { publish_at: futureDate } });
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      mockUpdatePublishDate.mockRejectedValue(new Error('db failure'));

      await updateRevisionPublicationDate(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('submitForPublication', () => {
    it('should reject when revision already approved', async () => {
      const revision = createMockRevision({ approvedAt: new Date() });
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      await submitForPublication(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should reject when canPublish is false', async () => {
      const revision = createMockRevision();
      const req = createMockRequest();
      (req as any).datasetService.getTasklistState.mockResolvedValue({ canPublish: false });
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      await submitForPublication(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should reject when pending publish task exists', async () => {
      const revision = createMockRevision();
      const req = createMockRequest();
      (req as any).datasetService.getTasklistState.mockResolvedValue({ canPublish: true });
      (req as any).datasetService.getPendingPublishTask.mockResolvedValue({ id: 'some-task' });
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      await submitForPublication(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should submit and return 201 with dataset on success', async () => {
      const datasetId = uuidV4();
      const revision = createMockRevision();
      const req = createMockRequest();
      (req as any).datasetService.getTasklistState.mockResolvedValue({ canPublish: true });
      (req as any).datasetService.getPendingPublishTask.mockResolvedValue(null);
      (req as any).datasetService.submitForPublication.mockResolvedValue(undefined);
      const res = createMockResponse({ locals: { datasetId, revision } });

      const mockDataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };
      mockDatasetGetById.mockResolvedValue(mockDataset);
      mockFromDataset.mockReturnValue(mockDto);

      await submitForPublication(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should pass errors through to next', async () => {
      const revision = createMockRevision();
      const req = createMockRequest();
      const testError = new Error('test error');
      (req as any).datasetService.getTasklistState.mockRejectedValue(testError);
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      await submitForPublication(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(testError);
    });
  });

  describe('withdrawFromPublication', () => {
    it('should withdraw and return 201 with updated dataset', async () => {
      const datasetId = uuidV4();
      const revision = createMockRevision();
      const req = createMockRequest();
      (req as any).datasetService.withdrawFromPublication.mockResolvedValue(undefined);
      const res = createMockResponse({ locals: { datasetId, revision } });

      const mockDataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };
      mockDatasetGetById.mockResolvedValue(mockDataset);
      mockFromDataset.mockReturnValue(mockDto);

      await withdrawFromPublication(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should pass errors to next', async () => {
      const revision = createMockRevision();
      const req = createMockRequest();
      const testError = new Error('withdraw failed');
      (req as any).datasetService.withdrawFromPublication.mockRejectedValue(testError);
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      await withdrawFromPublication(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(testError);
    });
  });

  describe('deleteDraftRevision', () => {
    it('should reject when revisionIndex !== 0', async () => {
      const revision = createMockRevision({ revisionIndex: 1 });
      const dataset = createMockDataset();
      const req = createMockRequest();
      const res = createMockResponse({ locals: { dataset, revision } });

      await deleteDraftRevision(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(NotAllowedException));
    });

    it('should delete and return 202 when revisionIndex === 0', async () => {
      const dataset = createMockDataset();
      const revision = createMockRevision({ revisionIndex: 0 });
      const req = createMockRequest();
      (req as any).datasetService.deleteDraftRevision.mockResolvedValue(undefined);
      const res = createMockResponse({ locals: { dataset, revision } });

      await deleteDraftRevision(req, res, mockNext);

      expect((req as any).datasetService.deleteDraftRevision).toHaveBeenCalledWith(dataset.id, revision.id);
      expect(res.status).toHaveBeenCalledWith(202);
      expect(res.end).toHaveBeenCalled();
    });

    it('should pass errors to next', async () => {
      const dataset = createMockDataset();
      const revision = createMockRevision({ revisionIndex: 0 });
      const req = createMockRequest();
      const testError = new Error('delete failed');
      (req as any).datasetService.deleteDraftRevision.mockRejectedValue(testError);
      const res = createMockResponse({ locals: { dataset, revision } });

      await deleteDraftRevision(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(testError);
    });
  });

  describe('getDataTable', () => {
    it('should throw NotFoundException when no dataTableId', async () => {
      const revision = createMockRevision({ dataTableId: undefined });
      const res = createMockResponse({ locals: { revision } });
      const req = createMockRequest();

      await expect(getDataTable(req, res, mockNext)).rejects.toThrow(NotFoundException);
    });

    it('should return DataTableDto on success', async () => {
      const dataTableId = uuidV4();
      const revision = createMockRevision({ dataTableId });
      const res = createMockResponse({ locals: { revision } });
      const req = createMockRequest();

      const mockDataTable = { id: dataTableId, dataTableDescriptions: [], revision: {} };
      (DataTable.findOneOrFail as jest.Mock).mockResolvedValue(mockDataTable);
      const mockDto = { id: dataTableId };
      mockFromDataTable.mockReturnValue(mockDto);

      await getDataTable(req, res, mockNext);

      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should call next with UnknownException on error', async () => {
      const dataTableId = uuidV4();
      const revision = createMockRevision({ dataTableId });
      const res = createMockResponse({ locals: { revision } });
      const req = createMockRequest();

      (DataTable.findOneOrFail as jest.Mock).mockRejectedValue(new Error('db error'));

      await getDataTable(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('getRevisionInfo', () => {
    it('should return RevisionDTO', async () => {
      const revision = createMockRevision();
      const req = createMockRequest();
      const res = createMockResponse({ locals: { revision } });

      const mockDto = { id: revision.id };
      mockFromRevision.mockReturnValue(mockDto);

      await getRevisionInfo(req, res);

      expect(mockFromRevision).toHaveBeenCalledWith(revision);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });
  });

  describe('getDataTablePreview', () => {
    it('should call next with NotFoundException when no dataTable', async () => {
      const revision = createMockRevision({ dataTable: null });
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      await getDataTablePreview(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundException));
    });

    it('should return preview with default pagination', async () => {
      const dataTable = { id: uuidV4() };
      const revision = createMockRevision({ dataTable: dataTable as any });
      const datasetId = uuidV4();
      const req = createMockRequest({ query: {} });
      const res = createMockResponse({ locals: { datasetId, revision } });

      const mockPreview = { headers: [], data: [] };
      (getFilePreview as jest.Mock).mockResolvedValue(mockPreview);

      await getDataTablePreview(req, res, mockNext);

      expect(getFilePreview).toHaveBeenCalledWith(datasetId, dataTable, 1, 100);
      expect(res.json).toHaveBeenCalledWith(mockPreview);
    });

    it('should set error status when preview returns ViewErrDTO', async () => {
      const dataTable = { id: uuidV4() };
      const revision = createMockRevision({ dataTable: dataTable as any });
      const datasetId = uuidV4();
      const req = createMockRequest({ query: {} });
      const res = createMockResponse({ locals: { datasetId, revision } });

      const mockErrPreview = { errors: ['something wrong'], status: 422 };
      (getFilePreview as jest.Mock).mockResolvedValue(mockErrPreview);

      await getDataTablePreview(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(422);
      expect(res.json).toHaveBeenCalledWith(mockErrPreview);
    });
  });

  describe('removeFactTableFromRevision', () => {
    it('should call next with NotFoundException when no dataTable', async () => {
      const revision = createMockRevision({ dataTable: null });
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: uuidV4(), revision } });

      await removeFactTableFromRevision(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundException));
    });

    it('should delete file and return updated dataset', async () => {
      const datasetId = uuidV4();
      const dataTable = { id: uuidV4(), filename: 'test.csv', remove: jest.fn() };
      const revision = createMockRevision({ dataTable: dataTable as any });
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId, revision } });

      const mockDataset = createMockDataset(datasetId);
      mockDataset.revisions = [{} as any, {} as any]; // more than 1 revision
      const mockUpdatedDataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };
      mockDatasetGetById
        .mockResolvedValueOnce(mockDataset) // first call for getById with factTable
        .mockResolvedValueOnce(mockUpdatedDataset); // second call for updated dataset
      mockFromDataset.mockReturnValue(mockDto);

      await removeFactTableFromRevision(req, res, mockNext);

      expect((req as any).fileService.delete).toHaveBeenCalledWith('test.csv', datasetId);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should call next with UnknownException on error', async () => {
      const datasetId = uuidV4();
      const dataTable = { id: uuidV4(), filename: 'test.csv', remove: jest.fn() };
      const revision = createMockRevision({ dataTable: dataTable as any });
      const req = createMockRequest();
      (req as any).fileService.delete.mockRejectedValue(new Error('delete failed'));
      const res = createMockResponse({ locals: { datasetId, revision } });

      await removeFactTableFromRevision(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('getRevisionBuildLog', () => {
    it('should return build logs with default pagination', async () => {
      const revision = createMockRevision();
      const req = createMockRequest({ query: {} });
      const res = createMockResponse({ locals: { revision } });

      mockHasError.mockResolvedValue(false);
      const mockLogs = [{ id: '1' }, { id: '2' }];
      mockGetByRevisionId.mockResolvedValue(mockLogs);
      mockFromBuildLogLite.mockImplementation((log: any) => ({ id: log.id }));

      await getRevisionBuildLog(req, res, mockNext);

      expect(mockGetByRevisionId).toHaveBeenCalledWith(revision.id, undefined, undefined, 30, 0);
      expect(res.status).toHaveBeenCalledWith(200);
    });

    it('should pass type/status filters to repository', async () => {
      const revision = createMockRevision();
      const req = createMockRequest({
        query: { type: CubeBuildType.FullCube, status: CubeBuildStatus.Completed }
      });
      const res = createMockResponse({ locals: { revision } });

      mockHasError.mockResolvedValue(false);
      mockGetByRevisionId.mockResolvedValue([]);

      await getRevisionBuildLog(req, res, mockNext);

      expect(mockGetByRevisionId).toHaveBeenCalledWith(
        revision.id,
        CubeBuildType.FullCube,
        CubeBuildStatus.Completed,
        30,
        0
      );
    });

    it('should reject invalid type', async () => {
      const revision = createMockRevision();
      const req = createMockRequest({ query: { type: 'invalid_type' } });
      const res = createMockResponse({ locals: { revision } });

      mockHasError.mockResolvedValueOnce(true); // type error

      await getRevisionBuildLog(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should reject invalid status', async () => {
      const revision = createMockRevision();
      const req = createMockRequest({ query: { status: 'invalid_status' } });
      const res = createMockResponse({ locals: { revision } });

      // type is not in query so hasError is only called once (for status)
      mockHasError.mockResolvedValueOnce(true);

      await getRevisionBuildLog(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });
  });
});
