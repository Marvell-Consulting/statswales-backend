import { Request, Response, NextFunction } from 'express';

import { BadRequestException } from '../../src/exceptions/bad-request.exception';
import { NotFoundException } from '../../src/exceptions/not-found.exception';
import { NotAllowedException } from '../../src/exceptions/not-allowed.exception';
import { UnknownException } from '../../src/exceptions/unknown.exception';
import { Dataset } from '../../src/entities/dataset/dataset';
import { Revision } from '../../src/entities/dataset/revision';
import { uuidV4 } from '../../src/utils/uuid';
import { DatasetInclude } from '../../src/enums/dataset-include';
import { TaskAction } from '../../src/enums/task-action';
import { OutputFormats } from '../../src/enums/output-formats';

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
const mockDatasetDeleteById = jest.fn();
const mockDatasetListForUser = jest.fn();
const mockDatasetListAll = jest.fn();
jest.mock('../../src/repositories/dataset', () => ({
  DatasetRepository: {
    getById: (...args: unknown[]) => mockDatasetGetById(...args),
    deleteById: (...args: unknown[]) => mockDatasetDeleteById(...args),
    listForUser: (...args: unknown[]) => mockDatasetListForUser(...args),
    listAll: (...args: unknown[]) => mockDatasetListAll(...args)
  },
  withStandardPreview: { revisions: true },
  withDeveloperPreview: { factTable: true },
  withLatestRevision: { endRevision: true },
  withDraftAndDataTable: { draftRevision: { dataTable: true } },
  withDraftAndMetadata: { draftRevision: { metadata: true } },
  withDraftAndProviders: { draftRevision: { revisionProviders: true } },
  withDraftAndTopics: { draftRevision: { revisionTopics: true } },
  withDraftAndMeasure: { measure: true },
  withDimensions: { dimensions: true },
  withFactTable: { factTable: true }
}));

// Mock UserGroupRepository
const mockGetByIdWithOrganisation = jest.fn();
jest.mock('../../src/repositories/user-group', () => ({
  UserGroupRepository: {
    getByIdWithOrganisation: (...args: unknown[]) => mockGetByIdWithOrganisation(...args)
  }
}));

// Mock QueryStoreRepository
const mockQueryStoreGetByRequest = jest.fn();
const mockQueryStoreGetById = jest.fn();
jest.mock('../../src/repositories/query-store', () => ({
  QueryStoreRepository: {
    getByRequest: (...args: unknown[]) => mockQueryStoreGetByRequest(...args),
    getById: (...args: unknown[]) => mockQueryStoreGetById(...args)
  }
}));

// Mock BuildLogRepository
const mockGetAllActiveBulkBuilds = jest.fn();
jest.mock('../../src/repositories/build-log', () => ({
  BuildLogRepository: {
    getAllActiveBulkBuilds: (...args: unknown[]) => mockGetAllActiveBulkBuilds(...args)
  }
}));

// Mock RevisionRepository
const mockGetAllRevisionIds = jest.fn();
const mockGetAllDraftRevisionIds = jest.fn();
jest.mock('../../src/repositories/revision', () => ({
  RevisionRepository: {
    getAllRevisionIds: (...args: unknown[]) => mockGetAllRevisionIds(...args),
    getAllDraftRevisionIds: (...args: unknown[]) => mockGetAllDraftRevisionIds(...args)
  }
}));

// Mock DatasetDTO
const mockFromDataset = jest.fn();
jest.mock('../../src/dtos/dataset-dto', () => ({
  DatasetDTO: {
    fromDataset: (...args: unknown[]) => mockFromDataset(...args)
  }
}));

// Mock RevisionProviderDTO
const mockFromRevisionProvider = jest.fn();
jest.mock('../../src/dtos/revision-provider-dto', () => ({
  RevisionProviderDTO: {
    fromRevisionProvider: (...args: unknown[]) => mockFromRevisionProvider(...args)
  }
}));

// Mock TopicDTO
const mockFromTopic = jest.fn();
jest.mock('../../src/dtos/topic-dto', () => ({
  TopicDTO: {
    fromTopic: (...args: unknown[]) => mockFromTopic(...args)
  }
}));

// Mock TopicSelectionDTO (class needed by dtoValidator)
jest.mock('../../src/dtos/topic-selection-dto', () => ({
  TopicSelectionDTO: class TopicSelectionDTO {}
}));

// Mock FactTableColumnDto
const mockFromFactTableColumn = jest.fn();
jest.mock('../../src/dtos/fact-table-column-dto', () => ({
  FactTableColumnDto: {
    fromFactTableColumn: (...args: unknown[]) => mockFromFactTableColumn(...args)
  }
}));

// Mock EventLogDTO
const mockFromEventLog = jest.fn();
jest.mock('../../src/dtos/event-log-dto', () => ({
  EventLogDTO: {
    fromEventLog: (...args: unknown[]) => mockFromEventLog(...args)
  }
}));

// Mock PublisherDTO
const mockFromUserGroup = jest.fn();
jest.mock('../../src/dtos/publisher-dto', () => ({
  PublisherDTO: {
    fromUserGroup: (...args: unknown[]) => mockFromUserGroup(...args)
  }
}));

// Mock DataOptionsDTO
jest.mock('../../src/dtos/data-options-dto', () => ({
  DataOptionsDTO: class DataOptionsDTO {},
  DEFAULT_DATA_OPTIONS: { option: 'default' },
  FRONTEND_DATA_OPTIONS: { option: 'frontend' }
}));

// Mock dtoValidator / arrayValidator
const mockDtoValidator = jest.fn();
const mockArrayValidator = jest.fn();
jest.mock('../../src/validators/dto-validator', () => ({
  dtoValidator: (...args: unknown[]) => mockDtoValidator(...args),
  arrayValidator: (...args: unknown[]) => mockArrayValidator(...args)
}));

// Mock validators (hasError, titleValidator, userGroupIdValidator)
const mockHasError = jest.fn();
jest.mock('../../src/validators', () => ({
  hasError: (...args: unknown[]) => mockHasError(...args),
  titleValidator: jest.fn().mockReturnValue('titleChain'),
  userGroupIdValidator: jest.fn().mockReturnValue('groupIdChain')
}));

// Mock virus-scanner
const mockUploadAvScan = jest.fn();
const mockCleanupTmpFile = jest.fn();
jest.mock('../../src/services/virus-scanner', () => ({
  uploadAvScan: (...args: unknown[]) => mockUploadAvScan(...args),
  cleanupTmpFile: (...args: unknown[]) => mockCleanupTmpFile(...args)
}));

// Mock dimension-processor
const mockValidateSourceAssignment = jest.fn();
const mockCreateDimensionsFromSourceAssignment = jest.fn();
jest.mock('../../src/services/dimension-processor', () => ({
  validateSourceAssignment: (...args: unknown[]) => mockValidateSourceAssignment(...args),
  createDimensionsFromSourceAssignment: (...args: unknown[]) =>
    mockCreateDimensionsFromSourceAssignment(...args)
}));

// Mock fact-table-validator
const mockFactTableValidatorFromSource = jest.fn();
jest.mock('../../src/services/fact-table-validator', () => ({
  factTableValidatorFromSource: (...args: unknown[]) => mockFactTableValidatorFromSource(...args)
}));

// Mock cube-builder
jest.mock('../../src/services/cube-builder', () => ({
  createAllCubeFiles: jest.fn().mockResolvedValue(undefined)
}));

// Mock lookup-table-utils
jest.mock('../../src/utils/lookup-table-utils', () => ({
  bootstrapCubeBuildProcess: jest.fn()
}));

// Mock consumer-view-v2
const mockBuildDataQuery = jest.fn();
const mockSendFrontendView = jest.fn();
const mockSendCsv = jest.fn();
const mockSendExcel = jest.fn();
const mockSendJson = jest.fn();
jest.mock('../../src/services/consumer-view-v2', () => ({
  buildDataQuery: (...args: unknown[]) => mockBuildDataQuery(...args),
  sendFrontendView: (...args: unknown[]) => mockSendFrontendView(...args),
  sendCsv: (...args: unknown[]) => mockSendCsv(...args),
  sendExcel: (...args: unknown[]) => mockSendExcel(...args),
  sendJson: (...args: unknown[]) => mockSendJson(...args)
}));

// Mock parsePageOptions
const mockParsePageOptions = jest.fn();
jest.mock('../../src/utils/parse-page-options', () => ({
  parsePageOptions: (...args: unknown[]) => mockParsePageOptions(...args)
}));

// Mock TaskService
const mockRequestUnpublish = jest.fn();
const mockRequestArchive = jest.fn();
const mockRequestUnarchive = jest.fn();
jest.mock('../../src/services/task', () => ({
  TaskService: jest.fn().mockImplementation(() => ({
    requestUnpublish: (...args: unknown[]) => mockRequestUnpublish(...args),
    requestArchive: (...args: unknown[]) => mockRequestArchive(...args),
    requestUnarchive: (...args: unknown[]) => mockRequestUnarchive(...args)
  }))
}));

// Mock dbManager
jest.mock('../../src/db/database-manager', () => ({
  dbManager: {
    getCubeDataSource: jest.fn().mockReturnValue({
      createQueryRunner: jest.fn().mockReturnValue({
        query: jest.fn().mockResolvedValue(undefined),
        release: jest.fn().mockResolvedValue(undefined)
      })
    })
  }
}));

// Mock pg-format
jest.mock('@scaleleap/pg-format/lib/pg-format', () => ({
  format: jest.fn((...args: unknown[]) => String(args[0]))
}));

// Mock file-utils
const mockCollectFiles = jest.fn();
jest.mock('../../src/utils/file-utils', () => ({
  collectFiles: (...args: unknown[]) => mockCollectFiles(...args),
  addDirectoryToZip: jest.fn()
}));

// Mock sleep
jest.mock('../../src/utils/sleep', () => ({
  sleep: jest.fn().mockResolvedValue(undefined)
}));

// Mock BuildLog entity
const mockStartBuild = jest.fn();
jest.mock('../../src/entities/dataset/build-log', () => ({
  BuildLog: {
    startBuild: (...args: unknown[]) => mockStartBuild(...args),
    findOneOrFail: jest.fn().mockResolvedValue({
      status: 'completed',
      reload: jest.fn()
    })
  },
  CompleteStatus: ['completed', 'failed']
}));

// Mock RevisionMetadataDTO (class needed by dtoValidator)
jest.mock('../../src/dtos/revistion-metadata-dto', () => ({
  RevisionMetadataDTO: class RevisionMetadataDTO {}
}));

// Mock i18next
jest.mock('i18next', () => ({
  t: jest.fn((key: string) => key)
}));

import {
  listUserDatasets,
  listAllDatasets,
  getDatasetById,
  deleteDraftDatasetById,
  createDataset,
  uploadDataTable,
  generateFilterId,
  datasetPreview,
  sendFormattedResponse,
  updateMetadata,
  getTasklist,
  getDataProviders,
  addDataProvider,
  updateDataProviders,
  getTopics,
  updateTopics,
  updateSources,
  getFactTableDefinition,
  listAllFilesInDataset,
  updateDatasetGroup,
  getHistory,
  datasetActionRequest,
  rebuildAll,
  rebuildDrafts
} from '../../src/controllers/dataset';

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
  Object.assign(rev, overrides);
  return rev;
}

function createMockRequest(overrides: Partial<Request> = {}): Request {
  return {
    params: {},
    query: {},
    body: {},
    language: 'en',
    user: {
      id: uuidV4(),
      name: 'test-user',
      groupRoles: [
        { groupId: 'group-1', roles: ['editor', 'approver'] }
      ]
    },
    datasetService: {
      createNew: jest.fn(),
      updateFactTable: jest.fn(),
      getDatasetOverview: jest.fn(),
      getTasklistState: jest.fn(),
      updateMetadata: jest.fn(),
      addDataProvider: jest.fn(),
      updateDataProviders: jest.fn(),
      updateTopics: jest.fn(),
      updateDatasetGroup: jest.fn(),
      getHistory: jest.fn()
    },
    fileService: {
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
    headersSent: false,
    ...overrides
  } as unknown as Response;
  (res.status as jest.Mock).mockReturnValue(res);
  (res.json as jest.Mock).mockReturnValue(res);
  return res;
}

describe('Dataset controller', () => {
  let mockNext: jest.MockedFunction<NextFunction>;

  beforeEach(() => {
    jest.clearAllMocks();
    mockNext = jest.fn();
  });

  describe('listUserDatasets', () => {
    it('should return dataset list with default pagination', async () => {
      const mockResults = { data: [], count: 0 };
      mockDatasetListForUser.mockResolvedValue(mockResults);
      const req = createMockRequest();
      const res = createMockResponse();

      await listUserDatasets(req, res, mockNext);

      expect(mockDatasetListForUser).toHaveBeenCalledWith(req.user, 'en', 1, 20, undefined);
      expect(res.json).toHaveBeenCalledWith(mockResults);
    });

    it('should pass search/page/limit to repository', async () => {
      const mockResults = { data: [{ id: '1' }], count: 1 };
      mockDatasetListForUser.mockResolvedValue(mockResults);
      const req = createMockRequest({ query: { search: 'test', page: '2', limit: '10' } });
      const res = createMockResponse();

      await listUserDatasets(req, res, mockNext);

      expect(mockDatasetListForUser).toHaveBeenCalledWith(req.user, 'en', 2, 10, 'test');
      expect(res.json).toHaveBeenCalledWith(mockResults);
    });

    it('should call next with UnknownException on error', async () => {
      mockDatasetListForUser.mockRejectedValue(new Error('db error'));
      const req = createMockRequest();
      const res = createMockResponse();

      await listUserDatasets(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('listAllDatasets', () => {
    it('should return dataset list with default pagination', async () => {
      const mockResults = { data: [], count: 0 };
      mockDatasetListAll.mockResolvedValue(mockResults);
      const req = createMockRequest();
      const res = createMockResponse();

      await listAllDatasets(req, res, mockNext);

      expect(mockDatasetListAll).toHaveBeenCalledWith('en', 1, 20, undefined);
      expect(res.json).toHaveBeenCalledWith(mockResults);
    });

    it('should pass search/page/limit to repository', async () => {
      const mockResults = { data: [{ id: '1' }], count: 1 };
      mockDatasetListAll.mockResolvedValue(mockResults);
      const req = createMockRequest({ query: { search: 'test', page: '3', limit: '5' } });
      const res = createMockResponse();

      await listAllDatasets(req, res, mockNext);

      expect(mockDatasetListAll).toHaveBeenCalledWith('en', 3, 5, 'test');
      expect(res.json).toHaveBeenCalledWith(mockResults);
    });

    it('should call next with UnknownException on error', async () => {
      mockDatasetListAll.mockRejectedValue(new Error('db error'));
      const req = createMockRequest();
      const res = createMockResponse();

      await listAllDatasets(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('getDatasetById', () => {
    it('should return plain dataset when no hydrate param', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };
      mockFromDataset.mockReturnValue(mockDto);

      const req = createMockRequest({ query: {} });
      const res = createMockResponse({ locals: { datasetId, dataset } });

      await getDatasetById(req, res);

      expect(mockDatasetGetById).not.toHaveBeenCalled();
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should load withStandardPreview and publisher for hydrate=preview', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      dataset.userGroupId = 'ug-1';
      const mockDto = { id: datasetId };
      const mockUserGroup = { id: 'ug-1' };
      const mockPublisher = { name: 'test' };

      mockDatasetGetById.mockResolvedValue(dataset);
      mockGetByIdWithOrganisation.mockResolvedValue(mockUserGroup);
      mockFromDataset.mockReturnValue(mockDto);
      mockFromUserGroup.mockReturnValue(mockPublisher);

      const req = createMockRequest({ query: { hydrate: DatasetInclude.Preview } });
      const res = createMockResponse({ locals: { datasetId, dataset: createMockDataset(datasetId) } });

      await getDatasetById(req, res);

      expect(mockDatasetGetById).toHaveBeenCalledWith(datasetId, { revisions: true });
      expect(mockGetByIdWithOrganisation).toHaveBeenCalledWith('ug-1');
      expect(mockDto.publisher).toBe(mockPublisher);
    });

    it('should load withDraftAndMetadata and publisher for hydrate=metadata', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      dataset.userGroupId = 'ug-2';
      const mockDto = { id: datasetId };
      const mockUserGroup = { id: 'ug-2' };
      const mockPublisher = { name: 'pub' };

      mockDatasetGetById.mockResolvedValue(dataset);
      mockGetByIdWithOrganisation.mockResolvedValue(mockUserGroup);
      mockFromDataset.mockReturnValue(mockDto);
      mockFromUserGroup.mockReturnValue(mockPublisher);

      const req = createMockRequest({ query: { hydrate: DatasetInclude.Meta } });
      const res = createMockResponse({ locals: { datasetId, dataset: createMockDataset(datasetId) } });

      await getDatasetById(req, res);

      expect(mockDatasetGetById).toHaveBeenCalledWith(datasetId, { draftRevision: { metadata: true } });
      expect(mockDto.publisher).toBe(mockPublisher);
    });

    it('should call datasetService.getDatasetOverview for hydrate=overview', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };

      const req = createMockRequest({ query: { hydrate: DatasetInclude.Overview } });
      (req as any).datasetService.getDatasetOverview.mockResolvedValue(dataset);
      mockFromDataset.mockReturnValue(mockDto);

      const res = createMockResponse({ locals: { datasetId, dataset: createMockDataset(datasetId) } });

      await getDatasetById(req, res);

      expect((req as any).datasetService.getDatasetOverview).toHaveBeenCalledWith(datasetId);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should load withDimensions for hydrate=dimensions', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };

      mockDatasetGetById.mockResolvedValue(dataset);
      mockFromDataset.mockReturnValue(mockDto);

      const req = createMockRequest({ query: { hydrate: DatasetInclude.Dimensions } });
      const res = createMockResponse({ locals: { datasetId, dataset: createMockDataset(datasetId) } });

      await getDatasetById(req, res);

      expect(mockDatasetGetById).toHaveBeenCalledWith(datasetId, { dimensions: true });
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });
  });

  describe('deleteDraftDatasetById', () => {
    it('should call next with NotAllowedException when dataset has publishedRevision', async () => {
      const dataset = createMockDataset();
      dataset.publishedRevision = createMockRevision() as any;
      const req = createMockRequest({ params: { dataset_id: dataset.id } });
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      await deleteDraftDatasetById(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(NotAllowedException));
    });

    it('should delete draft dataset and return 202', async () => {
      const dataset = createMockDataset();
      dataset.publishedRevision = null;
      const datasetWithDraft = createMockDataset(dataset.id);
      datasetWithDraft.draftRevision = null;
      datasetWithDraft.dimensions = [];

      mockDatasetGetById.mockResolvedValue(datasetWithDraft);
      mockDatasetDeleteById.mockResolvedValue(undefined);

      const req = createMockRequest({ params: { dataset_id: dataset.id } });
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      await deleteDraftDatasetById(req, res, mockNext);

      expect((req as any).fileService.deleteDirectory).toHaveBeenCalledWith(dataset.id);
      expect(mockDatasetDeleteById).toHaveBeenCalledWith(dataset.id);
      expect(res.status).toHaveBeenCalledWith(202);
      expect(res.end).toHaveBeenCalled();
    });

    it('should clean up cube DB schemas and data tables', async () => {
      const dataset = createMockDataset();
      dataset.publishedRevision = null;
      const draftRevision = createMockRevision();
      draftRevision.dataTable = { id: 'dt-1' } as any;
      const datasetWithDraft = createMockDataset(dataset.id);
      datasetWithDraft.draftRevision = draftRevision;
      datasetWithDraft.dimensions = [
        { lookupTable: { id: 'lt-1' } } as any
      ];

      mockDatasetGetById.mockResolvedValue(datasetWithDraft);
      mockDatasetDeleteById.mockResolvedValue(undefined);

      const req = createMockRequest({ params: { dataset_id: dataset.id } });
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      await deleteDraftDatasetById(req, res, mockNext);

      const { dbManager } = require('../../src/db/database-manager');
      const queryRunner = dbManager.getCubeDataSource().createQueryRunner();
      expect(queryRunner.query).toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(202);
    });
  });

  describe('createDataset', () => {
    it('should reject when title is missing', async () => {
      mockHasError.mockResolvedValueOnce(true); // title error
      const req = createMockRequest({ body: {} });
      const res = createMockResponse();

      await createDataset(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should reject when user_group_id is invalid', async () => {
      mockHasError
        .mockResolvedValueOnce(false) // title OK
        .mockResolvedValueOnce(true); // group id error
      const req = createMockRequest({ body: { title: 'Test' } });
      const res = createMockResponse();

      await createDataset(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should create dataset and return 201 with DatasetDTO on success', async () => {
      mockHasError.mockResolvedValue(false);
      const mockDataset = createMockDataset();
      const mockDto = { id: mockDataset.id };
      mockFromDataset.mockReturnValue(mockDto);

      const req = createMockRequest({ body: { title: 'Test', user_group_id: 'group-1' } });
      (req as any).datasetService.createNew.mockResolvedValue(mockDataset);
      const res = createMockResponse();

      await createDataset(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should call next with UnknownException on internal error', async () => {
      mockHasError.mockResolvedValue(false);
      const req = createMockRequest({ body: { title: 'Test', user_group_id: 'group-1' } });
      (req as any).datasetService.createNew.mockRejectedValue(new Error('fail'));
      const res = createMockResponse();

      await createDataset(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('uploadDataTable', () => {
    it('should return 201 with DatasetDTO on success', async () => {
      const dataset = createMockDataset();
      const tmpFile = { originalname: 'test.csv', mimetype: 'text/csv', size: 100 };
      mockUploadAvScan.mockResolvedValue(tmpFile);

      const updatedDataset = createMockDataset(dataset.id);
      const mockDto = { id: dataset.id };
      mockFromDataset.mockReturnValue(mockDto);

      const req = createMockRequest();
      (req as any).datasetService.updateFactTable.mockResolvedValue(updatedDataset);
      const res = createMockResponse({ locals: { dataset } });

      await uploadDataTable(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith(mockDto);
      expect(mockCleanupTmpFile).toHaveBeenCalledWith(tmpFile);
    });

    it('should return 500 with ViewErrDTO when updateFactTable fails', async () => {
      const dataset = createMockDataset();
      const tmpFile = { originalname: 'test.csv', mimetype: 'text/csv', size: 100 };
      mockUploadAvScan.mockResolvedValue(tmpFile);

      const req = createMockRequest();
      (req as any).datasetService.updateFactTable.mockRejectedValue(new Error('fail'));
      const res = createMockResponse({ locals: { dataset } });

      await uploadDataTable(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(500);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({ status: 500, dataset_id: dataset.id })
      );
      expect(mockCleanupTmpFile).toHaveBeenCalledWith(tmpFile);
    });

    it('should pass upload error to next and always clean up tmpFile', async () => {
      const dataset = createMockDataset();
      const uploadError = new Error('upload failed');
      mockUploadAvScan.mockRejectedValue(uploadError);

      const req = createMockRequest();
      const res = createMockResponse({ locals: { dataset } });

      await uploadDataTable(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(uploadError);
    });
  });

  describe('generateFilterId', () => {
    it('should call next with NotFoundException when no endRevisionId', async () => {
      const dataset = createMockDataset();
      dataset.endRevisionId = undefined;
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      await generateFilterId(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundException));
    });

    it('should return filterId on success', async () => {
      const dataset = createMockDataset();
      dataset.endRevisionId = uuidV4();
      const mockQueryStore = { id: uuidV4() };
      mockDtoValidator.mockResolvedValue({ option: 'test' });
      mockQueryStoreGetByRequest.mockResolvedValue(mockQueryStore);

      const req = createMockRequest({ body: {} });
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      await generateFilterId(req, res, mockNext);

      expect(res.json).toHaveBeenCalledWith({ filterId: mockQueryStore.id });
    });

    it('should call next with UnknownException on error', async () => {
      const dataset = createMockDataset();
      dataset.endRevisionId = uuidV4();
      mockDtoValidator.mockRejectedValue(new Error('fail'));

      const req = createMockRequest({ body: {} });
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      await generateFilterId(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('datasetPreview', () => {
    it('should call next with NotFoundException when no endRevisionId', async () => {
      const dataset = createMockDataset();
      dataset.endRevisionId = undefined;
      const req = createMockRequest({ params: {} });
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      await datasetPreview(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundException));
    });

    it('should create new QueryStore when no filterId', async () => {
      const dataset = createMockDataset();
      dataset.endRevisionId = uuidV4();
      const mockQueryStore = { id: uuidV4(), requestObject: {} };
      const pageOptions = { format: OutputFormats.Json, pageNumber: 1, pageSize: 100 };

      mockParsePageOptions.mockResolvedValue(pageOptions);
      mockQueryStoreGetByRequest.mockResolvedValue(mockQueryStore);
      mockBuildDataQuery.mockResolvedValue('SELECT 1');
      mockSendJson.mockResolvedValue(undefined);

      const req = createMockRequest({ params: {} });
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      await datasetPreview(req, res, mockNext);

      expect(mockQueryStoreGetByRequest).toHaveBeenCalled();
      expect(mockQueryStoreGetById).not.toHaveBeenCalled();
    });

    it('should use existing QueryStore when filterId provided', async () => {
      const dataset = createMockDataset();
      dataset.endRevisionId = uuidV4();
      const filterId = uuidV4();
      const mockQueryStore = { id: filterId, requestObject: {} };
      const pageOptions = { format: OutputFormats.Json, pageNumber: 1, pageSize: 100 };

      mockParsePageOptions.mockResolvedValue(pageOptions);
      mockQueryStoreGetById.mockResolvedValue(mockQueryStore);
      mockBuildDataQuery.mockResolvedValue('SELECT 1');
      mockSendJson.mockResolvedValue(undefined);

      const req = createMockRequest({ params: { filter_id: filterId } });
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      await datasetPreview(req, res, mockNext);

      expect(mockQueryStoreGetById).toHaveBeenCalledWith(filterId);
      expect(mockQueryStoreGetByRequest).not.toHaveBeenCalled();
    });

    it('should pass known exceptions (NotFoundException, BadRequestException) through to next', async () => {
      const dataset = createMockDataset();
      dataset.endRevisionId = uuidV4();

      mockParsePageOptions.mockRejectedValue(new BadRequestException('bad'));

      const req = createMockRequest({ params: {} });
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      await datasetPreview(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });
  });

  describe('sendFormattedResponse', () => {
    it('should route to correct sender based on format', async () => {
      const query = 'SELECT 1';
      const queryStore = { id: uuidV4() } as any;
      const res = createMockResponse();

      mockSendFrontendView.mockResolvedValue(undefined);
      await sendFormattedResponse(query, queryStore, { format: OutputFormats.Frontend } as any, res);
      expect(mockSendFrontendView).toHaveBeenCalled();

      mockSendCsv.mockResolvedValue(undefined);
      await sendFormattedResponse(query, queryStore, { format: OutputFormats.Csv } as any, res);
      expect(mockSendCsv).toHaveBeenCalled();

      mockSendExcel.mockResolvedValue(undefined);
      await sendFormattedResponse(query, queryStore, { format: OutputFormats.Excel } as any, res);
      expect(mockSendExcel).toHaveBeenCalled();

      mockSendJson.mockResolvedValue(undefined);
      await sendFormattedResponse(query, queryStore, { format: OutputFormats.Json } as any, res);
      expect(mockSendJson).toHaveBeenCalled();
    });

    it('should return 400 for unsupported format', async () => {
      const query = 'SELECT 1';
      const queryStore = { id: uuidV4() } as any;
      const res = createMockResponse();

      await sendFormattedResponse(query, queryStore, { format: 'unsupported' } as any, res);

      expect(res.status).toHaveBeenCalledWith(400);
      expect(res.json).toHaveBeenCalledWith({ error: 'Format not supported' });
    });
  });

  describe('updateMetadata', () => {
    it('should update metadata and return 201 on success', async () => {
      const datasetId = uuidV4();
      const updatedDataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };
      mockDtoValidator.mockResolvedValue({ title: 'new' });
      mockFromDataset.mockReturnValue(mockDto);

      const req = createMockRequest({ body: { title: 'new' } });
      (req as any).datasetService.updateMetadata.mockResolvedValue(updatedDataset);
      const res = createMockResponse({ locals: { datasetId } });

      await updateMetadata(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should pass BadRequestException to next on validation error', async () => {
      const validationErr = new BadRequestException('bad', 400, []);
      mockDtoValidator.mockRejectedValue(validationErr);

      const req = createMockRequest({ body: {} });
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await updateMetadata(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(validationErr);
    });

    it('should call next with UnknownException on other errors', async () => {
      mockDtoValidator.mockResolvedValue({ title: 'new' });

      const req = createMockRequest({ body: { title: 'new' } });
      (req as any).datasetService.updateMetadata.mockRejectedValue(new Error('fail'));
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await updateMetadata(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('getTasklist', () => {
    it('should return tasklist state', async () => {
      const datasetId = uuidV4();
      const mockTasklist = { canPublish: true };

      const req = createMockRequest();
      (req as any).datasetService.getTasklistState.mockResolvedValue(mockTasklist);
      const res = createMockResponse({ locals: { datasetId } });

      await getTasklist(req, res, mockNext);

      expect((req as any).datasetService.getTasklistState).toHaveBeenCalledWith(datasetId, 'en');
      expect(res.json).toHaveBeenCalledWith(mockTasklist);
    });

    it('should call next with UnknownException on error', async () => {
      const req = createMockRequest();
      (req as any).datasetService.getTasklistState.mockRejectedValue(new Error('fail'));
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await getTasklist(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('getDataProviders', () => {
    it('should return provider DTOs', async () => {
      const datasetId = uuidV4();
      const mockProvider = { id: 'p1' };
      const dataset = createMockDataset(datasetId);
      dataset.draftRevision = {
        revisionProviders: [mockProvider]
      } as any;

      mockDatasetGetById.mockResolvedValue(dataset);
      mockFromRevisionProvider.mockReturnValue({ id: 'p1' });

      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId } });

      await getDataProviders(req, res, mockNext);

      expect(res.json).toHaveBeenCalledWith([{ id: 'p1' }]);
    });

    it('should pass errors to next', async () => {
      mockDatasetGetById.mockRejectedValue(new Error('fail'));
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await getDataProviders(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(Error));
    });
  });

  describe('addDataProvider', () => {
    it('should add provider and return 201 on success', async () => {
      const datasetId = uuidV4();
      const updatedDataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };
      mockDtoValidator.mockResolvedValue({ name: 'provider' });
      mockFromDataset.mockReturnValue(mockDto);

      const req = createMockRequest({ body: { name: 'provider' } });
      (req as any).datasetService.addDataProvider.mockResolvedValue(updatedDataset);
      const res = createMockResponse({ locals: { datasetId } });

      await addDataProvider(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should pass BadRequestException on validation error', async () => {
      const validationErr = new BadRequestException('bad', 400, []);
      mockDtoValidator.mockRejectedValue(validationErr);

      const req = createMockRequest({ body: {} });
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await addDataProvider(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(validationErr);
    });

    it('should call next with UnknownException on other errors', async () => {
      mockDtoValidator.mockResolvedValue({ name: 'provider' });

      const req = createMockRequest({ body: { name: 'provider' } });
      (req as any).datasetService.addDataProvider.mockRejectedValue(new Error('fail'));
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await addDataProvider(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('updateDataProviders', () => {
    it('should update providers and return 201 on success', async () => {
      const datasetId = uuidV4();
      const updatedDataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };
      mockArrayValidator.mockResolvedValue([{ name: 'p1' }]);
      mockFromDataset.mockReturnValue(mockDto);

      const req = createMockRequest({ body: [{ name: 'p1' }] });
      (req as any).datasetService.updateDataProviders.mockResolvedValue(updatedDataset);
      const res = createMockResponse({ locals: { datasetId } });

      await updateDataProviders(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should pass BadRequestException on validation error', async () => {
      const validationErr = new BadRequestException('bad', 400, []);
      mockArrayValidator.mockRejectedValue(validationErr);

      const req = createMockRequest({ body: [] });
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await updateDataProviders(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(validationErr);
    });

    it('should call next with UnknownException on other errors', async () => {
      mockArrayValidator.mockResolvedValue([{ name: 'p1' }]);

      const req = createMockRequest({ body: [{ name: 'p1' }] });
      (req as any).datasetService.updateDataProviders.mockRejectedValue(new Error('fail'));
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await updateDataProviders(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('getTopics', () => {
    it('should return topic DTOs from draft revision', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      dataset.draftRevision = {
        revisionTopics: [{ topic: { id: 't1', name: 'Topic 1' } }]
      } as any;

      mockDatasetGetById.mockResolvedValue(dataset);
      mockFromTopic.mockReturnValue({ id: 't1', name: 'Topic 1' });

      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId } });

      await getTopics(req, res, mockNext);

      expect(res.json).toHaveBeenCalledWith([{ id: 't1', name: 'Topic 1' }]);
    });

    it('should return empty array when no draft revision', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      dataset.draftRevision = null;

      mockDatasetGetById.mockResolvedValue(dataset);

      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId } });

      await getTopics(req, res, mockNext);

      expect(res.json).toHaveBeenCalledWith([]);
    });
  });

  describe('updateTopics', () => {
    it('should update topics and return 201 on success', async () => {
      const datasetId = uuidV4();
      const updatedDataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };
      mockDtoValidator.mockResolvedValue({ topics: ['t1'] });
      mockFromDataset.mockReturnValue(mockDto);

      const req = createMockRequest({ body: { topics: ['t1'] } });
      (req as any).datasetService.updateTopics.mockResolvedValue(updatedDataset);
      const res = createMockResponse({ locals: { datasetId } });

      await updateTopics(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should pass BadRequestException on validation error', async () => {
      const validationErr = new BadRequestException('bad', 400, []);
      mockDtoValidator.mockRejectedValue(validationErr);

      const req = createMockRequest({ body: {} });
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await updateTopics(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(validationErr);
    });

    it('should call next with UnknownException on other errors', async () => {
      mockDtoValidator.mockResolvedValue({ topics: ['t1'] });

      const req = createMockRequest({ body: { topics: ['t1'] } });
      (req as any).datasetService.updateTopics.mockRejectedValue(new Error('fail'));
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await updateTopics(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('updateSources', () => {
    it('should reject when no sourceAssignment body', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      dataset.draftRevision = createMockRevision() as any;

      mockDatasetGetById.mockResolvedValue(dataset);

      const req = createMockRequest({ body: undefined as any });
      const res = createMockResponse({ locals: { datasetId } });

      await updateSources(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should reject when revision is not first revision', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      const revision = createMockRevision({ revisionIndex: 2 });
      revision.dataTable = { id: 'dt-1' } as any;
      dataset.draftRevision = revision;

      mockDatasetGetById.mockResolvedValue(dataset);

      const req = createMockRequest({ body: { col: 'dimension' } });
      const res = createMockResponse({ locals: { datasetId } });

      await updateSources(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });

    it('should reject when no dataTable on revision', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      const revision = createMockRevision({ revisionIndex: 1 });
      revision.dataTable = null;
      dataset.draftRevision = revision;

      mockDatasetGetById.mockResolvedValue(dataset);

      const req = createMockRequest({ body: { col: 'dimension' } });
      const res = createMockResponse({ locals: { datasetId } });

      await updateSources(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });

    it('should return error response when validateSourceAssignment fails', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      const revision = createMockRevision({ revisionIndex: 1 });
      revision.dataTable = { id: 'dt-1', dataTableDescriptions: [] } as any;
      dataset.draftRevision = revision;

      mockDatasetGetById.mockResolvedValue(dataset);
      const { SourceAssignmentException } = jest.requireMock('../../src/exceptions/source-assignment.exception') || {};
      const assignmentError = { status: 422, message: 'bad assignment' };
      mockValidateSourceAssignment.mockImplementation(() => {
        throw assignmentError;
      });

      const req = createMockRequest({ body: { col: 'dimension' } });
      const res = createMockResponse({ locals: { datasetId } });

      await updateSources(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(422);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({ status: 422, dataset_id: datasetId })
      );
    });

    it('should return dataset with build_id on success', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      const revision = createMockRevision({ revisionIndex: 1 });
      revision.dataTable = { id: 'dt-1', dataTableDescriptions: [] } as any;
      dataset.draftRevision = revision;

      const updatedDataset = createMockDataset(datasetId);
      const mockDto = { id: datasetId };

      mockDatasetGetById
        .mockResolvedValueOnce(dataset) // first call for updateSources
        .mockResolvedValueOnce(updatedDataset); // second call after source assignment
      mockValidateSourceAssignment.mockReturnValue({ validated: true });
      mockFactTableValidatorFromSource.mockResolvedValue(undefined);
      mockCreateDimensionsFromSourceAssignment.mockResolvedValue(undefined);
      mockFromDataset.mockReturnValue(mockDto);

      const req = createMockRequest({ body: { col: 'dimension' } });
      const res = createMockResponse({ locals: { datasetId } });

      await updateSources(req, res, mockNext);

      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({ dataset: mockDto, build_id: expect.any(String) })
      );
    });
  });

  describe('getFactTableDefinition', () => {
    it('should return fact table column DTOs', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      dataset.factTable = [{ id: datasetId, columnName: 'col1', columnIndex: 0 }] as any;

      mockDatasetGetById.mockResolvedValue(dataset);
      mockFromFactTableColumn.mockReturnValue({ columnName: 'col1' });

      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId } });

      await getFactTableDefinition(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.json).toHaveBeenCalledWith([{ columnName: 'col1' }]);
    });

    it('should return empty array when no fact table', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      dataset.factTable = null;

      mockDatasetGetById.mockResolvedValue(dataset);

      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId } });

      await getFactTableDefinition(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.json).toHaveBeenCalledWith([]);
    });
  });

  describe('listAllFilesInDataset', () => {
    it('should return file list', async () => {
      const datasetId = uuidV4();
      const filesMap = new Map([['file1.csv', { name: 'file1.csv' }]]);
      mockCollectFiles.mockResolvedValue(filesMap);

      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId } });

      await listAllFilesInDataset(req, res, mockNext);

      expect(mockCollectFiles).toHaveBeenCalledWith(datasetId);
      expect(res.json).toHaveBeenCalledWith([{ name: 'file1.csv' }]);
    });

    it('should call next with UnknownException on error', async () => {
      mockCollectFiles.mockRejectedValue(new Error('fail'));

      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await listAllFilesInDataset(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('updateDatasetGroup', () => {
    it('should reject when user_group_id is not in user approver groups', async () => {
      const dataset = createMockDataset();
      mockHasError.mockResolvedValue(true);

      const req = createMockRequest({ body: { user_group_id: 'bad-group' } });
      const res = createMockResponse({ locals: { dataset } });

      await updateDatasetGroup(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
    });

    it('should update group and return 201 on success', async () => {
      const dataset = createMockDataset();
      const updatedDataset = createMockDataset(dataset.id);
      const mockDto = { id: dataset.id };
      mockHasError.mockResolvedValue(false);
      mockFromDataset.mockReturnValue(mockDto);

      const req = createMockRequest({ body: { user_group_id: 'group-1' } });
      (req as any).datasetService.updateDatasetGroup.mockResolvedValue(updatedDataset);
      const res = createMockResponse({ locals: { dataset } });

      await updateDatasetGroup(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith(mockDto);
    });

    it('should call next with UnknownException on error', async () => {
      const dataset = createMockDataset();
      mockHasError.mockResolvedValue(false);

      const req = createMockRequest({ body: { user_group_id: 'group-1' } });
      (req as any).datasetService.updateDatasetGroup.mockRejectedValue(new Error('fail'));
      const res = createMockResponse({ locals: { dataset } });

      await updateDatasetGroup(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('getHistory', () => {
    it('should return event log DTOs', async () => {
      const datasetId = uuidV4();
      const mockEvents = [{ id: 'e1' }, { id: 'e2' }];

      const req = createMockRequest();
      (req as any).datasetService.getHistory.mockResolvedValue(mockEvents);
      mockFromEventLog.mockImplementation((e: any) => ({ id: e.id }));
      const res = createMockResponse({ locals: { datasetId } });

      await getHistory(req, res, mockNext);

      expect(res.json).toHaveBeenCalledWith([{ id: 'e1' }, { id: 'e2' }]);
    });

    it('should call next with UnknownException on error', async () => {
      const req = createMockRequest();
      (req as any).datasetService.getHistory.mockRejectedValue(new Error('fail'));
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await getHistory(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(UnknownException));
    });
  });

  describe('datasetActionRequest', () => {
    it('should call next() for invalid action (falls through to 404)', async () => {
      const req = createMockRequest({ params: { action: 'invalid_action' } });
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await datasetActionRequest(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith();
    });

    it('should throw BadRequestException for publish action', async () => {
      const req = createMockRequest({ params: { action: TaskAction.Publish } });
      const res = createMockResponse({ locals: { datasetId: uuidV4() } });

      await expect(datasetActionRequest(req, res, mockNext)).rejects.toThrow(BadRequestException);
    });

    it('should call taskService.requestUnpublish for unpublish action', async () => {
      const datasetId = uuidV4();
      mockRequestUnpublish.mockResolvedValue(undefined);

      const req = createMockRequest({
        params: { action: TaskAction.Unpublish },
        body: { reason: 'test reason' }
      });
      const res = createMockResponse({ locals: { datasetId } });

      await datasetActionRequest(req, res, mockNext);

      expect(mockRequestUnpublish).toHaveBeenCalledWith(datasetId, req.user, 'test reason');
      expect(res.status).toHaveBeenCalledWith(204);
      expect(res.end).toHaveBeenCalled();
    });

    it('should return 204 on success', async () => {
      const datasetId = uuidV4();
      mockRequestArchive.mockResolvedValue(undefined);

      const req = createMockRequest({
        params: { action: TaskAction.Archive },
        body: { reason: 'archiving' }
      });
      const res = createMockResponse({ locals: { datasetId } });

      await datasetActionRequest(req, res, mockNext);

      expect(res.status).toHaveBeenCalledWith(204);
      expect(res.end).toHaveBeenCalled();
    });
  });

  describe('rebuildAll', () => {
    it('should return 409 when active bulk build exists', async () => {
      mockGetAllActiveBulkBuilds.mockResolvedValue([{ id: 'build-1' }, { id: 'build-2' }]);

      const req = createMockRequest();
      const res = createMockResponse();

      await rebuildAll(req, res);

      expect(res.status).toHaveBeenCalledWith(409);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({ build_id: 'build-1' })
      );
    });

    it('should return 202 with build_id when no active builds', async () => {
      mockGetAllActiveBulkBuilds.mockResolvedValue([]);
      const mockBuildLog = {
        id: 'new-build-1',
        type: 'all_cubes',
        buildScript: null,
        status: 'queued',
        save: jest.fn().mockResolvedValue(undefined),
        completeBuild: jest.fn()
      };
      mockStartBuild.mockResolvedValue(mockBuildLog);
      mockGetAllRevisionIds.mockResolvedValue([]);

      const req = createMockRequest();
      const res = createMockResponse();

      await rebuildAll(req, res);

      expect(res.status).toHaveBeenCalledWith(202);
      expect(res.json).toHaveBeenCalledWith({ build_id: 'new-build-1' });
    });
  });

  describe('rebuildDrafts', () => {
    it('should return 409 when active bulk build exists', async () => {
      mockGetAllActiveBulkBuilds.mockResolvedValue([{ id: 'build-1' }, { id: 'build-2' }]);

      const req = createMockRequest();
      const res = createMockResponse();

      await rebuildDrafts(req, res);

      expect(res.status).toHaveBeenCalledWith(409);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({ build_id: 'build-1' })
      );
    });

    it('should return 202 with build_id when no active builds', async () => {
      mockGetAllActiveBulkBuilds.mockResolvedValue([]);
      const mockBuildLog = {
        id: 'new-build-2',
        type: 'draft_cubes',
        buildScript: null,
        status: 'queued',
        save: jest.fn().mockResolvedValue(undefined),
        completeBuild: jest.fn()
      };
      mockStartBuild.mockResolvedValue(mockBuildLog);
      mockGetAllDraftRevisionIds.mockResolvedValue([]);

      const req = createMockRequest();
      const res = createMockResponse();

      await rebuildDrafts(req, res);

      expect(res.status).toHaveBeenCalledWith(202);
      expect(res.json).toHaveBeenCalledWith({ build_id: 'new-build-2' });
    });
  });
});
