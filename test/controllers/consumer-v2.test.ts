import { Request, Response, NextFunction } from 'express';

import { NotFoundException } from '../../src/exceptions/not-found.exception';
import { Dataset } from '../../src/entities/dataset/dataset';
import { Revision } from '../../src/entities/dataset/revision';
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

// Mock PublishedRevisionRepository
const mockGetLatestByDatasetId = jest.fn();
jest.mock('../../src/repositories/published-revision', () => ({
  PublishedRevisionRepository: {
    getLatestByDatasetId: (...args: unknown[]) => mockGetLatestByDatasetId(...args)
  }
}));

// Mock QueryStoreRepository
const mockGetByRequest = jest.fn();
const mockGetById = jest.fn();
jest.mock('../../src/repositories/query-store', () => ({
  QueryStoreRepository: {
    getByRequest: (...args: unknown[]) => mockGetByRequest(...args),
    getById: (...args: unknown[]) => mockGetById(...args)
  }
}));

// Mock consumer-view-v2 service
const mockBuildDataQuery = jest.fn();
jest.mock('../../src/services/consumer-view-v2', () => ({
  buildDataQuery: (...args: unknown[]) => mockBuildDataQuery(...args),
  sendFormattedResponse: jest.fn(),
  sendCsv: jest.fn(),
  sendExcel: jest.fn(),
  sendFilters: jest.fn(),
  sendFrontendView: jest.fn(),
  sendHtml: jest.fn(),
  sendJson: jest.fn()
}));

// Mock consumer utils
const mockGetFilterTableQuery = jest.fn();
const mockGetFilterTable = jest.fn();
jest.mock('../../src/utils/consumer', () => ({
  getFilterTable: (...args: unknown[]) => mockGetFilterTable(...args),
  getFilterTableQuery: (...args: unknown[]) => mockGetFilterTableQuery(...args),
  resolveDimensionToFactTableColumn: jest.fn(),
  resolveFactColumnToDimension: jest.fn()
}));

// Mock parsePageOptions
jest.mock('../../src/utils/parse-page-options', () => ({
  parsePageOptions: jest.fn().mockResolvedValue({
    format: 'json',
    pageNumber: 1,
    pageSize: 100,
    sort: [],
    locale: 'en'
  })
}));

// Mock validators
jest.mock('../../src/validators', () => ({
  format2Validator: jest.fn().mockReturnValue({ run: jest.fn().mockResolvedValue({ isEmpty: () => true }) }),
  pageNumberValidator: jest.fn().mockReturnValue({ run: jest.fn().mockResolvedValue({ isEmpty: () => true }) }),
  pageSizeValidator: jest.fn().mockReturnValue({ run: jest.fn().mockResolvedValue({ isEmpty: () => true }) }),
  searchKeywordsValidator: jest.fn().mockReturnValue({ run: jest.fn().mockResolvedValue({ isEmpty: () => true }) }),
  searchModeValidator: jest.fn().mockReturnValue({ run: jest.fn().mockResolvedValue({ isEmpty: () => true }) })
}));

// Mock dto-validator
jest.mock('../../src/validators/dto-validator', () => ({
  dtoValidator: jest.fn().mockResolvedValue({})
}));

// Mock express-validator
jest.mock('express-validator', () => ({
  matchedData: jest.fn().mockReturnValue({}),
  FieldValidationError: jest.fn()
}));

// Mock pivots
jest.mock('../../src/services/pivots', () => ({
  createPivotQuery: jest.fn(),
  createPivotOutputUsingDuckDB: jest.fn(),
  langToLocale: jest.fn().mockReturnValue('en-GB')
}));

import {
  getPublishedDatasetData,
  getPublishedDatasetFilters,
  generateFilterId,
  generatePivotFilterId,
  getPublishedDatasetPivot,
  getPublishedDatasetPivotFromId
} from '../../src/controllers/consumer-v2';

function createMockDataset(id?: string): Dataset {
  const dataset = new Dataset();
  dataset.id = id || uuidV4();
  return dataset;
}

function createMockRevision(id?: string): Revision {
  const rev = new Revision();
  rev.id = id || uuidV4();
  return rev;
}

function createMockRequest(overrides: Partial<Request> = {}): Request {
  return {
    params: {},
    query: {},
    body: {},
    language: 'en',
    ...overrides
  } as unknown as Request;
}

function createMockResponse(overrides: Partial<Response> = {}): Response {
  return {
    locals: {},
    json: jest.fn(),
    status: jest.fn().mockReturnThis(),
    headersSent: false,
    ...overrides
  } as unknown as Response;
}

describe('consumer-v2 controller - scheduled publish date handling', () => {
  let mockNext: jest.MockedFunction<NextFunction>;

  beforeEach(() => {
    jest.clearAllMocks();
    mockNext = jest.fn();
  });

  describe('getPublishedDatasetData', () => {
    it('should call next with NotFoundException when no published revision exists', async () => {
      const dataset = createMockDataset();
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      mockGetLatestByDatasetId.mockResolvedValue(null);

      await getPublishedDatasetData(req, res, mockNext);

      expect(mockGetLatestByDatasetId).toHaveBeenCalledWith(dataset.id);
      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundException));
    });

    it('should use the published revision id from the repository, not dataset.publishedRevisionId', async () => {
      const dataset = createMockDataset();
      dataset.publishedRevisionId = uuidV4(); // stale pointer on the dataset entity
      const publishedRev = createMockRevision();

      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      mockGetLatestByDatasetId.mockResolvedValue(publishedRev);
      const mockQueryStore = { id: uuidV4(), requestObject: {} };
      mockGetByRequest.mockResolvedValue(mockQueryStore);
      mockBuildDataQuery.mockResolvedValue('SELECT 1');

      await getPublishedDatasetData(req, res, mockNext);

      expect(mockGetLatestByDatasetId).toHaveBeenCalledWith(dataset.id);
      // Should use the revision id from the repository lookup, NOT dataset.publishedRevisionId
      expect(mockGetByRequest).toHaveBeenCalledWith(dataset.id, publishedRev.id, expect.anything());
    });

    it('should look up the published revision by dataset id', async () => {
      const datasetId = uuidV4();
      const dataset = createMockDataset(datasetId);
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId, dataset } });

      mockGetLatestByDatasetId.mockResolvedValue(null);

      await getPublishedDatasetData(req, res, mockNext);

      expect(mockGetLatestByDatasetId).toHaveBeenCalledWith(datasetId);
    });
  });

  describe('getPublishedDatasetPivot', () => {
    it('should call next with NotFoundException when no published revision exists', async () => {
      const dataset = createMockDataset();
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      mockGetLatestByDatasetId.mockResolvedValue(null);

      await getPublishedDatasetPivot(req, res, mockNext);

      expect(mockGetLatestByDatasetId).toHaveBeenCalledWith(dataset.id);
      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundException));
    });
  });

  describe('getPublishedDatasetPivotFromId', () => {
    it('should call next with NotFoundException when no published revision exists', async () => {
      const dataset = createMockDataset();
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      mockGetLatestByDatasetId.mockResolvedValue(null);

      await getPublishedDatasetPivotFromId(req, res, mockNext);

      expect(mockGetLatestByDatasetId).toHaveBeenCalledWith(dataset.id);
      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundException));
    });
  });

  describe('generatePivotFilterId', () => {
    it('should call next with NotFoundException when no published revision exists', async () => {
      const dataset = createMockDataset();
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      mockGetLatestByDatasetId.mockResolvedValue(null);

      await generatePivotFilterId(req, res, mockNext);

      expect(mockGetLatestByDatasetId).toHaveBeenCalledWith(dataset.id);
      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundException));
    });

    it('should use the published revision id when generating filter table', async () => {
      const dataset = createMockDataset();
      const publishedRev = createMockRevision();
      const req = createMockRequest({ body: {} });
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      mockGetLatestByDatasetId.mockResolvedValue(publishedRev);

      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const { dtoValidator } = require('../../src/validators/dto-validator');
      dtoValidator.mockResolvedValue({
        pivot: { x: 'col_a', y: 'col_b' },
        options: { use_raw_column_names: false },
        locale: 'en',
        filters: []
      });
      mockGetFilterTable.mockResolvedValue([]);
      const mockQueryStore = { id: uuidV4() };
      mockGetByRequest.mockResolvedValue(mockQueryStore);

      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const { resolveDimensionToFactTableColumn } = require('../../src/utils/consumer');
      resolveDimensionToFactTableColumn.mockImplementation((col: string) => col);

      await generatePivotFilterId(req, res, mockNext);

      expect(mockGetFilterTable).toHaveBeenCalledWith(publishedRev.id);
      expect(mockGetByRequest).toHaveBeenCalledWith(dataset.id, publishedRev.id, expect.anything());
    });
  });

  describe('generateFilterId', () => {
    it('should call next with NotFoundException when no published revision exists', async () => {
      const dataset = createMockDataset();
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      mockGetLatestByDatasetId.mockResolvedValue(null);

      await generateFilterId(req, res, mockNext);

      expect(mockGetLatestByDatasetId).toHaveBeenCalledWith(dataset.id);
      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundException));
    });

    it('should use the published revision id for the query store request', async () => {
      const dataset = createMockDataset();
      const publishedRev = createMockRevision();
      const req = createMockRequest({ body: {} });
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      mockGetLatestByDatasetId.mockResolvedValue(publishedRev);
      const mockQueryStore = { id: uuidV4() };
      mockGetByRequest.mockResolvedValue(mockQueryStore);

      await generateFilterId(req, res, mockNext);

      expect(mockGetByRequest).toHaveBeenCalledWith(dataset.id, publishedRev.id, expect.anything());
      expect(res.json).toHaveBeenCalledWith({ filterId: mockQueryStore.id });
    });
  });

  describe('getPublishedDatasetFilters', () => {
    it('should call next with NotFoundException when no published revision exists', async () => {
      const dataset = createMockDataset();
      const req = createMockRequest();
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      mockGetLatestByDatasetId.mockResolvedValue(null);

      await getPublishedDatasetFilters(req, res, mockNext);

      expect(mockGetLatestByDatasetId).toHaveBeenCalledWith(dataset.id);
      expect(mockNext).toHaveBeenCalledWith(expect.any(NotFoundException));
    });

    it('should use the published revision id to query filters', async () => {
      const dataset = createMockDataset();
      const publishedRev = createMockRevision();
      const req = createMockRequest({ language: 'en' } as Partial<Request>);
      const res = createMockResponse({ locals: { datasetId: dataset.id, dataset } });

      mockGetLatestByDatasetId.mockResolvedValue(publishedRev);
      mockGetFilterTableQuery.mockResolvedValue('SELECT 1');

      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const { sendFilters } = require('../../src/services/consumer-view-v2');
      sendFilters.mockResolvedValue(undefined);

      await getPublishedDatasetFilters(req, res, mockNext);

      expect(mockGetFilterTableQuery).toHaveBeenCalledWith(publishedRev.id, 'en');
    });
  });
});
