import { Readable } from 'node:stream';

import { Request, Response, NextFunction } from 'express';

import { BadRequestException } from '../../../src/exceptions/bad-request.exception';
import { UnknownException } from '../../../src/exceptions/unknown.exception';
import { uuidV4 } from '../../../src/utils/uuid';

jest.mock('../../../src/utils/logger', () => ({
  logger: { debug: jest.fn(), info: jest.fn(), error: jest.fn(), warn: jest.fn(), trace: jest.fn() }
}));

const mockGetById = jest.fn();
jest.mock('../../../src/repositories/dataset', () => ({
  DatasetRepository: { getById: (...args: unknown[]) => mockGetById(...args) },
  withMetadataForTranslation: { metadata: true }
}));

const mockCollectTranslations = jest.fn();
jest.mock('../../../src/utils/collect-translations', () => ({
  collectTranslations: (...args: unknown[]) => mockCollectTranslations(...args)
}));

const mockFromDataset = jest.fn();
jest.mock('../../../src/dtos/dataset-dto', () => ({
  DatasetDTO: { fromDataset: (...args: unknown[]) => mockFromDataset(...args) }
}));

const mockEventSave = jest.fn();
jest.mock('../../../src/entities/event-log', () => ({
  EventLog: { getRepository: () => ({ save: (...args: unknown[]) => mockEventSave(...args) }) }
}));

const mockUploadAvScan = jest.fn();
const mockCleanupTmpFile = jest.fn();
jest.mock('../../../src/services/virus-scanner', () => ({
  uploadAvScan: (...args: unknown[]) => mockUploadAvScan(...args),
  cleanupTmpFile: (...args: unknown[]) => mockCleanupTmpFile(...args)
}));

const mockCreateAllCubeFiles = jest.fn();
jest.mock('../../../src/services/cube-builder', () => ({
  createAllCubeFiles: (...args: unknown[]) => mockCreateAllCubeFiles(...args)
}));

// stringify is piped to the response; we only need to observe the pipe, not real CSV output
const mockStringifyStream = { pipe: jest.fn() };
const mockStringify = jest.fn((..._args: unknown[]) => mockStringifyStream);
jest.mock('csv-stringify', () => ({ stringify: (...args: unknown[]) => mockStringify(...args) }));

// real createReadStream is replaced with an in-memory CSV stream so the real csv-parse can run against it
const mockCreateReadStream = jest.fn();
jest.mock('node:fs', () => ({
  ...(jest.requireActual('node:fs') as object),
  createReadStream: (...args: unknown[]) => mockCreateReadStream(...args)
}));

import {
  translationPreview,
  translationExport,
  validateImport,
  applyImport
} from '../../../src/controllers/translation';

const CSV_HEADER = 'type,key,english,cymraeg';

function csvStream(rows: string[]): Readable {
  return Readable.from([`${CSV_HEADER}\n${rows.join('\n')}\n`]);
}

function createMockRequest(overrides: Partial<Request> = {}): Request {
  return {
    params: {},
    query: {},
    body: {},
    user: { id: uuidV4() },
    fileService: {
      saveStream: jest.fn().mockResolvedValue(undefined),
      loadStream: jest.fn(),
      delete: jest.fn().mockResolvedValue(undefined)
    },
    datasetService: {
      updateTranslations: jest.fn()
    },
    ...overrides
  } as unknown as Request;
}

function createMockResponse(locals: Record<string, unknown> = {}): Response {
  const res = {
    locals: { datasetId: uuidV4(), ...locals },
    json: jest.fn(),
    status: jest.fn(),
    setHeader: jest.fn()
  } as unknown as Response;
  (res.status as jest.Mock).mockReturnValue(res);
  (res.json as jest.Mock).mockReturnValue(res);
  return res;
}

describe('Translation controller', () => {
  let mockNext: jest.MockedFunction<NextFunction>;

  beforeEach(() => {
    jest.clearAllMocks();
    mockNext = jest.fn();
  });

  describe('translationPreview', () => {
    it('returns collected translations (with ids) for the dataset', async () => {
      const dataset = { id: uuidV4() };
      const translations = [{ type: 'metadata', key: 'title', english: 'A', cymraeg: 'B' }];
      mockGetById.mockResolvedValue(dataset);
      mockCollectTranslations.mockReturnValue(translations);

      const res = createMockResponse();
      await translationPreview(createMockRequest(), res, mockNext);

      expect(mockCollectTranslations).toHaveBeenCalledWith(dataset, true);
      expect(res.json).toHaveBeenCalledWith(translations);
      expect(mockNext).not.toHaveBeenCalled();
    });

    it('passes an UnknownException to next when loading fails', async () => {
      mockGetById.mockRejectedValue(new Error('db down'));

      const res = createMockResponse();
      await translationPreview(createMockRequest(), res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
      expect(res.json).not.toHaveBeenCalled();
    });
  });

  describe('translationExport', () => {
    it('records an export event and streams the CSV to the response', async () => {
      const revisionId = uuidV4();
      const dataset = { id: uuidV4(), draftRevision: { id: revisionId } };
      const translations = [{ type: 'metadata', key: 'title', english: 'A', cymraeg: 'B' }];
      mockGetById.mockResolvedValue(dataset);
      mockCollectTranslations.mockReturnValue(translations);

      const req = createMockRequest();
      const res = createMockResponse();
      await translationExport(req, res, mockNext);

      expect(mockEventSave).toHaveBeenCalledWith(
        expect.objectContaining({ action: 'export', entity: 'translations', entityId: revisionId, data: translations })
      );
      expect(res.setHeader).toHaveBeenCalledWith('Content-Type', 'text/csv');
      expect(mockStringify).toHaveBeenCalledWith(translations, expect.objectContaining({ header: true }));
      expect(mockStringifyStream.pipe).toHaveBeenCalledWith(res);
    });

    it('passes an UnknownException to next when the export fails', async () => {
      mockGetById.mockRejectedValue(new Error('db down'));

      const res = createMockResponse();
      await translationExport(createMockRequest(), res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
    });
  });

  describe('validateImport', () => {
    it('stores the upload and returns the dataset DTO when the CSV matches the existing translations', async () => {
      const dataset = { id: uuidV4() };
      mockUploadAvScan.mockResolvedValue({ path: '/tmp/import.csv' });
      mockGetById.mockResolvedValue(dataset);
      mockCollectTranslations.mockReturnValue([{ type: 'metadata', key: 'title', english: 'old', cymraeg: 'hen' }]);
      mockCreateReadStream.mockImplementation(() => csvStream(['metadata,title,New EN,Newydd CY']));
      mockFromDataset.mockReturnValue({ id: dataset.id });

      const req = createMockRequest();
      const res = createMockResponse();
      await validateImport(req, res, mockNext);

      expect((req as any).fileService.saveStream).toHaveBeenCalledWith(
        'translation-import.csv',
        dataset.id,
        expect.anything()
      );
      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith({ id: dataset.id });
      expect(mockNext).not.toHaveBeenCalled();
      expect(mockCleanupTmpFile).toHaveBeenCalledWith({ path: '/tmp/import.csv' });
    });

    it('forwards the av-scan error to next when the upload fails', async () => {
      const scanError = new BadRequestException('errors.upload.virus');
      mockUploadAvScan.mockRejectedValue(scanError);

      const req = createMockRequest();
      const res = createMockResponse();
      await validateImport(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(scanError);
      expect(mockGetById).not.toHaveBeenCalled();
    });

    it('rejects with a row-count error when the CSV has a different number of rows', async () => {
      mockUploadAvScan.mockResolvedValue({ path: '/tmp/import.csv' });
      mockGetById.mockResolvedValue({ id: uuidV4() });
      mockCollectTranslations.mockReturnValue([
        { type: 'metadata', key: 'title', english: 'a', cymraeg: 'b' },
        { type: 'metadata', key: 'summary', english: 'c', cymraeg: 'd' }
      ]);
      mockCreateReadStream.mockImplementation(() => csvStream(['metadata,title,New EN,Newydd CY']));

      const req = createMockRequest();
      const res = createMockResponse();
      await validateImport(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(BadRequestException);
      expect((mockNext.mock.calls[0][0] as unknown as Error).message).toBe('errors.translation_file.invalid.row_count');
      expect((req as any).fileService.saveStream).not.toHaveBeenCalled();
      expect(mockCleanupTmpFile).toHaveBeenCalledWith({ path: '/tmp/import.csv' });
    });

    it('rejects with a keys error when a translation key is missing from the CSV', async () => {
      mockUploadAvScan.mockResolvedValue({ path: '/tmp/import.csv' });
      mockGetById.mockResolvedValue({ id: uuidV4() });
      mockCollectTranslations.mockReturnValue([{ type: 'metadata', key: 'title', english: 'a', cymraeg: 'b' }]);
      // same row count but a different key
      mockCreateReadStream.mockImplementation(() => csvStream(['metadata,summary,New EN,Newydd CY']));

      const req = createMockRequest();
      const res = createMockResponse();
      await validateImport(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect((mockNext.mock.calls[0][0] as unknown as Error).message).toBe('errors.translation_file.invalid.keys');
    });
  });

  describe('applyImport', () => {
    it('updates translations, rebuilds the cube and returns the dataset DTO', async () => {
      const dataset = { id: uuidV4(), draftRevisionId: uuidV4() };
      const req = createMockRequest();
      ((req as any).fileService.loadStream as jest.Mock).mockResolvedValue(
        csvStream(['metadata,title,New EN,Newydd CY'])
      );
      ((req as any).datasetService.updateTranslations as jest.Mock).mockResolvedValue(dataset);
      mockCreateAllCubeFiles.mockResolvedValue(undefined);
      mockFromDataset.mockReturnValue({ id: dataset.id });

      const res = createMockResponse();
      await applyImport(req, res, mockNext);

      expect((req as any).datasetService.updateTranslations).toHaveBeenCalledWith(
        res.locals.datasetId,
        expect.arrayContaining([expect.objectContaining({ type: 'metadata', key: 'title' })])
      );
      expect((req as any).fileService.delete).toHaveBeenCalledWith('translation-import.csv', dataset.id);
      expect(mockEventSave).toHaveBeenCalledWith(expect.objectContaining({ action: 'import', entity: 'translations' }));
      expect(mockCreateAllCubeFiles).toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(201);
      expect(res.json).toHaveBeenCalledWith({ id: dataset.id });
    });

    it('passes a cube-validation error to next when the rebuild fails', async () => {
      const dataset = { id: uuidV4(), draftRevisionId: uuidV4() };
      const req = createMockRequest();
      ((req as any).fileService.loadStream as jest.Mock).mockResolvedValue(
        csvStream(['metadata,title,New EN,Newydd CY'])
      );
      ((req as any).datasetService.updateTranslations as jest.Mock).mockResolvedValue(dataset);
      mockCreateAllCubeFiles.mockRejectedValue(new Error('cube broke'));

      const res = createMockResponse();
      await applyImport(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
      expect(res.json).not.toHaveBeenCalled();
    });

    it('passes an UnknownException to next when loading the stored import fails', async () => {
      const req = createMockRequest();
      ((req as any).fileService.loadStream as jest.Mock).mockRejectedValue(new Error('not found'));

      const res = createMockResponse();
      await applyImport(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
    });
  });
});
