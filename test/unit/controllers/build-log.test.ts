import { Request, Response, NextFunction } from 'express';

import { BadRequestException } from '../../../src/exceptions/bad-request.exception';
import { NotFoundException } from '../../../src/exceptions/not-found.exception';
import { CubeBuildStatus } from '../../../src/enums/cube-build-status';
import { CubeBuildType } from '../../../src/enums/cube-build-type';
import { uuidV4 } from '../../../src/utils/uuid';

jest.mock('../../../src/utils/logger', () => ({
  logger: { debug: jest.fn(), info: jest.fn(), error: jest.fn(), warn: jest.fn(), trace: jest.fn() }
}));

const mockGetBy = jest.fn();
jest.mock('../../../src/repositories/build-log', () => ({
  BuildLogRepository: {
    getBy: (...args: unknown[]) => mockGetBy(...args)
  }
}));

const mockFindOneByOrFail = jest.fn();
jest.mock('../../../src/entities/dataset/build-log', () => ({
  BuildLog: {
    findOneByOrFail: (...args: unknown[]) => mockFindOneByOrFail(...args)
  }
}));

const mockFromBuildLogLite = jest.fn();
const mockFromBuildLogFull = jest.fn();
jest.mock('../../../src/dtos/build-log', () => ({
  BuiltLogEntryDto: {
    fromBuildLogLite: (...args: unknown[]) => mockFromBuildLogLite(...args),
    fromBuildLogFull: (...args: unknown[]) => mockFromBuildLogFull(...args)
  }
}));

// validators: hasError is the only behaviour we vary per-test; the *Validator factories are inert
const mockHasError = jest.fn();
jest.mock('../../../src/validators', () => ({
  hasError: (...args: unknown[]) => mockHasError(...args),
  buildTypeValidator: jest.fn(() => 'buildTypeValidator'),
  buildStatusValidator: jest.fn(() => 'buildStatusValidator')
}));

import { getBuildLog, getBuiltLogEntry } from '../../../src/controllers/build-log';

function createMockRequest(overrides: Partial<Request> = {}): Request {
  return { params: {}, query: {}, ...overrides } as unknown as Request;
}

function createMockResponse(): Response {
  const res = { status: jest.fn(), send: jest.fn() } as unknown as Response;
  (res.status as jest.Mock).mockReturnValue(res);
  (res.send as jest.Mock).mockReturnValue(res);
  return res;
}

describe('Build log controller', () => {
  let mockNext: jest.MockedFunction<NextFunction>;

  beforeEach(() => {
    jest.clearAllMocks();
    mockNext = jest.fn();
    // mockReset clears any leftover mockResolvedValueOnce queue from a prior test
    mockHasError.mockReset();
    mockHasError.mockResolvedValue(false);
  });

  describe('getBuildLog', () => {
    it('returns mapped build logs with default paging when no query params supplied', async () => {
      const logs = [{ id: 'a' }, { id: 'b' }];
      mockGetBy.mockResolvedValue(logs);
      mockFromBuildLogLite.mockImplementation((log: { id: string }) => ({ dto: log.id }));

      const req = createMockRequest();
      const res = createMockResponse();

      await getBuildLog(req, res, mockNext);

      // default size 30, page 0, no type/status filters
      expect(mockGetBy).toHaveBeenCalledWith(undefined, undefined, 30, 0);
      expect(mockFromBuildLogLite).toHaveBeenCalledTimes(2);
      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.send).toHaveBeenCalledWith([{ dto: 'a' }, { dto: 'b' }]);
      expect(mockNext).not.toHaveBeenCalled();
    });

    it('applies size/page paging and passes type and status filters through', async () => {
      mockGetBy.mockResolvedValue([]);

      const req = createMockRequest({
        query: { size: '10', page: '2', type: CubeBuildType.FullCube, status: CubeBuildStatus.Completed } as never
      });
      const res = createMockResponse();

      await getBuildLog(req, res, mockNext);

      // pageNo = page * pageSize = 2 * 10 = 20
      expect(mockGetBy).toHaveBeenCalledWith(CubeBuildType.FullCube, CubeBuildStatus.Completed, 10, 20);
      expect(res.send).toHaveBeenCalledWith([]);
    });

    it('rejects with BadRequest when the type filter is invalid', async () => {
      mockHasError.mockResolvedValueOnce(true); // typeError

      const req = createMockRequest({ query: { type: 'nonsense' } as never });
      const res = createMockResponse();

      await getBuildLog(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(BadRequestException);
      expect(mockGetBy).not.toHaveBeenCalled();
    });

    it('rejects with BadRequest when the status filter is invalid', async () => {
      // type is absent so hasError is only called once (for status) -> make that call report an error
      mockHasError.mockResolvedValueOnce(true);

      const req = createMockRequest({ query: { status: 'nonsense' } as never });
      const res = createMockResponse();

      await getBuildLog(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(BadRequestException);
      expect(mockGetBy).not.toHaveBeenCalled();
    });
  });

  describe('getBuiltLogEntry', () => {
    it('returns the full build log entry DTO when found', async () => {
      const build = { id: uuidV4() };
      mockFindOneByOrFail.mockResolvedValue(build);
      mockFromBuildLogFull.mockReturnValue({ dto: build.id });

      const req = createMockRequest({ params: { build_id: build.id } });
      const res = createMockResponse();

      await getBuiltLogEntry(req, res);

      expect(mockFindOneByOrFail).toHaveBeenCalledWith({ id: build.id });
      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.send).toHaveBeenCalledWith({ dto: build.id });
    });

    it('throws NotFound when no build id is supplied', async () => {
      const req = createMockRequest({ params: {} });
      const res = createMockResponse();

      await expect(getBuiltLogEntry(req, res)).rejects.toBeInstanceOf(NotFoundException);
      expect(mockFindOneByOrFail).not.toHaveBeenCalled();
    });

    it('throws NotFound when the build cannot be loaded', async () => {
      mockFindOneByOrFail.mockRejectedValue(new Error('not in db'));

      const req = createMockRequest({ params: { build_id: uuidV4() } });
      const res = createMockResponse();

      await expect(getBuiltLogEntry(req, res)).rejects.toBeInstanceOf(NotFoundException);
    });
  });
});
