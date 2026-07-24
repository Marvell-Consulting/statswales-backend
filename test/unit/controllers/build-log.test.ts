import { Request, Response, NextFunction } from 'express';

import { BadRequestException } from '../../../src/exceptions/bad-request.exception';
import { NotFoundException } from '../../../src/exceptions/not-found.exception';
import { ForbiddenException } from '../../../src/exceptions/forbidden.exception';
import { CubeBuildStatus } from '../../../src/enums/cube-build-status';
import { CubeBuildType } from '../../../src/enums/cube-build-type';
import { GlobalRole } from '../../../src/enums/global-role';
import { User } from '../../../src/entities/user/user';
import { UserGroupRole } from '../../../src/entities/user/user-group-role';
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

const mockFindOneOrFail = jest.fn();
jest.mock('../../../src/entities/dataset/build-log', () => ({
  BuildLog: {
    findOneOrFail: (...args: unknown[]) => mockFindOneOrFail(...args)
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

function createMockUser(overrides: Partial<User> = {}): User {
  const user = new User();
  user.id = uuidV4();
  user.name = 'Test User';
  user.email = 'test@example.com';
  user.globalRoles = [];
  user.groupRoles = [];
  Object.assign(user, overrides);
  return user;
}

function createMockGroupRole(groupId: string): UserGroupRole {
  const groupRole = new UserGroupRole();
  groupRole.groupId = groupId;
  return groupRole;
}

// default test user can access everything: ServiceAdmin satisfies the getBuildLog role gate and
// Developer bypasses the getBuiltLogEntry group check, keeping tests that aren't about authorisation
// focused on their own behaviour. Tests that specifically exercise access control override this.
function createMockRequest(overrides: Partial<Request> = {}): Request {
  return {
    params: {},
    query: {},
    locals: {},
    user: createMockUser({ globalRoles: [GlobalRole.ServiceAdmin, GlobalRole.Developer] }),
    ...overrides
  } as unknown as Request;
}

function createMockResponse(overrides: Partial<Response> = {}): Response {
  const res = { status: jest.fn(), send: jest.fn(), locals: {}, ...overrides } as unknown as Response;
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

    it('rejects with Forbidden when the caller is neither a service admin nor a developer', async () => {
      const req = createMockRequest({ user: createMockUser({ globalRoles: [] }) });
      const res = createMockResponse();

      await getBuildLog(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(ForbiddenException);
      expect(mockGetBy).not.toHaveBeenCalled();
    });

    it('allows a service admin to list build logs', async () => {
      mockGetBy.mockResolvedValue([]);
      const req = createMockRequest({ user: createMockUser({ globalRoles: [GlobalRole.ServiceAdmin] }) });
      const res = createMockResponse();

      await getBuildLog(req, res, mockNext);

      expect(mockNext).not.toHaveBeenCalled();
      expect(mockGetBy).toHaveBeenCalled();
    });

    it('allows a developer to list build logs', async () => {
      mockGetBy.mockResolvedValue([]);
      const req = createMockRequest({ user: createMockUser({ globalRoles: [GlobalRole.Developer] }) });
      const res = createMockResponse();

      await getBuildLog(req, res, mockNext);

      expect(mockNext).not.toHaveBeenCalled();
      expect(mockGetBy).toHaveBeenCalled();
    });
  });

  describe('getBuiltLogEntry', () => {
    function createMockBuild(overrides: { revisionId?: string; userGroupId?: string | null } = {}) {
      const revisionId = overrides.revisionId ?? uuidV4();
      const userGroupId = overrides.userGroupId === undefined ? 'group-a' : overrides.userGroupId;
      return {
        id: uuidV4(),
        revisionId,
        revision: { id: revisionId, dataset: userGroupId ? { userGroupId } : null }
      };
    }

    it('returns the full build log entry DTO when found (developer bypasses group check)', async () => {
      const build = createMockBuild({ userGroupId: 'some-other-group' });
      mockFindOneOrFail.mockResolvedValue(build);
      mockFromBuildLogFull.mockReturnValue({ dto: build.id });

      const req = createMockRequest({ params: { build_id: build.id } });
      const res = createMockResponse();

      await getBuiltLogEntry(req, res);

      expect(mockFindOneOrFail).toHaveBeenCalledWith({
        where: { id: build.id },
        relations: { revision: { dataset: true } }
      });
      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.send).toHaveBeenCalledWith({ dto: build.id });
    });

    it('returns the entry when the caller belongs to the dataset group (non-developer)', async () => {
      const build = createMockBuild({ userGroupId: 'group-a' });
      mockFindOneOrFail.mockResolvedValue(build);
      mockFromBuildLogFull.mockReturnValue({ dto: build.id });

      const req = createMockRequest({
        params: { build_id: build.id },
        user: createMockUser({ globalRoles: [], groupRoles: [createMockGroupRole('group-a')] })
      });
      const res = createMockResponse();

      await getBuiltLogEntry(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.send).toHaveBeenCalledWith({ dto: build.id });
    });

    it('throws Forbidden when a group-A user requests a group-B build (IDOR on /build/:id)', async () => {
      const build = createMockBuild({ userGroupId: 'group-b' });
      mockFindOneOrFail.mockResolvedValue(build);

      const req = createMockRequest({
        params: { build_id: build.id },
        user: createMockUser({ globalRoles: [], groupRoles: [createMockGroupRole('group-a')] })
      });
      const res = createMockResponse();

      await expect(getBuiltLogEntry(req, res)).rejects.toBeInstanceOf(ForbiddenException);
      expect(res.send).not.toHaveBeenCalled();
    });

    it('throws Forbidden when the caller has no group membership at all', async () => {
      const build = createMockBuild({ userGroupId: 'group-b' });
      mockFindOneOrFail.mockResolvedValue(build);

      const req = createMockRequest({
        params: { build_id: build.id },
        user: createMockUser({ globalRoles: [], groupRoles: [] })
      });
      const res = createMockResponse();

      await expect(getBuiltLogEntry(req, res)).rejects.toBeInstanceOf(ForbiddenException);
    });

    it('throws Forbidden when the build has no associated dataset and the caller is not a developer', async () => {
      const build = createMockBuild({ userGroupId: null });
      mockFindOneOrFail.mockResolvedValue(build);

      const req = createMockRequest({
        params: { build_id: build.id },
        user: createMockUser({ globalRoles: [], groupRoles: [createMockGroupRole('group-a')] })
      });
      const res = createMockResponse();

      await expect(getBuiltLogEntry(req, res)).rejects.toBeInstanceOf(ForbiddenException);
    });

    it('throws NotFound on the nested revision route when the build belongs to a different revision', async () => {
      const build = createMockBuild({ revisionId: uuidV4(), userGroupId: 'group-a' });
      mockFindOneOrFail.mockResolvedValue(build);

      const req = createMockRequest({
        params: { build_id: build.id },
        user: createMockUser({ globalRoles: [], groupRoles: [createMockGroupRole('group-a')] })
      });
      const res = createMockResponse({ locals: { revision_id: uuidV4() } });

      await expect(getBuiltLogEntry(req, res)).rejects.toBeInstanceOf(NotFoundException);
    });

    it('succeeds on the nested revision route when the build belongs to the authorised revision', async () => {
      const build = createMockBuild({ userGroupId: 'group-a' });
      mockFindOneOrFail.mockResolvedValue(build);
      mockFromBuildLogFull.mockReturnValue({ dto: build.id });

      const req = createMockRequest({
        params: { build_id: build.id },
        user: createMockUser({ globalRoles: [], groupRoles: [createMockGroupRole('group-a')] })
      });
      const res = createMockResponse({ locals: { revision_id: build.revisionId } });

      await getBuiltLogEntry(req, res);

      expect(res.status).toHaveBeenCalledWith(200);
      expect(res.send).toHaveBeenCalledWith({ dto: build.id });
    });

    it('throws NotFound when no build id is supplied', async () => {
      const req = createMockRequest({ params: {} });
      const res = createMockResponse();

      await expect(getBuiltLogEntry(req, res)).rejects.toBeInstanceOf(NotFoundException);
      expect(mockFindOneOrFail).not.toHaveBeenCalled();
    });

    it('throws NotFound when the build cannot be loaded', async () => {
      mockFindOneOrFail.mockRejectedValue(new Error('not in db'));

      const req = createMockRequest({ params: { build_id: uuidV4() } });
      const res = createMockResponse();

      await expect(getBuiltLogEntry(req, res)).rejects.toBeInstanceOf(NotFoundException);
    });
  });
});
