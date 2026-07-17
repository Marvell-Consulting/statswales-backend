import { Request, Response, NextFunction } from 'express';
import { QueryFailedError } from 'typeorm';

import { BadRequestException } from '../../../src/exceptions/bad-request.exception';
import { NotFoundException } from '../../../src/exceptions/not-found.exception';
import { UnknownException } from '../../../src/exceptions/unknown.exception';
import { GlobalRole } from '../../../src/enums/global-role';
import { GroupRole } from '../../../src/enums/group-role';
import { UserGroupStatus } from '../../../src/enums/user-group-status';
import { UserStatus } from '../../../src/enums/user-status';
import { DatasetSimilarBy } from '../../../src/enums/dataset-similar-by';
import { uuidV4 } from '../../../src/utils/uuid';

jest.mock('../../../src/utils/logger', () => ({
  logger: { debug: jest.fn(), info: jest.fn(), error: jest.fn(), warn: jest.fn(), trace: jest.fn() }
}));

const userGroupRepo = {
  getById: jest.fn(),
  createGroup: jest.fn(),
  getAll: jest.fn(),
  listByLanguage: jest.fn(),
  getByIdWithDatasets: jest.fn(),
  updateGroup: jest.fn(),
  updateGroupStatus: jest.fn(),
  getDashboardStats: jest.fn()
};
jest.mock('../../../src/repositories/user-group', () => ({ UserGroupRepository: userGroupRepo }));

const userRepo = {
  getById: jest.fn(),
  listByLanguage: jest.fn(),
  createUser: jest.fn(),
  updateUserRoles: jest.fn(),
  updateUserStatus: jest.fn(),
  getDashboardStats: jest.fn()
};
jest.mock('../../../src/repositories/user', () => ({ UserRepository: userRepo }));

const datasetStatsRepo = {
  getDashboardStats: jest.fn(),
  shareSources: jest.fn(),
  shareDimensions: jest.fn(),
  similarTitles: jest.fn(),
  sameFactTable: jest.fn()
};
jest.mock('../../../src/repositories/dataset-stats', () => ({ DatasetStatsRepository: datasetStatsRepo }));

const searchLogRepo = { getByPeriod: jest.fn() };
jest.mock('../../../src/repositories/search-log', () => ({ SearchLogRepository: searchLogRepo }));

const mockHasError = jest.fn();
jest.mock('../../../src/validators', () => ({
  hasError: (...args: unknown[]) => mockHasError(...args),
  uuidValidator: jest.fn(() => 'uuidValidator'),
  groupStatusValidator: jest.fn(() => 'groupStatusValidator'),
  userStatusValidator: jest.fn(() => 'userStatusValidator'),
  similarByValidator: jest.fn(() => 'similarByValidator')
}));

const mockArrayValidator = jest.fn();
const mockDtoValidator = jest.fn();
jest.mock('../../../src/validators/dto-validator', () => ({
  arrayValidator: (...args: unknown[]) => mockArrayValidator(...args),
  dtoValidator: (...args: unknown[]) => mockDtoValidator(...args)
}));

const mockGroupFromUserGroup = jest.fn();
jest.mock('../../../src/dtos/user/user-group-dto', () => ({
  UserGroupDTO: { fromUserGroup: (...args: unknown[]) => mockGroupFromUserGroup(...args) }
}));
jest.mock('../../../src/dtos/user/user-group-metadata-dto', () => ({ UserGroupMetadataDTO: class {} }));

const mockUserFromUser = jest.fn();
jest.mock('../../../src/dtos/user/user-dto', () => ({
  UserDTO: { fromUser: (...args: unknown[]) => mockUserFromUser(...args) }
}));
jest.mock('../../../src/dtos/user/user-create-dto', () => ({ UserCreateDTO: class {} }));
jest.mock('../../../src/dtos/user/role-selection-dto', () => ({ RoleSelectionDTO: class {} }));

const mockStringifyStream = { pipe: jest.fn() };
const mockStringify = jest.fn((..._args: unknown[]) => mockStringifyStream);
jest.mock('csv-stringify', () => ({ stringify: (...args: unknown[]) => mockStringify(...args) }));

import {
  loadUserGroup,
  loadUser,
  listRoles,
  createUserGroup,
  getAllUserGroups,
  listUserGroups,
  getUserGroupById,
  updateUserGroup,
  updateUserGroupStatus,
  listUsers,
  createUser,
  getUserById,
  updateUserRoles,
  updateUserStatus,
  dashboard,
  similarDatasets,
  downloadSearchLogs
} from '../../../src/controllers/admin';

function createMockRequest(overrides: Partial<Request> = {}): Request {
  return { params: {}, query: {}, body: {}, language: 'en-GB', ...overrides } as unknown as Request;
}

function createMockResponse(locals: Record<string, unknown> = {}): Response {
  const res = {
    locals,
    json: jest.fn(),
    status: jest.fn(),
    setHeader: jest.fn()
  } as unknown as Response;
  (res.status as jest.Mock).mockReturnValue(res);
  (res.json as jest.Mock).mockReturnValue(res);
  return res;
}

describe('Admin controller', () => {
  let mockNext: jest.MockedFunction<NextFunction>;

  beforeEach(() => {
    jest.clearAllMocks();
    mockNext = jest.fn();
    mockHasError.mockReset();
    mockHasError.mockResolvedValue(false);
  });

  describe('loadUserGroup', () => {
    it('loads the group into res.locals and calls next', async () => {
      const group = { id: uuidV4() };
      userGroupRepo.getById.mockResolvedValue(group);

      const req = createMockRequest({ params: { user_group_id: group.id } });
      const res = createMockResponse();
      await loadUserGroup(req, res, mockNext);

      expect(res.locals.userGroup).toBe(group);
      expect(res.locals.userGroupId).toBe(group.id);
      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeUndefined();
    });

    it('passes NotFound to next when the id is not a valid uuid', async () => {
      mockHasError.mockResolvedValueOnce(true);

      const req = createMockRequest({ params: { user_group_id: 'not-a-uuid' } });
      const res = createMockResponse();
      await loadUserGroup(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(NotFoundException);
      expect(userGroupRepo.getById).not.toHaveBeenCalled();
    });

    // Regression (#686): the catch block previously called next(NotFoundException) without a `return`,
    // falling through to the unconditional next() at the end and calling next TWICE (once with the
    // error, once without) — the "responds twice" class of bug. It now returns after next(error),
    // matching the canonical datasetAuth middleware.
    it('calls next exactly once with NotFound when the group cannot be loaded', async () => {
      userGroupRepo.getById.mockRejectedValue(new Error('not in db'));

      const req = createMockRequest({ params: { user_group_id: uuidV4() } });
      const res = createMockResponse();
      await loadUserGroup(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(NotFoundException);
    });
  });

  describe('loadUser', () => {
    it('loads the user into res.locals and calls next', async () => {
      const user = { id: uuidV4() };
      userRepo.getById.mockResolvedValue(user);

      const req = createMockRequest({ params: { user_id: user.id } });
      const res = createMockResponse();
      await loadUser(req, res, mockNext);

      expect(res.locals.user).toBe(user);
      expect(res.locals.userId).toBe(user.id);
      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeUndefined();
    });

    it('passes NotFound to next when the id is not a valid uuid', async () => {
      mockHasError.mockResolvedValueOnce(true);

      const req = createMockRequest({ params: { user_id: 'nope' } });
      const res = createMockResponse();
      await loadUser(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(NotFoundException);
      expect(userRepo.getById).not.toHaveBeenCalled();
    });

    // Regression (#686): same missing `return` as loadUserGroup — next() used to be called twice on
    // the not-found path. It now returns after next(error).
    it('calls next exactly once with NotFound when the user cannot be loaded', async () => {
      userRepo.getById.mockRejectedValue(new Error('not in db'));

      const req = createMockRequest({ params: { user_id: uuidV4() } });
      const res = createMockResponse();
      await loadUser(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(NotFoundException);
    });
  });

  describe('listRoles', () => {
    it('returns the available global and group roles', async () => {
      const res = createMockResponse();
      await listRoles(createMockRequest(), res);

      expect(res.json).toHaveBeenCalledWith({
        global: Object.values(GlobalRole),
        group: Object.values(GroupRole)
      });
    });
  });

  describe('createUserGroup', () => {
    it('validates the body, creates the group and returns its DTO', async () => {
      const meta = [{ name: 'Group' }];
      const group = { id: uuidV4() };
      mockArrayValidator.mockResolvedValue(meta);
      userGroupRepo.createGroup.mockResolvedValue(group);
      mockGroupFromUserGroup.mockReturnValue({ id: group.id });

      const res = createMockResponse();
      await createUserGroup(createMockRequest({ body: meta as never }), res, mockNext);

      expect(userGroupRepo.createGroup).toHaveBeenCalledWith(meta);
      expect(res.json).toHaveBeenCalledWith({ id: group.id });
    });

    it('forwards a BadRequest from validation to next', async () => {
      const badRequest = new BadRequestException('errors.invalid');
      mockArrayValidator.mockRejectedValue(badRequest);

      const res = createMockResponse();
      await createUserGroup(createMockRequest(), res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(badRequest);
    });

    it('passes UnknownException to next on an unexpected error', async () => {
      mockArrayValidator.mockResolvedValue([]);
      userGroupRepo.createGroup.mockRejectedValue(new Error('boom'));

      const res = createMockResponse();
      await createUserGroup(createMockRequest(), res, mockNext);

      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
    });
  });

  describe('getAllUserGroups', () => {
    it('returns all groups mapped to DTOs', async () => {
      userGroupRepo.getAll.mockResolvedValue([{ id: '1' }, { id: '2' }]);
      mockGroupFromUserGroup.mockImplementation((g: { id: string }) => ({ dto: g.id }));

      const res = createMockResponse();
      await getAllUserGroups(createMockRequest({ query: { status: UserGroupStatus.Active } as never }), res, mockNext);

      expect(userGroupRepo.getAll).toHaveBeenCalledWith(UserGroupStatus.Active);
      expect(res.json).toHaveBeenCalledWith([{ dto: '1' }, { dto: '2' }]);
    });

    it('passes UnknownException to next on error', async () => {
      userGroupRepo.getAll.mockRejectedValue(new Error('boom'));

      const res = createMockResponse();
      await getAllUserGroups(createMockRequest(), res, mockNext);

      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
    });
  });

  describe('listUserGroups', () => {
    it('returns the paged listing results', async () => {
      const results = { data: [], count: 0 };
      userGroupRepo.listByLanguage.mockResolvedValue(results);

      const res = createMockResponse();
      await listUserGroups(
        createMockRequest({ query: { page: '2', limit: '5', search: '  hello  ' } as never }),
        res,
        mockNext
      );

      expect(userGroupRepo.listByLanguage).toHaveBeenCalledWith('en-GB', 2, 5, 'hello');
      expect(res.json).toHaveBeenCalledWith(results);
    });

    it('passes UnknownException to next on error', async () => {
      userGroupRepo.listByLanguage.mockRejectedValue(new Error('boom'));

      const res = createMockResponse();
      await listUserGroups(createMockRequest(), res, mockNext);

      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
    });
  });

  describe('getUserGroupById', () => {
    it('returns the group with datasets', async () => {
      const group = { id: uuidV4() };
      userGroupRepo.getByIdWithDatasets.mockResolvedValue(group);
      mockGroupFromUserGroup.mockReturnValue({ id: group.id });

      const res = createMockResponse({ userGroupId: group.id });
      await getUserGroupById(createMockRequest(), res);

      expect(userGroupRepo.getByIdWithDatasets).toHaveBeenCalledWith(group.id);
      expect(res.json).toHaveBeenCalledWith({ id: group.id });
    });
  });

  describe('updateUserGroup', () => {
    it('validates and updates the group', async () => {
      const dto = { name: 'X' };
      const group = { id: uuidV4() };
      mockDtoValidator.mockResolvedValue(dto);
      userGroupRepo.updateGroup.mockResolvedValue(group);
      mockGroupFromUserGroup.mockReturnValue({ id: group.id });

      const res = createMockResponse({ userGroupId: group.id });
      await updateUserGroup(createMockRequest(), res, mockNext);

      expect(userGroupRepo.updateGroup).toHaveBeenCalledWith(group.id, dto);
      expect(res.json).toHaveBeenCalledWith({ id: group.id });
    });

    it('forwards a BadRequest from validation to next', async () => {
      const badRequest = new BadRequestException('errors.invalid');
      mockDtoValidator.mockRejectedValue(badRequest);

      const res = createMockResponse({ userGroupId: uuidV4() });
      await updateUserGroup(createMockRequest(), res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(badRequest);
    });

    it('throws UnknownException on an unexpected error', async () => {
      mockDtoValidator.mockResolvedValue({});
      userGroupRepo.updateGroup.mockRejectedValue(new Error('boom'));

      const res = createMockResponse({ userGroupId: uuidV4() });
      await expect(updateUserGroup(createMockRequest(), res, mockNext)).rejects.toBeInstanceOf(UnknownException);
    });
  });

  describe('updateUserGroupStatus', () => {
    it('updates the status when valid', async () => {
      const group = { id: uuidV4(), datasets: [] };
      userGroupRepo.getByIdWithDatasets.mockResolvedValue(group);
      userGroupRepo.updateGroupStatus.mockResolvedValue(group);
      mockGroupFromUserGroup.mockReturnValue({ id: group.id });

      const req = createMockRequest({ body: { status: UserGroupStatus.Active } as never });
      const res = createMockResponse({ userGroupId: group.id });
      await updateUserGroupStatus(req, res, mockNext);

      expect(userGroupRepo.updateGroupStatus).toHaveBeenCalledWith(group.id, UserGroupStatus.Active);
      expect(res.json).toHaveBeenCalledWith({ id: group.id });
    });

    it('rejects deactivation when the group still has datasets', async () => {
      userGroupRepo.getByIdWithDatasets.mockResolvedValue({ id: uuidV4(), datasets: [{ id: 'd1' }] });

      const req = createMockRequest({ body: { status: UserGroupStatus.Inactive } as never });
      const res = createMockResponse({ userGroupId: uuidV4() });
      await updateUserGroupStatus(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(BadRequestException);
      expect(userGroupRepo.updateGroupStatus).not.toHaveBeenCalled();
    });

    it('rejects when the status value is invalid', async () => {
      userGroupRepo.getByIdWithDatasets.mockResolvedValue({ id: uuidV4(), datasets: [] });
      mockHasError.mockResolvedValueOnce(true);

      const req = createMockRequest({ body: { status: 'bogus' } as never });
      const res = createMockResponse({ userGroupId: uuidV4() });
      await updateUserGroupStatus(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(BadRequestException);
      expect(userGroupRepo.updateGroupStatus).not.toHaveBeenCalled();
    });
  });

  describe('listUsers', () => {
    it('returns the paged user listing', async () => {
      const results = { data: [], count: 0 };
      userRepo.listByLanguage.mockResolvedValue(results);

      const res = createMockResponse();
      await listUsers(createMockRequest({ query: { page: '1', limit: '20' } as never }), res);

      expect(userRepo.listByLanguage).toHaveBeenCalledWith('en-GB', 1, 20, undefined);
      expect(res.json).toHaveBeenCalledWith(results);
    });

    it('throws UnknownException on error', async () => {
      userRepo.listByLanguage.mockRejectedValue(new Error('boom'));

      const res = createMockResponse();
      await expect(listUsers(createMockRequest(), res)).rejects.toBeInstanceOf(UnknownException);
    });
  });

  describe('createUser', () => {
    it('validates and creates the user', async () => {
      const dto = { email: 'a@b.c' };
      const user = { id: uuidV4() };
      mockDtoValidator.mockResolvedValue(dto);
      userRepo.createUser.mockResolvedValue(user);
      mockUserFromUser.mockReturnValue({ id: user.id });

      const res = createMockResponse();
      await createUser(createMockRequest(), res);

      expect(userRepo.createUser).toHaveBeenCalledWith(dto);
      expect(res.json).toHaveBeenCalledWith({ id: user.id });
    });

    it('throws BadRequest when the user already exists (unique constraint)', async () => {
      mockDtoValidator.mockResolvedValue({});
      const dbError = new QueryFailedError('query', [], new Error('duplicate key value violates unique constraint'));
      userRepo.createUser.mockRejectedValue(dbError);

      const res = createMockResponse();
      await expect(createUser(createMockRequest(), res)).rejects.toBeInstanceOf(BadRequestException);
    });

    it('throws UnknownException on other errors', async () => {
      mockDtoValidator.mockResolvedValue({});
      userRepo.createUser.mockRejectedValue(new Error('boom'));

      const res = createMockResponse();
      await expect(createUser(createMockRequest(), res)).rejects.toBeInstanceOf(UnknownException);
    });
  });

  describe('getUserById', () => {
    it('returns the user DTO from res.locals', async () => {
      const user = { id: uuidV4() };
      mockUserFromUser.mockReturnValue({ id: user.id });

      const res = createMockResponse({ user });
      await getUserById(createMockRequest({ params: { user_id: user.id } }), res);

      expect(mockUserFromUser).toHaveBeenCalledWith(user, 'en-GB');
      expect(res.json).toHaveBeenCalledWith({ id: user.id });
    });
  });

  describe('updateUserRoles', () => {
    it('validates the selections and updates the roles', async () => {
      const selections = [{ type: 'group', groupId: uuidV4(), roles: [GroupRole.Editor] }];
      const user = { id: uuidV4() };
      mockArrayValidator.mockResolvedValue(selections);
      userRepo.updateUserRoles.mockResolvedValue(user);
      mockUserFromUser.mockReturnValue({ id: user.id });

      const res = createMockResponse({ userId: user.id });
      await updateUserRoles(createMockRequest({ body: selections as never }), res);

      expect(userRepo.updateUserRoles).toHaveBeenCalledWith(user.id, selections);
      expect(res.json).toHaveBeenCalledWith({ id: user.id });
    });

    it('throws UnknownException when a group selection is missing its group id', async () => {
      // the controller wraps the thrown BadRequest in its catch and rethrows UnknownException
      mockArrayValidator.mockResolvedValue([{ type: 'group', roles: [GroupRole.Editor] }]);

      const res = createMockResponse({ userId: uuidV4() });
      await expect(updateUserRoles(createMockRequest(), res)).rejects.toBeInstanceOf(UnknownException);
      expect(userRepo.updateUserRoles).not.toHaveBeenCalled();
    });

    it('throws UnknownException when a role value is invalid', async () => {
      mockArrayValidator.mockResolvedValue([{ type: 'global', roles: ['not-a-role'] }]);

      const res = createMockResponse({ userId: uuidV4() });
      await expect(updateUserRoles(createMockRequest(), res)).rejects.toBeInstanceOf(UnknownException);
    });
  });

  describe('updateUserStatus', () => {
    it('updates the user status when valid', async () => {
      const user = { id: uuidV4() };
      userRepo.updateUserStatus.mockResolvedValue(user);
      mockUserFromUser.mockReturnValue({ id: user.id });

      const req = createMockRequest({ body: { status: UserStatus.Active } as never });
      const res = createMockResponse({ userId: user.id });
      await updateUserStatus(req, res, mockNext);

      expect(userRepo.updateUserStatus).toHaveBeenCalledWith(user.id, UserStatus.Active);
      expect(res.json).toHaveBeenCalledWith({ id: user.id });
    });

    it('rejects when the status is invalid', async () => {
      mockHasError.mockResolvedValueOnce(true);

      const req = createMockRequest({ body: { status: 'bogus' } as never });
      const res = createMockResponse({ userId: uuidV4() });
      await updateUserStatus(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(BadRequestException);
      expect(userRepo.updateUserStatus).not.toHaveBeenCalled();
    });

    it('throws UnknownException on an unexpected error', async () => {
      userRepo.updateUserStatus.mockRejectedValue(new Error('boom'));

      const req = createMockRequest({ body: { status: UserStatus.Active } as never });
      const res = createMockResponse({ userId: uuidV4() });
      await expect(updateUserStatus(req, res, mockNext)).rejects.toBeInstanceOf(UnknownException);
    });
  });

  describe('dashboard', () => {
    it('aggregates dataset, user and group stats', async () => {
      datasetStatsRepo.getDashboardStats.mockResolvedValue({ d: 1 });
      userRepo.getDashboardStats.mockResolvedValue({ u: 2 });
      userGroupRepo.getDashboardStats.mockResolvedValue({ g: 3 });

      const res = createMockResponse();
      await dashboard(createMockRequest(), res, mockNext);

      expect(res.json).toHaveBeenCalledWith({ datasets: { d: 1 }, users: { u: 2 }, groups: { g: 3 } });
    });

    it('passes UnknownException to next on error', async () => {
      datasetStatsRepo.getDashboardStats.mockRejectedValue(new Error('boom'));
      userRepo.getDashboardStats.mockResolvedValue({});
      userGroupRepo.getDashboardStats.mockResolvedValue({});

      const res = createMockResponse();
      await dashboard(createMockRequest(), res, mockNext);

      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
    });
  });

  describe('similarDatasets', () => {
    it('streams the shared-sources report as CSV by default', async () => {
      datasetStatsRepo.shareSources.mockResolvedValue([
        {
          sources: ['s1'],
          datasets_count: 2,
          datasets: ['a', 'b'],
          dataset_ids: ['1', '2'],
          revision_ids: ['r1'],
          dimensions_count: 1,
          dimensions: ['d'],
          dimensions_common_count: 1,
          dimensions_common: ['d'],
          topics_count: 0,
          topics: null
        }
      ]);

      const res = createMockResponse();
      await similarDatasets(createMockRequest(), res, mockNext);

      expect(datasetStatsRepo.shareSources).toHaveBeenCalled();
      expect(res.setHeader).toHaveBeenCalledWith('Content-Type', 'text/csv');
      expect(mockStringifyStream.pipe).toHaveBeenCalledWith(res);
    });

    it('streams the shared-dimensions report when by=dimensions', async () => {
      datasetStatsRepo.shareDimensions.mockResolvedValue([
        { dimensions: ['d'], datasets_count: 1, datasets: ['a'], dataset_ids: ['1'] }
      ]);

      const res = createMockResponse();
      await similarDatasets(createMockRequest({ query: { by: DatasetSimilarBy.Dimensions } as never }), res, mockNext);

      expect(datasetStatsRepo.shareDimensions).toHaveBeenCalled();
      expect(mockStringifyStream.pipe).toHaveBeenCalledWith(res);
    });

    it('streams the title-similarity report when by=title', async () => {
      datasetStatsRepo.similarTitles.mockResolvedValue([{ title_1: 'a', title_2: 'b', similarity_score: 0.5 }]);

      const res = createMockResponse();
      await similarDatasets(createMockRequest({ query: { by: DatasetSimilarBy.Title } as never }), res, mockNext);

      expect(datasetStatsRepo.similarTitles).toHaveBeenCalled();
      expect(mockStringifyStream.pipe).toHaveBeenCalledWith(res);
    });

    it('streams the same-fact-table report when by=facts', async () => {
      datasetStatsRepo.sameFactTable.mockResolvedValue([
        { original_filenames: ['f.csv'], datatable_hash: 'h', count: 2, datasets: ['a', 'b'] }
      ]);

      const res = createMockResponse();
      await similarDatasets(createMockRequest({ query: { by: DatasetSimilarBy.Facts } as never }), res, mockNext);

      expect(datasetStatsRepo.sameFactTable).toHaveBeenCalled();
      expect(mockStringifyStream.pipe).toHaveBeenCalledWith(res);
    });

    it('rejects when the by value is invalid', async () => {
      mockHasError.mockResolvedValueOnce(true);

      const res = createMockResponse();
      await similarDatasets(createMockRequest({ query: { by: 'bogus' } as never }), res, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(1);
      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(BadRequestException);
    });

    it('passes UnknownException to next when the report query throws', async () => {
      datasetStatsRepo.shareSources.mockRejectedValue(new Error('boom'));

      const res = createMockResponse();
      await similarDatasets(createMockRequest(), res, mockNext);

      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
    });

    it('neutralizes formula-injection payloads in dataset titles (SW-1306 regression)', async () => {
      datasetStatsRepo.similarTitles.mockResolvedValue([
        { title_1: '=HYPERLINK("https://evil/")', title_2: 'Safe title', similarity_score: 0.5 }
      ]);

      const res = createMockResponse();
      await similarDatasets(createMockRequest({ query: { by: DatasetSimilarBy.Title } as never }), res, mockNext);

      const [rows] = mockStringify.mock.calls[0] as [Record<string, unknown>[]];
      expect(rows[0]).toMatchObject({ title_1: `'=HYPERLINK("https://evil/")`, title_2: 'Safe title' });
    });
  });

  describe('downloadSearchLogs', () => {
    it('streams the search-log report as CSV', async () => {
      searchLogRepo.getByPeriod.mockResolvedValue([
        { createdAt: new Date('2026-01-01T00:00:00Z'), mode: 'simple', keywords: 'tax', resultCount: 3 }
      ]);

      const res = createMockResponse();
      await downloadSearchLogs(
        createMockRequest({ query: { start: '2026-01-01', end: '2026-02-01' } as never }),
        res,
        mockNext
      );

      expect(searchLogRepo.getByPeriod).toHaveBeenCalled();
      expect(res.setHeader).toHaveBeenCalledWith('Content-Type', 'text/csv');
      expect(mockStringifyStream.pipe).toHaveBeenCalledWith(res);
    });

    it('neutralizes formula-injection payloads in search keywords (SW-1306 regression)', async () => {
      searchLogRepo.getByPeriod.mockResolvedValue([
        { createdAt: new Date('2026-01-01T00:00:00Z'), mode: 'simple', keywords: '=cmd|" /C calc"!A0', resultCount: 1 }
      ]);

      const res = createMockResponse();
      await downloadSearchLogs(
        createMockRequest({ query: { start: '2026-01-01', end: '2026-02-01' } as never }),
        res,
        mockNext
      );

      const [rows] = mockStringify.mock.calls[0] as [Record<string, unknown>[]];
      expect(rows[0]).toMatchObject({ keywords: `'=cmd|" /C calc"!A0` });
    });

    it('passes UnknownException to next on error', async () => {
      searchLogRepo.getByPeriod.mockRejectedValue(new Error('boom'));

      const res = createMockResponse();
      await downloadSearchLogs(createMockRequest(), res, mockNext);

      expect(mockNext.mock.calls[0][0]).toBeInstanceOf(UnknownException);
    });
  });
});
