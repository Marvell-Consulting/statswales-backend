import { Request, Response, NextFunction } from 'express';
import { BadRequestException } from '../../src/exceptions/bad-request.exception';
import { ForbiddenException } from '../../src/exceptions/forbidden.exception';
import { TaskStatus } from '../../src/enums/task-status';
import { TaskAction } from '../../src/enums/task-action';
import { uuidV4 } from '../../src/utils/uuid';
import { Task } from '../../src/entities/task/task';
import { User } from '../../src/entities/user/user';
import { Dataset } from '../../src/entities/dataset/dataset';

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

// Mock TaskService
const mockGetTasksForDataset = jest.fn();
const mockUpdate = jest.fn();
const mockRejectUnpublish = jest.fn();
const mockApproveArchive = jest.fn();
const mockRejectArchive = jest.fn();
const mockApproveUnarchive = jest.fn();
const mockRejectUnarchive = jest.fn();
jest.mock('../../src/services/task', () => ({
  TaskService: jest.fn().mockImplementation(() => ({
    getTasksForDataset: (...args: unknown[]) => mockGetTasksForDataset(...args),
    update: (...args: unknown[]) => mockUpdate(...args),
    rejectUnpublish: (...args: unknown[]) => mockRejectUnpublish(...args),
    approveArchive: (...args: unknown[]) => mockApproveArchive(...args),
    rejectArchive: (...args: unknown[]) => mockRejectArchive(...args),
    approveUnarchive: (...args: unknown[]) => mockApproveUnarchive(...args),
    rejectUnarchive: (...args: unknown[]) => mockRejectUnarchive(...args)
  }))
}));

// Mock TaskDTO
const mockFromTask = jest.fn();
jest.mock('../../src/dtos/task-dto', () => ({
  TaskDTO: {
    fromTask: (...args: unknown[]) => mockFromTask(...args)
  }
}));

// Mock TaskDecisionDTO
jest.mock('../../src/dtos/task-decision-dto', () => ({
  TaskDecisionDTO: class TaskDecisionDTO {}
}));

// Mock dtoValidator
const mockDtoValidator = jest.fn();
jest.mock('../../src/validators/dto-validator', () => ({
  dtoValidator: (...args: unknown[]) => mockDtoValidator(...args)
}));

// Mock get-permissions-for-user
const mockIsApproverForDataset = jest.fn();
jest.mock('../../src/utils/get-permissions-for-user', () => ({
  isApproverForDataset: (...args: unknown[]) => mockIsApproverForDataset(...args)
}));

import { getTask, getTasksForDataset, taskDecision } from '../../src/controllers/task';

function createMockTask(overrides: Partial<Task> = {}): Task {
  const task = new Task();
  task.id = uuidV4();
  task.action = TaskAction.Publish;
  task.status = TaskStatus.Requested;
  task.open = true;
  task.datasetId = uuidV4();
  task.createdAt = new Date();
  task.updatedAt = new Date();
  task.createdBy = null;
  task.updatedBy = null;
  Object.assign(task, overrides);
  return task;
}

function createMockDataset(overrides: Partial<Dataset> = {}): Dataset {
  const dataset = new Dataset();
  dataset.id = uuidV4();
  dataset.userGroupId = 'group-1';
  dataset.draftRevisionId = uuidV4();
  Object.assign(dataset, overrides);
  return dataset;
}

function createMockUser(overrides: Partial<User> = {}): User {
  const user = new User();
  user.id = uuidV4();
  user.name = 'Test User';
  user.email = 'test@example.com';
  user.groupRoles = [];
  Object.assign(user, overrides);
  return user;
}

function createMockRequest(overrides: Partial<Request> = {}): Request {
  return {
    params: {},
    query: {},
    body: {},
    user: createMockUser(),
    datasetService: {
      approvePublication: jest.fn(),
      rejectPublication: jest.fn(),
      approveUnpublish: jest.fn()
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

describe('Task controller', () => {
  let mockNext: jest.MockedFunction<NextFunction>;

  beforeEach(() => {
    jest.clearAllMocks();
    mockNext = jest.fn();
  });

  describe('getTask', () => {
    it('should return task DTO from res.locals.task', async () => {
      const task = createMockTask();
      const taskDTO = { id: task.id, action: task.action };
      mockFromTask.mockReturnValue(taskDTO);

      const req = createMockRequest();
      const res = createMockResponse({ locals: { task } });

      await getTask(req, res);

      expect(mockFromTask).toHaveBeenCalledWith(task);
      expect(res.json).toHaveBeenCalledWith(taskDTO);
    });
  });

  describe('getTasksForDataset', () => {
    it('should return all tasks when open query param is not provided', async () => {
      const datasetId = uuidV4();
      const tasks = [createMockTask(), createMockTask()];
      const taskDTOs = [{ id: tasks[0].id }, { id: tasks[1].id }];

      mockGetTasksForDataset.mockResolvedValue(tasks);
      mockFromTask.mockReturnValueOnce(taskDTOs[0]).mockReturnValueOnce(taskDTOs[1]);

      const req = createMockRequest({ params: { dataset_id: datasetId }, query: {} });
      const res = createMockResponse();

      await getTasksForDataset(req, res, mockNext);

      expect(mockGetTasksForDataset).toHaveBeenCalledWith(datasetId, undefined);
      expect(mockFromTask).toHaveBeenCalledTimes(2);
      expect(res.json).toHaveBeenCalledWith(taskDTOs);
    });

    it('should return only open tasks when open=true', async () => {
      const datasetId = uuidV4();
      const tasks = [createMockTask({ open: true })];
      const taskDTOs = [{ id: tasks[0].id }];

      mockGetTasksForDataset.mockResolvedValue(tasks);
      mockFromTask.mockReturnValue(taskDTOs[0]);

      const req = createMockRequest({ params: { dataset_id: datasetId }, query: { open: 'true' } });
      const res = createMockResponse();

      await getTasksForDataset(req, res, mockNext);

      expect(mockGetTasksForDataset).toHaveBeenCalledWith(datasetId, true);
      expect(res.json).toHaveBeenCalledWith(taskDTOs);
    });

    it('should return only closed tasks when open=false', async () => {
      const datasetId = uuidV4();
      const tasks = [createMockTask({ open: false })];
      const taskDTOs = [{ id: tasks[0].id }];

      mockGetTasksForDataset.mockResolvedValue(tasks);
      mockFromTask.mockReturnValue(taskDTOs[0]);

      const req = createMockRequest({ params: { dataset_id: datasetId }, query: { open: 'false' } });
      const res = createMockResponse();

      await getTasksForDataset(req, res, mockNext);

      expect(mockGetTasksForDataset).toHaveBeenCalledWith(datasetId, false);
      expect(res.json).toHaveBeenCalledWith(taskDTOs);
    });

    it('should call next with BadRequestException when dataset_id is missing', async () => {
      const req = createMockRequest({ params: {} });
      const res = createMockResponse();

      await getTasksForDataset(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
      const error = mockNext.mock.calls[0][0] as unknown as BadRequestException;
      expect(error.message).toBe('errors.dataset_id.missing');
    });

    it('should call next with error when service throws', async () => {
      const datasetId = uuidV4();
      const error = new Error('Service error');
      mockGetTasksForDataset.mockRejectedValue(error);

      const req = createMockRequest({ params: { dataset_id: datasetId } });
      const res = createMockResponse();

      await getTasksForDataset(req, res, mockNext);

      expect(mockNext).toHaveBeenCalledWith(error);
    });
  });

  describe('taskDecision', () => {
    describe('validation checks', () => {
      it('should reject if task is not open', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({ open: false, dataset });
        const user = createMockUser();

        const req = createMockRequest({ user, body: { decision: 'approve' } });
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
        const error = mockNext.mock.calls[0][0] as unknown as BadRequestException;
        expect(error.message).toBe('errors.task.not_open');
      });

      it('should reject if task status is not Requested', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({ open: true, status: TaskStatus.Approved, dataset });
        const user = createMockUser();

        const req = createMockRequest({ user, body: { decision: 'approve' } });
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
        const error = mockNext.mock.calls[0][0] as unknown as BadRequestException;
        expect(error.message).toBe('errors.task.invalid_status');
      });

      it('should reject if user is not an approver for the dataset', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({ open: true, status: TaskStatus.Requested, dataset });
        const user = createMockUser();

        mockIsApproverForDataset.mockReturnValue(false);

        const req = createMockRequest({ user, body: { decision: 'approve' } });
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(mockIsApproverForDataset).toHaveBeenCalledWith(user, dataset);
        expect(mockNext).toHaveBeenCalledWith(expect.any(ForbiddenException));
        const error = mockNext.mock.calls[0][0] as unknown as ForbiddenException;
        expect(error.message).toBe('errors.task.user_is_not_approver_for_this_dataset');
      });
    });

    describe('Publish action', () => {
      it('should approve publication when decision is approve', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({
          open: true,
          status: TaskStatus.Requested,
          action: TaskAction.Publish,
          dataset
        });
        const user = createMockUser();
        const updatedTask = { ...task, status: TaskStatus.Approved, open: false };
        const taskDTO = { id: task.id, status: TaskStatus.Approved };

        mockIsApproverForDataset.mockReturnValue(true);
        mockDtoValidator.mockResolvedValue({ decision: 'approve' });
        mockUpdate.mockResolvedValue(updatedTask);
        mockFromTask.mockReturnValue(taskDTO);

        const approvePublication = jest.fn().mockResolvedValue(undefined);
        const req = createMockRequest({
          user,
          body: { decision: 'approve' },
          datasetService: {
            approvePublication
          }
        } as any);
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(approvePublication).toHaveBeenCalledWith(dataset.id, dataset.draftRevisionId, user);
        expect(mockUpdate).toHaveBeenCalledWith(task.id, TaskStatus.Approved, false, user);
        expect(res.json).toHaveBeenCalledWith(taskDTO);
      });

      it('should reject publication when decision is reject', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({
          open: true,
          status: TaskStatus.Requested,
          action: TaskAction.Publish,
          dataset
        });
        const user = createMockUser();
        const updatedTask = { ...task, status: TaskStatus.Rejected, open: true };
        const taskDTO = { id: task.id, status: TaskStatus.Rejected };

        mockIsApproverForDataset.mockReturnValue(true);
        mockDtoValidator.mockResolvedValue({ decision: 'reject', reason: 'Not ready' });
        mockUpdate.mockResolvedValue(updatedTask);
        mockFromTask.mockReturnValue(taskDTO);

        const rejectPublication = jest.fn().mockResolvedValue(undefined);
        const req = createMockRequest({
          user,
          body: { decision: 'reject', reason: 'Not ready' },
          datasetService: {
            rejectPublication
          }
        } as any);
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(rejectPublication).toHaveBeenCalledWith(dataset.id, dataset.draftRevisionId);
        expect(mockUpdate).toHaveBeenCalledWith(task.id, TaskStatus.Rejected, true, user, 'Not ready');
        expect(res.json).toHaveBeenCalledWith(taskDTO);
      });
    });

    describe('Unpublish action', () => {
      it('should approve unpublish when decision is approve', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({
          open: true,
          status: TaskStatus.Requested,
          action: TaskAction.Unpublish,
          dataset
        });
        const user = createMockUser();
        const updatedTask = { ...task, status: TaskStatus.Approved, open: false };
        const taskDTO = { id: task.id, status: TaskStatus.Approved };

        mockIsApproverForDataset.mockReturnValue(true);
        mockDtoValidator.mockResolvedValue({ decision: 'approve' });
        mockUpdate.mockResolvedValue(updatedTask);
        mockFromTask.mockReturnValue(taskDTO);

        const approveUnpublish = jest.fn().mockResolvedValue(undefined);
        const req = createMockRequest({
          user,
          body: { decision: 'approve' },
          datasetService: {
            approveUnpublish
          }
        } as any);
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(approveUnpublish).toHaveBeenCalledWith(dataset.id, user);
        expect(mockUpdate).toHaveBeenCalledWith(task.id, TaskStatus.Approved, false, user, null);
        expect(res.json).toHaveBeenCalledWith(taskDTO);
      });

      it('should reject unpublish when decision is reject', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({
          open: true,
          status: TaskStatus.Requested,
          action: TaskAction.Unpublish,
          dataset
        });
        const user = createMockUser();
        const updatedTask = { ...task, status: TaskStatus.Rejected };
        const taskDTO = { id: task.id, status: TaskStatus.Rejected };

        mockIsApproverForDataset.mockReturnValue(true);
        mockDtoValidator.mockResolvedValue({ decision: 'reject', reason: 'Cannot unpublish' });
        mockRejectUnpublish.mockResolvedValue(updatedTask);
        mockFromTask.mockReturnValue(taskDTO);

        const req = createMockRequest({
          user,
          body: { decision: 'reject', reason: 'Cannot unpublish' }
        });
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(mockRejectUnpublish).toHaveBeenCalledWith(task.id, user, 'Cannot unpublish');
        expect(res.json).toHaveBeenCalledWith(taskDTO);
      });
    });

    describe('Archive action', () => {
      it('should approve archive when decision is approve', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({
          open: true,
          status: TaskStatus.Requested,
          action: TaskAction.Archive,
          dataset
        });
        const user = createMockUser();
        const updatedTask = { ...task, status: TaskStatus.Approved, open: false };
        const taskDTO = { id: task.id, status: TaskStatus.Approved };

        mockIsApproverForDataset.mockReturnValue(true);
        mockDtoValidator.mockResolvedValue({ decision: 'approve' });
        mockApproveArchive.mockResolvedValue(updatedTask);
        mockFromTask.mockReturnValue(taskDTO);

        const req = createMockRequest({ user, body: { decision: 'approve' } });
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(mockApproveArchive).toHaveBeenCalledWith(task.id, user);
        expect(res.json).toHaveBeenCalledWith(taskDTO);
      });

      it('should reject archive when decision is reject', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({
          open: true,
          status: TaskStatus.Requested,
          action: TaskAction.Archive,
          dataset
        });
        const user = createMockUser();
        const updatedTask = { ...task, status: TaskStatus.Rejected };
        const taskDTO = { id: task.id, status: TaskStatus.Rejected };

        mockIsApproverForDataset.mockReturnValue(true);
        mockDtoValidator.mockResolvedValue({ decision: 'reject', reason: 'Cannot archive' });
        mockRejectArchive.mockResolvedValue(updatedTask);
        mockFromTask.mockReturnValue(taskDTO);

        const req = createMockRequest({
          user,
          body: { decision: 'reject', reason: 'Cannot archive' }
        });
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(mockRejectArchive).toHaveBeenCalledWith(task.id, user, 'Cannot archive');
        expect(res.json).toHaveBeenCalledWith(taskDTO);
      });
    });

    describe('Unarchive action', () => {
      it('should approve unarchive when decision is approve', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({
          open: true,
          status: TaskStatus.Requested,
          action: TaskAction.Unarchive,
          dataset
        });
        const user = createMockUser();
        const updatedTask = { ...task, status: TaskStatus.Approved, open: false };
        const taskDTO = { id: task.id, status: TaskStatus.Approved };

        mockIsApproverForDataset.mockReturnValue(true);
        mockDtoValidator.mockResolvedValue({ decision: 'approve' });
        mockApproveUnarchive.mockResolvedValue(updatedTask);
        mockFromTask.mockReturnValue(taskDTO);

        const req = createMockRequest({ user, body: { decision: 'approve' } });
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(mockApproveUnarchive).toHaveBeenCalledWith(task.id, user);
        expect(res.json).toHaveBeenCalledWith(taskDTO);
      });

      it('should reject unarchive when decision is reject', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({
          open: true,
          status: TaskStatus.Requested,
          action: TaskAction.Unarchive,
          dataset
        });
        const user = createMockUser();
        const updatedTask = { ...task, status: TaskStatus.Rejected };
        const taskDTO = { id: task.id, status: TaskStatus.Rejected };

        mockIsApproverForDataset.mockReturnValue(true);
        mockDtoValidator.mockResolvedValue({ decision: 'reject', reason: 'Cannot unarchive' });
        mockRejectUnarchive.mockResolvedValue(updatedTask);
        mockFromTask.mockReturnValue(taskDTO);

        const req = createMockRequest({
          user,
          body: { decision: 'reject', reason: 'Cannot unarchive' }
        });
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(mockRejectUnarchive).toHaveBeenCalledWith(task.id, user, 'Cannot unarchive');
        expect(res.json).toHaveBeenCalledWith(taskDTO);
      });
    });

    describe('Invalid action', () => {
      it('should call next with BadRequestException for invalid action', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({
          open: true,
          status: TaskStatus.Requested,
          action: 'invalid-action' as TaskAction,
          dataset
        });
        const user = createMockUser();

        mockIsApproverForDataset.mockReturnValue(true);
        mockDtoValidator.mockResolvedValue({ decision: 'approve' });

        const req = createMockRequest({ user, body: { decision: 'approve' } });
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(mockNext).toHaveBeenCalledWith(expect.any(BadRequestException));
        const error = mockNext.mock.calls[0][0] as unknown as BadRequestException;
        expect(error.message).toBe('errors.task.invalid_action');
      });
    });

    describe('Error handling', () => {
      it('should call next with error when dtoValidator throws', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({
          open: true,
          status: TaskStatus.Requested,
          action: TaskAction.Publish,
          dataset
        });
        const user = createMockUser();
        const error = new Error('Validation error');

        mockIsApproverForDataset.mockReturnValue(true);
        mockDtoValidator.mockRejectedValue(error);

        const req = createMockRequest({ user, body: { decision: 'invalid' } });
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(mockNext).toHaveBeenCalledWith(error);
      });

      it('should call next with error when service operation throws', async () => {
        const dataset = createMockDataset();
        const task = createMockTask({
          open: true,
          status: TaskStatus.Requested,
          action: TaskAction.Archive,
          dataset
        });
        const user = createMockUser();
        const error = new Error('Service error');

        mockIsApproverForDataset.mockReturnValue(true);
        mockDtoValidator.mockResolvedValue({ decision: 'approve' });
        mockApproveArchive.mockRejectedValue(error);

        const req = createMockRequest({ user, body: { decision: 'approve' } });
        const res = createMockResponse({ locals: { task } });

        await taskDecision(req, res, mockNext);

        expect(mockNext).toHaveBeenCalledWith(error);
      });
    });
  });
});
