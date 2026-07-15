import { TaskStatus } from '../../../src/enums/task-status';
import { TaskAction } from '../../../src/enums/task-action';
import { PublishingStatus } from '../../../src/enums/publishing-status';
import { BadRequestException } from '../../../src/exceptions/bad-request.exception';
import { uuidV4 } from '../../../src/utils/uuid';
import { User } from '../../../src/entities/user/user';

jest.mock('../../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    trace: jest.fn()
  }
}));

// --- Task entity mock ---
const mockTaskCreate = jest.fn();
const mockTaskMerge = jest.fn();
const mockTaskFindOneOrFail = jest.fn();
const mockTaskFindOneByOrFail = jest.fn();
const mockTaskFind = jest.fn();
jest.mock('../../../src/entities/task/task', () => ({
  Task: {
    create: (...args: unknown[]) => mockTaskCreate(...args),
    merge: (...args: unknown[]) => mockTaskMerge(...args),
    findOneOrFail: (...args: unknown[]) => mockTaskFindOneOrFail(...args),
    findOneByOrFail: (...args: unknown[]) => mockTaskFindOneByOrFail(...args),
    find: (...args: unknown[]) => mockTaskFind(...args)
  }
}));

// --- Repository mocks ---
const mockDatasetGetById = jest.fn();
const mockDatasetArchive = jest.fn();
const mockDatasetUnarchive = jest.fn();
jest.mock('../../../src/repositories/dataset', () => ({
  DatasetRepository: {
    getById: (...args: unknown[]) => mockDatasetGetById(...args),
    archive: (...args: unknown[]) => mockDatasetArchive(...args),
    unarchive: (...args: unknown[]) => mockDatasetUnarchive(...args)
  }
}));

const mockPublishedGetById = jest.fn();
jest.mock('../../../src/repositories/published-dataset', () => ({
  PublishedDatasetRepository: {
    getById: (...args: unknown[]) => mockPublishedGetById(...args)
  }
}));

const mockGetPublishingStatus = jest.fn();
jest.mock('../../../src/utils/dataset-status', () => ({
  getPublishingStatus: (...args: unknown[]) => mockGetPublishingStatus(...args)
}));

// Import after mocks
import { TaskService } from '../../../src/services/task';

// --- Helpers ---

interface MockTask {
  id: string;
  action: TaskAction;
  status: TaskStatus;
  open: boolean;
  datasetId?: string;
  metadata?: Record<string, unknown>;
  comment?: string | null;
  createdBy?: User | null;
  updatedBy?: User | null;
  dataset?: unknown;
  save: jest.Mock;
}

function makeTask(overrides: Partial<MockTask> = {}): MockTask {
  const task = {
    id: uuidV4(),
    action: TaskAction.Publish,
    status: TaskStatus.Requested,
    open: true,
    datasetId: uuidV4(),
    createdBy: null,
    updatedBy: null,
    ...overrides
  } as MockTask;
  task.save = jest.fn().mockResolvedValue(task);
  return task;
}

function makeUser(): User {
  return { id: uuidV4(), name: 'test-user' } as unknown as User;
}

describe('TaskService', () => {
  let service: TaskService;
  let user: User;

  beforeEach(() => {
    jest.clearAllMocks();
    service = new TaskService();
    user = makeUser();

    // sensible defaults
    mockTaskCreate.mockImplementation((props: Partial<MockTask>) => makeTask(props));
    mockTaskMerge.mockImplementation((task: MockTask, props: Partial<MockTask>) => Object.assign(task, props));
    mockTaskFindOneByOrFail.mockImplementation(({ id }: { id: string }) => Promise.resolve(makeTask({ id })));
    mockTaskFindOneOrFail.mockImplementation(({ where }: { where: { id: string } }) =>
      Promise.resolve(makeTask({ id: where.id, dataset: { id: uuidV4() } }))
    );
    mockTaskFind.mockResolvedValue([]);
  });

  describe('create', () => {
    it('creates a Requested, open task and saves it', async () => {
      const metadata = { revisionId: 'rev-1' };
      const result = await service.create('ds-1', TaskAction.Publish, user, 'a comment', metadata);

      expect(mockTaskCreate).toHaveBeenCalledWith({
        datasetId: 'ds-1',
        action: TaskAction.Publish,
        createdBy: user,
        status: TaskStatus.Requested,
        open: true,
        comment: 'a comment',
        metadata
      });
      expect(result.save).toHaveBeenCalled();
    });
  });

  describe('getById', () => {
    it('fetches a task with the supplied relations', async () => {
      await service.getById('task-1', { dataset: true });
      expect(mockTaskFindOneOrFail).toHaveBeenCalledWith({ where: { id: 'task-1' }, relations: { dataset: true } });
    });

    it('defaults to no relations', async () => {
      await service.getById('task-1');
      expect(mockTaskFindOneOrFail).toHaveBeenCalledWith({ where: { id: 'task-1' }, relations: {} });
    });
  });

  describe('update', () => {
    it('merges the new status/open/comment and saves', async () => {
      const existing = makeTask({ id: 'task-1' });
      mockTaskFindOneByOrFail.mockResolvedValueOnce(existing);

      const result = await service.update('task-1', TaskStatus.Approved, false, user, 'done');

      expect(mockTaskMerge).toHaveBeenCalledWith(existing, {
        status: TaskStatus.Approved,
        open: false,
        updatedBy: user,
        comment: 'done'
      });
      expect(existing.save).toHaveBeenCalled();
      expect(result.status).toBe(TaskStatus.Approved);
      expect(result.open).toBe(false);
    });
  });

  describe('closeOpenPublishTasks', () => {
    it('closes every open publish task for the dataset', async () => {
      const t1 = makeTask({ id: 'task-1', action: TaskAction.Publish });
      const t2 = makeTask({ id: 'task-2', action: TaskAction.Publish });
      mockTaskFind.mockResolvedValueOnce([t1, t2]);

      await service.closeOpenPublishTasks('ds-1', user);

      expect(mockTaskMerge).toHaveBeenCalledWith(t1, {
        status: TaskStatus.Withdrawn,
        open: false,
        updatedBy: user
      });
      expect(mockTaskMerge).toHaveBeenCalledWith(t2, {
        status: TaskStatus.Withdrawn,
        open: false,
        updatedBy: user
      });
      expect(t1.save).toHaveBeenCalled();
      expect(t2.save).toHaveBeenCalled();
    });

    it('skips the excepted task', async () => {
      const keep = makeTask({ id: 'keep', action: TaskAction.Publish });
      const sibling = makeTask({ id: 'sibling', action: TaskAction.Publish });
      mockTaskFind.mockResolvedValueOnce([keep, sibling]);

      await service.closeOpenPublishTasks('ds-1', user, 'keep');

      expect(sibling.save).toHaveBeenCalled();
      expect(keep.save).not.toHaveBeenCalled();
    });

    it('only requests open tasks', async () => {
      mockTaskFind.mockResolvedValueOnce([]);
      await service.closeOpenPublishTasks('ds-1', user);
      expect(mockTaskFind).toHaveBeenCalledWith({
        where: { datasetId: 'ds-1', action: TaskAction.Publish, open: true },
        order: { createdAt: 'DESC' }
      });
    });
  });

  describe('withdrawApproved', () => {
    it('creates a closed withdrawn publish task carrying the revision id', async () => {
      const result = await service.withdrawApproved('ds-1', 'rev-1', user);

      expect(mockTaskCreate).toHaveBeenCalledWith({
        datasetId: 'ds-1',
        action: TaskAction.Publish,
        status: TaskStatus.Withdrawn,
        open: false,
        metadata: { revisionId: 'rev-1', note: 'previously approved' },
        createdBy: user,
        updatedBy: user
      });
      expect(result.save).toHaveBeenCalled();
    });
  });

  describe('requestUnpublish', () => {
    it('creates an Unpublish task when the dataset is published with no open tasks', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', tasks: [], endRevision: {}, endRevisionId: 'rev-1' });
      mockGetPublishingStatus.mockReturnValue(PublishingStatus.Published);

      await service.requestUnpublish('ds-1', user, 'because');

      expect(mockTaskCreate).toHaveBeenCalledWith(
        expect.objectContaining({
          datasetId: 'ds-1',
          action: TaskAction.Unpublish,
          status: TaskStatus.Requested,
          metadata: { revisionId: 'rev-1' }
        })
      );
    });

    it('rejects when the dataset has an open task', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', tasks: [{ open: true }], endRevision: {} });

      await expect(service.requestUnpublish('ds-1', user, 'because')).rejects.toThrow(BadRequestException);
      expect(mockTaskCreate).not.toHaveBeenCalled();
    });

    it('rejects when the dataset is not in a published state', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', tasks: [], endRevision: {} });
      mockGetPublishingStatus.mockReturnValue(PublishingStatus.Incomplete);

      await expect(service.requestUnpublish('ds-1', user, 'because')).rejects.toThrow(BadRequestException);
      expect(mockTaskCreate).not.toHaveBeenCalled();
    });
  });

  describe('rejectUnpublish', () => {
    it('updates the task to Rejected and closes it', async () => {
      const task = makeTask({ id: 'task-1', dataset: { id: 'ds-1' } });
      mockTaskFindOneOrFail.mockResolvedValueOnce(task);
      mockTaskFindOneByOrFail.mockResolvedValueOnce(task);

      await service.rejectUnpublish('task-1', user, 'no');

      expect(mockTaskMerge).toHaveBeenCalledWith(
        task,
        expect.objectContaining({ status: TaskStatus.Rejected, open: false, comment: 'no' })
      );
    });
  });

  describe('requestArchive', () => {
    it('creates an Archive task with no replacement', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', tasks: [], endRevisionId: 'rev-1' });

      await service.requestArchive('ds-1', user, 'reason');

      expect(mockTaskCreate).toHaveBeenCalledWith(
        expect.objectContaining({ action: TaskAction.Archive, metadata: { revisionId: 'rev-1' } })
      );
    });

    it('rejects when the dataset has an open task', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', tasks: [{ open: true }] });
      await expect(service.requestArchive('ds-1', user, 'reason')).rejects.toThrow(BadRequestException);
    });

    it('rejects when the replacement is the dataset itself', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', tasks: [], endRevisionId: 'rev-1' });
      await expect(service.requestArchive('ds-1', user, 'reason', 'ds-1')).rejects.toThrow(BadRequestException);
    });

    it('rejects when the replacement is itself archived', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', tasks: [], endRevisionId: 'rev-1' });
      mockPublishedGetById.mockResolvedValue({
        archivedAt: new Date('2020-01-01'),
        publishedRevision: { metadata: [] }
      });

      await expect(service.requestArchive('ds-1', user, 'reason', 'ds-2')).rejects.toThrow(BadRequestException);
    });

    it('rejects when the replacement is not published', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', tasks: [], endRevisionId: 'rev-1' });
      mockPublishedGetById.mockRejectedValue(new Error('not found'));

      await expect(service.requestArchive('ds-1', user, 'reason', 'ds-2')).rejects.toThrow(BadRequestException);
    });

    it('records the replacement title and auto-redirect in the task metadata', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', tasks: [], endRevisionId: 'rev-1' });
      mockPublishedGetById.mockResolvedValue({
        archivedAt: null,
        publishedRevision: { metadata: [{ language: 'en-GB', title: 'Replacement title' }] }
      });

      await service.requestArchive('ds-1', user, 'reason', 'ds-2', true);

      expect(mockTaskCreate).toHaveBeenCalledWith(
        expect.objectContaining({
          action: TaskAction.Archive,
          metadata: expect.objectContaining({
            replacementDatasetId: 'ds-2',
            replacementDatasetTitle: 'Replacement title',
            autoRedirect: true
          })
        })
      );
    });
  });

  describe('approveArchive', () => {
    it('archives the dataset and approves the task', async () => {
      const task = makeTask({
        id: 'task-1',
        dataset: { id: 'ds-1' },
        metadata: { replacementDatasetId: 'ds-2', autoRedirect: true }
      });
      mockTaskFindOneOrFail.mockResolvedValueOnce(task);
      mockTaskFindOneByOrFail.mockResolvedValueOnce(task);

      await service.approveArchive('task-1', user);

      expect(mockDatasetArchive).toHaveBeenCalledWith('ds-1', 'ds-2', true);
      expect(mockTaskMerge).toHaveBeenCalledWith(
        task,
        expect.objectContaining({ status: TaskStatus.Approved, open: false })
      );
    });
  });

  describe('rejectArchive', () => {
    it('updates the task to Rejected', async () => {
      const task = makeTask({ id: 'task-1', dataset: { id: 'ds-1' } });
      mockTaskFindOneOrFail.mockResolvedValueOnce(task);
      mockTaskFindOneByOrFail.mockResolvedValueOnce(task);

      await service.rejectArchive('task-1', user, 'nope');

      expect(mockTaskMerge).toHaveBeenCalledWith(
        task,
        expect.objectContaining({ status: TaskStatus.Rejected, open: false, comment: 'nope' })
      );
    });
  });

  describe('requestUnarchive', () => {
    it('creates an Unarchive task', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', tasks: [], endRevisionId: 'rev-1' });

      await service.requestUnarchive('ds-1', user, 'reason');

      expect(mockTaskCreate).toHaveBeenCalledWith(
        expect.objectContaining({ action: TaskAction.Unarchive, metadata: { revisionId: 'rev-1' } })
      );
    });

    it('rejects when the dataset has an open task', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', tasks: [{ open: true }] });
      await expect(service.requestUnarchive('ds-1', user, 'reason')).rejects.toThrow(BadRequestException);
    });
  });

  describe('approveUnarchive', () => {
    it('unarchives the dataset and approves the task', async () => {
      const task = makeTask({ id: 'task-1', dataset: { id: 'ds-1' } });
      mockTaskFindOneOrFail.mockResolvedValueOnce(task);
      mockTaskFindOneByOrFail.mockResolvedValueOnce(task);

      await service.approveUnarchive('task-1', user);

      expect(mockDatasetUnarchive).toHaveBeenCalledWith('ds-1');
      expect(mockTaskMerge).toHaveBeenCalledWith(
        task,
        expect.objectContaining({ status: TaskStatus.Approved, open: false })
      );
    });
  });

  describe('rejectUnarchive', () => {
    it('updates the task to Rejected', async () => {
      const task = makeTask({ id: 'task-1', dataset: { id: 'ds-1' } });
      mockTaskFindOneOrFail.mockResolvedValueOnce(task);
      mockTaskFindOneByOrFail.mockResolvedValueOnce(task);

      await service.rejectUnarchive('task-1', user, 'nope');

      expect(mockTaskMerge).toHaveBeenCalledWith(
        task,
        expect.objectContaining({ status: TaskStatus.Rejected, open: false, comment: 'nope' })
      );
    });
  });

  describe('getTasksForDataset', () => {
    it('queries tasks ordered by creation date, filtered by open flag', async () => {
      const tasks = [makeTask(), makeTask()];
      mockTaskFind.mockResolvedValueOnce(tasks);

      const result = await service.getTasksForDataset('ds-1', true);

      expect(mockTaskFind).toHaveBeenCalledWith({
        where: { datasetId: 'ds-1', open: true },
        order: { createdAt: 'DESC' }
      });
      expect(result).toBe(tasks);
    });

    it('passes undefined open flag through when omitted', async () => {
      await service.getTasksForDataset('ds-1');
      expect(mockTaskFind).toHaveBeenCalledWith({
        where: { datasetId: 'ds-1', open: undefined },
        order: { createdAt: 'DESC' }
      });
    });
  });
});
