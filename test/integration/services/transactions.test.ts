// SW-522 — multi-step writes run inside a transaction, so a failure part way through a flow rolls back
// the writes that already happened rather than leaving the dataset in a half-updated state. These tests
// run against the real DB and force a failure after the first write of each flow.

import { ensureWorkerDataSources, resetDatabase } from '../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../helpers/get-test-user';
import { dbManager } from '../../../src/db/database-manager';
import { Dataset } from '../../../src/entities/dataset/dataset';
import { Revision } from '../../../src/entities/dataset/revision';
import { RevisionTopic } from '../../../src/entities/dataset/revision-topic';
import { Topic } from '../../../src/entities/dataset/topic';
import { Task } from '../../../src/entities/task/task';
import { User } from '../../../src/entities/user/user';
import { UserGroup } from '../../../src/entities/user/user-group';
import { EventLog } from '../../../src/entities/event-log';
import { DatasetRepository } from '../../../src/repositories/dataset';
import { RevisionRepository } from '../../../src/repositories/revision';
import { DatasetService } from '../../../src/services/dataset';
import { TaskService } from '../../../src/services/task';
import { TaskAction } from '../../../src/enums/task-action';
import { TaskStatus } from '../../../src/enums/task-status';
import { Locale } from '../../../src/enums/locale';
import { StorageService } from '../../../src/interfaces/storage-service';
import { uuidV4 } from '../../../src/utils/uuid';

const fileService = { delete: jest.fn().mockResolvedValue(undefined) } as unknown as StorageService & {
  delete: jest.Mock;
};

const service = new DatasetService(Locale.EnglishGb, fileService);

interface Seeded {
  dataset: Dataset;
  revision: Revision;
  user: User;
}

async function seedDataset(
  revisionProps: Partial<Revision> = {},
  datasetProps: Partial<Dataset> = {}
): Promise<Seeded> {
  const user = await getTestUser('sw522-user').save();

  const dataset = new Dataset();
  dataset.id = uuidV4();
  dataset.createdBy = user;
  await dataset.save();

  const revision = Object.assign(new Revision(), {
    id: uuidV4(),
    datasetId: dataset.id,
    createdBy: user,
    revisionIndex: 1,
    publishAt: null,
    approvedAt: null,
    unpublishedAt: null,
    ...revisionProps
  });
  await revision.save();

  Object.assign(dataset, { draftRevision: revision, startRevision: revision, endRevision: revision, ...datasetProps });
  await dataset.save();

  return { dataset, revision, user };
}

// an approved revision that is scheduled for first publication in the future
async function seedScheduledDataset(): Promise<Seeded> {
  const publishAt = new Date(Date.now() + 24 * 60 * 60 * 1000);

  const seeded = await seedDataset({ approvedAt: new Date(), publishAt, onlineCubeFilename: 'cube.duckdb' });
  seeded.dataset.draftRevision = null;
  seeded.dataset.publishedRevision = seeded.revision;
  seeded.dataset.firstPublishedAt = publishAt;
  await seeded.dataset.save();

  return seeded;
}

function createTask(datasetId: string, action: TaskAction, user: User): Promise<Task> {
  return Task.create({ datasetId, action, status: TaskStatus.Requested, open: true, createdBy: user }).save();
}

describe('SW-522 — writes are rolled back when a flow fails part way through', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
  });

  afterEach(async () => {
    jest.restoreAllMocks();
    fileService.delete.mockClear();
    await resetDatabase();
  });

  describe('createNew', () => {
    it('leaves no dataset or revision behind when creating the metadata fails', async () => {
      const user = await getTestUser('sw522-user').save();
      const group = await dbManager.getPublisherDataSource().getRepository(UserGroup).save(getTestUserGroup());

      // scope counts to this test's rows - other test files sharing this worker's DB may leave rows behind
      const countEvents = (): Promise<number> =>
        EventLog.count({ where: [{ entity: 'dataset' }, { entity: 'revision' }] });
      const eventsBefore = await countEvents();

      jest
        .spyOn(Object.getPrototypeOf(RevisionRepository), 'createMetadata')
        .mockRejectedValueOnce(new Error('metadata failed'));

      await expect(service.createNew('My dataset', group.id!, user)).rejects.toThrow('metadata failed');

      expect(await Dataset.count({ where: { userGroupId: group.id } })).toBe(0);
      expect(await Revision.count({ where: { createdBy: { id: user.id } } })).toBe(0);
      expect(await countEvents()).toBe(eventsBefore);
    });

    it('commits everything when nothing fails', async () => {
      const user = await getTestUser('sw522-user').save();
      const group = await dbManager.getPublisherDataSource().getRepository(UserGroup).save(getTestUserGroup());

      const dataset = await service.createNew('My dataset', group.id!, user);

      expect(dataset.draftRevision?.metadata).toHaveLength(2);
      expect(await Dataset.count({ where: { userGroupId: group.id } })).toBe(1);
      expect(await Revision.count({ where: { createdBy: { id: user.id } } })).toBe(1);
    });
  });

  describe('updateTopics', () => {
    it('keeps the existing topics when saving the new ones fails', async () => {
      const { dataset, revision } = await seedDataset();
      const topic = await Topic.create({ path: '1', nameEN: 'Topic', nameCY: 'Pwnc' }).save();
      await RevisionTopic.create({ revisionId: revision.id, topicId: topic.id }).save();

      // a topic that doesn't exist violates the FK constraint, failing after the existing topics were removed
      await expect(service.updateTopics(dataset.id, ['999999'])).rejects.toThrow();

      const topics = await RevisionTopic.find({ where: { revisionId: revision.id } });
      expect(topics.map((t) => t.topicId)).toEqual([topic.id]);
    });
  });

  describe('withdrawFromPublication', () => {
    it('keeps the revision approved and the file in place when recording the withdrawal fails', async () => {
      const { dataset, revision, user } = await seedScheduledDataset();

      jest.spyOn(TaskService.prototype, 'withdrawApproved').mockRejectedValueOnce(new Error('task failed'));

      await expect(service.withdrawFromPublication(dataset.id, revision.id, user)).rejects.toThrow('task failed');

      const reloadedRevision = await Revision.findOneByOrFail({ id: revision.id });
      expect(reloadedRevision.approvedAt).not.toBeNull();
      expect(reloadedRevision.onlineCubeFilename).toBe('cube.duckdb');

      const reloadedDataset = await Dataset.findOneByOrFail({ id: dataset.id });
      expect(reloadedDataset.publishedRevisionId).toBe(revision.id);
      expect(reloadedDataset.draftRevisionId).toBeNull();

      expect(fileService.delete).not.toHaveBeenCalled();
    });

    it('reverts the revision to draft and records the withdrawal when nothing fails', async () => {
      const { dataset, revision, user } = await seedScheduledDataset();

      await service.withdrawFromPublication(dataset.id, revision.id, user);

      const reloadedRevision = await Revision.findOneByOrFail({ id: revision.id });
      expect(reloadedRevision.approvedAt).toBeNull();

      const reloadedDataset = await Dataset.findOneByOrFail({ id: dataset.id });
      expect(reloadedDataset.draftRevisionId).toBe(revision.id);

      const tasks = await Task.find({ where: { datasetId: dataset.id } });
      expect(tasks).toEqual([expect.objectContaining({ status: TaskStatus.Withdrawn, open: false })]);

      expect(fileService.delete).toHaveBeenCalledWith('cube.duckdb', dataset.id);
    });

    it('closes an open publish task that was created after the dataset was loaded', async () => {
      const { dataset, revision, user } = await seedScheduledDataset();
      const task = await createTask(dataset.id, TaskAction.Publish, user);

      // simulate the task being created concurrently, after the dataset (and its tasks) were read
      const getById = DatasetRepository.getById.bind(DatasetRepository);
      jest
        .spyOn(DatasetRepository, 'getById')
        .mockImplementationOnce(async (...args) => ({ ...(await getById(...args)), tasks: [] }) as Dataset);

      await service.withdrawFromPublication(dataset.id, revision.id, user);

      const tasks = await Task.find({ where: { datasetId: dataset.id } });
      expect(tasks).toEqual([expect.objectContaining({ id: task.id, status: TaskStatus.Withdrawn, open: false })]);
    });
  });

  describe('rejectPublication', () => {
    it('leaves the publish task requested when closing sibling tasks fails', async () => {
      const { dataset, revision, user } = await seedDataset();
      const task = await createTask(dataset.id, TaskAction.Publish, user);

      jest.spyOn(TaskService.prototype, 'closeOpenPublishTasks').mockRejectedValueOnce(new Error('close failed'));

      await expect(service.rejectPublication(dataset.id, revision.id, task.id, user, 'Not ready')).rejects.toThrow(
        'close failed'
      );

      const reloadedTask = await Task.findOneByOrFail({ id: task.id });
      expect(reloadedTask.status).toBe(TaskStatus.Requested);
      expect(reloadedTask.comment).toBeNull();
    });

    it('rejects the publish task when nothing fails', async () => {
      const { dataset, revision, user } = await seedDataset();
      const task = await createTask(dataset.id, TaskAction.Publish, user);

      const rejected = await service.rejectPublication(dataset.id, revision.id, task.id, user, 'Not ready');

      expect(rejected).toEqual(expect.objectContaining({ status: TaskStatus.Rejected, open: true }));
      const reloadedTask = await Task.findOneByOrFail({ id: task.id });
      expect(reloadedTask.status).toBe(TaskStatus.Rejected);
      expect(reloadedTask.comment).toBe('Not ready');
    });

    it('deletes the online cube file once the rejection is committed', async () => {
      const { dataset, revision, user } = await seedScheduledDataset();
      const task = await createTask(dataset.id, TaskAction.Publish, user);

      await service.rejectPublication(dataset.id, revision.id, task.id, user, 'Not ready');

      const reloadedRevision = await Revision.findOneByOrFail({ id: revision.id });
      expect(reloadedRevision.onlineCubeFilename).toBeNull();
      expect(fileService.delete).toHaveBeenCalledWith('cube.duckdb', dataset.id);
    });
  });

  describe('approveArchive', () => {
    it('does not archive the dataset when approving the task fails', async () => {
      const { dataset, user } = await seedDataset();
      const task = await createTask(dataset.id, TaskAction.Archive, user);

      jest.spyOn(TaskService.prototype, 'update').mockRejectedValueOnce(new Error('update failed'));

      await expect(new TaskService().approveArchive(task.id, user)).rejects.toThrow('update failed');

      const reloadedDataset = await Dataset.findOneByOrFail({ id: dataset.id });
      expect(reloadedDataset.archivedAt).toBeNull();
    });

    it('archives the dataset and approves the task when nothing fails', async () => {
      const { dataset, user } = await seedDataset();
      const task = await createTask(dataset.id, TaskAction.Archive, user);

      await new TaskService().approveArchive(task.id, user);

      const reloadedDataset = await Dataset.findOneByOrFail({ id: dataset.id });
      expect(reloadedDataset.archivedAt).not.toBeNull();
      const reloadedTask = await Task.findOneByOrFail({ id: task.id });
      expect(reloadedTask).toEqual(expect.objectContaining({ status: TaskStatus.Approved, open: false }));
    });
  });
});
