// SW-1300 — a dataset could end up with more than one open "publish" task in `requested`
// status. When one was approved (publishing the revision), the sibling open task was left
// open, so getPublishingStatus kept returning UpdatePendingApproval even though the revision
// was already approved and live. These tests exercise the real DB (so the partial unique
// index migration is applied) to prove it is impossible to accumulate duplicate open publish
// tasks via the two races called out in the ticket: near-simultaneous submits and rapid
// submit → withdraw → resubmit.

import { QueryFailedError } from 'typeorm';

import { ensureWorkerDataSources, resetDatabase } from '../../helpers/reset-database';
import { Dataset } from '../../../src/entities/dataset/dataset';
import { Revision } from '../../../src/entities/dataset/revision';
import { Task } from '../../../src/entities/task/task';
import { User } from '../../../src/entities/user/user';
import { DatasetService } from '../../../src/services/dataset';
import { TaskAction } from '../../../src/enums/task-action';
import { TaskStatus } from '../../../src/enums/task-status';
import { Locale } from '../../../src/enums/locale';
import { StorageService } from '../../../src/interfaces/storage-service';
import { getTestUser } from '../../helpers/get-test-user';
import { uuidV4 } from '../../../src/utils/uuid';

const fileService = { delete: jest.fn().mockResolvedValue(undefined) } as unknown as StorageService;

interface Seeded {
  datasetId: string;
  revisionId: string;
  user: User;
  service: DatasetService;
}

async function seedDraftDataset(): Promise<Seeded> {
  const user = await getTestUser('sw1300-user').save();

  const datasetId = uuidV4();
  const revisionId = uuidV4();

  const dataset = new Dataset();
  dataset.id = datasetId;
  dataset.createdBy = user;
  await dataset.save();

  const revision = new Revision();
  revision.id = revisionId;
  revision.datasetId = datasetId;
  revision.createdBy = user;
  revision.revisionIndex = 1;
  revision.publishAt = null;
  revision.approvedAt = null;
  revision.unpublishedAt = null;
  await revision.save();

  dataset.draftRevision = revision;
  dataset.startRevision = revision;
  dataset.endRevision = revision;
  await dataset.save();

  const service = new DatasetService(Locale.EnglishGb, fileService);

  return { datasetId, revisionId, user, service };
}

function countOpenPublishTasks(datasetId: string): Promise<number> {
  return Task.count({ where: { datasetId, action: TaskAction.Publish, open: true } });
}

describe('SW-1300 — at most one open publish task per dataset', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
  });

  afterEach(async () => {
    await resetDatabase();
  });

  it('near-simultaneous submissions create only one open publish task', async () => {
    const { datasetId, revisionId, user, service } = await seedDraftDataset();

    // Fire several submissions concurrently — without the partial unique index and the race
    // handling in submitForPublication these would each insert their own open publish task.
    await Promise.all(Array.from({ length: 5 }, () => service.submitForPublication(datasetId, revisionId, user)));

    expect(await countOpenPublishTasks(datasetId)).toBe(1);
  });

  it('rapid submit → withdraw → resubmit leaves only one open publish task', async () => {
    const { datasetId, revisionId, user, service } = await seedDraftDataset();

    await service.submitForPublication(datasetId, revisionId, user);
    expect(await countOpenPublishTasks(datasetId)).toBe(1);

    await service.withdrawFromPublication(datasetId, revisionId, user);
    expect(await countOpenPublishTasks(datasetId)).toBe(0);

    await service.submitForPublication(datasetId, revisionId, user);
    expect(await countOpenPublishTasks(datasetId)).toBe(1);
  });

  it('a duplicate submission after an existing pending task is a no-op', async () => {
    const { datasetId, revisionId, user, service } = await seedDraftDataset();

    await service.submitForPublication(datasetId, revisionId, user);
    await service.submitForPublication(datasetId, revisionId, user);

    expect(await countOpenPublishTasks(datasetId)).toBe(1);
  });

  it('the partial unique index rejects a second open publish task at the DB level', async () => {
    const { datasetId, user } = await seedDraftDataset();

    await Task.create({
      datasetId,
      action: TaskAction.Publish,
      status: TaskStatus.Requested,
      open: true,
      createdBy: user
    }).save();

    await expect(
      Task.create({
        datasetId,
        action: TaskAction.Publish,
        status: TaskStatus.Requested,
        open: true,
        createdBy: user
      }).save()
    ).rejects.toBeInstanceOf(QueryFailedError);

    expect(await countOpenPublishTasks(datasetId)).toBe(1);
  });
});
