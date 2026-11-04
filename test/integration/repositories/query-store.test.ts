import { ensureWorkerDataSources, resetDatabase } from '../../helpers/reset-database';
import { Dataset } from '../../../src/entities/dataset/dataset';
import { Revision } from '../../../src/entities/dataset/revision';
import { QueryStore } from '../../../src/entities/query-store';
import { QueryStoreRepository } from '../../../src/repositories/query-store';
import { getTestUser } from '../../helpers/get-test-user';
import { User } from '../../../src/entities/user/user';
import { uuidV4 } from '../../../src/utils/uuid';

jest.mock('../../../src/services/blob-storage', () => {
  return function BlobStorage() {
    return {
      getContainerClient: jest.fn().mockReturnValue({
        createIfNotExists: jest.fn().mockResolvedValue(true)
      })
    };
  };
});

const user: User = getTestUser('query-store-test');

const hoursFromNow = (hours: number): Date => new Date(Date.now() + hours * 60 * 60 * 1000);

async function createRevision(overrides: Partial<Revision>): Promise<Revision> {
  const dataset = new Dataset();
  dataset.id = uuidV4();
  dataset.createdBy = user;
  await dataset.save();

  const revision = new Revision();
  revision.id = uuidV4();
  revision.datasetId = dataset.id;
  revision.createdBy = user;
  revision.revisionIndex = 1;
  revision.approvedAt = null;
  revision.publishAt = null;
  revision.unpublishedAt = null;
  Object.assign(revision, overrides);
  return revision.save();
}

async function createEntry(revision: Revision): Promise<QueryStore> {
  const entry = new QueryStore();
  entry.id = uuidV4().slice(0, 12);
  entry.hash = uuidV4();
  entry.datasetId = revision.datasetId;
  entry.revisionId = revision.id;
  entry.requestObject = {};
  entry.query = {};
  entry.totalLines = 0;
  entry.columnMapping = [];
  return entry.save();
}

describe('QueryStoreRepository.rebuildAll', () => {
  let rebuildEntry: jest.SpyInstance;

  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await user.save();
  });

  beforeEach(() => {
    // rebuilding an entry needs a cube; this test only covers which entries are deleted
    rebuildEntry = jest.spyOn(QueryStoreRepository, 'rebuildQueryEntry').mockResolvedValue();
  });

  afterEach(() => {
    rebuildEntry.mockRestore();
  });

  it('keeps entries for published revisions when another revision is scheduled', async () => {
    const published = await createRevision({ approvedAt: hoursFromNow(-72), publishAt: hoursFromNow(-48) });
    const scheduled = await createRevision({ approvedAt: hoursFromNow(-2), publishAt: hoursFromNow(48) });
    const draft = await createRevision({});

    const publishedEntry = await createEntry(published);
    const scheduledEntry = await createEntry(scheduled);
    const draftEntry = await createEntry(draft);

    await QueryStoreRepository.rebuildAll();

    const remaining = await QueryStore.findBy({});
    const remainingIds = remaining.map((entry) => entry.id);

    expect(remainingIds).toContain(publishedEntry.id);
    expect(remainingIds).not.toContain(scheduledEntry.id);
    expect(remainingIds).not.toContain(draftEntry.id);
    expect(rebuildEntry).toHaveBeenCalledWith(publishedEntry.id);
  });
});
