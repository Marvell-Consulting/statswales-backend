import { EntityNotFoundError } from 'typeorm';

import { dbManager } from '../../src/db/database-manager';
import { Dataset } from '../../src/entities/dataset/dataset';
import { Revision } from '../../src/entities/dataset/revision';
import { PublishedRevisionRepository } from '../../src/repositories/published-revision';
import { getTestUser } from '../helpers/get-test-user';
import { User } from '../../src/entities/user/user';
import { uuidV4 } from '../../src/utils/uuid';

jest.mock('../../src/services/blob-storage', () => {
  return function BlobStorage() {
    return {
      getContainerClient: jest.fn().mockReturnValue({
        createIfNotExists: jest.fn().mockResolvedValue(true)
      })
    };
  };
});

const user: User = getTestUser('pub-rev-test');

function pastDate(hoursAgo: number): Date {
  return new Date(Date.now() - hoursAgo * 60 * 60 * 1000);
}

function futureDate(hoursAhead: number): Date {
  return new Date(Date.now() + hoursAhead * 60 * 60 * 1000);
}

async function createDataset(createdBy: User): Promise<Dataset> {
  const ds = new Dataset();
  ds.id = uuidV4();
  ds.createdBy = createdBy;
  return ds.save();
}

async function createRevision(
  dataset: Dataset,
  createdBy: User,
  revisionIndex: number,
  overrides: Partial<Revision> = {}
): Promise<Revision> {
  const rev = new Revision();
  rev.id = uuidV4();
  rev.datasetId = dataset.id;
  rev.createdBy = createdBy;
  rev.revisionIndex = revisionIndex;
  rev.publishAt = null;
  rev.approvedAt = null;
  rev.unpublishedAt = null;
  Object.assign(rev, overrides);
  return rev.save();
}

describe('PublishedRevisionRepository', () => {
  beforeAll(async () => {
    try {
      await dbManager.initDataSources();
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.getAppDataSource().runMigrations();
      await user.save();
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('Failed to initialise test database', err);
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.destroyDataSources();
      process.exit(1);
    }
  });

  afterAll(async () => {
    await dbManager.getAppDataSource().dropDatabase();
    await dbManager.destroyDataSources();
  });

  describe('getById', () => {
    let dataset: Dataset;
    let publishedRevision: Revision;
    let futurePublishRevision: Revision;
    let futureApprovalRevision: Revision;
    let unpublishedRevision: Revision;
    let draftRevision: Revision;

    beforeAll(async () => {
      dataset = await createDataset(user);

      // A fully published revision (publishAt and approvedAt in the past, no unpublishedAt)
      publishedRevision = await createRevision(dataset, user, 1, {
        publishAt: pastDate(48),
        approvedAt: pastDate(72)
      });

      // A revision with a future publish date (approved but not yet published)
      futurePublishRevision = await createRevision(dataset, user, 2, {
        publishAt: futureDate(24),
        approvedAt: pastDate(1)
      });

      // A revision with a future approval date
      futureApprovalRevision = await createRevision(dataset, user, 3, {
        publishAt: pastDate(1),
        approvedAt: futureDate(24)
      });

      // A revision that was published but has since been unpublished
      unpublishedRevision = await createRevision(dataset, user, 4, {
        publishAt: pastDate(48),
        approvedAt: pastDate(72),
        unpublishedAt: pastDate(1)
      });

      // A draft revision with null dates
      draftRevision = await createRevision(dataset, user, 5);
    });

    it('should return a revision that is published and approved in the past with no unpublishedAt', async () => {
      const result = await PublishedRevisionRepository.getById(publishedRevision.id);
      expect(result.id).toBe(publishedRevision.id);
    });

    it('should throw when the revision has a future publishAt date', async () => {
      await expect(PublishedRevisionRepository.getById(futurePublishRevision.id)).rejects.toThrow(EntityNotFoundError);
    });

    it('should throw when the revision has a future approvedAt date', async () => {
      await expect(PublishedRevisionRepository.getById(futureApprovalRevision.id)).rejects.toThrow(EntityNotFoundError);
    });

    it('should throw when the revision has been unpublished', async () => {
      await expect(PublishedRevisionRepository.getById(unpublishedRevision.id)).rejects.toThrow(EntityNotFoundError);
    });

    it('should throw when the revision is a draft (null publishAt and approvedAt)', async () => {
      await expect(PublishedRevisionRepository.getById(draftRevision.id)).rejects.toThrow(EntityNotFoundError);
    });

    it('should throw for a non-existent revision id', async () => {
      await expect(PublishedRevisionRepository.getById(uuidV4())).rejects.toThrow(EntityNotFoundError);
    });
  });

  describe('getLatestByDatasetId', () => {
    describe('with no published revisions', () => {
      let emptyDataset: Dataset;

      beforeAll(async () => {
        emptyDataset = await createDataset(user);
        // Only draft revisions
        await createRevision(emptyDataset, user, 1);
      });

      it('should return null when there are no published revisions', async () => {
        const result = await PublishedRevisionRepository.getLatestByDatasetId(emptyDataset.id);
        expect(result).toBeNull();
      });
    });

    describe('with a single published revision', () => {
      let dataset: Dataset;
      let publishedRev: Revision;

      beforeAll(async () => {
        dataset = await createDataset(user);
        publishedRev = await createRevision(dataset, user, 1, {
          publishAt: pastDate(24),
          approvedAt: pastDate(48)
        });
      });

      it('should return the published revision', async () => {
        const result = await PublishedRevisionRepository.getLatestByDatasetId(dataset.id);
        expect(result).not.toBeNull();
        expect(result!.id).toBe(publishedRev.id);
      });
    });

    describe('with multiple published revisions', () => {
      let dataset: Dataset;
      let newerRev: Revision;

      beforeAll(async () => {
        dataset = await createDataset(user);
        await createRevision(dataset, user, 1, {
          publishAt: pastDate(72),
          approvedAt: pastDate(96)
        });
        newerRev = await createRevision(dataset, user, 2, {
          publishAt: pastDate(24),
          approvedAt: pastDate(48)
        });
      });

      it('should return the most recent published revision (ordered by publishAt DESC)', async () => {
        const result = await PublishedRevisionRepository.getLatestByDatasetId(dataset.id);
        expect(result).not.toBeNull();
        expect(result!.id).toBe(newerRev.id);
      });
    });

    describe('filtering out future publish dates', () => {
      let dataset: Dataset;
      let currentRev: Revision;

      beforeAll(async () => {
        dataset = await createDataset(user);
        currentRev = await createRevision(dataset, user, 1, {
          publishAt: pastDate(24),
          approvedAt: pastDate(48)
        });
        // Scheduled for future - should not be returned
        await createRevision(dataset, user, 2, {
          publishAt: futureDate(24),
          approvedAt: pastDate(1)
        });
      });

      it('should not return revisions with a future publishAt', async () => {
        const result = await PublishedRevisionRepository.getLatestByDatasetId(dataset.id);
        expect(result).not.toBeNull();
        expect(result!.id).toBe(currentRev.id);
      });
    });

    describe('filtering out future approval dates', () => {
      let dataset: Dataset;
      let approvedRev: Revision;

      beforeAll(async () => {
        dataset = await createDataset(user);
        approvedRev = await createRevision(dataset, user, 1, {
          publishAt: pastDate(24),
          approvedAt: pastDate(48)
        });
        // Approved in the future - should not be returned
        await createRevision(dataset, user, 2, {
          publishAt: pastDate(1),
          approvedAt: futureDate(24)
        });
      });

      it('should not return revisions with a future approvedAt', async () => {
        const result = await PublishedRevisionRepository.getLatestByDatasetId(dataset.id);
        expect(result).not.toBeNull();
        expect(result!.id).toBe(approvedRev.id);
      });
    });

    describe('filtering out unpublished revisions', () => {
      let dataset: Dataset;
      let activeRev: Revision;

      beforeAll(async () => {
        dataset = await createDataset(user);
        activeRev = await createRevision(dataset, user, 1, {
          publishAt: pastDate(72),
          approvedAt: pastDate(96)
        });
        // Was published but has been unpublished - should not be returned
        await createRevision(dataset, user, 2, {
          publishAt: pastDate(24),
          approvedAt: pastDate(48),
          unpublishedAt: pastDate(1)
        });
      });

      it('should not return revisions where unpublishedAt is set', async () => {
        const result = await PublishedRevisionRepository.getLatestByDatasetId(dataset.id);
        expect(result).not.toBeNull();
        expect(result!.id).toBe(activeRev.id);
      });
    });

    describe('all revisions are unpublished', () => {
      let dataset: Dataset;

      beforeAll(async () => {
        dataset = await createDataset(user);
        await createRevision(dataset, user, 1, {
          publishAt: pastDate(48),
          approvedAt: pastDate(72),
          unpublishedAt: pastDate(1)
        });
      });

      it('should return null when all revisions are unpublished', async () => {
        const result = await PublishedRevisionRepository.getLatestByDatasetId(dataset.id);
        expect(result).toBeNull();
      });
    });

    describe('only scheduled (future) revisions exist', () => {
      let dataset: Dataset;

      beforeAll(async () => {
        dataset = await createDataset(user);
        await createRevision(dataset, user, 1, {
          publishAt: futureDate(24),
          approvedAt: pastDate(1)
        });
        await createRevision(dataset, user, 2, {
          publishAt: futureDate(48),
          approvedAt: pastDate(1)
        });
      });

      it('should return null when all revisions have future publish dates', async () => {
        const result = await PublishedRevisionRepository.getLatestByDatasetId(dataset.id);
        expect(result).toBeNull();
      });
    });

    describe('returns correct revision when latest is unpublished and earlier is scheduled', () => {
      let dataset: Dataset;
      let onlyValidRev: Revision;

      beforeAll(async () => {
        dataset = await createDataset(user);
        onlyValidRev = await createRevision(dataset, user, 1, {
          publishAt: pastDate(96),
          approvedAt: pastDate(120)
        });
        // Second revision was published then unpublished
        await createRevision(dataset, user, 2, {
          publishAt: pastDate(48),
          approvedAt: pastDate(72),
          unpublishedAt: pastDate(24)
        });
        // Third revision is scheduled for the future
        await createRevision(dataset, user, 3, {
          publishAt: futureDate(24),
          approvedAt: pastDate(1)
        });
      });

      it('should return the only valid published revision', async () => {
        const result = await PublishedRevisionRepository.getLatestByDatasetId(dataset.id);
        expect(result).not.toBeNull();
        expect(result!.id).toBe(onlyValidRev.id);
      });
    });

    it('should return null for a non-existent dataset id', async () => {
      const result = await PublishedRevisionRepository.getLatestByDatasetId(uuidV4());
      expect(result).toBeNull();
    });
  });
});
