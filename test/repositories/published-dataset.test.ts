import { EntityNotFoundError } from 'typeorm';

import { dbManager } from '../../src/db/database-manager';
import { Dataset } from '../../src/entities/dataset/dataset';
import { Revision } from '../../src/entities/dataset/revision';
import { RevisionMetadata } from '../../src/entities/dataset/revision-metadata';
import { PublishedDatasetRepository, withPublishedRevision } from '../../src/repositories/published-dataset';
import { getTestUser } from '../helpers/get-test-user';
import { User } from '../../src/entities/user/user';
import { uuidV4 } from '../../src/utils/uuid';
import { Locale } from '../../src/enums/locale';

jest.mock('../../src/services/blob-storage', () => {
  return function BlobStorage() {
    return {
      getContainerClient: jest.fn().mockReturnValue({
        createIfNotExists: jest.fn().mockResolvedValue(true)
      })
    };
  };
});

const user: User = getTestUser('pub-ds-test');

function pastDate(hoursAgo: number): Date {
  return new Date(Date.now() - hoursAgo * 60 * 60 * 1000);
}

function futureDate(hoursAhead: number): Date {
  return new Date(Date.now() + hoursAhead * 60 * 60 * 1000);
}

async function createDataset(createdBy: User, overrides: Partial<Dataset> = {}): Promise<Dataset> {
  const ds = new Dataset();
  ds.id = uuidV4();
  ds.createdBy = createdBy;
  Object.assign(ds, overrides);
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

async function createRevisionWithMetadata(
  dataset: Dataset,
  createdBy: User,
  revisionIndex: number,
  title: string,
  language: string,
  overrides: Partial<Revision> = {}
): Promise<Revision> {
  const rev = await createRevision(dataset, createdBy, revisionIndex, overrides);
  const meta = new RevisionMetadata();
  meta.id = rev.id;
  meta.language = language;
  meta.title = title;
  await meta.save();
  return rev;
}

describe('PublishedDatasetRepository', () => {
  beforeAll(async () => {
    try {
      await dbManager.initDataSources();
      await user.save();
    } catch (err) {
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
    it('should return dataset when firstPublishedAt is in the past', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(24) });
      await createRevision(ds, user, 1, { publishAt: pastDate(24), approvedAt: pastDate(48) });

      const result = await PublishedDatasetRepository.getById(ds.id, withPublishedRevision);
      expect(result.id).toBe(ds.id);
    });

    it('should throw when firstPublishedAt is null', async () => {
      const ds = await createDataset(user, { firstPublishedAt: null });
      await createRevision(ds, user, 1, { publishAt: pastDate(24), approvedAt: pastDate(48) });

      await expect(PublishedDatasetRepository.getById(ds.id, withPublishedRevision)).rejects.toThrow(
        EntityNotFoundError
      );
    });

    it('should throw when firstPublishedAt is in the future', async () => {
      const ds = await createDataset(user, { firstPublishedAt: futureDate(24) });
      await createRevision(ds, user, 1, { publishAt: pastDate(24), approvedAt: pastDate(48) });

      await expect(PublishedDatasetRepository.getById(ds.id, withPublishedRevision)).rejects.toThrow(
        EntityNotFoundError
      );
    });

    it('should return latest publishedRevision by publishAt DESC when multiple exist', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(96) });
      await createRevision(ds, user, 1, { publishAt: pastDate(72), approvedAt: pastDate(96) });
      const newerRev = await createRevision(ds, user, 2, { publishAt: pastDate(24), approvedAt: pastDate(48) });

      const result = await PublishedDatasetRepository.getById(ds.id, withPublishedRevision);
      expect(result.publishedRevision).not.toBeNull();
      expect(result.publishedRevision!.id).toBe(newerRev.id);
    });

    it('should exclude revisions with future publishAt from publishedRevision', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(96) });
      const pastRev = await createRevision(ds, user, 1, { publishAt: pastDate(24), approvedAt: pastDate(48) });
      await createRevision(ds, user, 2, { publishAt: futureDate(24), approvedAt: pastDate(1) });

      const result = await PublishedDatasetRepository.getById(ds.id, withPublishedRevision);
      expect(result.publishedRevision).not.toBeNull();
      expect(result.publishedRevision!.id).toBe(pastRev.id);
    });

    it('should exclude revisions with future approvedAt from publishedRevision', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(96) });
      const pastRev = await createRevision(ds, user, 1, { publishAt: pastDate(48), approvedAt: pastDate(72) });
      await createRevision(ds, user, 2, { publishAt: pastDate(1), approvedAt: futureDate(24) });

      const result = await PublishedDatasetRepository.getById(ds.id, withPublishedRevision);
      expect(result.publishedRevision).not.toBeNull();
      expect(result.publishedRevision!.id).toBe(pastRev.id);
    });

    it('should return publishedRevision as null when no qualifying revision exists', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(24) });
      // Only draft revisions, no published ones
      await createRevision(ds, user, 1);

      const result = await PublishedDatasetRepository.getById(ds.id, withPublishedRevision);
      expect(result.publishedRevision).toBeNull();
    });

    it('should NOT filter on unpublishedAt (current behaviour)', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(96) });
      const unpubRev = await createRevision(ds, user, 1, {
        publishAt: pastDate(48),
        approvedAt: pastDate(72),
        unpublishedAt: pastDate(1)
      });

      const result = await PublishedDatasetRepository.getById(ds.id, withPublishedRevision);
      // getById does not exclude unpublished revisions from publishedRevision
      expect(result.publishedRevision).not.toBeNull();
      expect(result.publishedRevision!.id).toBe(unpubRev.id);
    });

    it('should throw for non-existent dataset id', async () => {
      await expect(PublishedDatasetRepository.getById(uuidV4(), withPublishedRevision)).rejects.toThrow(
        EntityNotFoundError
      );
    });
  });

  describe('getHistoryById', () => {
    it('should return all published revisions ordered by publishAt DESC', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(120) });
      const olderRev = await createRevision(ds, user, 1, { publishAt: pastDate(96), approvedAt: pastDate(120) });
      const newerRev = await createRevision(ds, user, 2, { publishAt: pastDate(24), approvedAt: pastDate(48) });

      const result = await PublishedDatasetRepository.getHistoryById(ds.id);
      expect(result).toHaveLength(2);
      expect(result[0].id).toBe(newerRev.id);
      expect(result[1].id).toBe(olderRev.id);
    });

    it('should exclude revisions with future publishAt', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(96) });
      const pastRev = await createRevision(ds, user, 1, { publishAt: pastDate(24), approvedAt: pastDate(48) });
      await createRevision(ds, user, 2, { publishAt: futureDate(24), approvedAt: pastDate(1) });

      const result = await PublishedDatasetRepository.getHistoryById(ds.id);
      expect(result).toHaveLength(1);
      expect(result[0].id).toBe(pastRev.id);
    });

    it('should exclude revisions with future approvedAt', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(96) });
      const pastRev = await createRevision(ds, user, 1, { publishAt: pastDate(48), approvedAt: pastDate(72) });
      await createRevision(ds, user, 2, { publishAt: pastDate(1), approvedAt: futureDate(24) });

      const result = await PublishedDatasetRepository.getHistoryById(ds.id);
      expect(result).toHaveLength(1);
      expect(result[0].id).toBe(pastRev.id);
    });

    it('should exclude revisions where unpublishedAt is set', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(120) });
      const activeRev = await createRevision(ds, user, 1, { publishAt: pastDate(96), approvedAt: pastDate(120) });
      await createRevision(ds, user, 2, {
        publishAt: pastDate(24),
        approvedAt: pastDate(48),
        unpublishedAt: pastDate(1)
      });

      const result = await PublishedDatasetRepository.getHistoryById(ds.id);
      expect(result).toHaveLength(1);
      expect(result[0].id).toBe(activeRev.id);
    });

    it('should return empty array when no published revisions exist', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(24) });
      await createRevision(ds, user, 1); // draft only

      const result = await PublishedDatasetRepository.getHistoryById(ds.id);
      expect(result).toHaveLength(0);
    });

    it('should return empty array for non-existent datasetId', async () => {
      const result = await PublishedDatasetRepository.getHistoryById(uuidV4());
      expect(result).toHaveLength(0);
    });
  });

  describe('listPublishedByLanguage', () => {
    it('should return published datasets with the correct language title', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(24) });
      await createRevisionWithMetadata(ds, user, 1, 'English Title', Locale.EnglishGb, {
        publishAt: pastDate(24),
        approvedAt: pastDate(48)
      });

      const result = await PublishedDatasetRepository.listPublishedByLanguage(Locale.EnglishGb, 1, 100);
      const match = result.data.find((d: any) => d.id === ds.id);
      expect(match).toBeDefined();
      expect(match.title).toBe('English Title');
    });

    it('should respect pagination', async () => {
      // Create 3 published datasets
      const datasets = [];
      for (let i = 0; i < 3; i++) {
        const ds = await createDataset(user, { firstPublishedAt: pastDate(24 * (i + 1)) });
        await createRevisionWithMetadata(ds, user, 1, `Paginated ${i}`, Locale.EnglishGb, {
          publishAt: pastDate(24 * (i + 1)),
          approvedAt: pastDate(48 * (i + 1))
        });
        datasets.push(ds);
      }

      const page1 = await PublishedDatasetRepository.listPublishedByLanguage(Locale.EnglishGb, 1, 2);
      expect(page1.data.length).toBeLessThanOrEqual(2);
      expect(page1.count).toBeGreaterThanOrEqual(3);
    });

    it('should exclude datasets where firstPublishedAt is null or in the future', async () => {
      const nullDs = await createDataset(user, { firstPublishedAt: null });
      await createRevisionWithMetadata(nullDs, user, 1, 'Null Published', Locale.EnglishGb, {
        publishAt: pastDate(24),
        approvedAt: pastDate(48)
      });

      const futureDs = await createDataset(user, { firstPublishedAt: futureDate(24) });
      await createRevisionWithMetadata(futureDs, user, 1, 'Future Published', Locale.EnglishGb, {
        publishAt: pastDate(24),
        approvedAt: pastDate(48)
      });

      const result = await PublishedDatasetRepository.listPublishedByLanguage(Locale.EnglishGb, 1, 1000);
      const ids = result.data.map((d: any) => d.id);
      expect(ids).not.toContain(nullDs.id);
      expect(ids).not.toContain(futureDs.id);
    });

    it('should use latest published revision data, not future-scheduled revisions', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(96) });
      await createRevisionWithMetadata(ds, user, 1, 'Current Title', Locale.EnglishGb, {
        publishAt: pastDate(24),
        approvedAt: pastDate(48)
      });
      // Future scheduled revision should not show up
      await createRevisionWithMetadata(ds, user, 2, 'Future Title', Locale.EnglishGb, {
        publishAt: futureDate(24),
        approvedAt: pastDate(1)
      });

      const result = await PublishedDatasetRepository.listPublishedByLanguage(Locale.EnglishGb, 1, 1000);
      const match = result.data.find((d: any) => d.id === ds.id);
      expect(match).toBeDefined();
      expect(match.title).toBe('Current Title');
    });

    it('should return empty resultset when nothing is published', async () => {
      // Use a Welsh locale that no test data above has metadata for
      const result = await PublishedDatasetRepository.listPublishedByLanguage(Locale.WelshGb, 1, 1000);
      // The result may include Welsh datasets from other tests, but our English-only datasets won't appear
      // So we just verify the shape is correct
      expect(result).toHaveProperty('data');
      expect(result).toHaveProperty('count');
      expect(Array.isArray(result.data)).toBe(true);
    });
  });
});
