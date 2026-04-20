import { dbManager } from '../../src/db/database-manager';
import { Dataset } from '../../src/entities/dataset/dataset';
import { Revision } from '../../src/entities/dataset/revision';
import { RevisionMetadata } from '../../src/entities/dataset/revision-metadata';
import { DatasetStatsRepository } from '../../src/repositories/dataset-stats';
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

const user: User = getTestUser('ds-stats-test');

function pastDate(hoursAgo: number): Date {
  return new Date(Date.now() - hoursAgo * 60 * 60 * 1000);
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

describe('DatasetStatsRepository', () => {
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

  describe('similarTitles', () => {
    it('returns default empty result when no published datasets exist', async () => {
      const result = await DatasetStatsRepository.similarTitles(Locale.EnglishGb);
      expect(result).toEqual([{ similarity_score: 0, title_1: '', title_2: '' }]);
    });

    it('returns similar English titles for published revisions', async () => {
      const ds1 = await createDataset(user);
      const ds2 = await createDataset(user);

      await createRevisionWithMetadata(ds1, user, 1, 'Population Statistics Wales 2023', Locale.EnglishGb, {
        approvedAt: pastDate(48),
        publishAt: pastDate(24)
      });
      await createRevisionWithMetadata(ds2, user, 1, 'Population Statistics Wales 2024', Locale.EnglishGb, {
        approvedAt: pastDate(48),
        publishAt: pastDate(24)
      });

      const result = await DatasetStatsRepository.similarTitles(Locale.EnglishGb);

      expect(result.length).toBeGreaterThan(0);
      const titles = result.flatMap((r) => [r.title_1, r.title_2]);
      expect(titles).toContain('Population Statistics Wales 2023');
      expect(titles).toContain('Population Statistics Wales 2024');
      result.forEach((r) => expect(r.similarity_score).toBeGreaterThanOrEqual(0.6));
    });

    it('excludes pairs where similarity is below 0.6', async () => {
      const ds1 = await createDataset(user);
      const ds2 = await createDataset(user);

      await createRevisionWithMetadata(ds1, user, 1, 'Completely Unrelated Agriculture Report', Locale.EnglishGb, {
        approvedAt: pastDate(48),
        publishAt: pastDate(24)
      });
      await createRevisionWithMetadata(ds2, user, 1, 'Totally Different Housing Survey', Locale.EnglishGb, {
        approvedAt: pastDate(48),
        publishAt: pastDate(24)
      });

      const result = await DatasetStatsRepository.similarTitles(Locale.EnglishGb);

      const titles = result.flatMap((r) => [r.title_1, r.title_2]);
      expect(titles).not.toContain('Completely Unrelated Agriculture Report');
      expect(titles).not.toContain('Totally Different Housing Survey');
    });

    it('excludes unpublished revisions (no approvedAt)', async () => {
      const ds1 = await createDataset(user);
      const ds2 = await createDataset(user);

      await createRevisionWithMetadata(ds1, user, 1, 'Draft Similar Title Alpha 2023', Locale.EnglishGb, {
        approvedAt: null,
        publishAt: null
      });
      await createRevisionWithMetadata(ds2, user, 1, 'Draft Similar Title Alpha 2024', Locale.EnglishGb, {
        approvedAt: null,
        publishAt: null
      });

      const result = await DatasetStatsRepository.similarTitles(Locale.EnglishGb);

      const titles = result.flatMap((r) => [r.title_1, r.title_2]);
      expect(titles).not.toContain('Draft Similar Title Alpha 2023');
      expect(titles).not.toContain('Draft Similar Title Alpha 2024');
    });

    it('excludes unpublished revisions (unpublishedAt set)', async () => {
      const ds1 = await createDataset(user);
      const ds2 = await createDataset(user);

      await createRevisionWithMetadata(ds1, user, 1, 'Withdrawn Similar Report Beta 2023', Locale.EnglishGb, {
        approvedAt: pastDate(72),
        publishAt: pastDate(48),
        unpublishedAt: pastDate(1)
      });
      await createRevisionWithMetadata(ds2, user, 1, 'Withdrawn Similar Report Beta 2024', Locale.EnglishGb, {
        approvedAt: pastDate(72),
        publishAt: pastDate(48),
        unpublishedAt: pastDate(1)
      });

      const result = await DatasetStatsRepository.similarTitles(Locale.EnglishGb);

      const titles = result.flatMap((r) => [r.title_1, r.title_2]);
      expect(titles).not.toContain('Withdrawn Similar Report Beta 2023');
      expect(titles).not.toContain('Withdrawn Similar Report Beta 2024');
    });

    it('returns Welsh similar titles for Welsh locale', async () => {
      const ds1 = await createDataset(user);
      const ds2 = await createDataset(user);

      await createRevisionWithMetadata(ds1, user, 1, 'Ystadegau Poblogaeth Cymru 2023', Locale.WelshGb, {
        approvedAt: pastDate(48),
        publishAt: pastDate(24)
      });
      await createRevisionWithMetadata(ds2, user, 1, 'Ystadegau Poblogaeth Cymru 2024', Locale.WelshGb, {
        approvedAt: pastDate(48),
        publishAt: pastDate(24)
      });

      const result = await DatasetStatsRepository.similarTitles(Locale.WelshGb);

      expect(result.length).toBeGreaterThan(0);
      const titles = result.flatMap((r) => [r.title_1, r.title_2]);
      expect(titles).toContain('Ystadegau Poblogaeth Cymru 2023');
      expect(titles).toContain('Ystadegau Poblogaeth Cymru 2024');
    });

    it('does not return Welsh titles for English locale', async () => {
      const result = await DatasetStatsRepository.similarTitles(Locale.EnglishGb);

      const titles = result.flatMap((r) => [r.title_1, r.title_2]);
      expect(titles).not.toContain('Ystadegau Poblogaeth Cymru 2023');
      expect(titles).not.toContain('Ystadegau Poblogaeth Cymru 2024');
    });
  });
});
