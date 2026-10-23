import { dbManager } from '../../../src/db/database-manager';
import { Dataset } from '../../../src/entities/dataset/dataset';
import { Revision } from '../../../src/entities/dataset/revision';
import { RevisionMetadata } from '../../../src/entities/dataset/revision-metadata';
import { DatasetStatsRepository } from '../../../src/repositories/dataset-stats';
import { getTestUser } from '../../helpers/get-test-user';
import { User } from '../../../src/entities/user/user';
import { uuidV4 } from '../../../src/utils/uuid';
import { Locale } from '../../../src/enums/locale';
import { Dimension } from '../../../src/entities/dataset/dimension';
import { DimensionType } from '../../../src/enums/dimension-type';
import { Task } from '../../../src/entities/task/task';
import { TaskAction } from '../../../src/enums/task-action';
import { TaskStatus } from '../../../src/enums/task-status';

jest.mock('../../../src/services/blob-storage', () => {
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

async function createDimension(dataset: Dataset, type: DimensionType): Promise<Dimension> {
  const dim = new Dimension();
  dim.datasetId = dataset.id;
  dim.type = type;
  dim.factTableColumn = `col_${type}`;
  return dim.save();
}

async function createOpenTask(dataset: Dataset, action: TaskAction, createdBy: User): Promise<Task> {
  const task = new Task();
  task.datasetId = dataset.id;
  task.action = action;
  task.status = TaskStatus.Requested;
  task.open = true;
  task.createdBy = createdBy;
  task.updatedBy = createdBy;
  return task.save();
}

describe('DatasetStatsRepository', () => {
  beforeAll(async () => {
    try {
      await dbManager.initDataSources();
      await dbManager.getPublisherDataSource().dropDatabase();
      await dbManager.getPublisherDataSource().runMigrations();
      await user.save();
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('Failed to initialise test database', err);
      await dbManager.getPublisherDataSource().dropDatabase();
      await dbManager.destroyDataSources();
      process.exit(1);
    }
  });

  afterAll(async () => {
    await dbManager.getPublisherDataSource().dropDatabase();
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

  describe('getDashboardStats', () => {
    it('counts a dataset with a live published revision as published even when a newer draft revision exists', async () => {
      const before = (await DatasetStatsRepository.getDashboardStats(Locale.EnglishGb)).summary;

      const ds = await createDataset(user, { firstPublishedAt: pastDate(48) });

      // the currently live revision
      await createRevisionWithMetadata(ds, user, 1, 'SW-1329 Live Revision', Locale.EnglishGb, {
        approvedAt: pastDate(48),
        publishAt: pastDate(24)
      });

      // a newer, unapproved draft revision on top of the live one — the dataset is still
      // live on the consumer site via the revision above, so it should still count as 'published'
      await createRevisionWithMetadata(ds, user, 0, 'SW-1329 Draft Revision', Locale.EnglishGb, {
        approvedAt: null,
        publishAt: null
      });

      const after = (await DatasetStatsRepository.getDashboardStats(Locale.EnglishGb)).summary;

      expect(after.published).toBe(before.published + 1);
      expect(after.incomplete).toBe(before.incomplete);
      expect(after.total).toBe(before.total + 1);
    });

    it('counts a dataset with a live published revision as published even when a newer scheduled revision exists', async () => {
      const before = (await DatasetStatsRepository.getDashboardStats(Locale.EnglishGb)).summary;

      const ds = await createDataset(user, { firstPublishedAt: pastDate(48) });

      // the currently live revision
      await createRevisionWithMetadata(ds, user, 1, 'SW-1329 Live Revision Scheduled', Locale.EnglishGb, {
        approvedAt: pastDate(48),
        publishAt: pastDate(24)
      });

      // a newer, approved revision scheduled to go live in the future
      await createRevisionWithMetadata(ds, user, 0, 'SW-1329 Scheduled Revision', Locale.EnglishGb, {
        approvedAt: pastDate(1),
        publishAt: new Date(Date.now() + 24 * 60 * 60 * 1000)
      });

      const after = (await DatasetStatsRepository.getDashboardStats(Locale.EnglishGb)).summary;

      expect(after.published).toBe(before.published + 1);
      expect(after.scheduled).toBe(before.scheduled);
      expect(after.total).toBe(before.total + 1);
    });
  });

  describe('withoutDateDimension', () => {
    it('counts and lists non-archived datasets that have no date dimension', async () => {
      const before = (await DatasetStatsRepository.getDashboardStats(Locale.EnglishGb)).without_date_dimension;

      const noDates = await createDataset(user, { firstPublishedAt: pastDate(48) });
      await createRevisionWithMetadata(noDates, user, 1, 'SW-1133 No Date Dimension', Locale.EnglishGb, {
        approvedAt: pastDate(48),
        publishAt: pastDate(24)
      });
      await createDimension(noDates, DimensionType.LookupTable);
      await createDimension(noDates, DimensionType.Raw);

      const withDates = await createDataset(user);
      await createRevisionWithMetadata(withDates, user, 1, 'SW-1133 Has Date Dimension', Locale.EnglishGb);
      await createDimension(withDates, DimensionType.DatePeriod);

      const archived = await createDataset(user, { archivedAt: pastDate(1) });
      await createRevisionWithMetadata(archived, user, 1, 'SW-1133 Archived No Date Dimension', Locale.EnglishGb);
      await createDimension(archived, DimensionType.Text);

      const after = (await DatasetStatsRepository.getDashboardStats(Locale.EnglishGb)).without_date_dimension;
      expect(after).toBe(before + 1);

      const result = await DatasetStatsRepository.withoutDateDimension(Locale.EnglishGb);
      const ids = result.map((row) => row.dataset_id);

      expect(ids).not.toContain(withDates.id);
      expect(ids).not.toContain(archived.id);
      expect(result.find((row) => row.dataset_id === noDates.id)).toEqual({
        dataset_id: noDates.id,
        title: 'SW-1133 No Date Dimension',
        revision_by: user.name ?? user.email,
        status: 'live',
        publishing_status: 'published'
      });
    });

    it('returns one row per dataset when the dataset has more than one open task', async () => {
      const before = (await DatasetStatsRepository.getDashboardStats(Locale.EnglishGb)).without_date_dimension;

      const ds = await createDataset(user);
      await createRevisionWithMetadata(ds, user, 1, 'SW-1133 Multiple Open Tasks', Locale.EnglishGb);
      await createDimension(ds, DimensionType.Text);
      await createOpenTask(ds, TaskAction.Publish, user);
      await createOpenTask(ds, TaskAction.Archive, user);

      const after = (await DatasetStatsRepository.getDashboardStats(Locale.EnglishGb)).without_date_dimension;
      expect(after).toBe(before + 1);

      const result = await DatasetStatsRepository.withoutDateDimension(Locale.EnglishGb);
      expect(result.filter((row) => row.dataset_id === ds.id)).toHaveLength(1);
    });
  });
});
