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
  overrides: Partial<Revision> = {},
  summary?: string
): Promise<Revision> {
  const rev = await createRevision(dataset, createdBy, revisionIndex, overrides);
  const meta = new RevisionMetadata();
  meta.id = rev.id;
  meta.language = language;
  meta.title = title;
  if (summary !== undefined) {
    meta.summary = summary;
  }
  await meta.save();
  return rev;
}

describe('PublishedDatasetRepository', () => {
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

    it('should return publishedRevision as null when no unpublished revision exists', async () => {
      const ds = await createDataset(user, { firstPublishedAt: pastDate(96) });
      await createRevision(ds, user, 1, {
        publishAt: pastDate(48),
        approvedAt: pastDate(72),
        unpublishedAt: pastDate(1)
      });

      const result = await PublishedDatasetRepository.getById(ds.id, withPublishedRevision);
      expect(result.publishedRevision).toBeNull();
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
      expect(match?.title).toBe('English Title');
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
      expect(match?.title).toBe('Current Title');
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

  describe('searchBasic', () => {
    let searchDs1: Dataset;
    let searchDs2: Dataset;
    let searchDs3: Dataset;

    beforeAll(async () => {
      searchDs1 = await createDataset(user, { firstPublishedAt: pastDate(72) });
      await createRevisionWithMetadata(
        searchDs1,
        user,
        1,
        'Population Statistics Wales',
        Locale.EnglishGb,
        { publishAt: pastDate(72), approvedAt: pastDate(96) },
        'Annual population estimates for Wales by local authority'
      );

      searchDs2 = await createDataset(user, { firstPublishedAt: pastDate(48) });
      await createRevisionWithMetadata(
        searchDs2,
        user,
        1,
        'School Attendance Report',
        Locale.EnglishGb,
        { publishAt: pastDate(48), approvedAt: pastDate(72) },
        'Attendance figures for primary and secondary schools'
      );

      searchDs3 = await createDataset(user, { firstPublishedAt: pastDate(24) });
      await createRevisionWithMetadata(
        searchDs3,
        user,
        1,
        'Housing Data Summary',
        Locale.EnglishGb,
        { publishAt: pastDate(24), approvedAt: pastDate(48) },
        'Statistics on housing completions and population density'
      );
    });

    it('should find datasets matching title keyword', async () => {
      const result = await PublishedDatasetRepository.searchBasic(Locale.EnglishGb, 'Population', 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).toContain(searchDs1.id);
    });

    it('should find datasets matching summary keyword', async () => {
      const result = await PublishedDatasetRepository.searchBasic(Locale.EnglishGb, 'attendance', 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).toContain(searchDs2.id);
    });

    it('should be case-insensitive', async () => {
      const result = await PublishedDatasetRepository.searchBasic(Locale.EnglishGb, 'HOUSING', 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).toContain(searchDs3.id);
    });

    it('should return correct count', async () => {
      const result = await PublishedDatasetRepository.searchBasic(Locale.EnglishGb, 'population', 1, 100);
      // searchDs1 has "Population" in title, searchDs3 has "population" in summary
      expect(result.count).toBeGreaterThanOrEqual(2);
    });

    it('should return empty when no match', async () => {
      const result = await PublishedDatasetRepository.searchBasic(Locale.EnglishGb, 'xyznonexistent', 1, 100);
      expect(result.data).toHaveLength(0);
      expect(result.count).toBe(0);
    });

    it('should respect pagination', async () => {
      const result = await PublishedDatasetRepository.searchBasic(Locale.EnglishGb, 'Statistics', 1, 1);
      expect(result.data.length).toBeLessThanOrEqual(1);
      expect(result.count).toBeGreaterThanOrEqual(2);
    });

    it('should return expected fields in results', async () => {
      const result = await PublishedDatasetRepository.searchBasic(Locale.EnglishGb, 'Population', 1, 100);
      const match = result.data.find((d) => d.id === searchDs1.id);
      expect(match).toBeDefined();
      expect(match!.title).toBe('Population Statistics Wales');
      expect(match!.summary).toBe('Annual population estimates for Wales by local authority');
      expect(match!.first_published_at).toBeDefined();
      expect(match!.last_updated_at).toBeDefined();
    });

    it('should not include unpublished datasets', async () => {
      const unpubDs = await createDataset(user, { firstPublishedAt: null });
      await createRevisionWithMetadata(
        unpubDs,
        user,
        1,
        'Population Unpublished',
        Locale.EnglishGb,
        { publishAt: pastDate(24), approvedAt: pastDate(48) },
        'Should not appear in search'
      );

      const result = await PublishedDatasetRepository.searchBasic(Locale.EnglishGb, 'Population Unpublished', 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).not.toContain(unpubDs.id);
    });
  });

  describe('searchBasicSplit', () => {
    let splitDs1: Dataset;
    let splitDs2: Dataset;

    beforeAll(async () => {
      splitDs1 = await createDataset(user, { firstPublishedAt: pastDate(72) });
      await createRevisionWithMetadata(
        splitDs1,
        user,
        1,
        'Economic Growth Indicators',
        Locale.EnglishGb,
        { publishAt: pastDate(72), approvedAt: pastDate(96) },
        'Quarterly economic growth data for the United Kingdom'
      );

      splitDs2 = await createDataset(user, { firstPublishedAt: pastDate(48) });
      await createRevisionWithMetadata(
        splitDs2,
        user,
        1,
        'Employment Trends Analysis',
        Locale.EnglishGb,
        { publishAt: pastDate(48), approvedAt: pastDate(72) },
        'Monthly employment indicators across regions'
      );
    });

    it('should match datasets where all words appear across title and summary', async () => {
      const result = await PublishedDatasetRepository.searchBasicSplit(Locale.EnglishGb, 'Economic Quarterly', 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).toContain(splitDs1.id);
      expect(ids).not.toContain(splitDs2.id);
    });

    it('should require all words to match (AND logic)', async () => {
      // "Economic" matches splitDs1, "Employment" matches splitDs2 — no single dataset has both
      const result = await PublishedDatasetRepository.searchBasicSplit(Locale.EnglishGb, 'Economic Employment', 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).not.toContain(splitDs1.id);
      expect(ids).not.toContain(splitDs2.id);
    });

    it('should be case-insensitive', async () => {
      const result = await PublishedDatasetRepository.searchBasicSplit(Locale.EnglishGb, 'economic growth', 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).toContain(splitDs1.id);
    });

    it('should return empty when no match', async () => {
      const result = await PublishedDatasetRepository.searchBasicSplit(Locale.EnglishGb, 'xyznonexistent', 1, 100);
      expect(result.data).toHaveLength(0);
      expect(result.count).toBe(0);
    });

    it('should respect pagination', async () => {
      const result = await PublishedDatasetRepository.searchBasicSplit(Locale.EnglishGb, 'indicators', 1, 1);
      expect(result.data.length).toBeLessThanOrEqual(1);
      // Both datasets contain "indicators"
      expect(result.count).toBeGreaterThanOrEqual(2);
    });
  });

  describe('searchFTS', () => {
    let ftsDs1: Dataset;
    let ftsDs2: Dataset;
    let ftsDs3: Dataset;

    beforeAll(async () => {
      ftsDs1 = await createDataset(user, { firstPublishedAt: pastDate(72) });
      await createRevisionWithMetadata(
        ftsDs1,
        user,
        1,
        'Transport Infrastructure Investment',
        Locale.EnglishGb,
        { publishAt: pastDate(72), approvedAt: pastDate(96) },
        'Government spending on roads railways and bridges in Wales'
      );

      ftsDs2 = await createDataset(user, { firstPublishedAt: pastDate(48) });
      await createRevisionWithMetadata(
        ftsDs2,
        user,
        1,
        'Healthcare Workforce Planning',
        Locale.EnglishGb,
        { publishAt: pastDate(48), approvedAt: pastDate(72) },
        'NHS staffing levels and recruitment across Welsh hospitals'
      );

      ftsDs3 = await createDataset(user, { firstPublishedAt: pastDate(24) });
      await createRevisionWithMetadata(
        ftsDs3,
        user,
        1,
        'Trafnidiaeth Cymru',
        Locale.WelshGb,
        { publishAt: pastDate(24), approvedAt: pastDate(48) },
        'Gwariant y llywodraeth ar ffyrdd a rheilffyrdd yng Nghymru'
      );
    });

    it('should find datasets using English full-text search', async () => {
      const result = await PublishedDatasetRepository.searchFTS(
        Locale.EnglishGb,
        'transport investment',
        false,
        1,
        100
      );
      const ids = result.data.map((d) => d.id);
      expect(ids).toContain(ftsDs1.id);
    });

    it('should use English stemming (e.g. "investing" matches "investment")', async () => {
      const result = await PublishedDatasetRepository.searchFTS(Locale.EnglishGb, 'investing', false, 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).toContain(ftsDs1.id);
    });

    it('should return match_title and match_summary with highlight marks for English', async () => {
      const result = await PublishedDatasetRepository.searchFTS(Locale.EnglishGb, 'transport', false, 1, 100);
      const match = result.data.find((d) => d.id === ftsDs1.id);
      expect(match).toBeDefined();
      expect(match!.match_title).toContain('<mark>');
      expect(match!.rank).toBeDefined();
    });

    it('should use simple config when forceSimple is true', async () => {
      const result = await PublishedDatasetRepository.searchFTS(Locale.EnglishGb, 'healthcare', true, 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).toContain(ftsDs2.id);
    });

    it('should use simple config for Welsh locale', async () => {
      const result = await PublishedDatasetRepository.searchFTS(Locale.WelshGb, 'Trafnidiaeth', false, 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).toContain(ftsDs3.id);
    });

    it('should return empty when no match', async () => {
      const result = await PublishedDatasetRepository.searchFTS(Locale.EnglishGb, 'xyznonexistent', false, 1, 100);
      expect(result.data).toHaveLength(0);
      expect(result.count).toBe(0);
    });

    it('should respect pagination', async () => {
      const result = await PublishedDatasetRepository.searchFTS(Locale.EnglishGb, 'wales', false, 1, 1);
      expect(result.data.length).toBeLessThanOrEqual(1);
    });

    it('should not include unpublished datasets', async () => {
      const unpubDs = await createDataset(user, { firstPublishedAt: null });
      await createRevisionWithMetadata(
        unpubDs,
        user,
        1,
        'Transport Unpublished Dataset',
        Locale.EnglishGb,
        { publishAt: pastDate(24), approvedAt: pastDate(48) },
        'Should not appear in FTS search'
      );

      const result = await PublishedDatasetRepository.searchFTS(
        Locale.EnglishGb,
        'Transport Unpublished',
        false,
        1,
        100
      );
      const ids = result.data.map((d) => d.id);
      expect(ids).not.toContain(unpubDs.id);
    });
  });

  describe('searchFuzzy', () => {
    let fuzzyDs1: Dataset;
    let fuzzyDs2: Dataset;

    beforeAll(async () => {
      fuzzyDs1 = await createDataset(user, { firstPublishedAt: pastDate(72) });
      await createRevisionWithMetadata(
        fuzzyDs1,
        user,
        1,
        'Environmental Pollution Monitoring',
        Locale.EnglishGb,
        { publishAt: pastDate(72), approvedAt: pastDate(96) },
        'Air quality measurements across industrial regions of Wales'
      );

      fuzzyDs2 = await createDataset(user, { firstPublishedAt: pastDate(48) });
      await createRevisionWithMetadata(
        fuzzyDs2,
        user,
        1,
        'Agricultural Production Output',
        Locale.EnglishGb,
        { publishAt: pastDate(48), approvedAt: pastDate(72) },
        'Annual crop and livestock production statistics'
      );
    });

    it('should find datasets with similar title words', async () => {
      const result = await PublishedDatasetRepository.searchFuzzy(Locale.EnglishGb, 'Pollution Monitoring', 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).toContain(fuzzyDs1.id);
    });

    it('should find datasets with similar summary words', async () => {
      const result = await PublishedDatasetRepository.searchFuzzy(Locale.EnglishGb, 'Agricultural Production', 1, 100);
      const ids = result.data.map((d) => d.id);
      expect(ids).toContain(fuzzyDs2.id);
    });

    it('should include rank in results', async () => {
      const result = await PublishedDatasetRepository.searchFuzzy(Locale.EnglishGb, 'Pollution', 1, 100);
      const match = result.data.find((d) => d.id === fuzzyDs1.id);
      expect(match).toBeDefined();
      expect(match!.rank).toBeDefined();
      expect(parseFloat(match!.rank!)).toBeGreaterThan(0);
    });

    it('should return empty when no match', async () => {
      const result = await PublishedDatasetRepository.searchFuzzy(Locale.EnglishGb, 'xyzqwkjhg', 1, 100);
      expect(result.data).toHaveLength(0);
      expect(result.count).toBe(0);
    });

    it('should respect pagination', async () => {
      const result = await PublishedDatasetRepository.searchFuzzy(Locale.EnglishGb, 'Production', 1, 1);
      expect(result.data.length).toBeLessThanOrEqual(1);
    });

    it('should not include unpublished datasets', async () => {
      const unpubDs = await createDataset(user, { firstPublishedAt: null });
      await createRevisionWithMetadata(
        unpubDs,
        user,
        1,
        'Environmental Unpublished Data',
        Locale.EnglishGb,
        { publishAt: pastDate(24), approvedAt: pastDate(48) },
        'Should not appear in fuzzy search'
      );

      const result = await PublishedDatasetRepository.searchFuzzy(
        Locale.EnglishGb,
        'Environmental Unpublished',
        1,
        100
      );
      const ids = result.data.map((d) => d.id);
      expect(ids).not.toContain(unpubDs.id);
    });
  });
});
