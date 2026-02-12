import { EntityNotFoundError } from 'typeorm';

import { dbManager } from '../../src/db/database-manager';
import { Dataset } from '../../src/entities/dataset/dataset';
import { Revision } from '../../src/entities/dataset/revision';
import { DataTable } from '../../src/entities/dataset/data-table';
import { FactTableColumn } from '../../src/entities/dataset/fact-table-column';
import { RevisionMetadata } from '../../src/entities/dataset/revision-metadata';
import { UserGroup } from '../../src/entities/user/user-group';
import { UserGroupMetadata } from '../../src/entities/user/user-group-metadata';
import { DatasetRepository } from '../../src/repositories/dataset';
import { getTestUser } from '../helpers/get-test-user';
import { User } from '../../src/entities/user/user';
import { Locale } from '../../src/enums/locale';
import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
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

const user: User = getTestUser('dataset-repo-test');

let userGroup: UserGroup;

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
  overrides: Partial<Revision> = {}
): Promise<Revision> {
  const rev = await createRevision(dataset, createdBy, revisionIndex, overrides);

  const metaEn = new RevisionMetadata();
  metaEn.id = rev.id;
  metaEn.language = Locale.EnglishGb;
  metaEn.title = title;
  await metaEn.save();

  const metaCy = new RevisionMetadata();
  metaCy.id = rev.id;
  metaCy.language = Locale.WelshGb;
  metaCy.title = `${title} CY`;
  await metaCy.save();

  return rev;
}

describe('DatasetRepository', () => {
  beforeAll(async () => {
    try {
      await dbManager.initDataSources();
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.getAppDataSource().runMigrations();
      await user.save();

      // Create a user group with metadata (required for listAll / listForUser INNER JOINs)
      userGroup = new UserGroup();
      userGroup.id = uuidV4();
      await userGroup.save();

      const ugMetaEn = new UserGroupMetadata();
      ugMetaEn.id = userGroup.id;
      ugMetaEn.language = Locale.EnglishGb;
      ugMetaEn.name = 'Test Group EN';
      await ugMetaEn.save();

      const ugMetaCy = new UserGroupMetadata();
      ugMetaCy.id = userGroup.id;
      ugMetaCy.language = Locale.WelshGb;
      ugMetaCy.name = 'Test Group CY';
      await ugMetaCy.save();
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

    beforeAll(async () => {
      dataset = await createDataset(user);
      // Create a revision with a data table so we can test nested relations
      const rev = await createRevision(dataset, user, 1);

      // Link it as the draft revision
      dataset.draftRevision = rev;
      dataset.draftRevisionId = rev.id;
      dataset.startRevisionId = rev.id;
      dataset.endRevisionId = rev.id;
      await dataset.save();
    });

    it('should return dataset with no relations', async () => {
      const result = await DatasetRepository.getById(dataset.id);
      expect(result.id).toBe(dataset.id);
    });

    it('should return dataset with factTable relations ordered by columnIndex', async () => {
      // Create fact table columns out of order
      const col2 = FactTableColumn.create({
        id: dataset.id,
        columnName: 'col_b',
        columnIndex: 2,
        columnDatatype: 'varchar',
        columnType: FactTableColumnType.Dimension
      });
      const col1 = FactTableColumn.create({
        id: dataset.id,
        columnName: 'col_a',
        columnIndex: 1,
        columnDatatype: 'varchar',
        columnType: FactTableColumnType.DataValues
      });
      await FactTableColumn.save([col2, col1]);

      const result = await DatasetRepository.getById(dataset.id, { factTable: true });
      expect(result.factTable).toHaveLength(2);
      expect(result.factTable![0].columnIndex).toBe(1);
      expect(result.factTable![1].columnIndex).toBe(2);

      // cleanup
      await FactTableColumn.remove([col1, col2]);
    });

    it('should return dataset with nested revision relations', async () => {
      const result = await DatasetRepository.getById(dataset.id, {
        draftRevision: true
      });
      expect(result.draftRevision).toBeDefined();
      expect(result.draftRevision!.id).toBeDefined();
    });

    it('should throw EntityNotFoundError for non-existent id', async () => {
      await expect(DatasetRepository.getById(uuidV4())).rejects.toThrow(EntityNotFoundError);
    });
  });

  describe('deleteById', () => {
    it('should delete existing dataset', async () => {
      const ds = await createDataset(user);
      await DatasetRepository.deleteById(ds.id);

      await expect(DatasetRepository.getById(ds.id)).rejects.toThrow(EntityNotFoundError);
    });

    it('should not throw for non-existent dataset', async () => {
      await expect(DatasetRepository.deleteById(uuidV4())).resolves.not.toThrow();
    });
  });

  describe('replaceFactTable', () => {
    let dataset: Dataset;

    beforeAll(async () => {
      dataset = await createDataset(user);
    });

    it('should create fact table columns from data table descriptions', async () => {
      const dataTable = {
        dataTableDescriptions: [
          { columnName: 'area', columnIndex: 0, columnDatatype: 'varchar' },
          { columnName: 'value', columnIndex: 1, columnDatatype: 'numeric' }
        ]
      } as DataTable;

      await DatasetRepository.replaceFactTable(dataset, dataTable);

      const result = await DatasetRepository.getById(dataset.id, { factTable: true });
      expect(result.factTable).toHaveLength(2);
      expect(result.factTable!.map((c) => c.columnName).sort()).toEqual(['area', 'value']);
    });

    it('should replace existing fact table (old columns removed)', async () => {
      // Reload dataset with factTable so replaceFactTable can see existing columns
      const loadedDataset = await DatasetRepository.getById(dataset.id, { factTable: true });

      const newDataTable = {
        dataTableDescriptions: [{ columnName: 'new_col', columnIndex: 0, columnDatatype: 'text' }]
      } as DataTable;

      await DatasetRepository.replaceFactTable(loadedDataset, newDataTable);

      const result = await DatasetRepository.getById(dataset.id, { factTable: true });
      expect(result.factTable).toHaveLength(1);
      expect(result.factTable![0].columnName).toBe('new_col');
    });

    it('should set columnType to Unknown for all new columns', async () => {
      const result = await DatasetRepository.getById(dataset.id, { factTable: true });
      for (const col of result.factTable!) {
        expect(col.columnType).toBe(FactTableColumnType.Unknown);
      }
    });
  });

  describe('publish', () => {
    it('should set publishedRevision and clear draftRevision', async () => {
      const ds = await createDataset(user);
      const startRev = await createRevision(ds, user, 1, {
        publishAt: pastDate(48),
        approvedAt: pastDate(72)
      });
      ds.startRevisionId = startRev.id;
      ds.draftRevisionId = startRev.id;
      ds.draftRevision = startRev;
      await ds.save();

      const result = await DatasetRepository.publish(startRev);

      expect(result.publishedRevisionId).toBe(startRev.id);
      expect(result.draftRevision).toBeNull();
    });

    it('should set firstPublishedAt from startRevision.publishAt', async () => {
      const ds = await createDataset(user);
      const publishDate = pastDate(24);
      const startRev = await createRevision(ds, user, 1, {
        publishAt: publishDate,
        approvedAt: pastDate(48)
      });
      ds.startRevisionId = startRev.id;
      ds.draftRevisionId = startRev.id;
      ds.draftRevision = startRev;
      await ds.save();

      const result = await DatasetRepository.publish(startRev);

      expect(result.firstPublishedAt).toBeDefined();
      expect(result.firstPublishedAt!.getTime()).toBe(publishDate.getTime());
    });

    it('should throw when no startRevision exists', async () => {
      const ds = await createDataset(user);
      const rev = await createRevision(ds, user, 1, {
        publishAt: pastDate(24),
        approvedAt: pastDate(48)
      });
      // Don't set startRevisionId

      await expect(DatasetRepository.publish(rev)).rejects.toThrow();
    });
  });

  describe('archive / unarchive', () => {
    let dataset: Dataset;

    beforeAll(async () => {
      dataset = await createDataset(user);
    });

    it('should set archivedAt timestamp', async () => {
      const result = await DatasetRepository.archive(dataset.id);
      expect(result.archivedAt).toBeDefined();
      expect(result.archivedAt).toBeInstanceOf(Date);
    });

    it('should clear archivedAt on unarchive', async () => {
      const result = await DatasetRepository.unarchive(dataset.id);
      expect(result.archivedAt).toBeNull();
    });

    it('should re-archive a previously unarchived dataset', async () => {
      const result = await DatasetRepository.archive(dataset.id);
      expect(result.archivedAt).toBeDefined();
      expect(result.archivedAt).toBeInstanceOf(Date);
    });
  });

  describe('listAll', () => {
    let ds1: Dataset;
    let ds2: Dataset;
    let archivedDs: Dataset;

    beforeAll(async () => {
      // Dataset 1: new (no firstPublishedAt)
      ds1 = await createDataset(user, { userGroupId: userGroup.id });
      await createRevisionWithMetadata(ds1, user, 1, 'Alpha Dataset');

      // Dataset 2: live (has firstPublishedAt in past)
      ds2 = await createDataset(user, {
        userGroupId: userGroup.id,
        firstPublishedAt: pastDate(48)
      });
      const rev2 = await createRevisionWithMetadata(ds2, user, 1, 'Beta Dataset', {
        publishAt: pastDate(24),
        approvedAt: pastDate(48)
      });
      ds2.publishedRevisionId = rev2.id;
      await ds2.save();

      // Archived dataset
      archivedDs = await createDataset(user, {
        userGroupId: userGroup.id,
        archivedAt: pastDate(1)
      });
      await createRevisionWithMetadata(archivedDs, user, 1, 'Gamma Archived');
    });

    it('should return datasets with title and group name', async () => {
      const result = await DatasetRepository.listAll(Locale.EnglishGb, 1, 100);

      expect(result.data.length).toBeGreaterThanOrEqual(2);
      const titles = result.data.map((d: any) => d.title);
      expect(titles).toContain('Alpha Dataset');
      expect(titles).toContain('Beta Dataset');

      // group_name should be set
      const alphaItem = result.data.find((d: any) => d.title === 'Alpha Dataset');
      expect(alphaItem!.group_name).toBe('Test Group EN');
    });

    it('should apply search filter on title (ILIKE)', async () => {
      const result = await DatasetRepository.listAll(Locale.EnglishGb, 1, 100, 'Alpha');

      const titles = result.data.map((d: any) => d.title);
      expect(titles).toContain('Alpha Dataset');
      expect(titles).not.toContain('Beta Dataset');
    });

    it('should apply search filter on dataset ID (prefix match)', async () => {
      const prefix = ds1.id.substring(0, 8);
      const result = await DatasetRepository.listAll(Locale.EnglishGb, 1, 100, prefix);

      const ids = result.data.map((d: any) => d.id);
      expect(ids).toContain(ds1.id);
    });

    it('should respect pagination (offset/limit)', async () => {
      const page1 = await DatasetRepository.listAll(Locale.EnglishGb, 1, 1);
      const page2 = await DatasetRepository.listAll(Locale.EnglishGb, 2, 1);

      expect(page1.data).toHaveLength(1);
      expect(page2.data).toHaveLength(1);
      expect(page1.data[0].id).not.toBe(page2.data[0].id);
    });

    it('should return correct status badges (new, live, archived)', async () => {
      const result = await DatasetRepository.listAll(Locale.EnglishGb, 1, 100);

      const alphaItem = result.data.find((d: any) => d.title === 'Alpha Dataset');
      expect(alphaItem!.status).toBe('new');

      const betaItem = result.data.find((d: any) => d.title === 'Beta Dataset');
      expect(betaItem!.status).toBe('live');

      const gammaItem = result.data.find((d: any) => d.title === 'Gamma Archived');
      expect(gammaItem!.status).toBe('archived');
    });
  });

  describe('listForUser', () => {
    let userWithGroups: User;
    let userWithoutGroups: User;

    beforeAll(async () => {
      // Create users
      userWithGroups = getTestUser('list-user-with-groups');
      userWithGroups.groupRoles = [{ groupId: userGroup.id, roles: ['editor'] } as any];
      await userWithGroups.save();

      userWithoutGroups = getTestUser('list-user-no-groups');
      userWithoutGroups.groupRoles = [];
      await userWithoutGroups.save();

      // Create a dataset in the group for this test
      const ds = await createDataset(userWithGroups, { userGroupId: userGroup.id });
      await createRevisionWithMetadata(ds, userWithGroups, 1, 'User Group Dataset');
    });

    it('should return datasets for user groups only', async () => {
      const result = await DatasetRepository.listForUser(userWithGroups, Locale.EnglishGb, 1, 100);

      expect(result.data.length).toBeGreaterThanOrEqual(1);
      const titles = result.data.map((d: any) => d.title);
      expect(titles).toContain('User Group Dataset');
    });

    it('should return empty result when user has no groups', async () => {
      const result = await DatasetRepository.listForUser(userWithoutGroups, Locale.EnglishGb, 1, 100);

      expect(result.data).toEqual([]);
      expect(result.count).toBe(0);
    });

    it('should search within user filtered groups', async () => {
      const result = await DatasetRepository.listForUser(userWithGroups, Locale.EnglishGb, 1, 100, 'User Group');

      const titles = result.data.map((d: any) => d.title);
      expect(titles).toContain('User Group Dataset');
    });
  });
});
