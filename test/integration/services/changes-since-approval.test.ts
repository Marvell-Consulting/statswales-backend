import { dbManager } from '../../../src/db/database-manager';
import { User } from '../../../src/entities/user/user';
import { UserGroup } from '../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../src/entities/user/user-group-role';
import { Dimension } from '../../../src/entities/dataset/dimension';
import { DimensionMetadata } from '../../../src/entities/dataset/dimension-metadata';
import { LookupTable } from '../../../src/entities/dataset/lookup-table';
import { GroupRole } from '../../../src/enums/group-role';
import { FactTableColumnType } from '../../../src/enums/fact-table-column-type';
import { RevisionList, RevisionRepository } from '../../../src/repositories/revision';
import { getChangesSinceApproval } from '../../../src/services/changes-since-approval';
import { ensureWorkerDataSources, resetDatabase } from '../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../helpers/get-test-user';
import { seedPublishedDataset } from '../../helpers/seed-published-dataset';
import BlobStorage from '../../../src/services/blob-storage';

jest.mock('../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

const user: User = getTestUser('changes since approval test user');
let userGroup = getTestUserGroup('Changes Since Approval Test Group');

const DATASET_ID = 'cccccccc-cccc-4ccc-8cc1-cccccccccccc';
const REVISION_ID = 'cccccccc-cccc-4ccc-8cc1-aaaaaaaaaaaa';
const DATA_TABLE_ID = 'cccccccc-cccc-4ccc-8cc1-bbbbbbbbbbbb';
const AREA_LOOKUP_ID = 'cccccccc-0000-4000-8000-000000000001';
const YEAR_LOOKUP_ID = 'cccccccc-0000-4000-8000-000000000002';

const AREAS = ['W06000001', 'W06000002'];
const YEARS = [2022, 2023];

const rowBuilder = (i: number): unknown[] => {
  const area = AREAS[Math.floor(i / YEARS.length) % AREAS.length];
  const year = YEARS[i % YEARS.length];
  return [area, year, 'count', (i + 1) * 10, null];
};

describe('getChangesSinceApproval', () => {
  let revision: RevisionList;
  let areaDimensionId: string;

  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();

    userGroup = await dbManager.getPublisherDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();

    await seedPublishedDataset({
      user,
      datasetId: DATASET_ID,
      revisionId: REVISION_ID,
      dataTableId: DATA_TABLE_ID,
      factColumns: [
        { name: 'AreaCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
        { name: 'YearCode', datatype: 'BIGINT', columnType: FactTableColumnType.Dimension },
        { name: 'MeasureCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Measure },
        { name: 'Data', datatype: 'DOUBLE PRECISION', columnType: FactTableColumnType.DataValues },
        { name: 'NoteCode', datatype: 'VARCHAR', columnType: FactTableColumnType.NoteCodes }
      ],
      rowBuilder,
      rowCount: AREAS.length * YEARS.length,
      dimensions: [
        {
          factTableColumn: 'AreaCode',
          lookupTableId: AREA_LOOKUP_ID,
          name: { en: 'Area', cy: 'Ardal' },
          lookupRows: AREAS.flatMap((ref, idx) => [
            { reference: ref, language: 'en' as const, description: `Area ${ref}`, sortOrder: idx },
            { reference: ref, language: 'cy' as const, description: `Ardal ${ref}`, sortOrder: idx }
          ])
        },
        {
          factTableColumn: 'YearCode',
          lookupTableId: YEAR_LOOKUP_ID,
          referenceDatatype: 'BIGINT',
          name: { en: 'Year', cy: 'Blwyddyn' },
          lookupRows: YEARS.flatMap((year, idx) => [
            { reference: year, language: 'en' as const, description: String(year), sortOrder: idx },
            { reference: year, language: 'cy' as const, description: String(year), sortOrder: idx }
          ])
        }
      ]
    });

    const revisions = await RevisionRepository.getAllRevisionIds();
    revision = revisions.find((rev) => rev.id === REVISION_ID)!;

    // the seed helper backdates approval but creates the lookup tables now, so move them before approval
    const beforeApproval = new Date(new Date(revision.approved_at!).getTime() - 60_000);
    await dbManager
      .getPublisherDataSource()
      .getRepository(LookupTable)
      .update([AREA_LOOKUP_ID, YEAR_LOOKUP_ID], { uploadedAt: beforeApproval });

    const area = await dbManager
      .getPublisherDataSource()
      .getRepository(Dimension)
      .findOneByOrFail({ datasetId: DATASET_ID, factTableColumn: 'AreaCode' });
    areaDimensionId = area.id;
  }, 120_000);

  it('includes the approval time in the bulk revision list', () => {
    expect(revision.approved_at).toBeInstanceOf(Date);
  });

  it('finds no changes straight after publication', async () => {
    expect(await getChangesSinceApproval(revision)).toEqual([]);
  });

  it('reports a dimension renamed after approval', async () => {
    const metadataRepo = dbManager.getPublisherDataSource().getRepository(DimensionMetadata);
    await metadataRepo.update({ id: areaDimensionId, language: 'en-GB' }, { name: 'Unpublished area name' });

    try {
      expect(await getChangesSinceApproval(revision)).toEqual(['AreaCode (en-GB) renamed after approval']);
    } finally {
      await metadataRepo.update({ id: areaDimensionId, language: 'en-GB' }, { name: 'Area' });
    }
  });

  it('reports a missing filter table using the real database error', async () => {
    const noCube = { ...revision, id: 'cccccccc-cccc-4ccc-8cc1-dddddddddddd' };

    expect(await getChangesSinceApproval(noCube)).toEqual(['cube has no filter table, so names cannot be checked']);
  });

  it('reports a lookup table uploaded after approval', async () => {
    const lookupRepo = dbManager.getPublisherDataSource().getRepository(LookupTable);
    const later = new Date(new Date(revision.approved_at!).getTime() + 60_000);
    await lookupRepo.update({ id: YEAR_LOOKUP_ID }, { uploadedAt: later });

    expect(await getChangesSinceApproval(revision)).toEqual(['lookup table for YearCode replaced after approval']);
  });
});
