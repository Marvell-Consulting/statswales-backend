import request from 'supertest';

import app from '../../../../src/app';
import { dbManager } from '../../../../src/db/database-manager';
import { initPassport } from '../../../../src/middleware/passport-auth';
import { User } from '../../../../src/entities/user/user';
import { UserGroup } from '../../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../../src/entities/user/user-group-role';
import { Dimension } from '../../../../src/entities/dataset/dimension';
import { DimensionMetadata } from '../../../../src/entities/dataset/dimension-metadata';
import { GroupRole } from '../../../../src/enums/group-role';
import { FactTableColumnType } from '../../../../src/enums/fact-table-column-type';
import { ensureWorkerDataSources, resetDatabase } from '../../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../../helpers/get-test-user';
import { seedPublishedDataset } from '../../../helpers/seed-published-dataset';
import BlobStorage from '../../../../src/services/blob-storage';
import { PublishedDatasetRepository } from '../../../../src/repositories/published-dataset';

jest.mock('../../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

const user: User = getTestUser('v1 dimensions test user');
let userGroup = getTestUserGroup('V1 Dimensions Test Group');

const DATASET_ID = 'dddddddd-dddd-4ddd-8dd1-dddddddddddd';
const REVISION_ID = 'dddddddd-dddd-4ddd-8dd1-aaaaaaaaaaaa';
const DATA_TABLE_ID = 'dddddddd-dddd-4ddd-8dd1-bbbbbbbbbbbb';
const AREA_LOOKUP_ID = 'dddddddd-0000-4000-8000-000000000001';
const YEAR_LOOKUP_ID = 'dddddddd-0000-4000-8000-000000000002';

const AREAS = ['W06000001', 'W06000002'];
const YEARS = [2022, 2023];
const ROW_COUNT = AREAS.length * YEARS.length;

const areaLookupRows = AREAS.flatMap((ref, idx) => [
  { reference: ref, language: 'en' as const, description: `Area ${ref}`, sortOrder: idx },
  { reference: ref, language: 'cy' as const, description: `Ardal ${ref}`, sortOrder: idx }
]);

const yearLookupRows = YEARS.flatMap((year, idx) => [
  { reference: year, language: 'en' as const, description: String(year), sortOrder: idx },
  { reference: year, language: 'cy' as const, description: String(year), sortOrder: idx }
]);

const rowBuilder = (i: number): unknown[] => {
  const area = AREAS[Math.floor(i / YEARS.length) % AREAS.length];
  const year = YEARS[i % YEARS.length];
  return [area, year, 'count', (i + 1) * 10, null];
};

type DimensionBody = { factTableColumn: string; metadata?: { language: string; name: string }[] };

const getAreaDimension = (body: { dimensions: DimensionBody[] }): DimensionBody | undefined =>
  body.dimensions.find((dim) => dim.factTableColumn === 'AreaCode');

describe('Consumer V1 — dataset dimensions (/:dataset_id)', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport();

    userGroup = await dbManager.getPublisherDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();

    await seedPublishedDataset({
      user,
      datasetId: DATASET_ID,
      revisionId: REVISION_ID,
      dataTableId: DATA_TABLE_ID,
      title: { en: 'Dimensions fixture', cy: 'Ffynhonnell dimensiynau' },
      factColumns: [
        { name: 'AreaCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
        { name: 'YearCode', datatype: 'BIGINT', columnType: FactTableColumnType.Dimension },
        { name: 'MeasureCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Measure },
        { name: 'Data', datatype: 'DOUBLE PRECISION', columnType: FactTableColumnType.DataValues },
        { name: 'NoteCode', datatype: 'VARCHAR', columnType: FactTableColumnType.NoteCodes }
      ],
      rowBuilder,
      rowCount: ROW_COUNT,
      dimensions: [
        {
          factTableColumn: 'AreaCode',
          lookupTableId: AREA_LOOKUP_ID,
          name: { en: 'Area', cy: 'Ardal' },
          lookupRows: areaLookupRows
        },
        {
          factTableColumn: 'YearCode',
          lookupTableId: YEAR_LOOKUP_ID,
          referenceDatatype: 'BIGINT',
          name: { en: 'Year', cy: 'Blwyddyn' },
          lookupRows: yearLookupRows
        }
      ]
    });

    // Simulate a publisher renaming the dimension in a draft update after the revision was published.
    // Dimension records are shared between revisions, so the rename lands on the live record.
    const area = await dbManager
      .getPublisherDataSource()
      .getRepository(Dimension)
      .findOneByOrFail({ datasetId: DATASET_ID, factTableColumn: 'AreaCode' });

    await dbManager
      .getPublisherDataSource()
      .getRepository(DimensionMetadata)
      .update({ id: area.id, language: 'en-GB' }, { name: 'Unpublished area name', notes: 'Unpublished notes' });
  }, 120_000);

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('returns dimension names from the published cube, not from draft changes', async () => {
    const res = await request(app).get(`/v1/${DATASET_ID}`);
    expect(res.status).toBe(200);

    const area = getAreaDimension(res.body);
    expect(area?.metadata).toEqual(
      expect.arrayContaining([
        { language: 'en-GB', name: 'Area' },
        { language: 'cy-GB', name: 'Ardal' }
      ])
    );
    expect(JSON.stringify(res.body)).not.toContain('Unpublished');
  });

  it('does not expose cube builder internals on dimensions', async () => {
    const res = await request(app).get(`/v1/${DATASET_ID}`);
    expect(res.status).toBe(200);
    expect(res.body.dimensions.length).toBe(2);

    for (const dimension of res.body.dimensions) {
      expect(Object.keys(dimension).sort()).toEqual(['factTableColumn', 'id', 'isSliceDimension', 'metadata', 'type']);
      for (const meta of dimension.metadata) {
        expect(Object.keys(meta).sort()).toEqual(['language', 'name']);
      }
    }
  });

  it('omits names rather than falling back to draft changes when the cube cannot be read', async () => {
    jest
      .spyOn(PublishedDatasetRepository, 'getPublishedDimensionNames')
      .mockRejectedValueOnce(new Error('relation "filter_table" does not exist'));

    const res = await request(app).get(`/v1/${DATASET_ID}`);
    expect(res.status).toBe(200);
    expect(getAreaDimension(res.body)?.metadata).toEqual([]);
    expect(JSON.stringify(res.body)).not.toContain('Unpublished');
  });
});
