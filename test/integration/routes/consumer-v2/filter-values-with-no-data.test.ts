import request from 'supertest';

import app from '../../../../src/app';
import { dbManager } from '../../../../src/db/database-manager';
import { initPassport } from '../../../../src/middleware/passport-auth';
import { User } from '../../../../src/entities/user/user';
import { UserGroup } from '../../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../../src/entities/user/user-group-role';
import { GroupRole } from '../../../../src/enums/group-role';
import { FactTableColumnType } from '../../../../src/enums/fact-table-column-type';
import { DimensionType } from '../../../../src/enums/dimension-type';
import { getFilters } from '../../../../src/services/consumer-view';
import { DatasetRepository } from '../../../../src/repositories/dataset';
import { Dimension } from '../../../../src/entities/dataset/dimension';
import { ensureWorkerDataSources, resetDatabase } from '../../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../../helpers/get-test-user';
import { seedPublishedDataset, LookupRow } from '../../../helpers/seed-published-dataset';
import BlobStorage from '../../../../src/services/blob-storage';

jest.mock('../../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

// SW-1410: lookup tables can hold values the data never uses. The filters should leave them out.

const user: User = getTestUser('filter no-data test user');
let userGroup = getTestUserGroup('Filter No Data Test Group');

const DATASET_ID = 'dddddddd-dddd-4ddd-8dd2-dddddddddddd';
const REVISION_ID = 'dddddddd-dddd-4ddd-8dd2-aaaaaaaaaaaa';
const DATA_TABLE_ID = 'dddddddd-dddd-4ddd-8dd2-bbbbbbbbbbbb';

const VEHICLE_LOOKUP_ID = 'dabcdef0-0000-4000-8000-000000000001';
const AREA_LOOKUP_ID = 'dabcdef0-0000-4000-8000-000000000002';
const PERIOD_LOOKUP_ID = 'dabcdef0-0000-4000-8000-000000000003';

const bilingual = (row: Omit<LookupRow, 'language'>): LookupRow[] => [
  { ...row, language: 'en' },
  { ...row, language: 'cy' }
];

// A flat lookup: PED is in the lookup but not in the data.
const vehicleLookupRows: LookupRow[] = [
  ...bilingual({ reference: 'CAR', description: 'Cars', sortOrder: 1 }),
  ...bilingual({ reference: 'BUS', description: 'Buses', sortOrder: 2 }),
  ...bilingual({ reference: 'PED', description: 'Pedestrians', sortOrder: 3 })
];

// A hierarchy:
// - ALL has no rows of its own, but some of its children do, so it stays as a group
// - A03 is under ALL but has no data
// - OTHER and its child O01 have no data at all
const areaLookupRows: LookupRow[] = [
  ...bilingual({ reference: 'ALL', description: 'All areas', sortOrder: 1, hierarchy: null }),
  ...bilingual({ reference: 'A01', description: 'North', sortOrder: 1, hierarchy: 'ALL' }),
  ...bilingual({ reference: 'A02', description: 'South', sortOrder: 2, hierarchy: 'ALL' }),
  ...bilingual({ reference: 'A03', description: 'East', sortOrder: 3, hierarchy: 'ALL' }),
  ...bilingual({ reference: 'OTHER', description: 'Other areas', sortOrder: 2, hierarchy: null }),
  ...bilingual({ reference: 'O01', description: 'Offshore', sortOrder: 1, hierarchy: 'OTHER' })
];

// A date dimension: the lookup runs to a year the data does not reach yet.
const periodLookupRows: LookupRow[] = [
  ...bilingual({ reference: '2025', description: '2025', sortOrder: 1, hierarchy: null }),
  ...bilingual({ reference: '2025_1', description: 'Q1 2025', sortOrder: 1, hierarchy: '2025' }),
  ...bilingual({ reference: '2025_2', description: 'Q2 2025', sortOrder: 2, hierarchy: '2025' }),
  ...bilingual({ reference: '2026', description: '2026', sortOrder: 2, hierarchy: null }),
  ...bilingual({ reference: '2026_1', description: 'Q1 2026', sortOrder: 1, hierarchy: '2026' })
];

const VEHICLES_IN_DATA = ['CAR', 'BUS'];
const AREAS_IN_DATA = ['A01', 'A02'];
const PERIODS_IN_DATA = ['2025_1', '2025_2'];
const ROW_COUNT = VEHICLES_IN_DATA.length * AREAS_IN_DATA.length;

const rowBuilder = (i: number): unknown[] => [
  VEHICLES_IN_DATA[Math.floor(i / AREAS_IN_DATA.length)],
  AREAS_IN_DATA[i % AREAS_IN_DATA.length],
  PERIODS_IN_DATA[i % PERIODS_IN_DATA.length],
  'count',
  i + 1,
  null
];

interface FilterValue {
  reference: string;
  children?: FilterValue[];
}
interface FilterTable {
  factTableColumn: string;
  values: FilterValue[];
}

// Each value as its reference, or as { reference: children } for a value with children.
const tree = (values: FilterValue[]): unknown[] =>
  values.map((v) => (v.children ? { [v.reference]: tree(v.children) } : v.reference));

const filterFor = (filters: FilterTable[], factTableColumn: string): FilterTable => {
  const filter = filters.find((f) => f.factTableColumn === factTableColumn);
  if (!filter) throw new Error(`No filter returned for column ${factTableColumn}`);
  return filter;
};

const valuesFor = (filters: FilterTable[], factTableColumn: string): unknown[] =>
  tree(filterFor(filters, factTableColumn).values);

describe('Filters leave out values with no data (SW-1410)', () => {
  let dimensions: Dimension[] = [];

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
      title: { en: 'Filter no data fixture', cy: 'Ffynhonnell hidlo heb ddata' },
      factColumns: [
        { name: 'VehicleCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
        { name: 'AreaCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
        { name: 'PeriodCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
        { name: 'MeasureCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Measure },
        { name: 'Data', datatype: 'DOUBLE PRECISION', columnType: FactTableColumnType.DataValues },
        { name: 'NoteCode', datatype: 'VARCHAR', columnType: FactTableColumnType.NoteCodes }
      ],
      rowBuilder,
      rowCount: ROW_COUNT,
      dimensions: [
        {
          factTableColumn: 'VehicleCode',
          lookupTableId: VEHICLE_LOOKUP_ID,
          name: { en: 'Vehicle', cy: 'Cerbyd' },
          lookupRows: vehicleLookupRows
        },
        {
          factTableColumn: 'AreaCode',
          lookupTableId: AREA_LOOKUP_ID,
          name: { en: 'Area', cy: 'Ardal' },
          lookupRows: areaLookupRows
        },
        {
          factTableColumn: 'PeriodCode',
          type: DimensionType.DatePeriod,
          lookupTableId: PERIOD_LOOKUP_ID,
          name: { en: 'Period', cy: 'Cyfnod' },
          lookupRows: periodLookupRows
        }
      ]
    });

    const dataset = await DatasetRepository.getById(DATASET_ID, { dimensions: true });
    dimensions = dataset.dimensions ?? [];
  }, 120_000);

  describe('consumer v2 — GET /v2/:dataset_id/filters', () => {
    it('leaves out a lookup value that is not in the data', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/filters`);
      expect(res.status).toBe(200);
      expect(valuesFor(res.body, 'VehicleCode')).toEqual(['CAR', 'BUS']);
    });

    it('keeps a parent with no rows of its own when its children have data, and leaves out empty values', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/filters`);
      expect(res.status).toBe(200);
      expect(valuesFor(res.body, 'AreaCode')).toEqual([{ ALL: ['A01', 'A02'] }]);
    });

    it('leaves out a year in the date filter that has no data yet', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/filters`);
      expect(res.status).toBe(200);
      const period = filterFor(res.body, 'PeriodCode');
      expect(period.values.map((v) => v.reference)).toEqual(['2025']);
      expect((period.values[0].children ?? []).map((v) => v.reference)).toEqual(['2025_2', '2025_1']);
    });
  });

  describe('consumer v1 — GET /v1/:dataset_id/view/filters', () => {
    it('leaves out values with no data', async () => {
      const res = await request(app).get(`/v1/${DATASET_ID}/view/filters`).set('Accept-Language', 'en-GB');
      expect(res.status).toBe(200);
      expect(valuesFor(res.body, 'VehicleCode')).toEqual(['CAR', 'BUS']);
      expect(valuesFor(res.body, 'AreaCode')).toEqual([{ ALL: ['A01', 'A02'] }]);
    });
  });

  // getFilters() is behind the publisher preview filters as well as the v1 consumer filters.
  describe('preview — getFilters()', () => {
    it('leaves out values with no data', async () => {
      const filters = (await getFilters(REVISION_ID, 'en-gb', dimensions)) as unknown as FilterTable[];
      expect(valuesFor(filters, 'VehicleCode')).toEqual(['CAR', 'BUS']);
      expect(valuesFor(filters, 'AreaCode')).toEqual([{ ALL: ['A01', 'A02'] }]);
    });
  });
});
