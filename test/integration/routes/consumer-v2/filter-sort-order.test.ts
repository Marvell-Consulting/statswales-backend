import request from 'supertest';

import app from '../../../../src/app';
import { dbManager } from '../../../../src/db/database-manager';
import { initPassport } from '../../../../src/middleware/passport-auth';
import { User } from '../../../../src/entities/user/user';
import { UserGroup } from '../../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../../src/entities/user/user-group-role';
import { GroupRole } from '../../../../src/enums/group-role';
import { DimensionType } from '../../../../src/enums/dimension-type';
import { FactTableColumnType } from '../../../../src/enums/fact-table-column-type';
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

const user: User = getTestUser('filter sort-order test user');
let userGroup = getTestUserGroup('Filter Sort Order Test Group');

const DATASET_ID = 'cccccccc-cccc-4ccc-8cc2-cccccccccccc';
const REVISION_ID = 'cccccccc-cccc-4ccc-8cc2-aaaaaaaaaaaa';
const DATA_TABLE_ID = 'cccccccc-cccc-4ccc-8cc2-bbbbbbbbbbbb';

const REGION_LOOKUP_ID = 'cabcdef0-0000-4000-8000-000000000001';
const PERIOD_LOOKUP_ID = 'cabcdef0-0000-4000-8000-000000000002';
const CATEGORY_LOOKUP_ID = 'cabcdef0-0000-4000-8000-000000000003';
const AREA_LOOKUP_ID = 'cabcdef0-0000-4000-8000-000000000004';

// 12 regions — enough that a lexical (text) sort of the sort_order column diverges from a
// numeric one ("10" sorts before "2"). Descriptions deliberately descend alphabetically as
// sort_order ascends, so sorting by description instead of sort_order is also distinguishable.
const REGION_WORDS = [
  'Zulu',
  'Yankee',
  'Xray',
  'Whiskey',
  'Victor',
  'Uniform',
  'Tango',
  'Sierra',
  'Romeo',
  'Quebec',
  'Papa',
  'Oscar'
];
const REGION_REFS = REGION_WORDS.map((_, idx) => `REG${String(idx + 1).padStart(2, '0')}`);

const regionLookupRows: LookupRow[] = REGION_WORDS.flatMap((word, idx) => {
  const reference = REGION_REFS[idx];
  const sortOrder = idx + 1; // 1..12
  return [
    { reference, language: 'en' as const, description: word, sortOrder },
    { reference, language: 'cy' as const, description: word, sortOrder }
  ];
});

// A date dimension. Filters for dates should come back newest-first (descending).
const PERIOD_REFS = ['2020', '2021', '2022', '2023'];
const periodLookupRows: LookupRow[] = PERIOD_REFS.flatMap((year, idx) => [
  { reference: year, language: 'en' as const, description: year, sortOrder: idx + 1 },
  { reference: year, language: 'cy' as const, description: year, sortOrder: idx + 1 }
]);

// A dimension with NO sort order — these should fall back to ascending alphabetical order
// of the description. References are deliberately not in alphabetical order of description.
const CATEGORY_REFS = ['CAT1', 'CAT2', 'CAT3'];
const categoryDescriptions: Record<string, string> = { CAT1: 'Beta', CAT2: 'Alpha', CAT3: 'Gamma' };
const categoryLookupRows: LookupRow[] = CATEGORY_REFS.flatMap((reference) => [
  { reference, language: 'en' as const, description: categoryDescriptions[reference] },
  { reference, language: 'cy' as const, description: categoryDescriptions[reference] }
]);

// A hierarchical dimension: one root with 11 children. Children must nest under the root and
// be ordered by sort_order numerically — 11 children means a lexical sort would mis-place 10
// and 11. Child descriptions descend alphabetically as sort_order ascends.
const AREA_ROOT_REF = 'A00';
const AREA_CHILD_WORDS = [
  'Kilo',
  'Juliet',
  'India',
  'Hotel',
  'Golf',
  'Foxtrot',
  'Echo',
  'Delta',
  'Charlie',
  'Bravo',
  'Alpha'
];
const AREA_CHILD_REFS = AREA_CHILD_WORDS.map((_, idx) => `A${String(idx + 1).padStart(2, '0')}`);

const areaLookupRows: LookupRow[] = [
  { reference: AREA_ROOT_REF, language: 'en', description: 'All areas', sortOrder: 1, hierarchy: null },
  { reference: AREA_ROOT_REF, language: 'cy', description: 'Pob ardal', sortOrder: 1, hierarchy: null },
  ...AREA_CHILD_WORDS.flatMap((word, idx) => {
    const reference = AREA_CHILD_REFS[idx];
    const sortOrder = idx + 1; // 1..11
    return [
      { reference, language: 'en' as const, description: word, sortOrder, hierarchy: AREA_ROOT_REF },
      { reference, language: 'cy' as const, description: word, sortOrder, hierarchy: AREA_ROOT_REF }
    ];
  })
];

const ROW_COUNT = REGION_REFS.length * PERIOD_REFS.length; // 48 — one row per (region, period)

const rowBuilder = (i: number): unknown[] => {
  const region = REGION_REFS[Math.floor(i / PERIOD_REFS.length)];
  const period = PERIOD_REFS[i % PERIOD_REFS.length];
  const category = CATEGORY_REFS[i % CATEGORY_REFS.length];
  const area = AREA_CHILD_REFS[i % AREA_CHILD_REFS.length];
  return [region, period, category, area, 'count', Math.round((i + 1) * 10.5 * 100) / 100, null];
};

// Expected orderings per the product requirement:
//  - with a sort order in the lookup: order by that sort order (numerically)
//  - no sort order: ascending alphabetical by description
//  - dates: descending (newest first)
const EXPECTED_REGION_ORDER = REGION_REFS; // sort_order 1..12
const EXPECTED_PERIOD_ORDER = [...PERIOD_REFS].reverse(); // dates descending
const EXPECTED_CATEGORY_ORDER = ['CAT2', 'CAT1', 'CAT3']; // Alpha, Beta, Gamma
const EXPECTED_AREA_ROOTS = [AREA_ROOT_REF]; // only the root sits at the top level
const EXPECTED_AREA_CHILDREN = AREA_CHILD_REFS; // children ordered by sort_order 1..11

interface FilterValue {
  reference: string;
  description: string;
  children?: FilterValue[];
}
interface FilterTable {
  factTableColumn: string;
  columnName: string;
  values: FilterValue[];
}

const filterFor = (filters: FilterTable[], factTableColumn: string): FilterTable => {
  const filter = filters.find((f) => f.factTableColumn === factTableColumn);
  if (!filter) throw new Error(`No filter returned for column ${factTableColumn}`);
  return filter;
};

const referencesFor = (filters: FilterTable[], factTableColumn: string): string[] =>
  filterFor(filters, factTableColumn).values.map((v) => v.reference);

const childReferencesFor = (filters: FilterTable[], factTableColumn: string, parentRef: string): string[] => {
  const parent = filterFor(filters, factTableColumn).values.find((v) => v.reference === parentRef);
  if (!parent) throw new Error(`No top-level value ${parentRef} for column ${factTableColumn}`);
  return (parent.children ?? []).map((c) => c.reference);
};

describe('Preview & consumer filters — value sort order', () => {
  // The dataset's dimensions, loaded after seeding. getFilters() needs them to know which
  // columns are dates — exactly as the preview and v1 consumer controllers supply them.
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
      title: { en: 'Filter sort fixture', cy: 'Ffynhonnell trefn hidlo' },
      factColumns: [
        { name: 'RegionCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
        { name: 'PeriodCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
        { name: 'CategoryCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
        { name: 'AreaCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
        { name: 'MeasureCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Measure },
        { name: 'Data', datatype: 'DOUBLE PRECISION', columnType: FactTableColumnType.DataValues },
        { name: 'NoteCode', datatype: 'VARCHAR', columnType: FactTableColumnType.NoteCodes }
      ],
      rowBuilder,
      rowCount: ROW_COUNT,
      dimensions: [
        {
          factTableColumn: 'RegionCode',
          lookupTableId: REGION_LOOKUP_ID,
          name: { en: 'Region', cy: 'Rhanbarth' },
          lookupRows: regionLookupRows
        },
        {
          factTableColumn: 'PeriodCode',
          type: DimensionType.DatePeriod,
          lookupTableId: PERIOD_LOOKUP_ID,
          name: { en: 'Period', cy: 'Cyfnod' },
          lookupRows: periodLookupRows
        },
        {
          factTableColumn: 'CategoryCode',
          lookupTableId: CATEGORY_LOOKUP_ID,
          name: { en: 'Category', cy: 'Categori' },
          lookupRows: categoryLookupRows
        },
        {
          factTableColumn: 'AreaCode',
          lookupTableId: AREA_LOOKUP_ID,
          name: { en: 'Area', cy: 'Ardal' },
          lookupRows: areaLookupRows
        }
      ]
    });

    const dataset = await DatasetRepository.getById(DATASET_ID, { dimensions: true });
    dimensions = dataset.dimensions ?? [];
  }, 120_000);

  describe('consumer v2 — GET /v2/:dataset_id/filters', () => {
    it('orders lookup values by the lookup sort order (numerically, not lexically)', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/filters`);
      expect(res.status).toBe(200);
      expect(referencesFor(res.body, 'RegionCode')).toEqual(EXPECTED_REGION_ORDER);
    });

    it('orders date dimension values descending (newest first)', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/filters`);
      expect(res.status).toBe(200);
      expect(referencesFor(res.body, 'PeriodCode')).toEqual(EXPECTED_PERIOD_ORDER);
    });

    it('orders values with no sort order ascending alphabetically by description', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/filters`);
      expect(res.status).toBe(200);
      expect(referencesFor(res.body, 'CategoryCode')).toEqual(EXPECTED_CATEGORY_ORDER);
    });

    it('nests hierarchical values and orders children by sort order numerically', async () => {
      const res = await request(app).get(`/v2/${DATASET_ID}/filters`);
      expect(res.status).toBe(200);
      expect(referencesFor(res.body, 'AreaCode')).toEqual(EXPECTED_AREA_ROOTS);
      expect(childReferencesFor(res.body, 'AreaCode', AREA_ROOT_REF)).toEqual(EXPECTED_AREA_CHILDREN);
    });
  });

  // getFilters() is the shared code path behind the publisher preview filters
  // (GET /revision/by-id/:id/preview/filters) and the v1 consumer filters
  // (GET /v1/:dataset_id/view/filters). The preview must match what the consumer renders.
  describe('preview / v1 — getFilters()', () => {
    it('orders lookup values by the lookup sort order (numerically, not lexically)', async () => {
      const filters = (await getFilters(REVISION_ID, 'en-gb', dimensions)) as unknown as FilterTable[];
      expect(referencesFor(filters, 'RegionCode')).toEqual(EXPECTED_REGION_ORDER);
    });

    it('orders date dimension values descending (newest first)', async () => {
      const filters = (await getFilters(REVISION_ID, 'en-gb', dimensions)) as unknown as FilterTable[];
      expect(referencesFor(filters, 'PeriodCode')).toEqual(EXPECTED_PERIOD_ORDER);
    });

    it('orders values with no sort order ascending alphabetically by description', async () => {
      const filters = (await getFilters(REVISION_ID, 'en-gb', dimensions)) as unknown as FilterTable[];
      expect(referencesFor(filters, 'CategoryCode')).toEqual(EXPECTED_CATEGORY_ORDER);
    });

    it('nests hierarchical values and orders children by sort order numerically', async () => {
      const filters = (await getFilters(REVISION_ID, 'en-gb', dimensions)) as unknown as FilterTable[];
      expect(referencesFor(filters, 'AreaCode')).toEqual(EXPECTED_AREA_ROOTS);
      expect(childReferencesFor(filters, 'AreaCode', AREA_ROOT_REF)).toEqual(EXPECTED_AREA_CHILDREN);
    });

    it('preview filter order matches the consumer v2 filter order exactly', async () => {
      const previewFilters = (await getFilters(REVISION_ID, 'en-gb', dimensions)) as unknown as FilterTable[];
      const consumerRes = await request(app).get(`/v2/${DATASET_ID}/filters`);
      for (const column of ['RegionCode', 'PeriodCode', 'CategoryCode', 'AreaCode']) {
        expect(referencesFor(previewFilters, column)).toEqual(referencesFor(consumerRes.body, column));
      }
      expect(childReferencesFor(previewFilters, 'AreaCode', AREA_ROOT_REF)).toEqual(
        childReferencesFor(consumerRes.body, 'AreaCode', AREA_ROOT_REF)
      );
    });
  });
});
