import request from 'supertest';
// eslint-disable-next-line import/no-unresolved
import { parse as parseCsv } from 'csv-parse/sync';

import app from '../../../../src/app';
import { dbManager } from '../../../../src/db/database-manager';
import { initPassport } from '../../../../src/middleware/passport-auth';
import { User } from '../../../../src/entities/user/user';
import { UserGroup } from '../../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../../src/entities/user/user-group-role';
import { GroupRole } from '../../../../src/enums/group-role';
import { FactTableColumnType } from '../../../../src/enums/fact-table-column-type';
import { DimensionType } from '../../../../src/enums/dimension-type';
import { ensureWorkerDataSources, resetDatabase } from '../../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../../helpers/get-test-user';
import { seedPublishedDataset, LookupRow } from '../../../helpers/seed-published-dataset';
import BlobStorage from '../../../../src/services/blob-storage';

jest.mock('../../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

// SW-1411: the data table should list rows in the same order as downloads, with the newest dates first.

const user: User = getTestUser('default date order test user');
let userGroup = getTestUserGroup('Default Date Order Test Group');

const DATASET_ID = 'eeeeeeee-eeee-4eee-8ee2-eeeeeeeeeeee';
const REVISION_ID = 'eeeeeeee-eeee-4eee-8ee2-aaaaaaaaaaaa';
const DATA_TABLE_ID = 'eeeeeeee-eeee-4eee-8ee2-bbbbbbbbbbbb';

const AREA_LOOKUP_ID = 'eabcdef0-0000-4000-8000-000000000001';
const YEAR_LOOKUP_ID = 'eabcdef0-0000-4000-8000-000000000002';

const AREAS = ['W06000001', 'W06000002', 'W06000003'];
const AREA_NAMES = ['Isle of Anglesey', 'Gwynedd', 'Conwy'];
const YEARS = ['2020', '2021', '2022', '2023'];
const ROW_COUNT = AREAS.length * YEARS.length;

const bilingual = (row: Omit<LookupRow, 'language'>): LookupRow[] => [
  { ...row, language: 'en' },
  { ...row, language: 'cy' }
];

const areaLookupRows: LookupRow[] = AREAS.flatMap((reference, idx) =>
  bilingual({ reference, description: AREA_NAMES[idx], sortOrder: idx })
);

// Date lookups sort oldest first, as the real ones do.
const yearLookupRows: LookupRow[] = YEARS.flatMap((reference, idx) =>
  bilingual({ reference, description: reference, sortOrder: idx })
);

const rowBuilder = (i: number): unknown[] => [
  AREAS[Math.floor(i / YEARS.length)],
  YEARS[i % YEARS.length],
  'count',
  i + 1,
  null
];

interface Header {
  name: string;
}

// Each row has one cell per header, in the same order
const column = (body: { headers: Header[]; data: unknown[][] }, name: string): unknown[] => {
  const position = body.headers.findIndex((h) => h.name === name);
  if (position < 0) throw new Error(`No ${name} column in ${JSON.stringify(body.headers)}`);
  return body.data.map((row) => row[position]);
};

const yearsNewestFirst = [...YEARS].reverse();

describe('Data table lists rows in the download order, newest dates first (SW-1411)', () => {
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
      title: { en: 'Default date order fixture', cy: 'Ffynhonnell trefn dyddiadau' },
      // Area comes before Year, so the date is not the first column
      factColumns: [
        { name: 'AreaCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
        { name: 'YearCode', datatype: 'VARCHAR', columnType: FactTableColumnType.Dimension },
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
          type: DimensionType.DatePeriod,
          lookupTableId: YEAR_LOOKUP_ID,
          name: { en: 'Year', cy: 'Blwyddyn' },
          lookupRows: yearLookupRows
        }
      ]
    });
  }, 120_000);

  it('sorts by area, then the newest year first within each area', async () => {
    const res = await request(app).get(`/v2/${DATASET_ID}/data`).query({ format: 'frontend', page_size: ROW_COUNT });
    expect(res.status).toBe(200);
    expect(column(res.body, 'Area')).toEqual(AREA_NAMES.flatMap((area) => YEARS.map(() => area)));
    expect(column(res.body, 'Year')).toEqual(AREA_NAMES.flatMap(() => yearsNewestFirst));
  });

  it('lists rows in the same order as a CSV download', async () => {
    const table = await request(app).get(`/v2/${DATASET_ID}/data`).query({ format: 'frontend', page_size: ROW_COUNT });
    const csv = await request(app).get(`/v2/${DATASET_ID}/data`).query({ format: 'csv' });
    expect(csv.status).toBe(200);

    const csvRows: string[][] = parseCsv(csv.text, { from_line: 2 });
    const areaAndYear = (rows: unknown[][], areaAt: number, yearAt: number): string[] =>
      rows.map((row) => `${row[areaAt]} ${row[yearAt]}`);
    const csvHeader: string[] = parseCsv(csv.text, { to_line: 1 })[0];

    expect(
      areaAndYear(
        table.body.data,
        table.body.headers.findIndex((h: Header) => h.name === 'Area'),
        table.body.headers.findIndex((h: Header) => h.name === 'Year')
      )
    ).toEqual(areaAndYear(csvRows, csvHeader.indexOf('Area'), csvHeader.indexOf('Year')));
  });

  it('keeps the same order when paging with cursors', async () => {
    const all = await request(app).get(`/v2/${DATASET_ID}/data`).query({ format: 'frontend', page_size: ROW_COUNT });

    const first = await request(app).get(`/v2/${DATASET_ID}/data`).query({ format: 'frontend', page_size: 5 });
    expect(first.status).toBe(200);
    const rows: unknown[] = [...first.body.data];
    let cursor: string | null = first.body.page_info.next_cursor;
    while (cursor) {
      const r = await request(app).get(`/v2/${DATASET_ID}/data`).query({ format: 'frontend', page_size: 5, cursor });
      expect(r.status).toBe(200);
      rows.push(...r.body.data);
      cursor = r.body.page_info.next_cursor;
    }

    expect(rows).toEqual(all.body.data);
  });

  it('starts with the newest year in a filtered view', async () => {
    const created = await request(app)
      .post(`/v2/${DATASET_ID}/data`)
      .send({ filters: [{ AreaCode: ['W06000001'] }], options: { use_raw_column_names: true } });
    expect(created.status).toBe(200);

    const res = await request(app)
      .get(`/v2/${DATASET_ID}/data/${created.body.filterId}`)
      .query({ format: 'frontend', page_size: ROW_COUNT });
    expect(res.status).toBe(200);
    expect(column(res.body, 'Year')).toEqual(yearsNewestFirst);
  });

  it('still lets the user sort the year oldest first', async () => {
    const res = await request(app)
      .get(`/v2/${DATASET_ID}/data`)
      .query({ format: 'frontend', page_size: ROW_COUNT, sort_by: 'Year:ASC' });
    expect(res.status).toBe(200);
    expect(column(res.body, 'Year').slice(0, AREAS.length)).toEqual(AREAS.map(() => '2020'));
  });
});
