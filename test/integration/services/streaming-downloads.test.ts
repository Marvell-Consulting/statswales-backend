import { Pool } from 'pg';
import ExcelJS from 'exceljs';
// eslint-disable-next-line import/no-unresolved
import { parse as parseCsv } from 'csv-parse/sync';

import { dbManager } from '../../../src/db/database-manager';
import { initPassport } from '../../../src/middleware/passport-auth';
import { User } from '../../../src/entities/user/user';
import { UserGroup } from '../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../src/entities/user/user-group-role';
import { GroupRole } from '../../../src/enums/group-role';
import { FRONTEND_DATA_OPTIONS } from '../../../src/dtos/data-options-dto';
import { QueryStore } from '../../../src/entities/query-store';
import { QueryStoreRepository } from '../../../src/repositories/query-store';
import {
  createStreamingCSVFilteredView,
  createStreamingExcelFilteredView,
  createStreamingJSONFilteredView
} from '../../../src/services/consumer-view';
import { sendCsv, sendExcel, sendHtml, sendJson } from '../../../src/services/consumer-view-v2';
import BlobStorage from '../../../src/services/blob-storage';
import { ensureWorkerDataSources, resetDatabase } from '../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../helpers/get-test-user';
import { seedPublishedDataset } from '../../helpers/seed-published-dataset';
import { StalledResponse, createStalledResponse, settle } from '../../helpers/stalled-response';

jest.mock('../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.loadBuffer = jest.fn();

const user: User = getTestUser('streaming downloads test user');
let userGroup = getTestUserGroup('Streaming Downloads Test Group');

const DATASET_ID = 'ffffff01-ffff-4fff-8fff-ffffffffffff';
const REVISION_ID = 'ffffff01-ffff-4fff-8fff-eeeeeeeeeeee';
const ROW_COUNT = 20_000;
const AREAS = ['W06000001', 'W06000002', 'W06000003', 'W06000004'];

// Fails the test with a clear message instead of letting Jest time out if a download never finishes.
async function finishesWithin<T>(work: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`the download did not finish within ${ms}ms`)), ms);
  });
  try {
    return await Promise.race([work, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

function cubePool(): Pool {
  return (dbManager.getCubeDataSource().driver as unknown as { master: Pool }).master;
}

describe('Streaming downloads against a real cube database', () => {
  let queryStore: QueryStore;

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
      dataTableId: 'ffffff01-ffff-4fff-8fff-dddddddddddd',
      factColumns: [
        { name: 'AreaCode', datatype: 'VARCHAR' },
        { name: 'YearCode', datatype: 'BIGINT' },
        { name: 'Data', datatype: 'DOUBLE PRECISION' }
      ],
      rowBuilder: (i: number) => [AREAS[i % AREAS.length], 1900 + Math.floor(i / AREAS.length), i + 0.5],
      rowCount: ROW_COUNT
    });

    queryStore = await QueryStoreRepository.generate(DATASET_ID, REVISION_ID, FRONTEND_DATA_OPTIONS);
  }, 300_000);

  // Counts the data rows each format sends, so the test can check that every row arrived.
  const csvRows = (res: StalledResponse): number => parseCsv(res.body(), { columns: true }).length;
  const jsonRows = (res: StalledResponse): number => (JSON.parse(res.body()) as unknown[]).length;
  const htmlRows = (res: StalledResponse): number => (res.body().match(/<tr>/g) ?? []).length - 1; // minus header
  const excelRows = async (res: StalledResponse): Promise<number> => {
    const workbook = new ExcelJS.Workbook();
    // @ts-expect-error ExcelJS types expect old Buffer, Node 24 returns Buffer<ArrayBuffer>
    await workbook.xlsx.load(res.buffer());
    return workbook.worksheets.reduce((total, sheet) => total + sheet.actualRowCount - 1, 0); // minus headers
  };

  const downloads: [
    string,
    (res: StalledResponse) => Promise<void>,
    (res: StalledResponse) => number | Promise<number>
  ][] = [
    ['v1 CSV', (res) => createStreamingCSVFilteredView(res, REVISION_ID, 'en-GB'), csvRows],
    ['v1 JSON', (res) => createStreamingJSONFilteredView(res, REVISION_ID, 'en-GB'), jsonRows],
    ['v1 Excel', (res) => createStreamingExcelFilteredView(res, REVISION_ID, 'en-GB'), excelRows],
    ['v2 CSV', (res) => sendCsv(queryStore.query['en-GB'], queryStore, res), csvRows],
    ['v2 JSON', (res) => sendJson(queryStore.query['en-GB'], queryStore, res), jsonRows],
    ['v2 HTML', (res) => sendHtml(queryStore.query['en-GB'], queryStore, res), htmlRows],
    ['v2 Excel', (res) => sendExcel(queryStore.query['en-GB'], queryStore, res), excelRows]
  ];

  it.each(downloads)(
    '%s finishes and releases its connection when the client disconnects mid-download',
    async (_name, download) => {
      const pool = cubePool();
      const res = createStalledResponse();

      const done = download(res).catch(() => {});
      await settle(res.bytes);
      res.destroy();
      await finishesWithin(done, 10_000);

      expect(pool.totalCount - pool.idleCount).toBe(0);
    },
    30_000
  );

  it.each(downloads)(
    '%s sends the whole result once a slow client catches up',
    async (_name, download, countRows) => {
      const res = createStalledResponse();

      const done = download(res);
      await settle(res.bytes);
      res.resumeClient();
      await finishesWithin(done, 20_000);

      expect(res.writableEnded).toBe(true);
      expect(await countRows(res)).toBe(ROW_COUNT);
    },
    30_000
  );
});
